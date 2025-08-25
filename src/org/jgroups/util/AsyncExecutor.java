package org.jgroups.util;

import org.jgroups.Lifecycle;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

import static org.jgroups.conf.AttributeType.SCALAR;

/**
 * Used to execute asynchronous tasks, e.g. async-send (https://issues.redhat.com/browse/JGRP-2603). Uses a blockng
 * queue and a dequeuer thread, which passes removed tasks to the thread pool
 * @author Bela Ban
 * @since  5.3.5
 */
public class AsyncExecutor<T> implements Lifecycle {

    @Property(description="If not enabled, tasks will executed on the runner's thread")
    protected boolean         enabled=true;

    @ManagedAttribute(description="Total number of times a message was sent (includes rejected messages)",type=SCALAR)
    protected final LongAdder num_sends=new LongAdder();

    @ManagedAttribute(description="Number of rejected message due to an exhausted thread pool (includes dropped " +
      "messages and messages sent on the caller's thread",type=SCALAR)
    protected final LongAdder num_rejected=new LongAdder();

    @ManagedAttribute(description="Number of dropped tasks (when DONT_BLOCK flag is set in the message)",type=SCALAR)
    protected final LongAdder num_drops_on_full_thread_pool=new LongAdder();

    @ManagedAttribute(description="Messages that were sent on the caller's thread due to an exhausted pool",type=SCALAR)
    protected final LongAdder num_sends_on_callers_thread=new LongAdder();

    protected ThreadPool      thread_pool;
    protected Executor        executor;

    public boolean          enabled()                  {return enabled;}
    public AsyncExecutor<T> enable(boolean b)          {enabled=b; return this;}
    public ThreadPool       threadPool()               {return thread_pool;}
    public AsyncExecutor<T> threadPool(ThreadPool p)   {this.thread_pool=p; return this;}
    public long             numSends()                 {return num_sends.sum();}
    public long             numSendsOnCallersThread()  {return num_sends_on_callers_thread.sum();}
    public long             numDropsOnFullThreadPool() {return num_drops_on_full_thread_pool.sum();}
    public long             numRejected()              {return num_rejected.sum();}


    public AsyncExecutor() {
    }

    public AsyncExecutor(ThreadPool p) {
        this.thread_pool=p;
    }

    public void resetStats() {
        num_sends.reset();
        num_rejected.reset();
        num_drops_on_full_thread_pool.reset();
        num_sends_on_callers_thread.reset();
    }

    public CompletableFuture<T> execute(Supplier<T> t, boolean can_be_dropped) {
        Task<T> task=new Task<>(t, new CompletableFuture<>());
        Executor exec=executor;
        try {
            num_sends.increment();
            if(enabled && (exec=exec()) != null)
                return CompletableFuture.supplyAsync(t, exec);
            return CompletableFuture.completedFuture(t.get());
        }
        catch(RejectedExecutionException ex) {
            num_rejected.increment();
            if(!can_be_dropped) {
                task.run(); // if we cannot drop the task, run it on the caller thread
                num_sends_on_callers_thread.increment();
            }
            else {
                task.completeExceptionally(ex);
                num_drops_on_full_thread_pool.increment();
            }
        }
        return task.cf;
    }

    @Override
    public String toString() {
        return String.format("rejected: %,d, drops=%,d, sends_on_caller: %,d, pool: %s\n",
                             num_rejected.sum(), num_drops_on_full_thread_pool.sum(), num_sends_on_callers_thread.sum(),
                             thread_pool.toString());
    }

    protected Executor exec() {
        Executor exec=executor;
        return exec != null? exec : (exec=executor=thread_pool.pool());
    }


    protected record Task<T>(Supplier<T> task, CompletableFuture<T> cf) implements Runnable {

        protected Task<T> completeExceptionally(Throwable t) {
            cf.completeExceptionally(t);
            return this;
        }

        @Override
        public void run() {
            try {
                T result=task.get();
                cf.complete(result);
            }
            catch(Throwable t) {
                cf.completeExceptionally(t);
            }
        }
    }

}
