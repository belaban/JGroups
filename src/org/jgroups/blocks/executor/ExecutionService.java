package org.jgroups.blocks.executor;

import java.io.Externalizable;
import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.jgroups.JChannel;
import org.jgroups.protocols.Executing;
import org.jgroups.protocols.Locking;
import org.jgroups.util.FutureListener;
import org.jgroups.util.NotifyingFuture;
import org.jgroups.util.Streamable;

/**
 * This is a jgroups implementation of an ExecutorService, where the consumers
 * are running on any number of nodes.  The nodes should run 
 * {@link JGroupsExecutorRunner} to start picking up requests.
 * 
 * Every future object returned will be a {@link ListenableFuture} which
 * allows for not having to query the future and have a callback instead.  This
 * can then be used as a workflow to submit other tasks sequentially or also to
 * query the future for the value at that time. 
 * 
 * Copyright (c) 2011 RedPrairie Corporation
 * All Rights Reserved
 * 
 * @author wburns
 */
public class ExecutionService extends AbstractExecutorService {
    protected JChannel ch;
    protected Executing _execProt;
    
    protected Lock _unfinishedLock = new ReentrantLock();
    protected Condition _unfinishedCondition = _unfinishedLock.newCondition();
    
    protected Set<Future<?>> _unfinishedFutures = new HashSet<Future<?>>();
    
    protected AtomicBoolean _shutdown = new AtomicBoolean(false);

    public ExecutionService() {
        
    }

    public ExecutionService(JChannel ch) {
        setChannel(ch);
    }

    public void setChannel(JChannel ch) {
        this.ch=ch;
        _execProt=(Executing)ch.getProtocolStack().findProtocol(Executing.class);
        if(_execProt == null)
            throw new IllegalStateException("Channel configuration must include a locking protocol " +
                                              "(subclass of " + Locking.class.getName() + ")");
    }
    
    // @see java.util.concurrent.AbstractExecutorService#submit(java.lang.Runnable, java.lang.Object)
    @Override
    public <T> NotifyingFuture<T> submit(Runnable task, T result) {
        // This cast is okay cause we control creation of the task
        return (NotifyingFuture<T>)super.submit(task, result);
    }

    // @see java.util.concurrent.AbstractExecutorService#submit(java.util.concurrent.Callable)
    @Override
    public <T> NotifyingFuture<T> submit(Callable<T> task) {
        // This cast is okay cause we control creation of the task
        return (NotifyingFuture<T>)super.submit(task);
    }

    /**
     * This is basically a copy of the FutureTask in java.util.concurrent but
     * added serializable to it.  Also added in the NotifyingFuture
     * so that the channel can update the future when the value is calculated.
     * 
     * @param <V>
     * @author wburns
     */
    protected static class FutureImpl<V> implements RunnableFuture<V>, 
            ExecutorNotification, Serializable, NotifyingFuture<V> {
        private static final long serialVersionUID = -6137303690718895072L;
        
        // @see java.lang.Object#toString()
        @Override
        public String toString() {
            return "FutureImpl [callable=" + sync.callable + "]";
        }

        /** Synchronization control for FutureTask */
        private final Sync sync;
        
        /** The following values are only used on the client side */
        private final transient JChannel channel;
        private final transient Set<Future<?>> _unfinishedFutures;
        private final transient Lock _unfinishedLock;
        private final transient Condition _unfinishedCondition;
        private volatile transient FutureListener<V> _listener;
        
        /**
         * Creates a <tt>FutureTask</tt> that will upon running, execute the
         * given <tt>Callable</tt>.
         *
         * @param channel The channel that messages are sent down
         * @param unfinishedLock The lock which protects the futuresToFinish
         *        set object.
         * @param condition The condition to signal when this future finishes
         * @param futuresToFinish The set to remove this future from when
         *        it is finished. 
         * @param callable The callable to actually run on the server side
         */
        public FutureImpl(JChannel channel, Lock unfinishedLock,
                          Condition condition,
                          Set<Future<?>> futuresToFinish, 
                          Callable<V> callable) {
            if (callable == null)
                throw new NullPointerException();
            sync = new Sync(callable);
            this.channel = channel;
            // We keep the real copy to update the outside
            _unfinishedFutures = futuresToFinish;
            _unfinishedLock = unfinishedLock;
            _unfinishedCondition = condition;
        }

        /**
         * Creates a <tt>FutureTask</tt> that will upon running, execute the
         * given <tt>Runnable</tt>, and arrange that <tt>get</tt> will return the
         * given result on successful completion.
         *
         * @param channel The channel that messages are sent down
         * @param unfinishedLock The lock which protects the futuresToFinish
         *        set object.
         * @param condition The condition to signal when this future finishes
         * @param futuresToFinish The set to remove this future from when
         *        it is finished.
         * @param runnable the runnable task
         * @param result the result to return on successful completion. If
         * you don't need a particular result, consider using
         * constructions of the form:
         * <tt>Future&lt;?&gt; f = new FutureTask&lt;Object&gt;(runnable, null)</tt>
         * @throws NullPointerException if runnable is null
         */
        public FutureImpl(JChannel channel, Lock unfinishedLock,
                          Condition condition, Set<Future<?>> futuresToFinish, 
                          Runnable runnable, V result) {
            sync = new Sync(new RunnableAdapter<V>(runnable, result));
            this.channel = channel;
            // We keep the real copy to update the outside
            _unfinishedFutures = futuresToFinish;
            _unfinishedLock = unfinishedLock;
            _unfinishedCondition = condition;
        }
        
        public boolean isCancelled() {
            return sync.innerIsCancelled();
        }

        public boolean isDone() {
            return sync.innerIsDone();
        }

        public boolean cancel(boolean mayInterruptIfRunning) {
            if (sync.innerIsDone()) {
                return false;
            }
            // This will only happen on calling side since it is transient
            if (channel != null) {
                return (Boolean)channel.downcall(new ExecutorEvent(
                    ExecutorEvent.TASK_CANCEL, new Object[] {this, mayInterruptIfRunning}));
            }
            return sync.innerCancel(mayInterruptIfRunning);
        }

        /**
         * @throws CancellationException {@inheritDoc}
         */
        public V get() throws InterruptedException, ExecutionException {
            return sync.innerGet();
        }

        /**
         * @throws CancellationException {@inheritDoc}
         */
        public V get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
            return sync.innerGet(unit.toNanos(timeout));
        }

        /**
         * Protected method invoked when this task transitions to state
         * <tt>isDone</tt> (whether normally or via cancellation). The
         * default implementation does nothing.  Subclasses may override
         * this method to invoke completion callbacks or perform
         * bookkeeping. Note that you can query status inside the
         * implementation of this method to determine whether this task
         * has been cancelled.
         */
        protected void done() {
            // We assign the listener to a local variable so we don't have to 
            // worry about it becoming null inside the if
            FutureListener<V> listener = _listener;
            // We don't want this to run on server
            if (listener != null) {
                listener.futureDone(this);
            }
            
            _unfinishedLock.lock();
            try {
                _unfinishedFutures.remove(this);
                _unfinishedCondition.signalAll();
            }
            finally {
                _unfinishedLock.unlock();
            }
        }

        @Override
        public NotifyingFuture<V> setListener(FutureListener<V> listener) {
            _listener = listener;
            if (sync.innerIsDone()) {
                _listener.futureDone(this);
            }
            return this;
        }

        /**
         * Sets the result of this Future to the given value unless
         * this future has already been set or has been cancelled.
         * This method is invoked internally by the <tt>run</tt> method
         * upon successful completion of the computation.
         * @param v the value
         */
        protected void set(V v) {
            sync.innerSet(v);
        }

        /**
         * Causes this future to report an <tt>ExecutionException</tt>
         * with the given throwable as its cause, unless this Future has
         * already been set or has been cancelled.
         * This method is invoked internally by the <tt>run</tt> method
         * upon failure of the computation.
         * @param t the cause of failure
         */
        protected void setException(Throwable t) {
            sync.innerSetException(t);
        }

        // The following (duplicated) doc comment can be removed once
        //
        // 6270645: Javadoc comments should be inherited from most derived
        //          superinterface or superclass
        // is fixed.
        /**
         * Sets this Future to the result of its computation
         * unless it has been cancelled.
         */
        public void run() {
            sync.innerRun();
        }

        /**
         * Executes the computation without setting its result, and then
         * resets this Future to initial state, failing to do so if the
         * computation encounters an exception or is cancelled.  This is
         * designed for use with tasks that intrinsically execute more
         * than once.
         * @return true if successfully run and reset
         */
        protected boolean runAndReset() {
            return sync.innerRunAndReset();
        }

        /**
         * Synchronization control for FutureTask. Note that this must be
         * a non-static inner class in order to invoke the protected
         * <tt>done</tt> method. For clarity, all inner class support
         * methods are same as outer, prefixed with "inner".
         *
         * Uses AQS sync state to represent run status
         */
        private final class Sync extends AbstractQueuedSynchronizer {
            private static final long serialVersionUID = -7828117401763700385L;

            /** State value representing that task is running */
            private static final int RUNNING   = 1;
            /** State value representing that task ran */
            private static final int RAN       = 2;
            /** State value representing that task was cancelled */
            private static final int CANCELLED = 4;

            /** The underlying callable */
            private final Callable<V> callable;
            /** The result to return from get() */
            private V result;
            /** The exception to throw from get() */
            private Throwable exception;

            /**
             * The thread running task. When nulled after set/cancel, this
             * indicates that the results are accessible.  Must be
             * volatile, to ensure visibility upon completion.
             */
            private transient volatile Thread runner;

            Sync(Callable<V> callable) {
                this.callable = callable;
            }

            private boolean ranOrCancelled(int state) {
                return (state & (RAN | CANCELLED)) != 0;
            }

            /**
             * Implements AQS base acquire to succeed if ran or cancelled
             */
            protected int tryAcquireShared(int ignore) {
                return innerIsDone()? 1 : -1;
            }

            /**
             * Implements AQS base release to always signal after setting
             * final done status by nulling runner thread.
             */
            protected boolean tryReleaseShared(int ignore) {
                runner = null;
                return true;
            }

            boolean innerIsCancelled() {
                return getState() == CANCELLED;
            }

            boolean innerIsDone() {
                return ranOrCancelled(getState()) && runner == null;
            }

            V innerGet() throws InterruptedException, ExecutionException {
                acquireSharedInterruptibly(0);
                if (getState() == CANCELLED)
                    throw new CancellationException();
                if (exception != null)
                    throw new ExecutionException(exception);
                return result;
            }

            V innerGet(long nanosTimeout) throws InterruptedException, ExecutionException, TimeoutException {
                if (!tryAcquireSharedNanos(0, nanosTimeout))
                    throw new TimeoutException();
                if (getState() == CANCELLED)
                    throw new CancellationException();
                if (exception != null)
                    throw new ExecutionException(exception);
                return result;
            }

            void innerSet(V v) {
                for (;;) {
                    int s = getState();
                    if (s == RAN)
                        return;
                    if (s == CANCELLED) {
                        // aggressively release to set runner to null,
                        // in case we are racing with a cancel request
                        // that will try to interrupt runner
                        releaseShared(0);
                        return;
                    }
                    if (compareAndSetState(s, RAN)) {
                        result = v;
                        releaseShared(0);
                        done();
                        return;
                    }
                }
            }

            void innerSetException(Throwable t) {
                for (;;) {
                    int s = getState();
                    if (s == RAN)
                        return;
                    if (s == CANCELLED) {
                        // aggressively release to set runner to null,
                        // in case we are racing with a cancel request
                        // that will try to interrupt runner
                        releaseShared(0);
                        return;
                    }
                    if (compareAndSetState(s, RAN)) {
                        exception = t;
                        result = null;
                        releaseShared(0);
                        done();
                        return;
                    }
                }
            }

            boolean innerCancel(boolean mayInterruptIfRunning) {
                for (;;) {
                    int s = getState();
                    if (ranOrCancelled(s))
                        return false;
                    if (compareAndSetState(s, CANCELLED))
                        break;
                }
                if (mayInterruptIfRunning) {
                    Thread r = runner;
                    if (r != null)
                        r.interrupt();
                }
                releaseShared(0);
                done();
                return true;
            }

            void innerRun() {
                if (!compareAndSetState(0, RUNNING))
                    return;
                try {
                    runner = Thread.currentThread();
                    if (getState() == RUNNING) // recheck after setting thread
                        innerSet(callable.call());
                    else
                        releaseShared(0); // cancel
                } catch (Throwable ex) {
                    innerSetException(ex);
                }
            }

            boolean innerRunAndReset() {
                if (!compareAndSetState(0, RUNNING))
                    return false;
                try {
                    runner = Thread.currentThread();
                    if (getState() == RUNNING)
                        callable.call(); // don't set result
                    runner = null;
                    return compareAndSetState(RUNNING, 0);
                } catch (Throwable ex) {
                    innerSetException(ex);
                    return false;
                }
            }
        }

        // @see com.redprairie.moca.cluster.jgroups.ExecutorNotification#resultReturned(java.lang.Object)
        @SuppressWarnings("unchecked")
        @Override
        public void resultReturned(Object obj) {
            set((V)obj);
        }
        
        // @see com.redprairie.moca.cluster.jgroups.ExecutorNotification#throwableEncountered(java.lang.Throwable)
        @Override
        public void throwableEncountered(Throwable t) {
            setException(t);
        }
    }
    
    // @see java.util.concurrent.ExecutorService#shutdown()
    @Override
    public void shutdown() {
        _realShutdown(false);
    }
    
    @SuppressWarnings("unchecked")
    private List<Runnable> _realShutdown(boolean interrupt) {
        _shutdown.set(true);
        _unfinishedLock.lock();
        Set<Future<?>> futures;
        try {
             futures = new HashSet<Future<?>>(_unfinishedFutures);
        }
        finally {
            _unfinishedLock.unlock();
        }
        return (List<Runnable>)ch.downcall(new ExecutorEvent(
            ExecutorEvent.ALL_TASK_CANCEL, new Object[]{futures, interrupt}));
    }

    // @see java.util.concurrent.ExecutorService#shutdownNow()
    @Override
    public List<Runnable> shutdownNow() {
        return _realShutdown(true);
    }

    // @see java.util.concurrent.ExecutorService#isShutdown()
    @Override
    public boolean isShutdown() {
        return _shutdown.get();
    }

    // @see java.util.concurrent.ExecutorService#isTerminated()
    @Override
    public boolean isTerminated() {
        if (_shutdown.get()) {
            _unfinishedLock.lock();
            try {
                return _unfinishedFutures.isEmpty();
            }
            finally {
                _unfinishedLock.unlock();
            }
        }
        return false;
    }

    // @see java.util.concurrent.ExecutorService#awaitTermination(long, java.util.concurrent.TimeUnit)
    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit)
            throws InterruptedException {
        long nanoTimeWait = unit.toNanos(timeout);
        _unfinishedLock.lock();
        try {
            while (!_unfinishedFutures.isEmpty()) {
                if ((nanoTimeWait = _unfinishedCondition.awaitNanos(
                    nanoTimeWait)) <= 0) {
                    return false;
                }
            }
        }
        finally {
            _unfinishedLock.unlock();
        }

        return true;
    }

    // @see java.util.concurrent.Executor#execute(java.lang.Runnable)
    @Override
    public void execute(Runnable command) {
        if (!_shutdown.get()) {
            if (command instanceof Serializable || 
                    command instanceof Externalizable || 
                    command instanceof Streamable) {
                ch.down(new ExecutorEvent(ExecutorEvent.TASK_SUBMIT, command));
            }
            else {
                throw new IllegalArgumentException(
                    "Command was not Serializable, Externalizable, or Streamable - "
                            + command);
            }
        }
        else {
            throw new RejectedExecutionException();
        }
    }
    
    /**
     * This is copied from {@see java.util.concurrent.Executors} class which
     * contains RunnableAdapter.  However that adapter isn't serializable, and
     * is final and package level so we can' reference.
     */
    static final class RunnableAdapter<T> implements Callable<T>, Serializable {
        private static final long serialVersionUID = 228162442652528778L;
        final Runnable task;
        final T result;
        RunnableAdapter(Runnable  task, T result) {
            this.task = task;
            this.result = result;
        }
        public T call() {
            task.run();
            return result;
        }
    }

    // @see java.util.concurrent.AbstractExecutorService#newTaskFor(java.lang.Runnable, java.lang.Object)
    @Override
    protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
        FutureImpl<T> future = new FutureImpl<T>(ch, _unfinishedLock, 
                _unfinishedCondition, _unfinishedFutures, runnable, value);
        _execProt.addExecutorListener(future, future);
        _unfinishedLock.lock();
        try {
            _unfinishedFutures.add(future);
        }
        finally {
            _unfinishedLock.unlock();
        }
        return future;
    }

    // @see java.util.concurrent.AbstractExecutorService#newTaskFor(java.util.concurrent.Callable)
    @Override
    protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
        FutureImpl<T> future = new FutureImpl<T>(ch, _unfinishedLock, 
                _unfinishedCondition, _unfinishedFutures, callable);
        _execProt.addExecutorListener(future, future);
        _unfinishedLock.lock();
        try {
            _unfinishedFutures.add(future);
        }
        finally {
            _unfinishedLock.unlock();
        }
        return future;
    }
}
