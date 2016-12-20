package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.Util;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * All messages up the stack have to go through a barrier (read lock, RL). By default, the barrier is open.
 * When a CLOSE_BARRIER event is received, we close the barrier by acquiring a write lock (WL). This succeeds when all
 * previous messages have completed (by releasing their RLs). Thus, when we have acquired the WL, we know that there
 * are no pending messages processed.<br/>
 * When an OPEN_BARRIER event is received, we simply open the barrier again and let all messages pass in the up
 * direction. This is done by releasing the WL.
 * @author Bela Ban
 */
@MBean(description="Blocks all multicast threads when closed")
public class BARRIER extends Protocol {
    
    @Property(description="Max time barrier can be closed. Default is 60000 ms")
    protected long                       max_close_time=60000; // how long can the barrier stay closed (in ms) ? 0 means forever

    @Property(description="Max time (in ms) to wait until the threads which passed the barrier before it was closed " +
      "have completed. If this time elapses, an exception will be thrown and state transfer will fail. 0 = wait forever")
    protected long                       flush_timeout=5000;


    protected final Lock                 lock=new ReentrantLock();
    protected final AtomicBoolean        barrier_closed=new AtomicBoolean(false);

    /** signals to waiting threads that the barrier is open again */
    protected Condition                  no_pending_threads=lock.newCondition();
    protected Map<Thread,Object>         in_flight_threads=Util.createConcurrentMap();
    protected volatile Future<?>         barrier_opener_future;
    protected TimeScheduler              timer;
    protected Address                    local_addr;

    // mbrs from which unicasts should be accepted even if BARRIER is closed (PUNCH_HOLE adds, CLOSE_HOLE removes mbrs)
    protected final Set<Address>         holes=new HashSet<>();

    // queues multicast messages or message batches (dest == null)
    protected final Map<Address,Message> mcast_queue=new ConcurrentHashMap<>();

    // queues unicast messages or message batches (dest != null)
    protected final Map<Address,Message> ucast_queue=new ConcurrentHashMap<>();

    protected TP                         transport;

    protected static final Object        NULL=new Object();


    @ManagedAttribute(description="Shows whether the barrier closed")
    public boolean isClosed() {
        return barrier_closed.get();
    }

    @ManagedAttribute(description="Lists the members whose unicast messages are let through")
    public String getHoles() {return holes.toString();}

    public int getNumberOfInFlightThreads() {
        return in_flight_threads.size();
    }

    @ManagedAttribute
    public int getInFlightThreadsCount() {
        return getNumberOfInFlightThreads();
    }

    @ManagedAttribute
    public boolean isOpenerScheduled() {
        return barrier_opener_future != null && !barrier_opener_future.isDone() && !barrier_opener_future.isCancelled();
    }

    public void init() throws Exception {
        super.init();
        transport=getTransport();
        timer=transport.getTimer();
    }

    public void stop() {
        super.stop();
        openBarrier();
    }


    public void destroy() {
        super.destroy();
        openBarrier();
    }

    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.CLOSE_BARRIER:
                try {
                    closeBarrier();
                }
                catch(TimeoutException e) {
                    throw new RuntimeException(e);
                }
                return null;
            case Event.OPEN_BARRIER:
                openBarrier();
                return null;
            case Event.SET_LOCAL_ADDRESS:
                local_addr=evt.getArg();
                break;
            case Event.PUNCH_HOLE:
                Address mbr=evt.getArg();
                holes.add(mbr);
                return null;
            case Event.CLOSE_HOLE:
                mbr=evt.getArg();
                holes.remove(mbr);
                return null;
        }
        return down_prot.down(evt);
    }

    public Object up(Event evt) {
        switch(evt.getType()) {
            case Event.CLOSE_BARRIER:
                try {
                    closeBarrier();
                    return null;
                }
                catch(TimeoutException e) {
                    throw new RuntimeException(e);
                }

            case Event.OPEN_BARRIER:
                openBarrier();
                return null;
        }
        return up_prot.up(evt);
    }

    public Object up(Message msg) {
        // https://issues.jboss.org/browse/JGRP-1341: let unicast messages pass
        if(msg.isFlagSet(Message.Flag.SKIP_BARRIER) || msg.getDest() != null
          && ((msg.isFlagSet(Message.Flag.OOB) && msg.isFlagSet(Message.Flag.INTERNAL)) || holes.contains(msg.getSrc())))
            return up_prot.up(msg);

        if(barrier_closed.get()) {
            final Map<Address,Message> map=msg.getDest() == null? mcast_queue : ucast_queue;
            map.put(msg.getSrc(), msg);
            return null; // queue and drop the message
        }
        Thread current_thread=Thread.currentThread();
        in_flight_threads.put(current_thread, NULL);
        try {
            return up_prot.up(msg);
        }
        finally {
            unblock(current_thread);
        }
    }

    public void up(MessageBatch batch) {
        // let unicast message batches pass
        if(batch.dest() != null
          && (batch.mode() == MessageBatch.Mode.OOB && batch.mode() == MessageBatch.Mode.INTERNAL)
          || holes.contains(batch.sender())) {
            up_prot.up(batch);
            return;
        }

        if(barrier_closed.get()) {
            final Map<Address,Message> map=batch.dest() == null? mcast_queue : ucast_queue;
            map.put(batch.sender(), batch.last().putHeader(transport.getId(),new TpHeader(batch.clusterName())));
            return; // queue the last message of the batch and drop the batch
        }

        Thread current_thread=Thread.currentThread();
        in_flight_threads.put(current_thread, NULL);
        try {
            up_prot.up(batch);
        }
        finally {
            unblock(current_thread);
        }
    }


    protected void unblock(final Thread current_thread) {
        if(in_flight_threads.remove(current_thread) == NULL && in_flight_threads.isEmpty()) {
            lock.lock();
            try {
                no_pending_threads.signalAll();
            }
            finally {
                lock.unlock();
            }
        }
    }

    /** Close the barrier. Temporarily remove all threads which are waiting or blocked, re-insert them after the call */
    public void closeBarrier() throws TimeoutException {
        if(!barrier_closed.compareAndSet(false, true))
            return; // barrier is already closed

        long target_time=0, wait_time=0, start=System.currentTimeMillis();

        in_flight_threads.remove(Thread.currentThread());

        lock.lock();
        try {
            // wait until all pending threads have returned
            while(barrier_closed.get() && !in_flight_threads.isEmpty()) {
                if(target_time == 0 && flush_timeout > 0)
                    target_time=System.currentTimeMillis() + flush_timeout;
                // should be the same
                in_flight_threads.keySet().removeIf(thread -> !thread.isAlive() || thread.getState() == Thread.State.TERMINATED);
                if(in_flight_threads.isEmpty())
                    break;
                try {
                    if(flush_timeout <= 0)
                        no_pending_threads.await();
                    else {
                        if((wait_time=target_time - System.currentTimeMillis()) <= 0)
                            break;
                        no_pending_threads.await(wait_time, TimeUnit.MILLISECONDS);
                    }
                }
                catch(InterruptedException e) {
                }
            }
            if(flush_timeout > 0 && !in_flight_threads.isEmpty()) {
                long time=System.currentTimeMillis() - start;
                throw new TimeoutException(local_addr + ": failed flushing pending threads in " + time +
                                             " ms; threads:\n" + printInFlightThreads());
            }
        }
        finally {
            lock.unlock();
        }

        if(max_close_time > 0)
            scheduleBarrierOpener();
    }


    @ManagedOperation(description="Opens the barrier. No-op if already open")
    public void openBarrier() {
        if(!barrier_closed.compareAndSet(true, false))
            return; // barrier was already open

        cancelBarrierOpener(); // cancels if running

        synchronized(mcast_queue) {
            flushQueue(mcast_queue);
        }

        synchronized(ucast_queue) {
            flushQueue(ucast_queue);
        }
    }

    @ManagedOperation(description="Lists the in-flight threads")
    protected String printInFlightThreads() {
        StringBuilder sb=new StringBuilder();
        for(Thread thread: in_flight_threads.keySet())
            sb.append(thread.toString()).append("\n");
        return sb.toString();
    }

    protected void flushQueue(final Map<Address,Message> queue) {
        if(queue.isEmpty())
            return;

        for(Message msg: queue.values()) {
            boolean oob=msg.isFlagSet(Message.Flag.OOB), internal=msg.isFlagSet(Message.Flag.INTERNAL);
            transport.msg_processing_policy.process(msg, oob, internal);
        }
        queue.clear();
    }

    protected void scheduleBarrierOpener() {
        if(barrier_opener_future == null || barrier_opener_future.isDone()) {
            barrier_opener_future=timer.schedule(this::openBarrier, max_close_time, TimeUnit.MILLISECONDS, false);
        }
    }

    protected void cancelBarrierOpener() {
        if(barrier_opener_future != null) {
            barrier_opener_future.cancel(true);
            barrier_opener_future=null;
        }
    }
}
