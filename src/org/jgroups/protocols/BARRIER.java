package org.jgroups.protocols;

import org.jgroups.Event;
import org.jgroups.stack.Protocol;
import org.jgroups.util.TimeScheduler;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
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
 * @version $Id: BARRIER.java,v 1.6 2007/06/11 08:14:39 belaban Exp $
 */

public class BARRIER extends Protocol {
    long max_close_time=60000; // how long can the barrier stay closed (in ms) ? 0 means forever
    final Lock lock=new ReentrantLock();
    final AtomicBoolean barrier_closed=new AtomicBoolean(false);

    /** signals to waiting threads that the barrier is open again */
    Condition barrier_opened=lock.newCondition();
    Condition no_msgs_pending=lock.newCondition();
    ConcurrentMap<Thread,Object> in_flight_threads=new ConcurrentHashMap<Thread,Object>();
    Future barrier_opener_future=null;
    TimeScheduler timer;
    private static final Object NULL=new Object();


    public String getName() {
        return "BARRIER";
    }


    public boolean setProperties(Properties props) {
        String str;
        super.setProperties(props);
        str=props.getProperty("max_close_time");
        if(str != null) {
            max_close_time=Long.parseLong(str);
            props.remove("max_close_time");
        }

        if(!props.isEmpty()) {
            log.error("these properties are not recognized: " + props);
            return false;
        }
        return true;
    }

    public boolean isClosed() {
        return barrier_closed.get();
    }


    public int getNumberOfInFlightThreads() {
        return in_flight_threads.size();
    }

    public void init() throws Exception {
        super.init();
        timer=stack.timer;
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
                closeBarrier();
                return null;
            case Event.OPEN_BARRIER:
                openBarrier();
                return null;
        }
        return down_prot.down(evt);
    }

    public Object up(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                Thread current_thread=Thread.currentThread();
                in_flight_threads.put(current_thread, NULL);
                if(barrier_closed.get()) {
                    lock.lock();
                    try {
                        while(barrier_closed.get()) {
                            try {
                                barrier_opened.await();
                            }
                            catch(InterruptedException e) {
                            }
                        }
                    }
                    finally {
                        lock.unlock();
                    }
                }

                try {
                    return up_prot.up(evt);
                }
                finally {
                    lock.lock();
                    try {
                        if(in_flight_threads.remove(current_thread) == NULL &&
                                in_flight_threads.isEmpty() &&
                                barrier_closed.get()) {
                            no_msgs_pending.signalAll();
                        }
                    }
                    finally {
                        lock.unlock();
                    }
                }
            case Event.CLOSE_BARRIER:
                closeBarrier();
                return null;
            case Event.OPEN_BARRIER:
                openBarrier();
                return null;
        }
        return up_prot.up(evt);
    }


    private void closeBarrier() {
        if(!barrier_closed.compareAndSet(false, true))
            return; // barrier was already closed

        lock.lock();
        try {
            // wait until all pending (= in-progress) msgs have returned
            in_flight_threads.remove(Thread.currentThread());
            while(!in_flight_threads.isEmpty()) {
                try {
                    no_msgs_pending.await();
                }
                catch(InterruptedException e) {
                }
            }
        }
        finally {
            lock.unlock();
        }

        if(log.isTraceEnabled())
            log.trace("barrier was closed");

        if(max_close_time > 0)
            scheduleBarrierOpener();
    }

    private void openBarrier() {
        lock.lock();
        try {
            if(!barrier_closed.compareAndSet(true, false))
                return; // barrier was already open
            barrier_opened.signalAll();
        }
        finally {
            lock.unlock();
        }
        if(log.isTraceEnabled())
            log.trace("barrier was opened");
        cancelBarrierOpener(); // cancels if running
    }

    private void scheduleBarrierOpener() {
        if(barrier_opener_future == null || barrier_opener_future.isDone()) {
            barrier_opener_future=timer.schedule(new Runnable() {public void run() {openBarrier();}},
                                                 max_close_time, TimeUnit.MILLISECONDS
            );
        }
    }

    private void cancelBarrierOpener() {
        if(barrier_opener_future != null) {
            barrier_opener_future.cancel(true);
            barrier_opener_future=null;
        }
    }
}
