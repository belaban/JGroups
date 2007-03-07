package org.jgroups.protocols;

import org.jgroups.Event;
import org.jgroups.stack.Protocol;
import org.jgroups.util.TimeScheduler;

import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * All messages up the stack have to go through a barrier (read lock, RL). By default, the barrier is open.
 * When a CLOSE_BARRIER event is received, we close the barrier by acquiring a write lock (WL). This succeeds when all
 * previous messages have completed (by releasing their RLs). Thus, when we have acquired the WL, we know that there
 * are no pending messages processed.<br/>
 * When an OPEN_BARRIER event is received, we simply open the barrier again and let all messages pass in the up
 * direction. This is done by releasing the WL.
 * @author Bela Ban
 * @version $Id: BARRIER.java,v 1.1 2007/03/07 14:34:46 belaban Exp $
 */

public class BARRIER extends Protocol {
    long max_close_time=60000; // how long can the barrier stay closed (in ms) ? 0 means forever
    ReadWriteLock barrier=new ReentrantReadWriteLock();
    Lock rl=barrier.readLock();
    Lock wl=barrier.writeLock();
    Future barrier_opener_future=null;
    TimeScheduler timer;


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
                rl.lock();
                try {
                    return up_prot.up(evt);
                }
                finally {
                    rl.unlock();
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
        wl.lock();
        if(max_close_time > 0)
            scheduleBarrierOpener();
    }

    private void openBarrier() {
        try {
            wl.unlock();
        }
        catch(Throwable t) {
        }
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
