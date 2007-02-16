package org.jgroups.protocols;

import org.jgroups.stack.Protocol;
import org.jgroups.Event;
import org.jgroups.util.Util;
import org.jgroups.annotations.GuardedBy;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;
import java.util.LinkedList;
import java.util.Vector;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Class that waits for n PingRsp'es, or m milliseconds to return the initial membership
 * @author Bela Ban
 * @version $Id: PingWaiter.java,v 1.16 2007/02/16 08:22:55 belaban Exp $
 */
public class PingWaiter implements Runnable {
    @GuardedBy("thread_lock")
    Thread              thread=null;
    private final Lock  thread_lock=new ReentrantLock();
    final List          rsps=new LinkedList();
    long                timeout=3000;
    int                 num_rsps=3;
    Protocol            parent=null;
    PingSender          ping_sender;
    protected final Log log=LogFactory.getLog(this.getClass());
    private boolean     trace=log.isTraceEnabled();


    public PingWaiter(long timeout, int num_rsps, Protocol parent, PingSender ping_sender) {
        this.timeout=timeout;
        this.num_rsps=num_rsps;
        this.parent=parent;
        this.ping_sender=ping_sender;
    }


    void setTimeout(long timeout) {
        this.timeout=timeout;
    }

    void setNumRsps(int num) {
        this.num_rsps=num;
    }



    public void start() {
        thread_lock.lock();
        try {
            if(thread == null || !thread.isAlive()) {
                thread=new Thread(Util.getGlobalThreadGroup(), this, "PingWaiter");
                thread.setDaemon(true);
                thread.start();
            }
        }
        finally {
            thread_lock.unlock();
        }
    }

    public void stop() {
        ping_sender.stop();

        boolean stopped=false;
        thread_lock.lock();
        try {
            if(thread != null) {
                thread=null;
                stopped=true;
            }
        }
        finally {
            thread_lock.unlock();
        }

        // moved this out of the thread_lock scope to prevent deadlock with findInitialMembers() (different order of lock acquisition)
        if(stopped) {
            synchronized(rsps) {
                rsps.notifyAll();
            }
        }
    }



    public void addResponse(PingRsp rsp) {
        if(rsp != null) {
            synchronized(rsps) {
                if(rsps.contains(rsp))
                    rsps.remove(rsp); // overwrite existing element
                rsps.add(rsp);
                rsps.notifyAll();
            }
        }
    }

    public void clearResponses() {
        synchronized(rsps) {
            rsps.clear();
            rsps.notifyAll();
        }
    }




    public void run() {
        Vector responses=findInitialMembers();

        thread_lock.lock();
        try {
            thread=null;
        }
        finally {
            thread_lock.unlock();
        }

        if(parent != null) {
            parent.getUpProtocol().up(new Event(Event.FIND_INITIAL_MBRS_OK, responses));
        }
    }


    public Vector findInitialMembers() {
        long start_time, time_to_wait;

        synchronized(rsps) {
            rsps.clear();

            ping_sender.start();

            start_time=System.currentTimeMillis();
            time_to_wait=timeout;

            try {
                while(true) {
                    boolean cond=rsps.size() < num_rsps && time_to_wait > 0 && thread != null && Thread.currentThread().equals(thread);
                    if(!cond)
                        break;

                    if(trace) // +++ remove
                        log.trace(new StringBuilder("waiting for initial members: time_to_wait=").append(time_to_wait)
                                  .append(", got ").append(rsps.size()).append(" rsps"));

                    try {
                        rsps.wait(time_to_wait);
                    }
                    catch(InterruptedException intex) {
                    }
                    catch(Exception e) {
                        log.error("got an exception waiting for responses", e);
                    }
                    time_to_wait=timeout - (System.currentTimeMillis() - start_time);
                }
                if(trace)
                    log.trace(new StringBuffer("initial mbrs are ").append(rsps));
                return new Vector(rsps);
            }
            finally {
                if(ping_sender != null)
                    ping_sender.stop();
            }
        }
    }

}
