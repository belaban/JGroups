package org.jgroups.protocols;

import org.jgroups.stack.Protocol;
import org.jgroups.Event;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;
import java.util.LinkedList;
import java.util.Vector;

/**
 * Class that waits for n PingRsp'es, or m milliseconds to return the initial membership
 * @author Bela Ban
 * @version $Id: PingWaiter.java,v 1.3 2005/01/04 13:34:09 belaban Exp $
 */
public class PingWaiter implements Runnable {
    Thread              t=null;
    List                rsps=new LinkedList();
    long                timeout=3000;
    int                 num_rsps=3;
    Protocol            parent=null;
    protected final Log log=LogFactory.getLog(this.getClass());


    public PingWaiter(long timeout, int num_rsps, Protocol parent) {
        this.timeout=timeout;
        this.num_rsps=num_rsps;
        this.parent=parent;
    }


    public synchronized void start() {
        if(t == null || !t.isAlive()) {
            // clearResponses();
            t=new Thread(this, "PingWaiter");
            t.setDaemon(true);
            t.start();
        }
    }

    public synchronized void stop() {
        if(t != null) {
            Thread tmp=t;
            t=null;
            tmp.interrupt();
        }
    }


    public synchronized boolean isRunning() {
        return t != null && t.isAlive();
    }

    public void addResponse(PingRsp rsp) {
        if(rsp != null) {
            synchronized(rsps) {
                if(!rsps.contains(rsp)) {
                    rsps.add(rsp);
                    rsps.notifyAll();
                }
            }
        }
    }

    public void clearResponses() {
        synchronized(rsps) {
            rsps.clear();
            rsps.notifyAll();
        }
    }


    public List getResponses() {
        return rsps;
    }



    public void run() {
        long start_time, time_to_wait;

        synchronized(rsps) {
            start_time=System.currentTimeMillis();
            time_to_wait=timeout;

            while(rsps.size() < num_rsps && time_to_wait > 0 && t != null && Thread.currentThread().equals(t)) {
                if(log.isTraceEnabled()) // +++ remove
                    log.trace("waiting for initial members: time_to_wait=" + time_to_wait +
                              ", got " + rsps.size() + " rsps");

                try {
                    rsps.wait(time_to_wait);
                }
                catch(Exception e) {
                }
                time_to_wait=timeout - (System.currentTimeMillis() - start_time);
            }
            if(log.isDebugEnabled())
                log.debug("initial mbrs are " + rsps);
            // 3. Send response
            if(parent != null)
                parent.passUp(new Event(Event.FIND_INITIAL_MBRS_OK, new Vector(rsps)));
        }
    }
}
