package org.jgroups.protocols;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.util.Util;
import org.jgroups.annotations.GuardedBy;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Sends num_ping_request GET_MBRS_REQ messages, distributed over timeout ms
 * @author Bela Ban
 * @version $Id: PingSender.java,v 1.6 2007/01/20 11:55:18 belaban Exp $
 */
public class PingSender implements Runnable {
    @GuardedBy("lock")
    Thread              thread=null;
    final Lock          lock=new ReentrantLock();
    long                timeout=3000;
    double              interval;
    int                 num_requests=1;
    Discovery           discovery_prot;
    protected final Log log=LogFactory.getLog(this.getClass());
    protected boolean   trace=log.isTraceEnabled();


    public PingSender(long timeout, int num_requests, Discovery d) {
        this.timeout=timeout;
        this.num_requests=num_requests;
        this.discovery_prot=d;
        interval=timeout / (double)num_requests;
    }


    public void start() {
        lock.lock();
        try {
            if(thread == null || !thread.isAlive()) {
                thread=new Thread(Util.getGlobalThreadGroup(), this, "PingSender");
                thread.setDaemon(true);
                thread.start();
            }
        }
        finally {
            lock.unlock();
        }
    }

    public void stop() {
        lock.lock();
        try {
            if(thread != null) {
                Thread tmp=thread;
                thread=null;
                try {tmp.interrupt();} catch(SecurityException ex) {}
            }
        }
        finally {
            lock.unlock();
        }
    }



    public void run() {
        for(int i=0; i < num_requests; i++) {
            lock.lock();
            try {
                if(thread == null || !thread.equals(Thread.currentThread()))
                    break;
            }
            finally {
                lock.unlock();
            }
            if(trace)
                log.trace("sending GET_MBRS_REQ");
            discovery_prot.sendGetMembersRequest();
            Util.sleep((long)interval);
        }
    }
}
