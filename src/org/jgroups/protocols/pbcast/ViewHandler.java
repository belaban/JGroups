package org.jgroups.protocols.pbcast;

import org.jgroups.annotations.GuardedBy;
import org.jgroups.logging.Log;
import org.jgroups.util.BoundedList;
import org.jgroups.util.Util;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Responsible for dispatching JOIN/LEAVE/MERGE requests to the GMS protocol. Bundles multiple concurrent requests into
 * a request list
 * @param <R> the type of the request
 * @author Bela Ban
 * @since  4.0.5
 */
public class ViewHandler<R> {
    protected final Collection<R>         requests=new ConcurrentLinkedQueue<>();
    protected final Lock                  lock=new ReentrantLock();
    protected final AtomicBoolean         suspended=new AtomicBoolean(false);
    @GuardedBy("lock")
    protected final AtomicBoolean         processing=new AtomicBoolean(false);
    protected final Condition             processing_done=lock.newCondition();
    protected final GMS                   gms;
    protected Consumer<Collection<R>>     req_processor;
    protected BiPredicate<R,R>            req_matcher;
    protected final BoundedList<String>   history=new BoundedList<>(20); // maintains a list of the last 20 requests


    /**
     * Constructor
     * @param gms The ref to GMS
     * @param req_processor A request processor which processes a list of requests
     * @param req_matcher The matcher which determines whether any given 2 requests can be processed together
     */
    public ViewHandler(GMS gms, Consumer<Collection<R>> req_processor, BiPredicate<R,R> req_matcher) {
        if(req_processor == null)
            throw new IllegalArgumentException("request processor cannot be null");
        this.gms=gms;
        this.req_processor=req_processor;
        this.req_matcher=req_matcher != null? req_matcher : (a,b) -> true;
    }

    public boolean                 suspended()                             {return suspended.get();}
    public boolean                 processing()                            {return processing.get();}
    public int                     size()                                  {return requests.size();}
    public ViewHandler<R>          reqProcessor(Consumer<Collection<R>> p) {req_processor=p; return this;}
    public Consumer<Collection<R>> reqProcessor()                          {return req_processor;}
    public ViewHandler<R>          reqMatcher(BiPredicate<R,R> m)          {req_matcher=m; return this;}
    public BiPredicate<R,R>        reqMatcher()                            {return req_matcher;}

    public ViewHandler<R> add(R req) {
        if(_add(req))
            process();
        return this;
    }

    @SuppressWarnings("unchecked")
    public ViewHandler<R> add(R ... reqs) {
        if(_add(reqs))
            process();
        return this;
    }

    public ViewHandler<R> add(Collection<R> reqs) {
        if(_add(reqs))
            process();
        return this;
    }


    /** Clears the queue and discards new requests from now on */
    public void suspend() {
        if(suspended.compareAndSet(false, true)) {
            lock.lock();
            try {
                requests.clear();
            }
            finally {
                lock.unlock();
            }
        }
    }


    public void resume() {
        suspended.compareAndSet(true, false);
    }

    /** Blocks the caller until the <em>current</em> set of requests being processed have been completed. Returns
     * immediately if no requests are currently being processed */
    public void waitUntilComplete() {
        lock.lock();
        try {
            while(processing.get()) {
                try {
                    processing_done.await();
                }
                catch(InterruptedException ignored) {
                }
            }
        }
        finally {
            lock.unlock();
        }
    }

    /** Blocks the caller until the <em>current</em> set of requests being processed have been completed, or the timeout
     * elapsed.<br/>
     * Returns immediately if no requests are currently being processed
     * @param timeout Max time to wait in milliseconds
     */
    public void waitUntilComplete(long timeout) {
        long base=System.currentTimeMillis();
        long now=0;
        lock.lock();
        try {
            while(processing.get()) {
                long delay=timeout-now;
                if(delay <= 0)
                    break;
                try {
                    processing_done.await(delay, TimeUnit.MILLISECONDS);
                    now=System.currentTimeMillis()-base;
                }
                catch(InterruptedException e) {
                }
            }
        }
        finally {
            lock.unlock();
        }
    }

    /** To be used by testing only! */
    public <T extends ViewHandler<R>> T processing(boolean flag) {
        lock.lock();
        try {
            processing.set(flag);
            if(flag == false)
                processing_done.signalAll();
            return (T)this;
        }
        finally {
            lock.unlock();
        }
    }

    public String dumpQueue() {
        return requests.stream().map(Object::toString).collect(Collectors.joining("\n"));
    }

    public String dumpHistory() {
        return String.join("\n", history);
    }

    public String toString() {
        return Util.printListWithDelimiter(requests, ", ");
    }

    protected Log log() {return gms.getLog();}


    protected boolean _add(R req) {
        if(req == null)
            return false;
        if(suspended.get()) {
            log().trace("%s: queue is suspended; request %s is discarded", gms.getAddress(), req);
            return false;
        }
        String log=new Date() + ": " + req;
        lock.lock();
        try {
            if(!requests.contains(req)) { // non-null check already performed (above)
                requests.add(req);
                history.add(log);
            }
            return processing.compareAndSet(false, true);
        }
        finally {
            lock.unlock();
        }
    }

    @SuppressWarnings("unchecked")
    protected boolean _add(R ... reqs) {
        if(reqs == null || reqs.length == 0)
            return false;
        return _add(Arrays.asList(reqs));
    }


    protected boolean _add(Collection<R> reqs) {
        if(reqs == null || reqs.isEmpty())
            return false;
        if(suspended.get()) {
            log().trace("%s: queue is suspended; requests %s are discarded", gms.getAddress(), reqs);
            return false;
        }

        lock.lock();
        try {
            for(R req: reqs) {
                if(req != null && !requests.contains(req)) {
                    requests.add(req);
                    history.add(new Date() + ": " + req);
                }
            }
            return processing.compareAndSet(false, true);
        }
        finally {
            lock.unlock();
        }
    }

    /** We're guaranteed that only one thread will be called with this method at any time */
    protected void process() {
        for(;;) {
            while(!requests.isEmpty()) {
                Collection<R> reqs=null;
                lock.lock();
                try {
                    reqs=remove(requests); // remove matching requests
                }
                finally {
                    lock.unlock();
                }
                if(reqs != null && !reqs.isEmpty()) {
                    try {
                        req_processor.accept(reqs); // process outside of the lock scope
                    }
                    catch(Throwable t) {
                        log().error("%s: failed processsing requests: %s", gms.addr(), t);
                        lock.lock();
                        try {
                            if(processing.compareAndSet(true, false))
                                processing_done.signalAll();
                            throw t;
                        }
                        finally {
                            lock.unlock();
                        }
                    }
                }
            }
            lock.lock();
            try {
                if(requests.isEmpty()) {
                    if(processing.compareAndSet(true, false))
                        processing_done.signalAll();
                    return;
                }
            }
            finally {
                lock.unlock();
            }
        }
    }

    /**
     * Removes requests as long as they match - breaks at the first non-matching request or when requests is empty
     * This method must catch all exceptions; or else process() might return without setting processing to true again!
     */
    @GuardedBy("lock")
    protected Collection<R> remove(Collection<R> requests) {
        Collection<R> removed=new ArrayList<>();
        R             first_req=null;
        Iterator<R>   it=requests.iterator();
        try {
            if(it.hasNext()) { // safeguard to prevent https://issues.redhat.com/browse/JGRP-2718
                first_req=it.next();
                removed.add(first_req);
                it.remove();
            }
            while(it.hasNext()) {
                R next=it.next();
                if(req_matcher.test(first_req, next)) {
                    removed.add(next);
                    it.remove();
                }
                else
                    break;
            }
        }
        catch(Throwable t) {
            log().error("failed processing requests", t);
        }
        return removed;
    }


}
