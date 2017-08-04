package org.jgroups.protocols.pbcast;

import org.jgroups.logging.Log;
import org.jgroups.util.BoundedList;
import org.jgroups.util.Util;

import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiPredicate;
import java.util.function.Consumer;

/**
 * Responsible for dispatching JOIN/LEAVE/MERGE requests to the GMS protocol. Bundles multiple concurrent requests into
 * a request list
 * @param <R> the type of the request
 * @author Bela Ban
 * @since  4.0.5
 */
public class ViewHandler2<R> {
    protected final Collection<R>           requests=new LinkedHashSet<>();
    protected R                             first_req;
    protected final Lock                    lock=new ReentrantLock();
    protected final AtomicInteger           count=new AtomicInteger();
    protected volatile boolean              suspended;
    protected final GMS                     gms;
    protected final Consumer<Collection<R>> req_processor;
    protected final BiPredicate<R,R>        req_matcher;
    protected final BoundedList<String>     history=new BoundedList<>(20); // maintains a list of the last 20 requests
    protected final R                       END_MARKER=(R)new Object() {public String toString() {return "[end]";}};
    protected static final long             THREAD_WAIT_TIME=5000;


    /**
     * Constructor
     * @param gms The ref to GMS
     * @param req_processor A request processor which processes a list of requests
     * @param req_matcher The matcher which determines whether any given 2 requests can be processed together
     */
    public ViewHandler2(GMS gms, Consumer<Collection<R>> req_processor, BiPredicate<R,R> req_matcher) {
        if(req_processor == null)
            throw new IllegalArgumentException("request processor cannot be null");
        this.gms=gms;
        this.req_processor=req_processor;
        this.req_matcher=req_matcher != null? req_matcher : (a,b) -> true;
    }

    public boolean        suspended()         {return suspended;}
    public int            size()              {return requests.size();}

    public void add(R req) {
        if(suspended) {
            log().trace("%s: queue is suspended; request %s is discarded", gms.getLocalAddress(), req);
            return;
        }
        count.incrementAndGet();
        history.add(new Date() + ": " + req.toString());

        lock.lock();
        try {
            if(first_req == null) {
                first_req=req;
                requests.add(req);
            }
            else {
                if(req_matcher.test(first_req, req))
                    requests.add(req);
                else
                    process(requests);
            }
            if(count.decrementAndGet() == 0)
                process(requests);
        }
        finally {
            lock.unlock();
        }
    }



    @SuppressWarnings("unchecked")
    public void add(R ... reqs) {
        if(suspended) {
            log().trace("%s: queue is suspended; requests are discarded", gms.getLocalAddress());
            return;
        }

        count.incrementAndGet();
        lock.lock();
        try {
            for(R req: reqs) {
                history.add(new Date() + ": " + req.toString());
                if(first_req == null) {
                    first_req=req;
                    requests.add(req);
                }
                else {
                    if(req_matcher.test(first_req, req))
                        requests.add(req);
                    else {
                        process(requests);
                        first_req=req;
                        requests.add(req);
                    }
                }
            }
            if(count.decrementAndGet() == 0)
                process(requests);
        }
        finally {
            lock.unlock();
        }

    }



    /**
     * Waits until the current requests in the queue have been processed, then clears the queue and discards new
     * requests from now on
     */
    public void suspend() {
        lock.lock();
        try {
            if(!suspended) {
                suspended=true;
                requests.clear();
            }
        }
        finally {
            lock.unlock();
        }
    }


    public void resume() {
        lock.lock();
        try {
            if(suspended)
                suspended=false;
        }
        finally {
            lock.unlock();
        }
    }




    public String dumpQueue() {
        return requests.stream()
          .collect(StringBuilder::new, (sb,el) -> sb.append(el).append("\n"), StringBuilder::append).toString();
    }

    public String dumpHistory() {
        return history.stream()
          .collect(StringBuilder::new, (sb,el) -> sb.append(el + "\n"), StringBuilder::append).toString();
    }

    public String toString() {
        return Util.printListWithDelimiter(requests, ", ");
    }

    protected Log log() {return gms.getLog();}



    protected void process(Collection<R> requests) {
        try {
            req_processor.accept(requests);
        }
        catch(Throwable t) {
        }
        requests.clear();
        first_req=null;
    }

    protected static void join(Thread t) {
        try {
            t.join(THREAD_WAIT_TIME);
        }
        catch(InterruptedException e) {
        }
    }


}
