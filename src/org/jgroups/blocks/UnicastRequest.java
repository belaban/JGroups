package org.jgroups.blocks;


import org.jgroups.*;
import org.jgroups.annotations.GuardedBy;
import org.jgroups.protocols.relay.SiteAddress;
import org.jgroups.util.Rsp;

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


/**
 * Sends a request to a single target destination
 *
 * @author Bela Ban
 */
public class UnicastRequest<T> extends Request {
    protected final Rsp<T>     result;
    protected final Address    target;
    protected int              num_received=0;



    public UnicastRequest(RequestCorrelator corr, Address target, RequestOptions options) {
        super(corr, options);
        this.target=target;
        result=new Rsp<>(target);
    }


    protected void sendRequest(final Message request_msg) throws Exception {
        try {
            corr.sendUnicastRequest(target, request_msg, options.getMode() == ResponseMode.GET_NONE? null : this);
        }
        catch(Exception ex) {
            corr.done(this);
            throw ex;
        }
    }
    

    /* ---------------------- Interface RspCollector -------------------------- */
    /**
     * <b>Callback</b> (called by RequestCorrelator or Transport).
     * Adds a response to the response table. When all responses have been received, {@code execute()} returns.
     */
    public void receiveResponse(Object response_value, Address sender, boolean is_exception) {
        RspFilter rsp_filter=options.getRspFilter();

        lock.lock();
        try {
            if(done)
                return;
            if(!result.wasReceived()) {
                num_received++;
                if(rsp_filter == null || rsp_filter.isAcceptable(response_value, sender)) {
                    if(is_exception && response_value instanceof Throwable)
                        result.setException((Throwable)response_value);
                    else
                        result.setValue((T)response_value);
                }
            }
            done=responsesComplete() || (rsp_filter != null && !rsp_filter.needMoreResponses());
            if(done && corr != null)
                corr.done(this);
        }
        finally {
            cond.signal(true); // wakes up execute()
            lock.unlock();
        }
        checkCompletion(this);
    }

    public boolean responseReceived() {return num_received >= 1;}


    /**
     * <b>Callback</b> (called by RequestCorrelator or Transport).
     * Report to {@code GroupRequest} that a member is reported as faulty (suspected).
     * This method would probably be called when getting a suspect message from a failure detector
     * (where available). It is used to exclude faulty members from the response list.
     */
    public void suspect(Address suspected_member) {
        if(!Objects.equals(suspected_member, target))
            return;

        lock.lock();
        try {
            if(done)
                return;
            if(result != null && !result.wasReceived())
                result.setSuspected();
            done=true;
            if(corr != null)
                corr.done(this);
            cond.signal(true);
        }
        finally {
            lock.unlock();
        }
        checkCompletion(this);
    }

    public void siteUnreachable(String site) {
        if(!(target instanceof SiteAddress))
            return;

        if(!((SiteAddress)target).getSite().equals(site))
            return;

        lock.lock();
        try {
            if(done)
                return;
            if(result != null && !result.wasUnreachable())
                result.setUnreachable();
            done=true;
            if(corr != null)
                corr.done(this);
            cond.signal(true);
        }
        finally {
            lock.unlock();
        }
        checkCompletion(this);
    }

    /**
     * If the target address is not a member of the new view, we'll mark the response as not received and unblock
     * the caller of execute()
     */
    public void viewChange(View new_view) {
        Collection<Address> mbrs=new_view != null? new_view.getMembers() : null;
        if(mbrs == null)
            return;

        lock.lock();
        try {
            // SiteAddresses are not checked as they might be in a different cluster
            if(!(target instanceof SiteAddress) && !mbrs.contains(target)) {
                result.setSuspected();
                done=true;
                if(corr != null)
                    corr.done(this);
                cond.signal(true);
            }
        }
        finally {
            lock.unlock();
        }
        
        checkCompletion(this);
    }

    public void transportClosed() {
        lock.lock();
        try {
            if(done)
                return;
            if(result != null && !result.wasReceived())
                result.setException(new IllegalStateException("transport was closed"));
            done=true;
            if(corr != null)
                corr.done(this);
            cond.signal(true);
        }
        finally {
            lock.unlock();
        }
        checkCompletion(this);
    }

    /* -------------------- End of Interface RspCollector ----------------------------------- */

    public Rsp<T> getResult() {
        return result;
    }



    public T getValue() throws ExecutionException {
        if(result.wasSuspected())
            throw new ExecutionException(new SuspectedException(target));

        if(result.hasException())
            throw new ExecutionException(result.getException());

        if(result.wasUnreachable())
            throw new ExecutionException(new UnreachableException(target));
        if(!result.wasReceived())
            throw new ExecutionException(new TimeoutException("timeout sending message to " + target));
        return result.getValue();
    }


    public T get() throws InterruptedException, ExecutionException {
        lock.lock();
        try {
            waitForResults(0);
            return getValue();
        }
        finally {
            lock.unlock();
        }
    }

    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        boolean ok;
        lock.lock();
        try {
            ok=waitForResults(unit.toMillis(timeout));
        }
        finally {
            lock.unlock();
        }
        if(!ok)
            throw new TimeoutException();
        return getValue();
    }

    public String toString() {
        return String.format("%s, target=%s", super.toString(), target);
    }



    @GuardedBy("lock")
    protected boolean responsesComplete() {
        return done || options.getMode() == ResponseMode.GET_NONE || result.wasReceived() ||
          result.wasSuspected() || result.wasUnreachable() || num_received >= 1;
    }



}