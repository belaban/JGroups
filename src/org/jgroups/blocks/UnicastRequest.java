package org.jgroups.blocks;


import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.Transport;
import org.jgroups.View;
import org.jgroups.annotations.GuardedBy;
import org.jgroups.util.Rsp;

import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


/**
 * Sends a request to a single target destination
 *
 * @author Bela Ban
 * @version $Id: UnicastRequest.java,v 1.1 2010/01/13 13:18:39 belaban Exp $
 */
public class UnicastRequest extends Request implements Future<Rsp> {
    protected final Rsp        result;
    protected final Address    target;



    /**
     @param timeout Time to wait for responses (ms). A value of <= 0 means wait indefinitely
     (e.g. if a suspicion service is available; timeouts are not needed).
     */
    public UnicastRequest(Message m, RequestCorrelator corr, Address target, int rsp_mode, long timeout) {
        super(m, corr, null, null, rsp_mode, timeout);
        this.target=target;
        result=new Rsp(target);
    }


    /**
     * @param timeout Time to wait for responses (ms). A value of <= 0 means wait indefinitely
     *                       (e.g. if a suspicion service is available; timeouts are not needed).
     */
    public UnicastRequest(Message m, Transport transport, Address target, int rsp_mode, long timeout) {
        super(m, null, transport, null, rsp_mode, timeout);
        this.target=target;
        result=new Rsp(target);
    }


    protected void sendRequest() throws Exception {
        try {
            if(log.isTraceEnabled()) log.trace(new StringBuilder("sending request (id=").append(req_id).append(')'));
            if(corr != null) {
                corr.sendUnicastRequest(req_id, target, request_msg, rsp_mode == GET_NONE? null : this);
            }
            else {
                transport.send(request_msg);
            }
        }
        catch(Exception ex) {
            if(corr != null)
                corr.done(req_id);
            throw ex;
        }
    }
    

    /* ---------------------- Interface RspCollector -------------------------- */
    /**
     * <b>Callback</b> (called by RequestCorrelator or Transport).
     * Adds a response to the response table. When all responses have been received,
     * <code>execute()</code> returns.
     */
    public void receiveResponse(Object response_value, Address sender) {
        if(done)
            return;

        lock.lock();
        try {
            if(!result.wasReceived()) {
                boolean responseReceived=(rsp_filter == null) || rsp_filter.isAcceptable(response_value, sender);
                result.setValue(response_value);
                result.setReceived(responseReceived);
                if(log.isTraceEnabled())
                    log.trace(new StringBuilder("received response for request ").append(req_id)
                            .append(", sender=").append(sender).append(", val=").append(response_value));
            }
            // done=rsp_filter != null && !rsp_filter.needMoreResponses();
            done=rsp_filter == null? responsesComplete() : !rsp_filter.needMoreResponses();
            if(done && corr != null)
                corr.done(req_id);
        }
        finally {
            completed.signalAll(); // wakes up execute()
            lock.unlock();
        }
    }


    /**
     * <b>Callback</b> (called by RequestCorrelator or Transport).
     * Report to <code>GroupRequest</code> that a member is reported as faulty (suspected).
     * This method would probably be called when getting a suspect message from a failure detector
     * (where available). It is used to exclude faulty members from the response list.
     */
    public void suspect(Address suspected_member) {
        if(suspected_member == null)
            return;

        lock.lock();
        try {
            if(done)
                return;
            if(result != null && !result.wasReceived()) {
                result.setSuspected(true);
                completed.signalAll();
            }
        }
        finally {
            lock.unlock();
        }
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
            if(!mbrs.contains(target)) {
                result.setReceived(false);
                done=true;
                completed.signalAll();
            }
        }
        finally {
            lock.unlock();
        }
    }


    /* -------------------- End of Interface RspCollector ----------------------------------- */

    public Rsp getResult() {
        return result;
    }


    public boolean cancel(boolean mayInterruptIfRunning) {
        lock.lock();
        try {
            boolean retval=!done;
            done=true;
            if(corr != null)
                corr.done(req_id);
            completed.signalAll();
            return retval;
        }
        finally {
            lock.unlock();
        }
    }

    public boolean isCancelled() {
        lock.lock();
        try {
            return done;
        }
        finally {
            lock.unlock();
        }
    }

    public Rsp get() throws InterruptedException, ExecutionException {
        waitForResults(0);
        return result;
    }

    public Rsp get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        boolean ok=waitForResults(unit.toMillis(timeout));
        if(!ok)
            throw new TimeoutException();
        return result;
    }

    public String toString() {
        StringBuilder ret=new StringBuilder(128);
        ret.append(super.toString());
        ret.append(", target=" + target);
        return ret.toString();
    }



    @GuardedBy("lock")
    protected boolean responsesComplete() {
        return done || rsp_mode == GET_NONE || result.wasReceived() || result.wasSuspected();
    }



}