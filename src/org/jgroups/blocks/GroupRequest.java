package org.jgroups.blocks;


import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.GuardedBy;
import org.jgroups.protocols.relay.SiteAddress;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


/**
 * Sends a message to all members of the group and waits for all responses (or timeout). Returns a boolean value
 * (success or failure). Results (if any) can be retrieved when done.
 * <p>
 * The supported transport to send requests is currently either a RequestCorrelator or a generic Transport. One of
 * them has to be given in the constructor. It will then be used to send a request. When a message is received by
 * either one, the receiveResponse() of this class has to be called (this class does not actively receive
 * requests/responses itself). Also, when a view change or suspicion is received, the methods viewChange() or suspect()
 * of this class have to be called.
 * <p>
 * When started, an array of responses, correlating to the membership, is created. Each response is added to the
 * corresponding field in the array. When all fields have been set, the algorithm terminates. This algorithm can
 * optionally use a suspicion service (failure detector) to detect (and exclude from the membership) fauly members.
 * If no suspicion service is available, timeouts can be used instead (see {@code execute()}). When done, a
 * list of suspected members can be retrieved.
 * <p>
 * Because a channel might deliver requests, and responses to <em>different</em> requests, the {@code GroupRequest}
 * class cannot itself receive and process requests/responses from the channel. A mechanism outside this class
 * has to do this; it has to determine what the responses are for the message
 * sent by the {@code execute()} method and call {@code receiveResponse()} to do so.
 *
 * @author Bela Ban
 */
public class GroupRequest<T> extends Request<RspList<T>> {

    /** Correlates requests and responses */
    @GuardedBy("lock")
    protected final Map<Address,Rsp<T>> requests;

    @GuardedBy("lock")
    protected int num_valid;       // the number of valid responses (values or exceptions that passed the response filter)

    @GuardedBy("lock")
    protected int num_received;    // number of responses (values, exceptions or suspicions)



    
     /**
     * @param corr The request correlator to be used. A request correlator sends requests tagged with a unique ID and
     *             notifies the sender when matching responses are received. The reason {@code GroupRequest} uses
     *             it instead of a {@code Transport} is that multiple requests/responses might be sent/received concurrently
     * @param targets The targets, which are supposed to receive the message. Any receiver not in this set will
     *                discard the message. Targets are always a subset of the current membership
     * @param options The request options to be used for this call
     */
    public GroupRequest(RequestCorrelator corr, Collection<Address> targets, RequestOptions options) {
        super(corr, options);
        int size=targets.size();
        requests=new HashMap<>(size);
        setTargets(targets);
    }


    public boolean getAnycasting() {
        return options.getAnycasting();
    }

    public GroupRequest setAnycasting(boolean anycasting) {
        options.setAnycasting(anycasting);
        return this;
    }


    public void sendRequest(final Message req) throws Exception {
        sendRequest(req, requests.keySet());
    }

    /* ---------------------- Interface RspCollector -------------------------- */
    /**
     * <b>Callback</b> (called by RequestCorrelator or Transport).
     * Adds a response to the response table. When all responses have been received, {@code execute()} returns.
     */
    @SuppressWarnings("unchecked")
    public void receiveResponse(Object response_value, Address sender, boolean is_exception) {
        if(done)
            return;
        Rsp<T> rsp=requests.get(sender);
        if(rsp == null)
            return;

        RspFilter rsp_filter=options.getRspFilter();
        boolean responseReceived=false;

        lock.lock();
        try {
            if(!rsp.wasReceived()) {
                if(!(rsp.wasSuspected() || rsp.wasUnreachable()))
                    num_received++;
                if((responseReceived=(rsp_filter == null) || rsp_filter.isAcceptable(response_value, sender))) {
                    if(is_exception && response_value instanceof Throwable)
                        rsp.setException((Throwable)response_value);
                    else
                        rsp.setValue((T)response_value);
                    num_valid++;
                }
            }

            done=responsesComplete() || (rsp_filter != null && !rsp_filter.needMoreResponses());
            if(responseReceived || done)
                cond.signal(true); // wakes up execute()
            if(done && corr != null && this.req_id > 0)
                corr.done(this.req_id);
        }
        finally {
            lock.unlock();
        }
        if(responseReceived || done)
            checkCompletion(this);
    }


    /**
     * <b>Callback</b> (called by RequestCorrelator or Transport).
     * Report to {@code GroupRequest} that a member is reported as faulty (suspected).
     * This method would probably be called when getting a suspect message from a failure detector
     * (where available). It is used to exclude faulty members from the response list.
     */
    public void suspect(Address suspected_member) {
        if(suspected_member == null)
            return;

        boolean changed=false;
        Rsp<T> rsp=requests.get(suspected_member);
        if(rsp !=  null && rsp.setSuspected()) {
            changed=true;
            lock.lock();
            try {
                if(!(rsp.wasReceived() || rsp.wasUnreachable()))
                    num_received++;
                cond.signal(true);
            }
            finally {
                lock.unlock();
            }
        }

        if(changed)
            checkCompletion(this);
    }

    public void siteUnreachable(String site) {
        boolean changed=false;

        for(Map.Entry<Address, Rsp<T>> entry: requests.entrySet()) {
            Address member=entry.getKey();
            if(!(member instanceof SiteAddress))
                continue;
            SiteAddress addr=(SiteAddress)member;
            if(addr.getSite().equals(site)) {
                Rsp<T> rsp=entry.getValue();
                if(rsp !=  null && rsp.setUnreachable()) {
                    changed=true;
                    lock.lock();
                    try {
                        if(!(rsp.wasReceived() || rsp.wasSuspected()))
                            num_received++;
                        cond.signal(true);
                    }
                    finally {
                        lock.unlock();
                    }
                }

                if(changed)
                    checkCompletion(this);
            }
        }
    }

    /**
     * Any member of 'membership' that is not in the new view is flagged as
     * SUSPECTED. Any member in the new view that is <em>not</em> in the
     * membership (ie, the set of responses expected for the current RPC) will
     * <em>not</em> be added to it. If we did this we might run into the
     * following problem:
     * <ul>
     * <li>Membership is {A,B}
     * <li>A sends a synchronous group RPC (which sleeps for 60 secs in the
     * invocation handler)
     * <li>C joins while A waits for responses from A and B
     * <li>If this would generate a new view {A,B,C} and if this expanded the
     * response set to {A,B,C}, A would wait forever on C's response because C
     * never received the request in the first place, therefore won't send a
     * response.
     * </ul>
     */
    public void viewChange(View new_view) {
        if(new_view == null || requests == null || requests.isEmpty())
            return;

        List<Address> mbrs=new_view.getMembers();
        if(mbrs == null)
            return;

        boolean changed=false;

        lock.lock();
        try {
            for(Map.Entry<Address,Rsp<T>> entry: requests.entrySet()) {
                Address mbr=entry.getKey();
                // SiteAddresses are not checked as they might be in a different cluster
                if(!(mbr instanceof SiteAddress) && !mbrs.contains(mbr)) {
                    Rsp<T> rsp=entry.getValue();
                    if(rsp.setSuspected()) {
                        if(!(rsp.wasReceived() || rsp.wasUnreachable()))
                            num_received++;
                        changed=true;
                    }
                }
            }
            if(changed)
                cond.signal(true);
        }
        finally {
            lock.unlock();
        }
        if(changed)
            checkCompletion(this);
    }

    /** Marks all responses with an exception (unless a response was already marked as done) */
    public void transportClosed() {
        boolean changed=false;

        lock.lock();
        try {
            for(Map.Entry<Address, Rsp<T>> entry: requests.entrySet()) {
                Rsp<T> rsp=entry.getValue();
                if(rsp != null && !(rsp.wasReceived() || rsp.wasSuspected() || rsp.wasUnreachable())) {
                    rsp.setException(new IllegalStateException("transport was closed"));
                    num_received++;
                    changed=true;
                }
            }
            if(changed) {
                cond.signal(true);
            }
        }
        finally {
            lock.unlock();
        }
        if(changed)
            checkCompletion(this);
    }

    /* -------------------- End of Interface RspCollector ----------------------------------- */



    /** Returns the results as a RspList */
    public RspList<T> getResults() {
        return new RspList<>(requests);
    }



    public RspList<T> get() throws InterruptedException, ExecutionException {
        lock.lock();
        try {
            waitForResults(0);
        }
        finally {
            lock.unlock();
        }
        return getResults();
    }

    public RspList<T> get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
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
        return getResults();
    }

    public String toString() {
        StringBuilder ret=new StringBuilder(128);
        ret.append(super.toString());

        if(!requests.isEmpty()) {
            ret.append(", entries:\n");
            for(Map.Entry<Address,Rsp<T>> entry: requests.entrySet()) {
                Address mbr=entry.getKey();
                Rsp<T> rsp=entry.getValue();
                ret.append(mbr).append(": ").append(rsp).append("\n");
            }
        }
        return ret.toString();
    }


    /* --------------------------------- Private Methods -------------------------------------*/

    private void setTargets(Collection<Address> mbrs) {
        for(Address mbr: mbrs)
            requests.put(mbr, new Rsp<>());
    }

    private static int determineMajority(int i) {
        return i < 2? i : (i / 2) + 1;
    }


    private void sendRequest(final Message request_msg, final Collection<Address> targetMembers) throws Exception {
        try {
            corr.sendRequest(targetMembers, request_msg, options.getMode() == ResponseMode.GET_NONE? null : this, options);
        }
        catch(Exception ex) {
            if(this.req_id > 0)
                corr.done(this.req_id);
            throw ex;
        }
    }


    @GuardedBy("lock")
    protected boolean responsesComplete() {
        if(done)
            return true;

        final int num_total=requests.size();

        switch(options.getMode()) {
            case GET_FIRST:
                return num_valid >= 1 || num_received >= num_total;
            case GET_ALL:
                return num_valid >= num_total || num_received >= num_total;
            case GET_NONE:
                return true;
            default:
                if(log.isErrorEnabled()) log.error("rsp_mode " + options.getMode() + " unknown !");
                break;
        }
        return false;
    }




}
