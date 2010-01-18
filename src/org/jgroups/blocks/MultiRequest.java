package org.jgroups.blocks;


import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.Transport;
import org.jgroups.View;
import org.jgroups.annotations.GuardedBy;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


/**
 * Sends a request to multiple destinations. Alternative implementation when we have few targets: between UnicastRequest
 * with 1 target and GroupRequest with many destination members. Performance is about the same as for GroupRequest, but
 * this class should use less memory as it doesn't create hashmaps. Don't use with many targets as we have to do
 * a linear search through an array of targets to match a response to a request.<p/>
 * MultiRequest is currently not used
 *
 * @author Bela Ban
 * @version $Id: MultiRequest.java,v 1.1 2010/01/18 14:29:50 belaban Exp $
 * @since 2.9
 */
public class MultiRequest extends Request {
    @GuardedBy("lock")
    private final Rsp[] responses;

    protected final int expected_mbrs;

    protected boolean use_anycasting;

    @GuardedBy("lock")
    int num_received, num_not_received, num_suspected;


    /**
     * @param m
     *                The message to be sent
     * @param corr
     *                The request correlator to be used. A request correlator
     *                sends requests tagged with a unique ID and notifies the
     *                sender when matching responses are received. The reason
     *                <code>GroupRequest</code> uses it instead of a
     *                <code>Transport</code> is that multiple
     *                requests/responses might be sent/received concurrently.
     * @param members
     *                The initial membership. This value reflects the membership
     *                to which the request is sent (and from which potential
     *                responses are expected). Is reset by reset().
     * @param rsp_mode
     *                How many responses are expected. Can be
     *                <ol>
     *                <li><code>GET_ALL</code>: wait for all responses from
     *                non-suspected members. A suspicion service might warn us
     *                when a member from which a response is outstanding has
     *                crashed, so it can be excluded from the responses. If no
     *                suspicion service is available, a timeout can be used (a
     *                value of 0 means wait forever). <em>If a timeout of
     *                0 is used, no suspicion service is available and a member from which we
     *                expect a response has crashed, this methods blocks forever !</em>.
     *                <li><code>GET_FIRST</code>: wait for the first
     *                available response.
     *                <li><code>GET_MAJORITY</code>: wait for the majority
     *                of all responses. The majority is re-computed when a
     *                member is suspected.
     *                <li><code>GET_ABS_MAJORITY</code>: wait for the
     *                majority of <em>all</em> members. This includes failed
     *                members, so it may block if no timeout is specified.
     *                <li><code>GET_N</CODE>: wait for N members. Return if
     *                n is >= membership+suspects.
     *                <li><code>GET_NONE</code>: don't wait for any
     *                response. Essentially send an asynchronous message to the
     *                group members.
     *                </ol>
     */
    public MultiRequest(Message m, RequestCorrelator corr, Vector<Address> members, int rsp_mode) {
        this(m,corr,members,rsp_mode,0,0);
    }


    /**
     @param timeout Time to wait for responses (ms). A value of <= 0 means wait indefinitely
     (e.g. if a suspicion service is available; timeouts are not needed).
     */
    public MultiRequest(Message m, RequestCorrelator corr, Collection<Address> mbrs, int rsp_mode,
                        long timeout, int expected_mbrs) {
        super(m, corr, null, null, rsp_mode, timeout);
        this.expected_mbrs=expected_mbrs;
        responses=new Rsp[mbrs.size()];
        setTargets(mbrs);
    }

    public MultiRequest(Message m, RequestCorrelator corr, Address target, int rsp_mode,
                        long timeout, int expected_mbrs) {
        super(m, corr, null, null, rsp_mode, timeout);
        this.expected_mbrs=expected_mbrs;
        responses=new Rsp[1];
        setTarget(target);
    }


    public MultiRequest(Message m, Transport transport, Vector<Address> members, int rsp_mode) {
        this(m,transport,members,rsp_mode,0,0);
    }


    /**
     * @param timeout Time to wait for responses (ms). A value of <= 0 means wait indefinitely
     *                       (e.g. if a suspicion service is available; timeouts are not needed).
     */
    public MultiRequest(Message m, Transport transport, Collection<Address> mbrs, int rsp_mode,
                        long timeout, int expected_mbrs) {
        super(m, null, transport, null, rsp_mode, timeout);
        this.expected_mbrs=expected_mbrs;
        responses=new Rsp[1];
        setTargets(mbrs);
    }

    void setTarget(Address mbr) {
        responses[0]=new Rsp(mbr);
        num_not_received++;
    }

    void setTargets(Collection<Address> mbrs) {
        int index=0;
        for(Address mbr: mbrs) {
            responses[index++]=new Rsp(mbr);
            num_not_received++;
        }
    }

    public boolean getAnycasting() {
        return use_anycasting;
    }

    public void setAnycasting(boolean anycasting) {
        this.use_anycasting=anycasting;
    }



    public void sendRequest() throws Exception {
        List<Address> targets=null;
        targets=new ArrayList<Address>(responses.length);
        for(Rsp rsp: responses)
            targets.add(rsp.getSender());

        sendRequest(targets, req_id, use_anycasting);
    }

    Rsp findResponse(Address target) {
        for(Rsp rsp: responses) {
            if(rsp != null && target.equals(rsp.getSender()))
                return rsp;
        }
        return null;
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
        Rsp rsp=findResponse(sender);
        if(rsp == null)
            return;

        boolean responseReceived=false;
        if(!rsp.wasReceived()) {
            if((responseReceived=(rsp_filter == null) || rsp_filter.isAcceptable(response_value, sender)))
                rsp.setValue(response_value);
            rsp.setReceived(responseReceived);
        }

        lock.lock();
        try {
            if(responseReceived)
                num_received++;
            done=rsp_filter == null? responsesComplete() : !rsp_filter.needMoreResponses();
            if(responseReceived || done)
                completed.signalAll(); // wakes up execute()
            if(done && corr != null)
                corr.done(req_id);
        }
        finally {
            lock.unlock();
        }
        if(responseReceived || done)
            checkCompletion(this);
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

        boolean changed=false;
        Rsp rsp=findResponse(suspected_member);
        if(rsp !=  null) {
            if(rsp.setSuspected(true)) {
                rsp.setValue(null);
                changed=true;
                lock.lock();
                try {
                    num_suspected++;
                    completed.signalAll();
                }
                finally {
                    lock.unlock();
                }
            }
        }

        if(changed)
            checkCompletion(this);
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
        Vector<Address> mbrs=new_view != null? new_view.getMembers() : null;
        if(mbrs == null)
            return;

        boolean changed=false;
        if(responses == null || responses.length == 0)
                return;

        lock.lock();
        try {
            for(Rsp rsp: responses) {
                Address mbr=rsp.getSender();
                if(!mbrs.contains(mbr)) {
                    rsp.setValue(null);
                    if(rsp.setSuspected(true)) {
                        num_suspected++;
                        changed=true;
                    }
                }
            }
            if(changed)
                completed.signalAll();
        }
        finally {
            lock.unlock();
        }
        if(changed)
            checkCompletion(this);
    }


    /* -------------------- End of Interface RspCollector ----------------------------------- */



    /** Returns the results as a RspList */
    public RspList getResults() {
        RspList list=new RspList();
        for(Rsp rsp: responses)
            list.put(rsp.getSender(), rsp);
        return list;
    }



    public RspList get() throws InterruptedException, ExecutionException {
        lock.lock();
        try {
            waitForResults(0);
        }
        finally {
            lock.unlock();
        }
        return getResults();
    }

    public RspList get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
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

        lock.lock();
        try {
            if(!(responses.length == 0)) {
                ret.append(", entries:\n");
                for(Rsp rsp: responses) {
                    Address mbr=rsp.getSender();
                    ret.append(mbr).append(": ").append(rsp).append("\n");
                }
            }
        }
        finally {
            lock.unlock();
        }
        return ret.toString();
    }




    /* --------------------------------- Private Methods -------------------------------------*/

    private static int determineMajority(int i) {
        return i < 2? i : (i / 2) + 1;
    }




    private void sendRequest(List<Address> targetMembers, long requestId,boolean use_anycasting) throws Exception {
        try {
            if(log.isTraceEnabled()) log.trace(new StringBuilder("sending request (id=").append(req_id).append(')'));
            if(corr != null) {
                corr.sendRequest(requestId, targetMembers, request_msg, rsp_mode == GET_NONE? null : this, use_anycasting);
            }
            else {
                if(use_anycasting) {
                    for(Address mbr: targetMembers) {
                        Message copy=request_msg.copy(true);
                        copy.setDest(mbr);
                        transport.send(copy);
                    }
                }
                else {
                    transport.send(request_msg);
                }
            }
        }
        catch(Exception ex) {
            if(corr != null)
                corr.done(requestId);
            throw ex;
        }
    }


    @GuardedBy("lock")
    protected boolean responsesComplete() {
        if(done)
            return true;

        final int num_total=responses.length;

        switch(rsp_mode) {
            case GET_FIRST:
                if(num_received > 0)
                    return true;
                if(num_suspected >= num_total)
                // e.g. 2 members, and both suspected
                    return true;
                break;
            case GET_ALL:
                return num_received + num_suspected >= num_total;
            case GET_MAJORITY:
                int majority=determineMajority(num_total);
                if(num_received + num_suspected >= majority)
                    return true;
                break;
            case GET_ABS_MAJORITY:
                majority=determineMajority(num_total);
                if(num_received >= majority)
                    return true;
                break;
            case GET_N:
                if(expected_mbrs >= num_total) {
                    return responsesComplete();
                }
                return num_received >= expected_mbrs || num_received + num_not_received < expected_mbrs && num_received + num_suspected >= expected_mbrs;
            case GET_NONE:
                return true;
            default :
                if(log.isErrorEnabled()) log.error("rsp_mode " + rsp_mode + " unknown !");
                break;
        }
        return false;
    }



}