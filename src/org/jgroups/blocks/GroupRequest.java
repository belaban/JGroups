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
 * Sends a message to all members of the group and waits for all responses (or
 * timeout). Returns a boolean value (success or failure). Results (if any) can
 * be retrieved when done.
 * <p>
 * The supported transport to send requests is currently either a
 * RequestCorrelator or a generic Transport. One of them has to be given in the
 * constructor. It will then be used to send a request. When a message is
 * received by either one, the receiveResponse() of this class has to be called
 * (this class does not actively receive requests/responses itself). Also, when
 * a view change or suspicion is received, the methods viewChange() or suspect()
 * of this class have to be called.
 * <p>
 * When started, an array of responses, correlating to the membership, is
 * created. Each response is added to the corresponding field in the array. When
 * all fields have been set, the algorithm terminates. This algorithm can
 * optionally use a suspicion service (failure detector) to detect (and exclude
 * from the membership) fauly members. If no suspicion service is available,
 * timeouts can be used instead (see <code>execute()</code>). When done, a
 * list of suspected members can be retrieved.
 * <p>
 * Because a channel might deliver requests, and responses to <em>different</em>
 * requests, the <code>GroupRequest</code> class cannot itself receive and
 * process requests/responses from the channel. A mechanism outside this class
 * has to do this; it has to determine what the responses are for the message
 * sent by the <code>execute()</code> method and call
 * <code>receiveResponse()</code> to do so.
 * <p>
 * <b>Requirements</b>: lossless delivery, e.g. acknowledgment-based message
 * confirmation.
 * 
 * @author Bela Ban
 * @version $Id: GroupRequest.java,v 1.51 2010/01/18 14:35:57 belaban Exp $
 */
public class GroupRequest extends Request {

    /** Correlates requests and responses */
    @GuardedBy("lock")
    private final Map<Address,Rsp> requests;

    @Deprecated protected final int expected_mbrs;

    protected boolean use_anycasting;

    @GuardedBy("lock")
    int num_received, num_not_received, num_suspected;



    
    /**
     * @param msg The message to be sent
     * @param corr The request correlator to be used. A request correlator sends requests tagged with a unique ID and
     *             notifies the sender when matching responses are received. The reason <code>GroupRequest</code> uses
     *             it instead of a <code>Transport</code> is that multiple requests/responses might be sent/received concurrently
     * @param targets The targets, which are supposed to receive the message. Any receiver not in this set will
     *                discard the message. Targets are always a subset of the current membership
     * @param rsp_mode How many responses are expected. Can be
     *                <ol>
     *                <li><code>GET_ALL</code>: wait for all responses from non-suspected members. A suspicion service
     *                might warn us when a member from which a response is pending has crashed, so it can be excluded
     *                from the responses. If no suspicion service is available, a timeout can be used (a value of 0
     *                means wait forever). <em>If a timeout of 0 is used, no suspicion service is available and a member
     *                from which we expect a response has crashed, this methods blocks forever !</em>.
     *                <li><code>GET_FIRST</code>: wait for the first available response.
     *                <li><code>GET_MAJORITY</code>: wait for the majority of all responses. The majority is
     *                re-computed when a member is suspected.
     *                <li><code>GET_ABS_MAJORITY</code>: wait for the majority of <em>all</em> members. This includes
     *                failed members, so it may block if no timeout is specified.
     *                <li><code>GET_N</CODE>: wait for N members. Return if n is >= membership+suspects.
     *                <li><code>GET_NONE</code>: don't wait for any response. Essentially send an asynchronous
     *                message to the group members.
     *                </ol>
     */
    public GroupRequest(Message msg, RequestCorrelator corr, Vector<Address> targets, int rsp_mode) {
        this(msg, corr, targets, rsp_mode, 0, 0);
    }


    /**
     @param timeout Time to wait for responses (ms). A value of <= 0 means wait indefinitely
     (e.g. if a suspicion service is available; timeouts are not needed).
     */
    public GroupRequest(Message m, RequestCorrelator corr, Collection<Address> mbrs, int rsp_mode,
                        long timeout, int expected_mbrs) {
        super(m, corr, null, null, rsp_mode, timeout);
        this.expected_mbrs=expected_mbrs;
        int size=mbrs.size();
        requests=new HashMap<Address,Rsp>(size);
        setTargets(mbrs);
    }

    public GroupRequest(Message m, RequestCorrelator corr, Address target, int rsp_mode,
                        long timeout, int expected_mbrs) {
        super(m, corr, null, null, rsp_mode, timeout);
        this.expected_mbrs=expected_mbrs;
        requests=new HashMap<Address,Rsp>(1);
        setTarget(target);
    }


    public GroupRequest(Message m, Transport transport, Vector<Address> members, int rsp_mode) {
        this(m,transport,members,rsp_mode,0,0);
    }


    /**
     * @param timeout Time to wait for responses (ms). A value of <= 0 means wait indefinitely
     *                       (e.g. if a suspicion service is available; timeouts are not needed).
     */
    public GroupRequest(Message m, Transport transport, Collection<Address> mbrs, int rsp_mode,
                        long timeout, int expected_mbrs) {
        super(m, null, transport, null, rsp_mode, timeout);
        this.expected_mbrs=expected_mbrs;
        int size=mbrs.size();
        requests=new HashMap<Address,Rsp>(size);
        setTargets(mbrs);
    }



    public boolean getAnycasting() {
        return use_anycasting;
    }

    public void setAnycasting(boolean anycasting) {
        this.use_anycasting=anycasting;
    }


    public void sendRequest() throws Exception {
        sendRequest(requests.keySet(), req_id, use_anycasting);
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
        Rsp rsp=requests.get(sender);
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
        Rsp rsp=requests.get(suspected_member);
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
        if(requests == null || requests.isEmpty())
                return;

        lock.lock();
        try {
            for(Map.Entry<Address,Rsp> entry: requests.entrySet()) {
                Address mbr=entry.getKey();
                if(!mbrs.contains(mbr)) {
                    Rsp rsp=entry.getValue();
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
        Collection<Rsp> rsps=requests.values();
        return new RspList(rsps);
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

        if(!requests.isEmpty()) {
            ret.append(", entries:\n");
            for(Map.Entry<Address,Rsp> entry: requests.entrySet()) {
                Address mbr=entry.getKey();
                Rsp rsp=entry.getValue();
                ret.append(mbr).append(": ").append(rsp).append("\n");
            }
        }
        return ret.toString();
    }


    /* --------------------------------- Private Methods -------------------------------------*/

    private void setTarget(Address mbr) {
        requests.put(mbr, new Rsp(mbr));
        num_not_received=1;
    }

    private void setTargets(Collection<Address> mbrs) {
        for(Address mbr: mbrs)
            requests.put(mbr, new Rsp(mbr));
        num_not_received=requests.size();
    }

    private static int determineMajority(int i) {
        return i < 2? i : (i / 2) + 1;
    }


    private void sendRequest(final Collection<Address> targetMembers, long requestId,boolean use_anycasting) throws Exception {
        try {
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

        final int num_total=requests.size();

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
