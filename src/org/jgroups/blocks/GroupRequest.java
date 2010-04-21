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
 * @version $Id: GroupRequest.java,v 1.53 2010/04/21 10:55:46 belaban Exp $
 */
public class GroupRequest extends Request {

    /** Correlates requests and responses */
    @GuardedBy("lock")
    private final Map<Address,Rsp> requests;

    @GuardedBy("lock")
    int num_received, num_not_received, num_suspected;



    
     /**
     * @param msg The message to be sent
     * @param corr The request correlator to be used. A request correlator sends requests tagged with a unique ID and
     *             notifies the sender when matching responses are received. The reason <code>GroupRequest</code> uses
     *             it instead of a <code>Transport</code> is that multiple requests/responses might be sent/received concurrently
     * @param targets The targets, which are supposed to receive the message. Any receiver not in this set will
     *                discard the message. Targets are always a subset of the current membership
     * @param options The request options to be used for this call
     */
    public GroupRequest(Message msg, RequestCorrelator corr, Collection<Address> targets, RequestOptions options) {
        super(msg, corr, null, options);
        int size=targets.size();
        requests=new HashMap<Address,Rsp>(size);
        setTargets(targets);
    }

    public GroupRequest(Message m, RequestCorrelator corr, Address target, RequestOptions options) {
        super(m, corr, null, options);
        requests=new HashMap<Address,Rsp>(1);
        setTarget(target);
    }


    public GroupRequest(Message m, Transport transport, Collection<Address> mbrs, RequestOptions options) {
        super(m, null, transport, options);
        int size=mbrs.size();
        requests=new HashMap<Address,Rsp>(size);
        setTargets(mbrs);
    }



    public boolean getAnycasting() {
        return options.getAnycasting();
    }

    public void setAnycasting(boolean anycasting) {
        options.setAnycasting(anycasting);
    }


    public void sendRequest() throws Exception {
        sendRequest(requests.keySet(), req_id);
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

        RspFilter rsp_filter=options.getRspFilter();
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


    private void sendRequest(final Collection<Address> targetMembers, long requestId) throws Exception {
        try {
            if(corr != null) {
                corr.sendRequest(requestId, targetMembers, request_msg, options.getMode() == GET_NONE? null : this, options);
            }
            else {
                if(options.getAnycasting()) {                                                          
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

        switch(options.getMode()) {
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
                return true;
            case GET_NONE:
                return true;
            default :
                if(log.isErrorEnabled()) log.error("rsp_mode " + options.getMode() + " unknown !");
                break;
        }
        return false;
    }




}
