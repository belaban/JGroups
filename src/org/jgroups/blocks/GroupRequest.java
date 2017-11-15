package org.jgroups.blocks;


import org.jgroups.Address;
import org.jgroups.View;
import org.jgroups.annotations.GuardedBy;
import org.jgroups.protocols.relay.SiteAddress;
import org.jgroups.util.ByteArray;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


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
    protected final Lock        lock=new ReentrantLock();

    /** Correlates requests and responses */
    @GuardedBy("lock")
    protected final RspList<T>  rsps;

    @GuardedBy("lock")
    protected int               num_valid;       // valid responses (values or exceptions that passed the response filter)

    @GuardedBy("lock")
    protected int               num_received;    // number of responses (values, exceptions or suspicions)



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
        rsps=new RspList<>(targets.size());
        targets.forEach(target -> rsps.put(target, new Rsp<>()));
    }


    public void sendRequest(ByteArray data) throws Exception {
        sendRequest(data, rsps.keySet());
    }

    /* ---------------------- Interface RspCollector -------------------------- */
    /**
     * <b>Callback</b> (called by RequestCorrelator or Transport).
     * Adds a response to the response table. When all responses have been received, {@code execute()} returns.
     */
    @SuppressWarnings("unchecked")
    public void receiveResponse(Object response_value, Address sender, boolean is_exception) {
        Rsp<T> rsp;
        if(isDone() || (rsp=rsps.get(sender)) == null)
            return;

        RspFilter rsp_filter=options.rspFilter();
        lock.lock();
        try {
            if(!rsp.wasReceived()) {
                if(!(rsp.wasSuspected() || rsp.wasUnreachable()))
                    num_received++;
                if(rsp_filter == null || rsp_filter.isAcceptable(response_value, sender)) {
                    if(is_exception && response_value instanceof Throwable)
                        rsp.setException((Throwable)response_value);
                    else
                        rsp.setValue((T)response_value);
                    num_valid++;
                }
            }

            if(responsesComplete() || (rsp_filter != null && !rsp_filter.needMoreResponses())) {
                complete(this.rsps);
                corrDone();
            }
        }
        finally {
            lock.unlock();
        }
    }



    public void siteUnreachable(String site) {
        lock.lock();
        try {
            for(Map.Entry<Address,Rsp<T>> entry : rsps.entrySet()) {
                Address member=entry.getKey();
                if(!(member instanceof SiteAddress))
                    continue;
                SiteAddress addr=(SiteAddress)member;
                if(addr.getSite().equals(site)) {
                    Rsp<T> rsp=entry.getValue();
                    if(rsp != null && rsp.setUnreachable()) {
                        lock.lock();
                        try {
                            if(!(rsp.wasReceived() || rsp.wasSuspected()))
                                num_received++;
                        }
                        finally {
                            lock.unlock();
                        }
                    }
                }
            }
            if(responsesComplete()) {
                complete(this.rsps);
                corrDone();
            }
        }
        finally {
            lock.unlock();
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
    public void viewChange(View view) {
        if(view == null || rsps == null || rsps.isEmpty())
            return;

        boolean changed=false;
        lock.lock();
        try {
            for(Map.Entry<Address,Rsp<T>> entry: rsps.entrySet()) {
                Address mbr=entry.getKey();
                // SiteAddresses are not checked as they might be in a different cluster
                if(!(mbr instanceof SiteAddress) && !view.containsMember(mbr)) {
                    Rsp<T> rsp=entry.getValue();
                    if(rsp.setSuspected()) {
                        if(!(rsp.wasReceived() || rsp.wasUnreachable()))
                            num_received++;
                        changed=true;
                    }
                }
            }
            if(changed && responsesComplete()) {
                complete(this.rsps);
                corrDone();
            }
        }
        finally {
            lock.unlock();
        }
    }

    /** Marks all responses with an exception (unless a response was already marked as done) */
    public void transportClosed() {
        boolean changed=false;

        lock.lock();
        try {
            for(Map.Entry<Address, Rsp<T>> entry: rsps.entrySet()) {
                Rsp<T> rsp=entry.getValue();
                if(rsp != null && !(rsp.wasReceived() || rsp.wasSuspected() || rsp.wasUnreachable())) {
                    rsp.setException(new IllegalStateException("transport was closed"));
                    num_received++;
                    changed=true;
                }
            }
            if(changed && responsesComplete()) {
                complete(this.rsps);
                corrDone();
            }
        }
        finally {
            lock.unlock();
        }
    }

    /* -------------------- End of Interface RspCollector ----------------------------------- */



    public boolean getResponsesComplete() {
        lock.lock();
        try {
            return responsesComplete();
        }
        finally {
            lock.unlock();
        }
    }

    public RspList<T> get() throws InterruptedException, ExecutionException {
        return waitForCompletion();
    }

    public RspList<T> get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return waitForCompletion(timeout, unit);
    }

    public RspList<T> join() {
        return doAndComplete(super::join);
    }

    public RspList<T> getNow(RspList<T> valueIfAbsent) {
        return doAndComplete(() -> super.getNow(valueIfAbsent));
    }

    public RspList<T> waitForCompletion(long timeout, TimeUnit unit) {
        return doAndComplete(() -> super.get(timeout, unit));
    }

    public RspList<T> waitForCompletion() throws ExecutionException, InterruptedException {
        return doAndComplete(super::get);
    }

    public String toString() {
        StringBuilder ret=new StringBuilder(128);
        ret.append(super.toString());

        if(!rsps.isEmpty()) {
            ret.append(", entries:\n");
            for(Map.Entry<Address,Rsp<T>> entry: rsps.entrySet()) {
                Address mbr=entry.getKey();
                Rsp<T> rsp=entry.getValue();
                ret.append(mbr).append(": ").append(rsp).append("\n");
            }
        }
        return ret.toString();
    }

    protected RspList<T> doAndComplete(Callable<RspList<T>> supplier) {
        try {
            return supplier.call();
        }
        catch(Throwable t) {
            complete(this.rsps);
            return this.rsps;
        }
        finally {
            corrDone();
        }
    }

    protected void sendRequest(ByteArray data, final Collection<Address> targetMembers) throws Exception {
        try {
            corr.sendRequest(targetMembers, data, options.mode() == ResponseMode.GET_NONE? null : this, options);
        }
        catch(Exception ex) {
            corrDone();
            throw ex;
        }
    }


    @GuardedBy("lock")
    protected boolean responsesComplete() {
        if(isDone())
            return true;
        final int num_total=rsps.size();
        switch(options.mode()) {
            case GET_FIRST: return num_valid >= 1 || num_received >= num_total;
            case GET_ALL:   return num_valid >= num_total || num_received >= num_total;
            case GET_NONE:  return true;
        }
        return false;
    }




}
