// $Id: CoordGmsImpl.java,v 1.82.2.20 2009/09/08 12:25:44 belaban Exp $

package org.jgroups.protocols.pbcast;


import org.jgroups.*;
import org.jgroups.annotations.GuardedBy;
import org.jgroups.util.Digest;
import org.jgroups.util.MutableDigest;

import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Coordinator role of the Group MemberShip (GMS) protocol. Accepts JOIN and LEAVE requests and emits view changes
 * accordingly.
 * @author Bela Ban
 */
public class CoordGmsImpl extends GmsImpl {
    private volatile boolean        merging=false;
    private final MergeTask         merge_task=new MergeTask();
    private final Vector<MergeData> merge_rsps=new Vector<MergeData>(11);
    // for MERGE_REQ/MERGE_RSP correlation, contains MergeData elements
    private ViewId                  merge_id=null;

    @GuardedBy("merge_canceller_lock")
    private Future<?>                  merge_canceller_future=null;

    private final Lock              merge_canceller_lock=new ReentrantLock();

    /** the max time in ms to suspend message garbage collection */
    private final Long              MAX_SUSPEND_TIMEOUT=new Long(30000);


    public CoordGmsImpl(GMS g) {
        super(g);
    }


    private void setMergeId(ViewId merge_id) {
        this.merge_id=merge_id;
        if(this.merge_id != null) {
            stopMergeCanceller();
            startMergeCanceller();
        }
        else { // merge completed
            stopMergeCanceller();
        }
    }

    private void startMergeCanceller() {
        merge_canceller_lock.lock();
        try {
            if(merge_canceller_future == null || merge_canceller_future.isDone()) {
                MergeCanceller task=new MergeCanceller(this.merge_id, gms.merge_timeout);
                merge_canceller_future=gms.timer.schedule(task, gms.merge_timeout, TimeUnit.MILLISECONDS);
            }
        }
        finally {
            merge_canceller_lock.unlock();
        }
    }

    private void stopMergeCanceller() {
        merge_canceller_lock.lock();
        try {
            if(merge_canceller_future != null) {
                merge_canceller_future.cancel(true);
                merge_canceller_future=null;
            }
        }
        finally {
            merge_canceller_lock.unlock();
        }
    }

    public void init() throws Exception {
        super.init();
        cancelMerge();
    }

    public void join(Address mbr) {
        wrongMethod("join");
    }
    
    public void joinWithStateTransfer(Address mbr) {
        wrongMethod("join");
    }

    /** The coordinator itself wants to leave the group */
    public void leave(Address mbr) {
        if(mbr == null) {
            if(log.isErrorEnabled()) log.error("member's address is null !");
            return;
        }
        if(mbr.equals(gms.local_addr))
            leaving=true;
        gms.getViewHandler().add(new Request(Request.LEAVE, mbr, false, null));
        gms.getViewHandler().stop(true); // wait until all requests have been processed, then close the queue and leave
        gms.getViewHandler().waitUntilCompleted(gms.leave_timeout);
    }

    public void handleJoinResponse(JoinRsp join_rsp) {
    }

    public void handleLeaveResponse() {
    }

    public void suspect(Address mbr) {
        if(mbr.equals(gms.local_addr)) {
            if(log.isWarnEnabled()) log.warn("I am the coord and I'm suspected -- will probably leave shortly");
            return;
        }        
        Collection<Request> suspected=new LinkedHashSet<Request>(1);
        suspected.add(new Request(Request.SUSPECT,mbr,true,null));
        handleMembershipChange(suspected);
    }

    public void unsuspect(Address mbr) {

    }

    /**
     * Invoked upon receiving a MERGE event from the MERGE layer. Starts the merge protocol.
     * See description of protocol in DESIGN.
     * @param other_coords A list of coordinators (including myself) found by MERGE protocol
     */
    public void merge(Vector<Address> other_coords) {
        Membership tmp;

        if(merging) {
            if(log.isWarnEnabled()) log.warn("merge already in progress, discarded MERGE event (I am " + gms.local_addr + ")");
            return;
        }       
        if(other_coords == null) {
            if(log.isWarnEnabled()) log.warn("list of other coordinators is null. Will not start merge.");
            return;
        }

        if(other_coords.size() <= 1) {
            if(log.isErrorEnabled())
                log.error("number of coordinators found is " + other_coords.size() + "; will not perform merge");
            return;
        }

        /* Establish deterministic order, so that coords can elect leader */
        tmp=new Membership(other_coords);
        tmp.sort();
        Address  merge_leader=tmp.elementAt(0);
        if(log.isDebugEnabled()) log.debug("Determining merge leader from coordinators: " + tmp);
        if(merge_leader.equals(gms.local_addr) || gms.merge_leader) {
            if(log.isDebugEnabled())
                log.debug("I (" + gms.local_addr + ") will be the leader. Starting the merge task for " + other_coords);
            startMergeTask(other_coords);
        }
        else {
            if(log.isDebugEnabled()) log.debug("I (" + gms.local_addr + ") am not the merge leader, " +
                    "waiting for merge leader (" + merge_leader + ") to initiate merge");
        }
    }

    /**
     * Get the view and digest and send back both (MergeData) in the form of a MERGE_RSP to the sender.
     * If a merge is already in progress, send back a MergeData with the merge_rejected field set to true.
     */
    public void handleMergeRequest(Address sender, ViewId merge_id) {
        Digest digest;
        View view;

        if(sender == null) {
            if(log.isErrorEnabled()) log.error("sender == null; cannot send back a response");
            return;
        }
        if(merging) {
            if(log.isErrorEnabled()) log.error("For merge participant " + gms.local_addr +" merge is already in progress");
            sendMergeRejectedResponse(sender, merge_id);
            return;
        }
        merging=true;

        if(log.isDebugEnabled()) log.debug("Suspending view handler at " + gms.local_addr);
        /* Clears the view handler queue and discards all JOIN/LEAVE/MERGE requests until after the MERGE  */
        gms.getViewHandler().suspend(merge_id);

        setMergeId(merge_id);
        if(log.isDebugEnabled()) log.debug(gms.local_addr + " got merge request from " + sender + ", merge_id=" + merge_id);
        view=new View(gms.view_id.copy(), gms.members.getMembers());
        
        //[JGRP-524] - FLUSH and merge: flush doesn't wrap entire merge process
        //[JGRP-770] - Concurrent startup of many channels doesn't stabilize
        //[JGRP-700] - FLUSH: flushing should span merge
        
        /*if flush is in stack, let this coordinator flush its cluster island */
        boolean suceesfulFlush = false;
        try {
            suceesfulFlush = gms.startFlush(view);
        } finally {
            if (suceesfulFlush) {
                digest = gms.getDigest();
                sendMergeResponse(sender, view, digest);
                if (log.isDebugEnabled())
                    log.debug(gms.local_addr + " responded to " + sender+ ", merge_id=" + merge_id);
            } else {
                sendMergeRejectedResponse(sender, merge_id);
                gms.getViewHandler().resume(merge_id);
                merging = false;
                if (log.isWarnEnabled())
                    log.warn("Since flush failed at " + gms.local_addr + " rejected merge to "
                                    + sender + ", merge_id=" + merge_id);
            }
        }
    }

    public void handleMergeResponse(MergeData data, ViewId merge_id) {                     
    	if(merge_id == null || this.merge_id == null) {
            if(log.isErrorEnabled())
                log.error("merge_id ("
                    + merge_id
                    + ") or this.merge_id ("
                    + this.merge_id
                    + ") is null (sender="
                    + data.getSender()
                    + ").");
            return;
        }

        if(!this.merge_id.equals(merge_id)) {
            if(log.isErrorEnabled()) log.error("this.merge_id ("
                    + this.merge_id
                    + ") is different from merge_id ("
                    + merge_id
                    + ')');
            return;
        }

        synchronized(merge_rsps) {
            if(!merge_rsps.contains(data)) {
                merge_rsps.addElement(data);
                merge_rsps.notifyAll();
            }
        }
    }

    /**
     * If merge_id is not equal to this.merge_id then discard.
     * Else cast the view/digest to all members of this group.
     */
    public void handleMergeView(final MergeData data,final ViewId merge_id) {
        if(merge_id == null
                || this.merge_id == null
                || !this.merge_id.equals(merge_id)) {
            if(log.isErrorEnabled()) log.error("merge_ids don't match (or are null); merge view discarded");
            return;
        }       

        // only send to our *current* members, if we have A and B being merged (we are B), then we would *not*
        // receive a VIEW_ACK from A because A doesn't see us in the pre-merge view yet and discards the view

        //[JGRP-700] - FLUSH: flushing should span merge        

        //we have to send new view only to current members and we should not wait 
        //for view acks from newly merged mebers
        List<Address> newViewMembers=new Vector<Address>(data.view.getMembers());
        newViewMembers.removeAll(gms.members.getMembers());
        
        
        gms.castViewChangeWithDest(data.view, data.digest, null, newViewMembers);
        /*
         * if we have flush in stack send ack back to merge coordinator                 
         * */
        if(gms.flushProtocolInStack) {
            Message ack=new Message(data.getSender(), null, null);
            ack.setFlag(Message.OOB);
            GMS.GmsHeader ack_hdr=new GMS.GmsHeader(GMS.GmsHeader.INSTALL_MERGE_VIEW_OK);
            ack.putHeader(gms.getName(), ack_hdr);
            gms.getDownProtocol().down(new Event(Event.MSG, ack));
        }
        merging=false;
        gms.getViewHandler().resume(merge_id);
    }

    public void handleMergeCancelled(ViewId merge_id) {
        gms.stopFlush();
        if(merge_id != null && this.merge_id != null && this.merge_id.equals(merge_id)) {
            if(log.isDebugEnabled())
                log.debug("merge was cancelled at merge participant " + gms.local_addr+ " (merge_id="+ merge_id+ ")");

            setMergeId(null);
            merging=false;
            gms.getViewHandler().resume(merge_id);
        } 
        else {
            if(log.isWarnEnabled())
                log.warn("merge was supposed to be cancelled at merge participant " + gms.local_addr
                         + " (merge_id="
                         + merge_id
                         + "), but it is not since merge ids do not match");
        }
    }


    private void cancelMerge() {
        Object tmp=merge_id;
        if(merge_id != null && log.isDebugEnabled()) log.debug("cancelling merge (merge_id=" + merge_id + ')');
        setMergeId(null);        
        stopMergeTask();
        merging=false;
        synchronized(merge_rsps) {
            merge_rsps.clear();
        }
        gms.getViewHandler().resume(tmp);
    }


    public void handleMembershipChange(Collection<Request> requests) {
        boolean joinAndStateTransferInitiated=false;
        Collection<Address> new_mbrs=new LinkedHashSet<Address>(requests.size());
        Collection<Address> suspected_mbrs=new LinkedHashSet<Address>(requests.size());
        Collection<Address> leaving_mbrs=new LinkedHashSet<Address>(requests.size());

        for(Request req: requests) {
            switch(req.type) {
                case Request.JOIN:
                    new_mbrs.add(req.mbr);
                    break;
                case Request.JOIN_WITH_STATE_TRANSFER:
                    new_mbrs.add(req.mbr);
                    joinAndStateTransferInitiated=true;
                    break;
                case Request.LEAVE:
                    if(req.suspected)
                        suspected_mbrs.add(req.mbr);
                    else
                        leaving_mbrs.add(req.mbr);
                    break;
                case Request.SUSPECT:
                    suspected_mbrs.add(req.mbr);
                    break;
            }
        }

        new_mbrs.remove(gms.local_addr); // remove myself - cannot join myself (already joined)        

        if(gms.view_id == null) {
            // we're probably not the coord anymore (we just left ourselves), let someone else do it
            // (client will retry when it doesn't get a response)
            if(log.isDebugEnabled())
                log.debug("gms.view_id is null, I'm not the coordinator anymore (leaving=" + leaving +
                        "); the new coordinator will handle the leave request");
            return;
        }

        Vector<Address> current_members=gms.members.getMembers();
        leaving_mbrs.retainAll(current_members); // remove all elements of leaving_mbrs which are not current members
        if(suspected_mbrs.remove(gms.local_addr)) {
            if(log.isWarnEnabled()) log.warn("I am the coord and I'm being suspected -- will probably leave shortly");
        }
        suspected_mbrs.retainAll(current_members); // remove all elements of suspected_mbrs which are not current members

        // for the members that have already joined, return the current digest and membership
        for(Iterator<Address> it=new_mbrs.iterator(); it.hasNext();) {
            Address mbr=it.next();
            if(gms.members.contains(mbr)) { // already joined: return current digest and membership
                JoinRsp join_rsp;
                if(gms.reject_join_from_existing_member) {
                    join_rsp=new JoinRsp("member " + mbr + " is already part of the group, JOIN request is rejected");
                }
                else {
                    if(log.isWarnEnabled())
                        log.warn(mbr + " already present; returning existing view " + gms.view);
                    join_rsp=new JoinRsp(new View(gms.view_id, gms.members.getMembers()), gms.getDigest());
                }
                gms.sendJoinResponse(join_rsp, mbr);
                it.remove();
            }
        }

        if(new_mbrs.isEmpty() && leaving_mbrs.isEmpty() && suspected_mbrs.isEmpty()) {
            if(log.isTraceEnabled())
                log.trace("found no members to add or remove, will not create new view");
            return;
        }
        
        View new_view=gms.getNextView(new_mbrs, leaving_mbrs, suspected_mbrs);
        gms.up(new Event(Event.PREPARE_VIEW,new_view));
        gms.down(new Event(Event.PREPARE_VIEW,new_view));
        
        if(log.isDebugEnabled())
            log.debug("new=" + new_mbrs + ", suspected=" + suspected_mbrs + ", leaving=" + leaving_mbrs +
                    ", new view: " + new_view);
             
        JoinRsp join_rsp=null;
        boolean hasJoiningMembers=!new_mbrs.isEmpty();
        try {            
            boolean successfulFlush = gms.startFlush(new_view);
            if(!successfulFlush && hasJoiningMembers){
                //see http://jira.jboss.org/jira/browse/JGRP-759
                //We should NOT send back a join response if the flush fails. 
                //The joiner should block until the previous FLUSH completed
                //we still have to send potential leave responses
                sendLeaveResponses(leaving_mbrs); 
                //but let the joining client timeout and send another join request
                return;
            }
            
            // we cannot garbage collect during joining a new member *if* we're the only member
            // Example: {A}, B joins, after returning JoinRsp to B, A garbage collects messages higher than those
            // in the digest returned to the client, so the client will *not* be able to ask for retransmission
            // of those messages if he misses them            
            if(hasJoiningMembers) {
                gms.getDownProtocol().down(new Event(Event.SUSPEND_STABLE, MAX_SUSPEND_TIMEOUT));
                Digest tmp=gms.getDigest(); // get existing digest
                MutableDigest join_digest=null;
                if(tmp == null){
                    log.error("received null digest from GET_DIGEST: will cause JOIN to fail");
                }
                else {
                    // create a new digest, which contains the new member
                    join_digest=new MutableDigest(tmp.size() + new_mbrs.size());
                    join_digest.add(tmp); // add the existing digest to the new one
                    for(Address member:new_mbrs)
                        join_digest.add(member, 0, 0); // ... and add the new members. their first seqno will be 1
                }
                join_rsp=new JoinRsp(new_view, join_digest != null? join_digest.copy() : null);
            }

            sendLeaveResponses(leaving_mbrs); // no-op if no leaving members                            
            gms.castViewChangeWithDest(new_view, null,join_rsp,new_mbrs);                      
        }
        finally {
            if(hasJoiningMembers)
                gms.getDownProtocol().down(new Event(Event.RESUME_STABLE));
            if(!joinAndStateTransferInitiated)
                gms.stopFlush();
            if(leaving) {
                gms.initState(); // in case connect() is called again
            }
        }
    }


    /**
     * Called by the GMS when a VIEW is received.
     * @param new_view The view to be installed
     * @param digest   If view is a MergeView, digest contains the seqno digest of all members and has to
     *                 be set by GMS
     */
    public void handleViewChange(View new_view, Digest digest) {
        Vector<Address> mbrs=new_view.getMembers();
        if(log.isDebugEnabled()) {
            if(digest != null)
                log.debug("view=" + new_view + ", digest=" + digest);
            else
                log.debug("view=" + new_view);
        }

        if(leaving && !mbrs.contains(gms.local_addr))
            return;
        gms.installView(new_view, digest);
    }

    public void handleExit() {
        cancelMerge();
    }

    public void stop() {
        super.stop(); // sets leaving=false
        stopMergeTask();
    }

    /* ------------------------------------------ Private methods ----------------------------------------- */

    void startMergeTask(Vector<Address> coords) {
        synchronized(merge_task) {
            merge_task.start(coords);
        }
    }

    void stopMergeTask() {
        synchronized(merge_task) {
            merge_task.stop();
        }
    }  

    private void sendLeaveResponses(Collection<Address> leaving_members) {
        for(Address address:leaving_members){
            Message msg=new Message(address, null, null); // send an ack to the leaving member
            msg.setFlag(Message.OOB);
            GMS.GmsHeader hdr=new GMS.GmsHeader(GMS.GmsHeader.LEAVE_RSP);
            msg.putHeader(gms.getName(), hdr);
            gms.getDownProtocol().down(new Event(Event.MSG, msg));
        }       
    }


    /**
     * Sends a MERGE_REQ to all coords and populates a list of MergeData (in merge_rsps). Returns after coords.size()
     * response have been received, or timeout msecs have elapsed (whichever is first).<p>
     * If a subgroup coordinator rejects the MERGE_REQ (e.g. because of participation in a different merge),
     * <em>that member will be removed from coords !</em>
     * @param coords A list of Addresses of subgroup coordinators (inluding myself)
     * @param timeout Max number of msecs to wait for the merge responses from the subgroup coords
     */
    private boolean getMergeDataFromSubgroupCoordinators(final Vector<Address> coords, long timeout) {        
           
    	boolean gotAllResponses = false;
        long start=System.currentTimeMillis();        
        synchronized(merge_rsps) {
            merge_rsps.removeAllElements();
            if(log.isDebugEnabled())
                log.debug("Merge leader " + gms.local_addr + " sending MERGE_REQ to " + coords);            
            
            for(Address coord:coords) {               
                // this allows UNICAST to remove coord from previous_members in case of a merge

                Message msg=new Message(coord, null, null);
                msg.setFlag(Message.OOB);
                GMS.GmsHeader hdr=new GMS.GmsHeader(GMS.GmsHeader.MERGE_REQ);
                hdr.mbr=gms.local_addr;
                hdr.merge_id=merge_id;
                msg.putHeader(gms.getName(), hdr);
                gms.getDownProtocol().down(new Event(Event.MSG, msg));
                
                if(log.isDebugEnabled())
                    log.debug("Merge leader " + gms.local_addr + " sent MERGE_REQ to " + coord);
            }

            // wait until num_rsps_expected >= num_rsps or timeout elapsed
            int num_rsps_expected=coords.size();
            long curr_time=System.currentTimeMillis();
            long end_time=curr_time + timeout;
            while(end_time > curr_time && !gotAllResponses) {
                long time_to_wait=end_time - curr_time;
                if(log.isDebugEnabled()) log.debug("Merge leader " + gms.local_addr +" waiting " + time_to_wait + " msecs for merge responses");
                if(merge_rsps.size() < num_rsps_expected) {
                    try {
                        merge_rsps.wait(500);
                    }
                    catch(Exception ex) {
                    }
                }
                if(log.isDebugEnabled())
                    log.debug("Merge leader " + gms.local_addr +" expects " + num_rsps_expected + " responses, so far got " + merge_rsps.size() + " responses");

                gotAllResponses = merge_rsps.size() >= num_rsps_expected;               
                curr_time=System.currentTimeMillis();
            }
            long stop=System.currentTimeMillis();
            if(log.isDebugEnabled())
                log.debug("Merge leader " + gms.local_addr + " collected " + merge_rsps.size() + " merge response(s) in " + (stop-start) + " ms");
        }
        return gotAllResponses;
    }

    /**
     * Generates a unique merge id by taking the local address and the current time
     */
    private ViewId generateMergeId() {
        return new ViewId(gms.local_addr, System.currentTimeMillis());
        // we're (ab)using ViewId as a merge id
    }

    /**
     * Merge all MergeData. All MergeData elements should be disjunct (both views and digests). However,
     * this method is prepared to resolve duplicate entries (for the same member). Resolution strategy for
     * views is to merge only 1 of the duplicate members. Resolution strategy for digests is to take the higher
     * seqnos for duplicate digests.<p>
     * After merging all members into a Membership and subsequent sorting, the first member of the sorted membership
     * will be the new coordinator. This method has a lock on merge_rsps.
     * @param merge_rsps A list of MergeData items. Elements with merge_rejected=true were removed before. Is guaranteed
     *          not to be null and to contain at least 1 member.
     */
    private MergeData consolidateMergeData(Vector<MergeData> merge_rsps) {
        MergeData ret;       
        long logical_time=0; // for new_vid
        ViewId new_vid, tmp_vid;
        MergeView new_view;
        View tmp_view;
        Membership new_mbrs=new Membership();       
        Address new_coord;
        Vector<View> subgroups=new Vector<View>(11);
        // contains a list of Views, each View is a subgroup

        for(MergeData tmp_data:merge_rsps) {           
            if(log.isDebugEnabled()) log.debug("Merge leader " + gms.local_addr + " is consolidating merge data " + tmp_data);
            tmp_view=tmp_data.getView();
            if(tmp_view != null) {
                tmp_vid=tmp_view.getVid();
                if(tmp_vid != null) {
                    // compute the new view id (max of all vids +1)
                    logical_time=Math.max(logical_time, tmp_vid.getId());
                }
                // merge all membership lists into one (prevent duplicates)
                new_mbrs.add(tmp_view.getMembers());
                subgroups.addElement((View)tmp_view.clone());
            }
        }

        // the new coordinator is the first member of the consolidated & sorted membership list
        new_mbrs.sort();       
        new_coord = new_mbrs.size() > 0 ? new_mbrs.elementAt(0) : null;
        if(new_coord == null) {
            if(log.isErrorEnabled()) log.error("new_coord == null");
            return null;
        }
        // should be the highest view ID seen up to now plus 1
        new_vid=new ViewId(new_coord, logical_time + 1);

        // determine the new view
        new_view=new MergeView(new_vid, new_mbrs.getMembers(), subgroups);
        if(log.isDebugEnabled())
            log.debug("Merge leader " + gms.local_addr + " computed new merged view that will be " + new_view);

        // determine the new digest
        Digest new_digest=consolidateDigests(merge_rsps, new_mbrs.size());
        if(new_digest == null) {
            if(log.isErrorEnabled()) log.error("Merge leader " + gms.local_addr + "could not consolidate digest for merge");
            return null;
        }
        if(log.isDebugEnabled()) log.debug("Merge leader " + gms.local_addr + "consolidated digest=" + new_digest);
        ret=new MergeData(gms.local_addr, new_view, new_digest);
        return ret;
    }

    /**
     * Merge all digests into one. For each sender, the new value is min(low_seqno), max(high_seqno),
     * max(high_seqno_seen). This method has a lock on merge_rsps
     */
    private Digest consolidateDigests(Vector<MergeData> merge_rsps, int num_mbrs) {               
        MutableDigest retval=new MutableDigest(num_mbrs);

        for(MergeData data:merge_rsps) {            
            Digest tmp_digest=data.getDigest();
            if(tmp_digest == null) {
                if(log.isErrorEnabled()) log.error("tmp_digest == null; skipping");
                continue;
            }
            retval.merge(tmp_digest);
        }
        return retval.copy();
    }

    /**
     * Sends the new view and digest to all subgroup coordinors in coords. Each coord will in turn
     * <ol>
     * <li>cast the new view and digest to all the members of its subgroup (MergeView)
     * <li>on reception of the view, if it is a MergeView, each member will set the digest and install
     *     the new view
     * </ol>
     */
    private void sendMergeView(Vector<Address> coords, MergeData combined_merge_data) {                      
        View v;
        Digest d;

        if(coords == null || combined_merge_data == null)
            return;

        v=combined_merge_data.view;
        d=combined_merge_data.digest;
        if(v == null || d == null) {
            if(log.isErrorEnabled()) log.error("view or digest is null, cannot send consolidated merge view/digest");
            return;
        }

        if(log.isDebugEnabled())
            log.debug(gms.local_addr + " is sending merge view " + v.getVid() + " to coordinators " + coords);
        
        gms.merge_ack_collector.reset(coords);
        int size=gms.merge_ack_collector.size();
        long timeout=gms.view_ack_collection_timeout;         
        
        long start = System.currentTimeMillis();
        for(Address coord:coords) {            
            Message msg=new Message(coord, null, null);
            GMS.GmsHeader hdr=new GMS.GmsHeader(GMS.GmsHeader.INSTALL_MERGE_VIEW);
            hdr.view=v;
            hdr.my_digest=d;
            hdr.merge_id=merge_id;
            msg.putHeader(gms.getName(), hdr);
            gms.getDownProtocol().down(new Event(Event.MSG, msg));
        }
        
        //[JGRP-700] - FLUSH: flushing should span merge
        //if flush is in stack wait for acks from separated island coordinators
        if(gms.flushProtocolInStack) {          
            try {
                gms.merge_ack_collector.waitForAllAcks(timeout);
                long stop=System.currentTimeMillis();
                if(log.isTraceEnabled())
                    log.trace("received all ACKs (" + size
                              + ") for merged view "
                              + v
                              + " in "
                              + (stop - start)
                              + "ms");
            }
            catch(TimeoutException e) {
                log.warn("Merge coordinator " + gms.local_addr + " failed to collect all ACKs for merge (" + size
                         + ") for view "
                         + v
                         + " after "
                         + timeout
                         + "ms, missing ACKs from "
                         + gms.merge_ack_collector.printMissing()
                         + ", local_addr="
                         + gms.local_addr);
            }
        }
    }

    /**
     * Send back a response containing view and digest to sender
     */
    private void sendMergeResponse(Address sender, View view, Digest digest) {
        Message msg=new Message(sender, null, null);
        msg.setFlag(Message.OOB);
        GMS.GmsHeader hdr=new GMS.GmsHeader(GMS.GmsHeader.MERGE_RSP);
        hdr.merge_id=merge_id;
        hdr.view=view;
        hdr.my_digest=digest;
        msg.putHeader(gms.getName(), hdr);
        if(log.isDebugEnabled()) log.debug("response=" + hdr);
        gms.getDownProtocol().down(new Event(Event.MSG, msg));
    }


    private void sendMergeCancelledMessage(Vector<Address> coords, ViewId merge_id) {              
        if(coords == null || merge_id == null) {
            if(log.isErrorEnabled()) log.error("coords or merge_id == null");
            return;
        }
        for(Address coord:coords) {            
            Message msg=new Message(coord, null, null);
            // msg.setFlag(Message.OOB);
            GMS.GmsHeader hdr=new GMS.GmsHeader(GMS.GmsHeader.CANCEL_MERGE);
            hdr.merge_id=merge_id;
            msg.putHeader(gms.getName(), hdr);
            gms.getDownProtocol().down(new Event(Event.MSG, msg));
            if(log.isDebugEnabled()) log.debug("Merge leader " + gms.local_addr + " send cancel merge to " + coord);
        }
    }

    /** Removed rejected merge requests from merge_rsps and coords. This method has a lock on merge_rsps */
    private void removeRejectedMergeRequests(Vector<Address> coords) {        
        for(Iterator<MergeData> it=merge_rsps.iterator(); it.hasNext();) {
            MergeData data=it.next();
            if(data.merge_rejected) {
                if(data.getSender() != null && coords != null)
                    coords.removeElement(data.getSender());
                it.remove();
                if(log.isDebugEnabled()) log.debug("removed element " + data);
            }
        }
    }

    /* --------------------------------------- End of Private methods ------------------------------------- */

    /**
     * Starts the merge protocol (only run by the merge leader). Essentially sends a MERGE_REQ to all
     * coordinators of all subgroups found. Each coord receives its digest and view and returns it.
     * The leader then computes the digest and view for the new group from the return values. Finally, it
     * sends this merged view/digest to all subgroup coordinators; each coordinator will install it in their
     * subgroup.
     */
    private class MergeTask implements Runnable {
        Thread t=null;
        Vector<Address> coords=null; // list of subgroup coordinators to be contacted

        public void start(Vector<Address> groupCoord) {
            this.coords = groupCoord != null ? new Vector<Address>(groupCoord) : null;
            if(!isRunning()) {              
                t=gms.getThreadFactory().newThread(this, "MergeTask");               
                t.setDaemon(true);
                t.start();
            }
        }

        public void stop() {
            Thread tmp=t;
            if(isRunning()) {
                t=null;
                tmp.interrupt();
            }
            t=null;            
        }

        public boolean isRunning() {
            return t != null && t.isAlive();
        }

        /**
         * Runs the merge protocol as a leader
         */
        public void run() {
            if(merging) {
                if(log.isWarnEnabled())
                    log.warn(gms.local_addr + " running merge task, but merge is is already in progress, terminating");
                return;
            }

            if(coords == null || coords.size() <= 1) {
                if(log.isErrorEnabled())
                    log.error("coords == null or size <= 1");
                return;
            }

            if(log.isDebugEnabled())
                log.debug(gms.local_addr + " running merge task, coordinators are " + this.coords);
            Vector<Address> coordsCopy=new Vector<Address>(coords);
            /* 1. Generate a merge_id that uniquely identifies the merge in progress */
            ViewId generatedMergeId=generateMergeId();
            
            try {               
                setMergeId(generatedMergeId);

                /* 2. Fetch the current Views/Digests from all subgroup coordinators */
                boolean success=getMergeDataFromSubgroupCoordinators(coords, gms.merge_timeout);

                if(!success) {
                    throw new Exception("Merge aborted. Merge leader did not get MergeData from all subgroup coordinators " + coords);
                }

                /*
                 * 3. Remove rejected MergeData elements from merge_rsp and
                 * coords (so we'll send the new view only to members who
                 * accepted the merge request)
                 */
                MergeData combined_merge_data=null;
                synchronized(merge_rsps) {
                    removeRejectedMergeRequests(coords);
                    if(merge_rsps.size() <= 1) {
                        throw new Exception("Merge leader " + gms.local_addr
                                            + " did not get all merge responses from subgroup coordinators  ("
                                            + merge_rsps + ")");
                    }
                    else {
                        /* 4. Combine all views and digests into 1 View/1 Digest */
                        combined_merge_data=consolidateMergeData(merge_rsps);
                        if(combined_merge_data == null) {
                            throw new Exception("Merge leader " + gms.local_addr
                                                + " could not consolidate merge");
                        }
                    }
                }
                /* 4. Send the new View/Digest to all coordinators (including myself). On reception, they will
                   install the digest and view in all of their subgroup members */
                sendMergeView(coords, combined_merge_data);
            }
            catch(Throwable ex) {
                if(log.isWarnEnabled())
                    log.warn(ex.getLocalizedMessage());
                sendMergeCancelledMessage(coordsCopy, generatedMergeId);
            }
            finally {
                gms.getViewHandler().resume(generatedMergeId);
                stopMergeCanceller(); // this is probably not necessary

                /*5. if flush is in stack stop the flush for entire cluster 
                 [JGRP-700] - FLUSH: flushing should span merge */

                gms.stopFlush();

                merging=false;
                if(log.isDebugEnabled())
                    log.debug("Merge leader " + gms.local_addr + " completed merge task");
                t=null;
            }
        }
    }   


    private class MergeCanceller implements Runnable {
        private Object my_merge_id=null;
        private long timeout;

        MergeCanceller(Object my_merge_id, long timeout) {
            this.my_merge_id=my_merge_id;
            this.timeout=timeout;
        }


        public void run() {
            if(merge_id != null && my_merge_id.equals(merge_id)) {
                if(log.isDebugEnabled())
                    log.debug("At " + gms.local_addr + " cancelling merge due to timer timeout (" + timeout + " ms)");
                cancelMerge();
            }
            else {
                if(log.isWarnEnabled())
                    log.warn("At " + gms.local_addr +" timer kicked in after " + timeout + " ms, but no (or different) merge was in progress: " +
                              "merge_id=" + merge_id + ", my_merge_id=" + my_merge_id);
            }
        }
    }

}
