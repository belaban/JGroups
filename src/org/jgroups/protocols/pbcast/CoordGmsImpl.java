
package org.jgroups.protocols.pbcast;


import org.jgroups.*;
import org.jgroups.annotations.GuardedBy;
import org.jgroups.util.*;

import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Coordinator role of the Group MemberShip (GMS) protocol. Accepts JOIN and LEAVE requests and emits view changes
 * accordingly.
 * @author Bela Ban
 * @version $Id: CoordGmsImpl.java,v 1.115 2009/06/17 11:34:01 belaban Exp $
 */
public class CoordGmsImpl extends GmsImpl {
    private final MergeTask         merge_task=new MergeTask();

     /** For MERGE_REQ/MERGE_RSP correlation, contains MergeData elements */
    private final ResponseCollector<MergeData> merge_rsps=new ResponseCollector<MergeData>();

    /** For GET_DIGEST / DIGEST_RSP correlation */
    private final ResponseCollector<Digest> digest_collector=new ResponseCollector<Digest>();

    /** To serialize access to merge_id */
    private final Lock                merge_lock=new ReentrantLock();

    @GuardedBy("merge_lock")
    private MergeId                   merge_id=null;

    @GuardedBy("merge_canceller_lock")
    private Future<?>                 merge_canceller_future=null;

    private final Lock              merge_canceller_lock=new ReentrantLock();

    /** the max time in ms to suspend message garbage collection */
    private final Long              MAX_SUSPEND_TIMEOUT=new Long(30000);


    public CoordGmsImpl(GMS g) {
        super(g);
    }


    private void cancelMerge(MergeId id) {
        if(setMergeId(id, null)) {
            merge_task.stop();
            merge_rsps.reset();
            gms.getViewHandler().resume(id);
        }
    }


    private boolean setMergeId(MergeId expected, MergeId new_value) {
        merge_lock.lock();
        try {
            boolean match=Util.match(this.merge_id, expected);
            if(match) {
                this.merge_id=new_value;
                stopMergeCanceller();
                if(this.merge_id != null)
                    startMergeCanceller();
            }
            return match;
        }
        finally {
            merge_lock.unlock();
        }
    }

    /** Only used for testing, might get removed any time. Do not use ! */
    public MergeId getMergeId() {
        merge_lock.lock();
        try {
            return merge_id;
        }
        finally {
            merge_lock.unlock();
        }
    }

    private boolean isMergeInProgress() {
        merge_lock.lock();
        try {
            return merge_id != null;
        }
        finally {
            merge_lock.unlock();
        }
    }

    private boolean matchMergeId(MergeId id) {
        merge_lock.lock();
        try {
            return Util.match(this.merge_id, id);
        }
        finally {
            merge_lock.unlock();
        }
    }

    private void startMergeCanceller() {
        merge_canceller_lock.lock();
        try {
            if(merge_canceller_future == null || merge_canceller_future.isDone()) {
                MergeCanceller task=new MergeCanceller(this.merge_id);
                merge_canceller_future=gms.timer.schedule(task, (long)(gms.merge_timeout * 1.5), TimeUnit.MILLISECONDS);
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
        cancelMerge(null);
    }

    public void join(Address mbr,boolean useFlushIfPresent) {
        wrongMethod("join");
    }
    
    public void joinWithStateTransfer(Address mbr,boolean useFlushIfPresent) {
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
        gms.getViewHandler().add(new Request(Request.LEAVE, mbr, false));
        gms.getViewHandler().stop(true); // wait until all requests have been processed, then close the queue and leave
        gms.getViewHandler().waitUntilCompleted(gms.leave_timeout);
    }


    public void suspect(Address mbr) {
        if(mbr.equals(gms.local_addr)) {
            if(log.isWarnEnabled()) log.warn("I am the coord and I'm suspected -- will probably leave shortly");
            return;
        }        
        Collection<Request> suspected=new LinkedHashSet<Request>(1);
        suspected.add(new Request(Request.SUSPECT,mbr,true));
        handleMembershipChange(suspected);
    }


    /**
     * Invoked upon receiving a MERGE event from the MERGE layer. Starts the merge protocol.
     * See description of protocol in DESIGN.
     * @param views A List of <em>different</em> views detected by the merge protocol
     */
    public void merge(List<View> views) {
        if(isMergeInProgress()) {
            if(log.isTraceEnabled()) log.trace(gms.local_addr + ": merge is already running (merge_id=" + merge_id + ")");
            return;
        }

        Collection<Address> coords=determineCoords(views);
        Membership tmp=new Membership(coords); // establish a deterministic order, so that coords can elect leader
        tmp.sort();
        Address merge_leader=tmp.elementAt(0);
        if(log.isDebugEnabled()) log.debug("determining merge leader from coordinators " + tmp);
        if(merge_leader.equals(gms.local_addr)) {
            if(log.isDebugEnabled())
                log.debug("I (" + gms.local_addr + ") will be the leader. Starting the merge task for " + coords);
            merge_task.start(views);
        }
        else {
            if(log.isDebugEnabled()) log.debug("I (" + gms.local_addr + ") am not the merge leader, " +
                    "waiting for merge leader (" + merge_leader + ") to initiate merge");
        }
    }

    /**
     * Get the view and digest and send back both (MergeData) in the form of a MERGE_RSP to the sender.
     * If a merge is already in progress, send back a MergeData with the merge_rejected field set to true.
     * @param sender The address of the merge leader
     * @param merge_id The merge ID
     * @param mbrs The set of members from which we expect responses
     */
    public void handleMergeRequest(Address sender, MergeId merge_id, Collection<? extends Address> mbrs) {
        boolean success=matchMergeId(merge_id) || setMergeId(null, merge_id);
        if(!success) {
            if(log.isErrorEnabled()) log.error(gms.local_addr + ": merge is already in progress");
            sendMergeRejectedResponse(sender, merge_id);
            return;
        }

        /* Clears the view handler queue and discards all JOIN/LEAVE/MERGE requests until after the MERGE  */
        gms.getViewHandler().suspend(merge_id);
        if(log.isDebugEnabled())
            log.debug(gms.local_addr + ": got merge request from " + sender + ", merge_id=" + merge_id + ", mbrs=" + mbrs);

        // merge the membership of the current view with mbrs
        List<Address> members=new LinkedList<Address>(gms.members.getMembers());
        if(mbrs != null) { // didn't use a set because we didn't want to change the membership order at this time (although not incorrect)
            for(Address mbr: mbrs) {
                if(!members.contains(mbr))
                    members.add(mbr);
            }
        }
        View view=new View(gms.view_id.copy(), new Vector<Address>(members));
        
        //[JGRP-524] - FLUSH and merge: flush doesn't wrap entire merge process
        //[JGRP-770] - Concurrent startup of many channels doesn't stabilize
        //[JGRP-700] - FLUSH: flushing should span merge
        
        /*if flush is in stack, let this coordinator flush its cluster island */
        boolean successfulFlush=gms.startFlush(view);
        if(!successfulFlush) {
            sendMergeRejectedResponse(sender, merge_id);
            if(log.isWarnEnabled())
                log.warn(gms.local_addr + ": flush failed; sending merge rejected message to "+ sender+ ", merge_id="+ merge_id);
            cancelMerge(merge_id);
            return;
        }
        Digest digest=fetchDigestsFromAllMembersInSubPartition(members);
        sendMergeResponse(sender, view, digest, merge_id);
    }

    public void handleMergeResponse(MergeData data, MergeId merge_id) {
        if(!matchMergeId(merge_id)) {
            if(log.isErrorEnabled())
                log.error(gms.local_addr + ": this.merge_id (" + this.merge_id + ") is different from merge_id (" + merge_id + ')');
            return;
        }
        merge_rsps.add(data.getSender(), data);
    }

    
    public void handleDigestResponse(Address sender, Digest digest) {
        digest_collector.add(sender, digest);
    }


    /**
     * If merge_id is not equal to this.merge_id then discard.
     * Else cast the view/digest to all members of this group.
     */
    public void handleMergeView(final MergeData data,final MergeId merge_id) {
        if(!matchMergeId(merge_id)) {
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
         // if we have flush in stack send ack back to merge coordinator                 
        if(gms.flushProtocolInStack) {
            Message ack=new Message(data.getSender(), null, null);
            ack.setFlag(Message.OOB);
            GMS.GmsHeader ack_hdr=new GMS.GmsHeader(GMS.GmsHeader.INSTALL_MERGE_VIEW_OK);
            ack.putHeader(gms.getName(), ack_hdr);
            gms.getDownProtocol().down(new Event(Event.MSG, ack));
        }
        cancelMerge(merge_id);
    }

    public void handleMergeCancelled(MergeId merge_id) {
        gms.stopFlush();
        if(matchMergeId(merge_id)) {
            if(log.isDebugEnabled())
                log.debug(gms.local_addr + ": merge " + merge_id + " is cancelled");
            cancelMerge(merge_id);
        }
    }

    /**
     * Multicasts a GET_DIGEST_REQ to all current members and waits for all responses (GET_DIGEST_RSP) or N ms.
     * @return
     */
    private Digest fetchDigestsFromAllMembersInSubPartition(List<Address> current_mbrs) {
        if(current_mbrs == null)
            return null;

        GMS.GmsHeader hdr=new GMS.GmsHeader(GMS.GmsHeader.GET_DIGEST_REQ);
        Message get_digest_req=new Message();
        get_digest_req.setFlag(Message.OOB);
        get_digest_req.putHeader(gms.getName(), hdr);

        long max_wait_time=gms.merge_timeout > 0? (long)(gms.merge_timeout * 0.8) : 2000L;
        digest_collector.reset(current_mbrs);
        gms.getDownProtocol().down(new Event(Event.MSG, get_digest_req));
        digest_collector.waitForAllResponses(max_wait_time);
        Map<Address,Digest> responses=new HashMap<Address,Digest>(digest_collector.getResults());
        MutableDigest retval=new MutableDigest(responses.size());
        for(Digest digest: responses.values()) {
            if(digest != null)
                retval.add(digest);
        }
        return retval;
    }

    /**
     * Fetches the digests from all members and installs them again. Used only for diagnosis and support; don't
     * use this otherwise !
     */
    void fixDigests() {
        Digest digest=fetchDigestsFromAllMembersInSubPartition(gms.view.getMembers());
        Message msg=new Message();
        GMS.GmsHeader hdr=new GMS.GmsHeader(GMS.GmsHeader.INSTALL_DIGEST);
        hdr.my_digest=digest;
        msg.putHeader(gms.getName(), hdr);
        gms.getDownProtocol().down(new Event(Event.MSG, msg));
    }



    public void handleMembershipChange(Collection<Request> requests) {
        boolean joinAndStateTransferInitiated=false;
        boolean useFlushIfPresent=gms.use_flush_if_present;
        Collection<Address> new_mbrs=new LinkedHashSet<Address>(requests.size());
        Collection<Address> suspected_mbrs=new LinkedHashSet<Address>(requests.size());
        Collection<Address> leaving_mbrs=new LinkedHashSet<Address>(requests.size());

        for(Request req: requests) {
            switch(req.type) {
                case Request.JOIN:
                    new_mbrs.add(req.mbr);
                    useFlushIfPresent=req.useFlushIfPresent;
                    break;
                case Request.JOIN_WITH_STATE_TRANSFER:
                    new_mbrs.add(req.mbr);
                    joinAndStateTransferInitiated=true;
                    useFlushIfPresent=req.useFlushIfPresent;
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
                if(log.isWarnEnabled())
                    log.warn(mbr + " already present; returning existing view " + gms.view);
                JoinRsp join_rsp=new JoinRsp(new View(gms.view_id, gms.members.getMembers()), gms.getDigest());
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

        if(new_view.size() == 0 && gms.local_addr != null && gms.local_addr.equals(new_view.getCreator())) {
            if(log.isTraceEnabled())
                log.trace("view " + new_view + " is empty: will not multicast it (last view)");
            if(leaving) {
                gms.initState(); // in case connect() is called again
            }
            return;
        }

        gms.up(new Event(Event.PREPARE_VIEW,new_view));
        gms.down(new Event(Event.PREPARE_VIEW,new_view));
        
        if(log.isDebugEnabled())
            log.debug("new=" + new_mbrs + ", suspected=" + suspected_mbrs + ", leaving=" + leaving_mbrs +
                    ", new view: " + new_view);
             
        JoinRsp join_rsp=null;
        boolean hasJoiningMembers=!new_mbrs.isEmpty();
        try {            
            boolean successfulFlush =!useFlushIfPresent || gms.startFlush(new_view);
            if(!successfulFlush && hasJoiningMembers){
                // Don't send a join response if the flush fails (http://jira.jboss.org/jira/browse/JGRP-759)
                // The joiner should block until the previous FLUSH completed
                sendLeaveResponses(leaving_mbrs); // we still have to send potential leave responses
                // but let the joining client timeout and send another join request
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
            gms.castViewChangeWithDest(new_view, join_rsp != null? join_rsp.getDigest() : null, join_rsp,new_mbrs);                      
        }
        finally {
            if(hasJoiningMembers)
                gms.getDownProtocol().down(new Event(Event.RESUME_STABLE));
            if(!joinAndStateTransferInitiated && useFlushIfPresent)
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
        cancelMerge(merge_id);
    }

    public void stop() {
        super.stop(); // sets leaving=false
        merge_task.stop();
    }

    /* ------------------------------------------ Private methods ----------------------------------------- */

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
     * @param coords A map of coordinatgor addresses and associated membership lists
     * @param new_merge_id The new merge id
     * @param timeout Max number of msecs to wait for the merge responses from the subgroup coords
     */
    private boolean getMergeDataFromSubgroupCoordinators(Map<Address,Collection<Address>> coords, MergeId new_merge_id, long timeout) {
    	boolean gotAllResponses = false;
        long start=System.currentTimeMillis();        
        merge_rsps.reset(coords.keySet());
        if(log.isDebugEnabled())
            log.debug(gms.local_addr + ": sending MERGE_REQ to " + coords.keySet());
            
        for(Map.Entry<Address,Collection<Address>> entry: coords.entrySet()) {
            Address coord=entry.getKey();
            Collection<Address> mbrs=entry.getValue();
            Message msg=new Message(coord, null, null);
            msg.setFlag(Message.OOB);
            GMS.GmsHeader hdr=new GMS.GmsHeader(GMS.GmsHeader.MERGE_REQ, mbrs);
            hdr.mbr=gms.local_addr;
            hdr.merge_id=new_merge_id;
            msg.putHeader(gms.getName(), hdr);
            gms.getDownProtocol().down(new Event(Event.MSG, msg));
        }

        // wait until num_rsps_expected >= num_rsps or timeout elapsed
        merge_rsps.waitForAllResponses(timeout);
        gotAllResponses=merge_rsps.hasAllResponses();
        long stop=System.currentTimeMillis();
        if(log.isDebugEnabled())
            log.debug(gms.local_addr + ": collected " + merge_rsps.size() + " merge response(s) in " + (stop-start) + " ms");
        return gotAllResponses;
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
        long logical_time=0; // for new_vid
        Membership new_mbrs=new Membership();
        Vector<View> subgroups=new Vector<View>(11); // contains a list of Views, each View is a subgroup

        for(MergeData tmp_data: merge_rsps) {
            View tmp_view=tmp_data.getView();
            if(tmp_view != null) {
                ViewId tmp_vid=tmp_view.getVid();
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
        Address new_coord = new_mbrs.size() > 0 ? new_mbrs.elementAt(0) : null;
        if(new_coord == null) {
            if(log.isErrorEnabled()) log.error("new_coord == null");
            return null;
        }
        // should be the highest view ID seen up to now plus 1
        ViewId new_vid=new ViewId(new_coord, logical_time + 1);

        // determine the new view
        MergeView new_view=new MergeView(new_vid, new_mbrs.getMembers(), subgroups);

        // determine the new digest
        Digest new_digest=consolidateDigests(merge_rsps, new_mbrs.size());
        if(new_digest == null) {
            if(log.isErrorEnabled()) log.error("Merge leader " + gms.local_addr + ": could not consolidate digest for merge");
            return null;
        }
        if(log.isDebugEnabled()) log.debug("Merge leader " + gms.local_addr + ": consolidated view=" + new_view +
                "\nconsolidated digest=" + new_digest);
        return new MergeData(gms.local_addr, new_view, new_digest);
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

    protected static Collection<Address> determineCoords(List<View> views) {
        Set<Address> retval=new HashSet<Address>();
        if(views != null) {
            for(View view: views) {
                Address coord=view.getCreator();
                if(coord != null)
                    retval.add(coord);
            }
        }
        return retval;
    }

    /**
     * Sends the new view and digest to all subgroup coordinors in coords. Each coord will in turn
     * <ol>
     * <li>broadcast the new view and digest to all the members of its subgroup (MergeView)
     * <li>on reception of the view, if it is a MergeView, each member will set the digest and install the new view
     * </ol>
     */
    private void sendMergeView(Collection<Address> coords, MergeData combined_merge_data, MergeId merge_id) {
        if(coords == null || combined_merge_data == null)
            return;

        View view=combined_merge_data.view;
        Digest digest=combined_merge_data.digest;
        if(view == null || digest == null) {
            if(log.isErrorEnabled()) log.error("view or digest is null, cannot send consolidated merge view/digest");
            return;
        }

        if(log.isDebugEnabled())
            log.debug(gms.local_addr + ": sending merge view " + view.getVid() + " to coordinators " + coords);
        
        gms.merge_ack_collector.reset(coords);
        int size=gms.merge_ack_collector.size();
        long timeout=gms.view_ack_collection_timeout;         
        
        long start=System.currentTimeMillis();
        for(Address coord:coords) {            
            Message msg=new Message(coord, null, null);
            GMS.GmsHeader hdr=new GMS.GmsHeader(GMS.GmsHeader.INSTALL_MERGE_VIEW);
            hdr.view=view;
            hdr.my_digest=digest;
            hdr.merge_id=merge_id;
            msg.putHeader(gms.getName(), hdr);
            gms.getDownProtocol().down(new Event(Event.MSG, msg));
        }
        
        //[JGRP-700] - FLUSH: flushing should span merge
        // if flush is in stack wait for acks from separated island coordinators
        if(gms.flushProtocolInStack) {          
            try {
                gms.merge_ack_collector.waitForAllAcks(timeout);
                long stop=System.currentTimeMillis();
                if(log.isTraceEnabled())
                    log.trace("received all ACKs (" + size + ") for merge view " + view + " in " + (stop - start) + "ms");
            }
            catch(TimeoutException e) {
                log.warn(gms.local_addr + ": failed to collect all ACKs for merge (" + size + ") for view " + view
                         + " after " + timeout + "ms, missing ACKs from " + gms.merge_ack_collector.printMissing()
                         + " (received=" + gms.merge_ack_collector.printReceived() + "), local_addr=" + gms.local_addr);
            }
        }
    }

    /** Send back a response containing view and digest to sender */
    private void sendMergeResponse(Address sender, View view, Digest digest, MergeId merge_id) {
        Message msg=new Message(sender, null, null);
        msg.setFlag(Message.OOB);
        GMS.GmsHeader hdr=new GMS.GmsHeader(GMS.GmsHeader.MERGE_RSP);
        hdr.merge_id=merge_id;
        hdr.view=view;
        hdr.my_digest=digest;
        msg.putHeader(gms.getName(), hdr);
        if(log.isDebugEnabled()) log.debug(gms.local_addr + ": sending merge response=" + hdr);
        gms.getDownProtocol().down(new Event(Event.MSG, msg));
    }


    private void sendMergeCancelledMessage(Collection<Address> coords, MergeId merge_id) {
        if(coords == null || merge_id == null)
            return;
        
        for(Address coord:coords) {
            Message msg=new Message(coord, null, null);
            // msg.setFlag(Message.OOB);
            GMS.GmsHeader hdr=new GMS.GmsHeader(GMS.GmsHeader.CANCEL_MERGE);
            hdr.merge_id=merge_id;
            msg.putHeader(gms.getName(), hdr);
            if(log.isDebugEnabled()) log.debug(gms.local_addr + ": sending cancel merge to " + coord);
            gms.getDownProtocol().down(new Event(Event.MSG, msg));
        }
    }

    /** Removed rejected merge requests from merge_rsps and coords. This method has a lock on merge_rsps */
    private void removeRejectedMergeRequests(Collection<Address> coords) {
        for(Map.Entry<Address,MergeData> entry: merge_rsps.getResults().entrySet()) {
            Address member=entry.getKey();
            MergeData data=entry.getValue();
            if(data.merge_rejected) {
                if(data.getSender() != null)
                    coords.remove(data.getSender());
                merge_rsps.remove(member);
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
        private Thread thread=null;

        /** List of all subpartition coordinators and their members */
        private final Map<Address,Collection<Address>> coords=new ConcurrentHashMap<Address,Collection<Address>>();

        /**
         * @param all_coords Guaranteed to be non-null and to have >= 2 members, or else this thread would not be started
         */
        public synchronized void start(Collection<View> views) {
            if(thread == null || thread.isAlive()) {
                coords.clear();
                determineCoordsAndMembers(views);
                thread=gms.getThreadFactory().newThread(this, "MergeTask");
                thread.setDaemon(true);
                thread.start();
            }
        }

        /**
         * Adds all different coordinators of the views into the hashmap and sets their members
         * @param views
         */
        private void determineCoordsAndMembers(Collection<View> views) {
            for(View view: views) {
                Address coord=view.getCreator();
                if(coord != null) {
                    Collection<Address> members=coords.get(coord);
                    if(members == null)
                        coords.put(coord, members=new HashSet<Address>());
                    members.addAll(view.getMembers());
                }
            }
        }

        public synchronized void stop() {
            Thread tmp=thread;
            if(thread != null && thread.isAlive())
                tmp.interrupt();
            thread=null;
        }


        /** Runs the merge protocol as a leader */
        public void run() {

            // 1. Generate merge_id
            MergeId new_merge_id=MergeId.create(gms.local_addr);
            Collection<Address> coordsCopy=null;

            try {
                boolean success=setMergeId(null, new_merge_id);
                if(!success) {
                    log.warn("failed to set my own merge_id (" + merge_id + ") to " + new_merge_id);
                    return;
                }

                coordsCopy=new ArrayList<Address>(coords.keySet());

                /* 2. Fetch the current Views/Digests from all subgroup coordinators */
                success=getMergeDataFromSubgroupCoordinators(coords, new_merge_id, gms.merge_timeout);
                if(!success)
                    throw new Exception("merge leader did not get data from all partition coordinators " + coords.keySet());

                /* 3. Remove rejected MergeData elements from merge_rsp and coords (so we'll send the new view
                 * only to members who accepted the merge request) */
                removeRejectedMergeRequests(coords.keySet());
                if(merge_rsps.size() == 0)
                    throw new Exception("did not get any merge responses from partition coordinators");

                if(!coords.keySet().contains(gms.local_addr)) // another member might have invoked a merge req on us before we got there...
                    throw new Exception("merge leader rejected merge request");

                /* 4. Combine all views and digests into 1 View/1 Digest */
                Vector<MergeData> merge_data=new Vector<MergeData>(merge_rsps.getResults().values());
                MergeData combined_merge_data=consolidateMergeData(merge_data);
                if(combined_merge_data == null)
                    throw new Exception("could not consolidate merge");
                /* 4. Send the new View/Digest to all coordinators (including myself). On reception, they will
                   install the digest and view in all of their subgroup members */
                sendMergeView(coords.keySet(), combined_merge_data, new_merge_id);
            }
            catch(Throwable ex) {
                if(log.isWarnEnabled())
                    log.warn(gms.local_addr + ": " + ex.getLocalizedMessage() + ", merge is cancelled");
                sendMergeCancelledMessage(coordsCopy, new_merge_id);
            }
            finally {
                gms.getViewHandler().resume(new_merge_id);
                stopMergeCanceller(); // this is probably not necessary

                /*5. if flush is in stack stop the flush for entire cluster [JGRP-700] - FLUSH: flushing should span merge */
                gms.stopFlush();
                if(log.isDebugEnabled())
                    log.debug(gms.local_addr + ": merge leader completed merge task");
                thread=null;
            }
        }
    }   


    private class MergeCanceller implements Runnable {
        private final MergeId my_merge_id;

        MergeCanceller(MergeId my_merge_id) {
            this.my_merge_id=my_merge_id;
        }


        public void run() {
            cancelMerge(my_merge_id);
        }
    }
}
