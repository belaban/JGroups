// $Id: CoordGmsImpl.java,v 1.17 2004/10/05 15:30:06 belaban Exp $

package org.jgroups.protocols.pbcast;


import org.jgroups.*;
import org.jgroups.util.TimeScheduler;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Vector;




/**
 * Coordinator role of the Group MemberShip (GMS) protocol. Accepts JOIN and LEAVE requests and emits view changes
 * accordingly.
 * @author Bela Ban
 */
public class CoordGmsImpl extends GmsImpl {
    private boolean          merging=false;
    private final MergeTask  merge_task=new MergeTask();
    private final Vector     merge_rsps=new Vector(11);
    // for MERGE_REQ/MERGE_RSP correlation, contains MergeData elements
    private Serializable     merge_id=null;

    private Address          merge_leader=null;

    private MergeCanceller   merge_canceller=null;



    public CoordGmsImpl(GMS g) {
        gms=g;
    }


    void setMergeId(Serializable merge_id) {
        this.merge_id=merge_id;
        if(this.merge_id != null) {
            stopMergeCanceller();
            merge_canceller=new MergeCanceller(this.merge_id, gms.merge_timeout);
            gms.timer.add(merge_canceller);
        }
        else { // merge completed
            stopMergeCanceller();
        }
    }

    private void stopMergeCanceller() {
        if(merge_canceller != null) {
            merge_canceller.cancel();
            merge_canceller=null;
        }
    }

    public void init() throws Exception {
        super.init();
        cancelMerge();
    }

    public void join(Address mbr) {
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
        handleLeave(mbr, false); // regular leave
    }

    public void handleJoinResponse(JoinRsp join_rsp) {
        wrongMethod("handleJoinResponse");
    }

    public void handleLeaveResponse() {
        ; // safely ignore this
    }

    public void suspect(Address mbr) {
        handleSuspect(mbr);
    }

    public void unsuspect(Address mbr) {

    }

    /**
     * Invoked upon receiving a MERGE event from the MERGE layer. Starts the merge protocol.
     * See description of protocol in DESIGN.
     * @param other_coords A list of coordinators (including myself) found by MERGE protocol
     */
    public void merge(Vector other_coords) {
        Membership tmp;

        if(merging) {
            if(log.isWarnEnabled()) log.warn("merge already in progress, discarded MERGE event");
            return;
        }
        merge_leader=null;
        if(other_coords == null) {
            if(log.isWarnEnabled()) log.warn("list of other coordinators is null. Will not start merge.");
            return;
        }

        if(other_coords.size() <= 1) {
            if(log.isErrorEnabled()) log.error("number of coordinators found is "
                                               + other_coords.size() + "; will not perform merge");
            return;
        }

        /* Establish deterministic order, so that coords can elect leader */
        tmp=new Membership(other_coords);
        tmp.sort();
        merge_leader=(Address)tmp.elementAt(0);
        if(log.isDebugEnabled()) log.debug("coordinators in merge protocol are: " + tmp);
        if(merge_leader.equals(gms.local_addr)) {
            if(log.isDebugEnabled()) log.debug("I (" + merge_leader + ") will be the leader. Starting the merge task");
            startMergeTask(other_coords);
        }
    }

    /**
     * Get the view and digest and send back both (MergeData) in the form of a MERGE_RSP to the sender.
     * If a merge is already in progress, send back a MergeData with the merge_rejected field set to true.
     */
    public void handleMergeRequest(Address sender, Object merge_id) {
        Digest digest;
        View view;

        if(sender == null) {
            if(log.isErrorEnabled()) log.error("sender == null; cannot send back a response");
            return;
        }
        if(merging) {
            if(log.isErrorEnabled()) log.error("merge already in progress");
            sendMergeRejectedResponse(sender);
            return;
        }
        merging=true;
        setMergeId((Serializable)merge_id);
        if(log.isDebugEnabled()) log.debug("sender=" + sender + ", merge_id=" + merge_id);
        digest=gms.getDigest();
        view=new View(gms.view_id.copy(), gms.members.getMembers());
        sendMergeResponse(sender, view, digest);
    }


    MergeData getMergeResponse(Address sender, Object merge_id) {
        Digest         digest;
        View           view;
        MergeData      retval;

        if(sender == null) {
            if(log.isErrorEnabled()) log.error("sender == null; cannot send back a response");
            return null;
        }
        if(merging) {
            if(log.isErrorEnabled()) log.error("merge already in progress");
            retval=new MergeData(sender, null, null);
            retval.merge_rejected=true;
            return retval;
        }
        merging=true;
        setMergeId((Serializable)merge_id);
        if(log.isDebugEnabled()) log.debug("sender=" + sender + ", merge_id=" + merge_id);

        digest=gms.getDigest();
        view=new View(gms.view_id.copy(), gms.members.getMembers());
        retval=new MergeData(sender, view, digest);
        retval.view=view;
        retval.digest=digest;
        return retval;
    }


    public void handleMergeResponse(MergeData data, Object merge_id) {
        if(data == null) {
            if(log.isErrorEnabled()) log.error("merge data is null");
            return;
        }
        if(merge_id == null || this.merge_id == null) {
            if(log.isErrorEnabled()) log.error("merge_id ("
                    + merge_id
                    + ") or this.merge_id ("
                    + this.merge_id
                    + ") == null (sender="
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
     * If merge_id != this.merge_id --> discard
     * Else cast the view/digest to all members of this group.
     */
    public void handleMergeView(MergeData data, Object merge_id) {
        if(merge_id == null
                || this.merge_id == null
                || !this.merge_id.equals(merge_id)) {
            if(log.isErrorEnabled()) log.error("merge_ids don't match (or are null); merge view discarded");
            return;
        }
        gms.castViewChange(data.view, data.digest);
        merging=false;
        merge_id=null;
    }

    public void handleMergeCancelled(Object merge_id) {
        if(merge_id != null
                && this.merge_id != null
                && this.merge_id.equals(merge_id)) {
            if(log.isDebugEnabled()) log.debug("merge was cancelled (merge_id=" + merge_id + ')');
            setMergeId(null);
            this.merge_leader=null;
            merging=false;
        }
    }


    private void cancelMerge() {
        if(merge_id != null && log.isDebugEnabled()) log.debug("cancelling merge (merge_id=" + merge_id + ')');
        setMergeId(null);
        this.merge_leader=null;
        stopMergeTask();
        merging=false;
        synchronized(merge_rsps) {
            merge_rsps.clear();
        }
    }

    /**
     * Computes the new view (including the newly joined member) and get the digest from PBCAST.
     * Returns both in the form of a JoinRsp
     */
    public synchronized JoinRsp handleJoin(Address mbr) {
        Vector new_mbrs=new Vector(1);
        View v=null;
        Digest d, tmp;

        if(log.isDebugEnabled()) log.debug("mbr=" + mbr);
        if(gms.local_addr.equals(mbr)) {
            if(log.isErrorEnabled()) log.error("cannot join myself !");
            return null;
        }

        if(gms.members.contains(mbr)) {
            if(log.isErrorEnabled())
                log.error("member " + mbr + " already present; returning existing view " + gms.members.getMembers());
            return new JoinRsp(new View(gms.view_id, gms.members.getMembers()), gms.getDigest());
            // already joined: return current digest and membership
        }
        new_mbrs.addElement(mbr);
        tmp=gms.getDigest(); // get existing digest
        if(tmp == null) {
            if(log.isErrorEnabled()) log.error("received null digest from GET_DIGEST: will cause JOIN to fail");
            return null;
        }
        if(log.isDebugEnabled()) log.debug("got digest=" + tmp);

        d=new Digest(tmp.size() + 1);
        // create a new digest, which contains 1 more member
        d.add(tmp); // add the existing digest to the new one
        d.add(mbr, 0, 0);
        // ... and add the new member. it's first seqno will be 1
        v=gms.getNextView(new_mbrs, null, null);
        if(log.isDebugEnabled()) log.debug("joined member " + mbr + ", view is " + v);
        return new JoinRsp(v, d);
    }

    /**
     Exclude <code>mbr</code> from the membership. If <code>suspected</code> is true, then
     this member crashed and therefore is forced to leave, otherwise it is leaving voluntarily.
     */
    public synchronized void handleLeave(Address mbr, boolean suspected) {
        Vector v=new Vector(1);
        // contains either leaving mbrs or suspected mbrs
        if(log.isDebugEnabled()) log.debug("mbr=" + mbr);
        if(!gms.members.contains(mbr)) {
            if(log.isErrorEnabled()) log.error("mbr " + mbr + " is not a member !");
            return;
        }

        if(gms.view_id == null) {
            // we're probably not the coord anymore (we just left ourselves), let someone else do it
            // (client will retry when it doesn't get a response
            if(log.isDebugEnabled())
                log.debug("gms.view_id is null, I'm not the coordinator anymore (leaving=" + leaving +
                          "); the new coordinator will handle the leave request");
            return;
        }

        sendLeaveResponse(mbr); // send an ack to the leaving member

        v.addElement(mbr);
        if(suspected)
            gms.castViewChange(null, null, v);
        else
            gms.castViewChange(null, v, null);
    }

    void sendLeaveResponse(Address mbr) {
        Message msg=new Message(mbr, null, null);
        GMS.GmsHeader hdr=new GMS.GmsHeader(GMS.GmsHeader.LEAVE_RSP);
        msg.putHeader(gms.getName(), hdr);
        gms.passDown(new Event(Event.MSG, msg));
    }

    /**
     * Called by the GMS when a VIEW is received.
     * @param new_view The view to be installed
     * @param digest   If view is a MergeView, digest contains the seqno digest of all members and has to
     *                 be set by GMS
     */
    public void handleViewChange(View new_view, Digest digest) {
        Vector mbrs=new_view.getMembers();
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

    public void handleSuspect(Address mbr) {
        if(mbr.equals(gms.local_addr)) {
            if(log.isWarnEnabled()) log.warn("I am the coord and I'm being am suspected -- will probably leave shortly");
            return;
        }
        handleLeave(mbr, true); // irregular leave - forced
    }

    public void handleExit() {
        cancelMerge();
    }

    public void stop() {
        super.stop(); // sets leaving=false
        stopMergeTask();
    }

    /* ------------------------------------------ Private methods ----------------------------------------- */

    void startMergeTask(Vector coords) {
        synchronized(merge_task) {
            merge_task.start(coords);
        }
    }

    void stopMergeTask() {
        synchronized(merge_task) {
            merge_task.stop();
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
    void getMergeDataFromSubgroupCoordinators(Vector coords, long timeout) {
        Message msg;
        GMS.GmsHeader hdr;
        Address coord;
        long curr_time, time_to_wait=0, end_time;
        int num_rsps_expected=0;

        if(coords == null || coords.size() <= 1) {
            if(log.isErrorEnabled()) log.error("coords == null or size <= 1");
            return;
        }

        synchronized(merge_rsps) {
            merge_rsps.removeAllElements();

            if(log.isDebugEnabled()) log.debug("sending MERGE_REQ to " + coords);
            for(int i=0; i < coords.size(); i++) {
                coord=(Address)coords.elementAt(i);

                if(gms.local_addr != null && gms.local_addr.equals(coord)) {
                    merge_rsps.add(getMergeResponse(gms.local_addr, merge_id));
                    continue;
                }

                msg=new Message(coord, null, null);
                hdr=new GMS.GmsHeader(GMS.GmsHeader.MERGE_REQ);
                hdr.mbr=gms.local_addr;
                hdr.merge_id=merge_id;
                msg.putHeader(gms.getName(), hdr);
                gms.passDown(new Event(Event.MSG, msg));
            }

            // wait until num_rsps_expected >= num_rsps or timeout elapsed
            num_rsps_expected=coords.size();
            curr_time=System.currentTimeMillis();
            end_time=curr_time + timeout;
            while(end_time > curr_time) {
                time_to_wait=end_time - curr_time;
                if(log.isDebugEnabled()) log.debug("waiting " + time_to_wait + " msecs for merge responses");
                if(merge_rsps.size() < num_rsps_expected) {
                    try {
                        merge_rsps.wait(time_to_wait);
                    }
                    catch(Exception ex) {
                    }
                }
                if(log.isDebugEnabled())
                    log.debug("num_rsps_expected=" + num_rsps_expected + ", actual responses=" + merge_rsps.size());

                if(merge_rsps.size() >= num_rsps_expected)
                    break;
                curr_time=System.currentTimeMillis();
            }
        }
    }

    /**
     * Generates a unique merge id by taking the local address and the current time
     */
    Serializable generateMergeId() {
        return new ViewId(gms.local_addr, System.currentTimeMillis());
        // we're (ab)using ViewId as a merge id
    }

    /**
     * Merge all MergeData. All MergeData elements should be disjunct (both views and digests). However,
     * this method is prepared to resolve duplicate entries (for the same member). Resolution strategy for
     * views is to merge only 1 of the duplicate members. Resolution strategy for digests is to take the higher
     * seqnos for duplicate digests.<p>
     * After merging all members into a Membership and subsequent sorting, the first member of the sorted membership
     * will be the new coordinator.
     * @param v A list of MergeData items. Elements with merge_rejected=true were removed before. Is guaranteed
     *          not to be null and to contain at least 1 member.
     */
    MergeData consolidateMergeData(Vector v) {
        MergeData ret=null;
        MergeData tmp_data;
        long logical_time=0; // for new_vid
        ViewId new_vid, tmp_vid;
        MergeView new_view;
        View tmp_view;
        Membership new_mbrs=new Membership();
        int num_mbrs=0;
        Digest new_digest=null;
        Address new_coord;
        Vector subgroups=new Vector(11);
        // contains a list of Views, each View is a subgroup

        for(int i=0; i < v.size(); i++) {
            tmp_data=(MergeData)v.elementAt(i);
            if(log.isDebugEnabled()) log.debug("merge data is " + tmp_data);
            tmp_view=tmp_data.getView();
            if(tmp_view != null) {
                tmp_vid=tmp_view.getVid();
                if(tmp_vid != null) {
                    // compute the new view id (max of all vids +1)
                    logical_time=Math.max(logical_time, tmp_vid.getId());
                }
                // merge all membership lists into one (prevent duplicates)
                new_mbrs.add(tmp_view.getMembers());
                subgroups.addElement(tmp_view.clone());
            }
        }

        // the new coordinator is the first member of the consolidated & sorted membership list
        new_mbrs.sort();
        num_mbrs=new_mbrs.size();
        new_coord=num_mbrs > 0? (Address)new_mbrs.elementAt(0) : null;
        if(new_coord == null) {
            if(log.isErrorEnabled()) log.error("new_coord == null");
            return null;
        }
        // should be the highest view ID seen up to now plus 1
        new_vid=new ViewId(new_coord, logical_time + 1);

        // determine the new view
        new_view=new MergeView(new_vid, new_mbrs.getMembers(), subgroups);
        if(log.isDebugEnabled()) log.debug("new merged view will be " + new_view);

        // determine the new digest
        new_digest=consolidateDigests(v, num_mbrs);
        if(new_digest == null) {
            if(log.isErrorEnabled()) log.error("digest could not be consolidated");
            return null;
        }
        if(log.isDebugEnabled()) log.debug("consolidated digest=" + new_digest);
        ret=new MergeData(gms.local_addr, new_view, new_digest);
        return ret;
    }

    /**
     * Merge all digests into one. For each sender, the new value is min(low_seqno), max(high_seqno),
     * max(high_seqno_seen)
     */
    Digest consolidateDigests(Vector v, int num_mbrs) {
        MergeData data;
        Digest tmp_digest, retval=new Digest(num_mbrs);

        for(int i=0; i < v.size(); i++) {
            data=(MergeData)v.elementAt(i);
            tmp_digest=data.getDigest();
            if(tmp_digest == null) {
                if(log.isErrorEnabled()) log.error("tmp_digest == null; skipping");
                continue;
            }
            retval.merge(tmp_digest);
        }
        return retval;
    }

    /**
     * Sends the new view and digest to all subgroup coordinors in coords. Each coord will in turn
     * <ol>
     * <li>cast the new view and digest to all the members of its subgroup (MergeView)
     * <li>on reception of the view, if it is a MergeView, each member will set the digest and install
     *     the new view
     * </ol>
     */
    void sendMergeView(Vector coords, MergeData combined_merge_data) {
        Message msg;
        GMS.GmsHeader hdr;
        Address coord;
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

        for(int i=0; i < coords.size(); i++) {
            coord=(Address)coords.elementAt(i);
            msg=new Message(coord, null, null);
            hdr=new GMS.GmsHeader(GMS.GmsHeader.INSTALL_MERGE_VIEW);
            hdr.view=v;
            hdr.digest=d;
            hdr.merge_id=merge_id;
            msg.putHeader(gms.getName(), hdr);
            gms.passDown(new Event(Event.MSG, msg));
        }
    }

    /**
     * Send back a response containing view and digest to sender
     */
    void sendMergeResponse(Address sender, View view, Digest digest) {
        Message msg=new Message(sender, null, null);
        GMS.GmsHeader hdr=new GMS.GmsHeader(GMS.GmsHeader.MERGE_RSP);
        hdr.merge_id=merge_id;
        hdr.view=view;
        hdr.digest=digest;
        msg.putHeader(gms.getName(), hdr);
        if(log.isDebugEnabled()) log.debug("response=" + hdr);
        gms.passDown(new Event(Event.MSG, msg));
    }

    void sendMergeRejectedResponse(Address sender) {
        Message msg=new Message(sender, null, null);
        GMS.GmsHeader hdr=new GMS.GmsHeader(GMS.GmsHeader.MERGE_RSP);
        hdr.merge_rejected=true;
        hdr.merge_id=merge_id;
        msg.putHeader(gms.getName(), hdr);
        if(log.isDebugEnabled()) log.debug("response=" + hdr);
        gms.passDown(new Event(Event.MSG, msg));
    }

    void sendMergeCancelledMessage(Vector coords, Serializable merge_id) {
        Message msg;
        GMS.GmsHeader hdr;
        Address coord;

        if(coords == null || merge_id == null) {
            if(log.isErrorEnabled()) log.error("coords or merge_id == null");
            return;
        }
        for(int i=0; i < coords.size(); i++) {
            coord=(Address)coords.elementAt(i);
            msg=new Message(coord, null, null);
            hdr=new GMS.GmsHeader(GMS.GmsHeader.CANCEL_MERGE);
            hdr.merge_id=merge_id;
            msg.putHeader(gms.getName(), hdr);
            gms.passDown(new Event(Event.MSG, msg));
        }
    }

    /** Removed rejected merge requests from merge_rsps and coords */
    void removeRejectedMergeRequests(Vector coords) {
        MergeData data;
        for(Iterator it=merge_rsps.iterator(); it.hasNext();) {
            data=(MergeData)it.next();
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
        Vector coords=null; // list of subgroup coordinators to be contacted

        public void start(Vector coords) {
            if(t == null) {
                this.coords=(Vector)(coords != null? coords.clone() : null);
                t=new Thread(this, "MergeTask thread");
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
            coords=null;
        }

        public boolean isRunning() {
            return t != null && t.isAlive();
        }

        /**
         * Runs the merge protocol as a leader
         */
        public void run() {
            MergeData combined_merge_data=null;

            if(merging == true) {
                if(log.isWarnEnabled()) log.warn("merge is already in progress, terminating");
                return;
            }

            if(log.isDebugEnabled()) log.debug("merge task started");
            try {

                /* 1. Generate a merge_id that uniquely identifies the merge in progress */
                setMergeId(generateMergeId());

                /* 2. Fetch the current Views/Digests from all subgroup coordinators */
                getMergeDataFromSubgroupCoordinators(coords, gms.merge_timeout);

                /* 3. Remove rejected MergeData elements from merge_rsp and coords (so we'll send the new view only
                   to members who accepted the merge request) */
                removeRejectedMergeRequests(coords);

                if(merge_rsps.size() <= 1) {
                    if(log.isWarnEnabled())
                        log.warn("merge responses from subgroup coordinators <= 1 (" + merge_rsps + "). Cancelling merge");
                    sendMergeCancelledMessage(coords, merge_id);
                    return;
                }

                /* 4. Combine all views and digests into 1 View/1 Digest */
                combined_merge_data=consolidateMergeData(merge_rsps);
                if(combined_merge_data == null) {
                    if(log.isErrorEnabled()) log.error("combined_merge_data == null");
                    sendMergeCancelledMessage(coords, merge_id);
                    return;
                }

                /* 5. Send the new View/Digest to all coordinators (including myself). On reception, they will
                   install the digest and view in all of their subgroup members */
                sendMergeView(coords, combined_merge_data);
            }
            catch(Throwable ex) {
                if(log.isErrorEnabled()) log.error("exception=" + ex);
            }
            finally {
                merging=false;
                merge_leader=null;
                if(log.isDebugEnabled()) log.debug("merge task terminated");
                t=null;
            }
        }
    }


    private class MergeCanceller implements TimeScheduler.Task {
        private Object my_merge_id=null;
        private long timeout;
        private boolean cancelled=false;

        public MergeCanceller(Object my_merge_id, long timeout) {
            this.my_merge_id=my_merge_id;
            this.timeout=timeout;
        }

        public boolean cancelled() {
            return cancelled;
        }

        public void cancel() {
            cancelled=true;
        }

        public long nextInterval() {
            return timeout;
        }

        public void run() {
            if(merge_id != null && my_merge_id.equals(merge_id)) {
                if(log.isTraceEnabled())
                    log.trace("cancelling merge due to timer timeout (" + timeout + " ms)");
                cancelMerge();
                cancelled=true;
            }
            else {
                if(log.isTraceEnabled())
                    log.trace("timer kicked in after " + timeout + " ms, but no (or different) merge was in progress: " +
                              "merge_id=" + merge_id + ", my_merge_id=" + my_merge_id);
            }
        }
    }

}
