// $Id: CoordGmsImpl.java,v 1.3 2003/11/21 01:59:07 belaban Exp $

package org.jgroups.protocols.pbcast;


import org.jgroups.*;
import org.jgroups.log.Trace;
import org.jgroups.util.Promise;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Vector;




/**
 * Coordinator role of the Group MemberShip (GMS) protocol. Accepts JOIN and LEAVE requests and emits view changes
 * accordingly.
 * @author Bela Ban
 */
public class CoordGmsImpl extends GmsImpl {
    boolean      merging=false;
    boolean      leaving=false;
    Promise      leave_promise=null;
    MergeTask    merge_task=new MergeTask();
    Vector       merge_rsps=new Vector();
    // for MERGE_REQ/MERGE_RSP correlation, contains MergeData elements
    Serializable merge_id=null;

    public CoordGmsImpl(GMS g) {
        gms=g;
    }

    public void join(Address mbr) {
        wrongMethod("join");
    }

    /** The coordinator itself wants to leave the group */
    public void leave(Address mbr) {
        if(mbr == null) {
            Trace.error("CoordGmsImpl.leave()", "member's address is null !");
            return;
        }

        if(leave_promise == null)
            leave_promise=new Promise();
        else
            leave_promise.reset();

        if(mbr.equals(gms.local_addr))
            leaving=true;

        handleLeave(mbr, false); // regular leave
    }

    public void handleJoinResponse(JoinRsp join_rsp) {
        wrongMethod("handleJoinResponse");
    }

    public void handleLeaveResponse() {
        wrongMethod("handleLeaveResponse");
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
        Address leader=null;

        if(merging) {
            if(Trace.trace)
                Trace.warn("CoordGmsImpl.merge()",
                           "merge already in progress, discarded MERGE event");
            return;
        }

        if(other_coords == null) {
            Trace.warn("CoordGmsImpl.merge()",
                       "list of other coordinators is null. Will not start merge.");
            return;
        }

        if(other_coords.size() <= 1) {
            Trace.error("CoordGmsImpl.merge()",
                        "number of coordinators found is "
                        + other_coords.size() + "; will not perform merge");
            return;
        }

        /* Establish deterministic order, so that coords can elect leader */
        tmp=new Membership(other_coords);
        tmp.sort();
        leader=(Address)tmp.elementAt(0);
        Trace.info("CoordGmsImpl.merge()", "coordinators in merge protocol are: " + tmp);
        if(leader.equals(gms.local_addr)) {
            Trace.info("CoordGmsImpl.merge()",
                       "I (" + leader + ") will be the leader. Starting the merge task");
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
            Trace.error(
                    "CoordGmsImpl.handleMergeRequest()",
                    "sender == null; cannot send back a response");
            return;
        }
        if(merging) {
            Trace.error(
                    "CoordGmsImpl.handleMergeRequest()",
                    "merge already in progress");
            sendMergeRejectedResponse(sender);
            return;
        }
        merging=true;
        this.merge_id=(Serializable)merge_id;
        if(Trace.trace)
            Trace.info(
                    "CoordGmsImpl.handleMergeRequest()",
                    "sender=" + sender + ", merge_id=" + merge_id);

        digest=gms.getDigest();
        view=new View(gms.view_id.copy(), gms.members.getMembers());
        sendMergeResponse(sender, view, digest);
    }

    public void handleMergeResponse(MergeData data, Object merge_id) {
        if(data == null) {
            Trace.error(
                    "CoordGmsImpl.handleMergeResponse()",
                    "merge data is null");
            return;
        }
        if(merge_id == null || this.merge_id == null) {
            Trace.error(
                    "CoordGmsImpl.handleMergeResponse()",
                    "merge_id ("
                    + merge_id
                    + ") or this.merge_id ("
                    + this.merge_id
                    + ") == null (sender="
                    + data.getSender()
                    + ").");
            return;
        }

        if(!this.merge_id.equals(merge_id)) {
            Trace.error(
                    "CoordGmsImpl.handleMergeResponse()",
                    "this.merge_id ("
                    + this.merge_id
                    + ") is different from merge_id ("
                    + merge_id
                    + ")");
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
            Trace.error(
                    "CoordGmsImpl.handleMergeView()",
                    "merge_ids don't match (or are null); merge view discarded");
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
            if(Trace.trace)
                Trace.info(
                        "CoordGmsImpl.handleMergeCancelled()",
                        "merge was cancelled (merge_id=" + merge_id + ")");
            this.merge_id=null;
            merging=false;
        }
    }

    /**
     * Computes the new view (including the newly joined member) and get the digest from PBCAST.
     * Returns both in the form of a JoinRsp
     */
    public synchronized JoinRsp handleJoin(Address mbr) {
        Vector new_mbrs=new Vector();
        View v=null;
        Digest d, tmp;

        if(Trace.trace)
            Trace.info("CoordGmsImpl.handleJoin()", "mbr=" + mbr);

        if(gms.local_addr.equals(mbr)) {
            Trace.error("CoordGmsImpl.handleJoin()", "cannot join myself !");
            return null;
        }

        if(gms.members.contains(mbr)) {
            if(Trace.trace)
                Trace.error("CoordGmsImpl.handleJoin()", "member " + mbr
                        + " already present; returning existing view " + gms.members.getMembers());
            return new JoinRsp(new View(gms.view_id, gms.members.getMembers()), gms.getDigest());
            // already joined: return current digest and membership
        }
        new_mbrs.addElement(mbr);
        tmp=gms.getDigest(); // get existing digest
        if(tmp == null) {
            Trace.error("CoordGmsImpl.handleJoin()", "received null digest from GET_DIGEST: will cause JOIN to fail");
            return null;
        }
        if(Trace.trace)
            Trace.info("CoordGmsImpl.handleJoin()", "got digest=" + tmp);

        d=new Digest(tmp.size() + 1);
        // create a new digest, which contains 1 more member
        d.add(tmp); // add the existing digest to the new one
        d.add(mbr, 0, 0);
        // ... and add the new member. it's first seqno will be 1
        v=gms.getNextView(new_mbrs, null, null);
        if(Trace.trace)
            Trace.debug("CoordGmsImpl.handleJoin()", "joined member " + mbr + ", view is " + v);
        return new JoinRsp(v, d);
    }

    /**
     Exclude <code>mbr</code> from the membership. If <code>suspected</code> is true, then
     this member crashed and therefore is forced to leave, otherwise it is leaving voluntarily.
     */
    public synchronized void handleLeave(Address mbr, boolean suspected) {
        Vector v=new Vector();
        // contains either leaving mbrs or suspected mbrs

        if(Trace.trace)
            Trace.info("CoordGmsImpl.handleLeave()", "mbr=" + mbr);
        if(!gms.members.contains(mbr)) {
            if(Trace.trace)
                Trace.error(
                        "CoordGmsImpl.handleLeave()",
                        "mbr " + mbr + " is not a member !");
            return;
        }
        v.addElement(mbr);
        if(suspected)
            gms.castViewChange(null, null, v);
        else
            gms.castViewChange(null, v, null);
    }

    /**
     * Called by the GMS when a VIEW is received.
     * @param new_view The view to be installed
     * @param digest   If view is a MergeView, digest contains the seqno digest of all members and has to
     *                 be set by GMS
     */
    public void handleViewChange(View new_view, Digest digest) {
        Vector mbrs=new_view.getMembers();
        if(Trace.trace) {
            if(digest != null)
                Trace.info(
                        "CoordGmsImpl.handleViewChange()",
                        "view=" + new_view + ", digest=" + digest);
            else
                Trace.info(
                        "CoordGmsImpl.handleViewChange()",
                        "view=" + new_view);
        }
        if(leaving && !mbrs.contains(gms.local_addr)) {
            if(leave_promise != null) {
                leave_promise.setResult(Boolean.TRUE);
            }
            return;
        }
        gms.installView(new_view, digest);
    }

    public void handleSuspect(Address mbr) {
        if(mbr.equals(gms.local_addr)) {
            Trace.warn(
                    "CoordGmsImpl.handleSuspect()",
                    "I am the coord and I'm being am suspected -- will probably leave shortly");
            return;
        }
        handleLeave(mbr, true); // irregular leave - forced
    }

    public void stop() {
        merge_task.stop();
    }

    /* ------------------------------------------ Private methods ----------------------------------------- */

    void startMergeTask(Vector coords) {
        merge_task.start(coords);
    }

    void stopMergeTask() {
        merge_task.stop();
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
            Trace.error(
                    "CoordGmsImpl.getMergeDataFromSubgroupCoordinators()",
                    "coords == null or size <= 1");
            return;
        }

        synchronized(merge_rsps) {
            merge_rsps.removeAllElements();
            if(Trace.trace)
                Trace.info(
                        "CoordGmsImpl.getMergeDataFromSubgroupCoordinators()",
                        "sending MERGE_REQ to " + coords);
            for(int i=0; i < coords.size(); i++) {
                coord=(Address)coords.elementAt(i);
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
                if(Trace.trace)
                    Trace.info(
                            "CoordGmsImpl.getMergeDataFromSubgroupCoordinators()",
                            "waiting for "
                            + time_to_wait
                            + " msecs for merge responses");
                try {
                    merge_rsps.wait(time_to_wait);
                }
                catch(Exception ex) {
                }
                if(Trace.trace)
                    Trace.info("CoordGmsImpl.getMergeDataFromSubgroupCoordinators()",
                               "num_rsps_expected=" + num_rsps_expected
                               + ", actual responses=" + merge_rsps.size());

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
        Vector subgroups=new Vector();
        // contains a list of Views, each View is a subgroup

        for(int i=0; i < v.size(); i++) {
            tmp_data=(MergeData)v.elementAt(i);
            if(Trace.trace)
                Trace.info(
                        "CoordGmsImpl.consolidateMergeData()",
                        "merge data is " + tmp_data);
            tmp_view=tmp_data.getView();
            if(tmp_view != null) {
                tmp_vid=tmp_view.getVid();
                if(tmp_vid != null) {
                    // compute the new view id (max of all vids +1)
                    logical_time=Math.max(logical_time, tmp_vid.getId());
                }
            }
            // merge all membership lists into one (prevent duplicates)
            new_mbrs.add(tmp_view.getMembers());
            subgroups.addElement(tmp_view.clone());
        }

        // the new coordinator is the first member of the consolidated & sorted membership list
        new_mbrs.sort();
        num_mbrs=new_mbrs.size();
        new_coord=num_mbrs > 0? (Address)new_mbrs.elementAt(0) : null;
        if(new_coord == null) {
            Trace.error(
                    "CoordGmsImpl.consolidateMergeData()",
                    "new_coord == null");
            return null;
        }
        new_vid=new ViewId(new_coord, logical_time + 1);

        // determine the new view
        new_view=new MergeView(new_vid, new_mbrs.getMembers(), subgroups);
        if(Trace.trace)
            Trace.info(
                    "CoordGmsImpl.consolidateMergeData()",
                    "new merged view will be " + new_view);

        // determine the new digest
        new_digest=consolidateDigests(v, num_mbrs);
        if(new_digest == null) {
            Trace.error(
                    "CoordGmsImpl.consolidateMergeData()",
                    "digest could not be consolidated");
            return null;
        }
        if(Trace.trace)
            Trace.info(
                    "CoordGmsImpl.consolidateMergeData()",
                    "consolidated digest=" + new_digest);

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
                Trace.error(
                        "CoordGmsImpl.consolidateDigests()",
                        "tmp_digest == null; skipping");
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
            Trace.error(
                    "CoordGmsImpl.sendMergeView()",
                    "view or digest is null, cannot send consolidated merge view/digest");
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
        if(Trace.trace)
            Trace.info("CoordGmsImpl.sendMergeResponse()", "response=" + hdr);
        gms.passDown(new Event(Event.MSG, msg));
    }

    void sendMergeRejectedResponse(Address sender) {
        Message msg=new Message(sender, null, null);
        GMS.GmsHeader hdr=new GMS.GmsHeader(GMS.GmsHeader.MERGE_RSP);
        hdr.merge_rejected=true;
        hdr.merge_id=merge_id;
        msg.putHeader(gms.getName(), hdr);
        if(Trace.trace)
            Trace.info(
                    "CoordGmsImpl.sendMergeRejectedResponse()",
                    "response=" + hdr);
        gms.passDown(new Event(Event.MSG, msg));
    }

    void sendMergeCancelledMessage(Vector coords, Serializable merge_id) {
        Message msg;
        GMS.GmsHeader hdr;
        Address coord;

        if(coords == null || merge_id == null) {
            Trace.error(
                    "CoordGmsImpl.sendMergeCancelledMessage()",
                    "coords or merge_id == null");
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
                if(Trace.trace)
                    Trace.info(
                            "CoordGmsImpl.removeRejectedMergeRequests()",
                            "removed element " + data);
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
                this.coords=coords;
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
                Trace.warn(
                        "CoordGmsImpl.MergeTask.run()",
                        "merge is already in progress, terminating");
                return;
            }

            if(Trace.trace)
                Trace.info(
                        "CoordGmsImpl.MergeTask.run()",
                        "merge task started");
            try {

                /* 1. Generate a merge_id that uniquely identifies the merge in progress */
                merge_id=generateMergeId();

                /* 2. Fetch the current Views/Digests from all subgroup coordinators */
                getMergeDataFromSubgroupCoordinators(coords, gms.merge_timeout);

                /* 3. Remove rejected MergeData elements from merge_rsp and coords (so we'll send the new view only
                   to members who accepted the merge request) */
                removeRejectedMergeRequests(coords);

                if(merge_rsps.size() <= 1) {
                    Trace.warn(
                            "CoordGmsImpl.MergeTask.run()",
                            "merge responses from subgroup coordinators <= 1 (" +
                            merge_rsps + "). Cancelling merge");
                    sendMergeCancelledMessage(coords, merge_id);
                    return;
                }

                /* 4. Combine all views and digests into 1 View/1 Digest */
                combined_merge_data=consolidateMergeData(merge_rsps);
                if(combined_merge_data == null) {
                    Trace.error(
                            "CoordGmsImpl.MergeTask.run()",
                            "combined_merge_data == null");
                    sendMergeCancelledMessage(coords, merge_id);
                    return;
                }

                /* 5. Send the new View/Digest to all coordinators (including myself). On reception, they will
                   install the digest and view in all of their subgroup members */
                sendMergeView(coords, combined_merge_data);
            }
            catch(Throwable ex) {
                Trace.error("CoordGmsImpl.MergeTask.run()", "exception=" + ex);
            }
            finally {
                merging=false;
                if(Trace.trace)
                    Trace.info(
                            "CoordGmsImpl.MergeTask.run()",
                            "merge task terminated");
                t=null;
            }
        }

    }

}
