package org.jgroups.protocols.pbcast;

import org.jgroups.Address;
import org.jgroups.EmptyMessage;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.util.Digest;
import org.jgroups.util.MergeId;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;

/**
 * Common super class for CoordGmsImpl and ParticipantGmsImpl
 * @author Bela Ban
 */
public abstract class ServerGmsImpl extends GmsImpl {


    protected ServerGmsImpl(GMS gms) {
        super(gms);
    }

    public void init() throws Exception {
        super.init();
    }

    /**
     * Invoked upon receiving a MERGE event from the MERGE layer. Starts the merge protocol.
     * See description of protocol in DESIGN.
     * @param views A List of <em>different</em> views detected by the merge protocol
     */
    public void merge(Map<Address, View> views) {
        merger.merge(views);
    }


    /**
     * Get the view and digest and send back both (MergeData) in the form of a MERGE_RSP to the sender.
     * If a merge is already in progress, send back a MergeData with the merge_rejected field set to true.
     * @param sender The address of the merge leader
     * @param merge_id The merge ID
     * @param mbrs The set of members from which we expect responses
     */
    public void handleMergeRequest(Address sender, MergeId merge_id, Collection<? extends Address> mbrs) {
        merger.handleMergeRequest(sender, merge_id, mbrs);
    }

    public void handleMergeResponse(MergeData data, MergeId merge_id) {
        merger.handleMergeResponse(data, merge_id);
    }

    public void handleMergeCancelled(MergeId merge_id) {
        merger.handleMergeCancelled(merge_id);
    }

    /**
     * Called by the GMS when a VIEW is received.
     * @param view The view to be installed
     * @param digest   If view is a MergeView, the digest contains the seqnos of all members and has to be set by GMS
     */
    public void handleViewChange(View view, Digest digest) {
        if(gms.isLeaving() && !view.containsMember(gms.local_addr))
            return;
        View prev_view=gms.view();
        gms.installView(view, digest);
        Address prev_coord=prev_view != null? prev_view.getCoord() : null, curr_coord=view.getCoord();
        if(!Objects.equals(curr_coord, prev_coord))
            coordChanged(prev_coord, curr_coord);
    }

    public void handleMergeView(final MergeData data,final MergeId merge_id) {
        merger.handleMergeView(data, merge_id);
    }

    public void handleDigestResponse(Address sender, Digest digest) {
        merger.handleDigestResponse(sender, digest);
    }

    protected void coordChanged(Address from, Address to) {}
}
