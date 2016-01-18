package org.jgroups.protocols.pbcast;

import org.jgroups.Address;
import org.jgroups.util.MergeId;
import org.jgroups.util.Digest;

import java.util.Collection;

/**
 * Common super class for CoordGmsImpl and ParticipantGmsImpl
 * @author Bela Ban
 */
public abstract class AbstractServerGmsImpl extends AbstractGmsImpl {

    protected AbstractServerGmsImpl(GMS gms) {
        super(gms);
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

    /**
     * If merge_id is not equal to this.merge_id then discard.
     * Else cast the view/digest to all members of this group.
     */
    public void handleMergeView(final MergeData data,final MergeId merge_id) {
        merger.handleMergeView(data, merge_id);
    }

    public void handleDigestResponse(Address sender, Digest digest) {
        merger.handleDigestResponse(sender, digest);
    }
}
