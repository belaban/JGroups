
package org.jgroups.protocols.pbcast;

import org.jgroups.*;
import org.jgroups.util.Digest;
import org.jgroups.util.Promise;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;


/**
 * @author Bela Ban
 */
public class ParticipantGmsImpl extends ServerGmsImpl {
    private final List<Address>     suspected_mbrs=new ArrayList<>(11);
    private final Promise<Boolean>  leave_promise=new Promise<>();


    public ParticipantGmsImpl(GMS g) {
        super(g);
    }


    public void init() throws Exception {
        super.init();
        suspected_mbrs.clear();
        leave_promise.reset();
    }

    public void join(Address mbr, boolean useFlushIfPresent) {
        wrongMethod("join");
    }

    public void joinWithStateTransfer(Address mbr,boolean useFlushIfPresent) {
        wrongMethod("join");
    }


    /**
     * Loop: determine coord. If coord is me --> handleLeave().
     * Else send handleLeave() to coord until success
     */
    public void leave(Address mbr) {
        leave_promise.reset();
        if(mbr.equals(gms.local_addr))
            leaving=true;

        Address coord=gms.determineCoordinator();
        if(coord != null) {
            if(gms.local_addr.equals(coord)) { // I'm the coordinator
                gms.becomeCoordinator();
                gms.getImpl().leave(mbr);      // regular leave
                return;
            }

            log.trace("%s: sending LEAVE request to %s", gms.local_addr, coord);
            sendLeaveMessage(coord, mbr);
            Boolean result=leave_promise.getResult(gms.leave_timeout);
            if(result != null)
                log.trace("%s: got LEAVE response from %s", gms.local_addr, coord);
        }
        gms.becomeClient();
    }


    /** In case we get a different JOIN_RSP from a previous JOIN_REQ sent by us (as a client), we simply apply the
     * new view if it is greater than ours
     *
     * @param join_rsp
     */
    public void handleJoinResponse(JoinRsp join_rsp) {
        View v=join_rsp.getView();
        ViewId tmp_vid=v != null? v.getViewId() : null;
        ViewId my_view=gms.getViewId();
        if(tmp_vid != null && my_view != null && tmp_vid.compareToIDs(my_view) > 0) {
            Digest d=join_rsp.getDigest();
            gms.installView(v, d);
        }
    }

    public void handleLeaveResponse() {
        leave_promise.setResult(true);  // unblocks thread waiting in leave()
    }


    public void suspect(Address mbr) {
        Collection<Request> suspected=new LinkedHashSet<>(1);
        suspected.add(new Request(Request.SUSPECT,mbr,true));
        handleMembershipChange(suspected);
    }


    /** Removes previously suspected member from list of currently suspected members */
    public void unsuspect(Address mbr) {
        if(mbr != null)
            suspected_mbrs.remove(mbr);
    }


    public void handleMembershipChange(Collection<Request> requests) {
        Collection<Address> suspectedMembers=requests.stream().filter(req -> req.type == Request.SUSPECT)
          .collect(() -> new LinkedHashSet(requests.size()), (list,req) -> list.add(req.mbr), (l,r) -> {});

        if(suspectedMembers.isEmpty())
            return;
        suspectedMembers.stream().filter(mbr -> !suspected_mbrs.contains(mbr)).forEach(suspected_mbrs::add);

        if(wouldIBeCoordinator()) {
            log.debug("%s: members are %s, coord=%s: I'm the new coord !", gms.local_addr, gms.members, gms.local_addr);

            gms.becomeCoordinator();
            suspected_mbrs.forEach(mbr -> {
                gms.getViewHandler().add(new Request(Request.SUSPECT, mbr, true));
                gms.ack_collector.suspect(mbr);
            });
            suspected_mbrs.clear();
        }
    }


    /**
     * If we are leaving, we have to wait for the view change (last msg in the current view) that
     * excludes us before we can leave.
     * @param new_view The view to be installed
     * @param digest   If view is a MergeView, digest contains the seqno digest of all members and has to
     *                 be set by GMS
     */
    public void handleViewChange(View new_view, Digest digest) {
        suspected_mbrs.clear();
        if(leaving && !new_view.containsMember(gms.local_addr)) // received a view in which I'm not member: ignore
            return;
        gms.installView(new_view, digest);
    }



    /* ---------------------------------- Private Methods --------------------------------------- */

    /**
     * Determines whether this member is the new coordinator given a list of suspected members.  This is
     * computed as follows: the list of currently suspected members (suspected_mbrs) is removed from the current
     * membership. If the first member of the resulting list is equals to the local_addr, then it is true,
     * otherwise false. Example: own address is B, current membership is {A, B, C, D}, suspected members are {A,
     * D}. The resulting list is {B, C}. The first member of {B, C} is B, which is equal to the
     * local_addr. Therefore, true is returned.
     */
    boolean wouldIBeCoordinator() {
        List<Address> mbrs=gms.computeNewMembership(gms.members.getMembers(), null, null, suspected_mbrs);
        if(mbrs.isEmpty()) return false;
        Address new_coord=mbrs.get(0);
        return gms.local_addr.equals(new_coord);
    }


    void sendLeaveMessage(Address coord, Address mbr) {
        Message msg=new Message(coord).setFlag(Message.Flag.OOB)
          .putHeader(gms.getId(), new GMS.GmsHeader(GMS.GmsHeader.LEAVE_REQ, mbr));
        gms.getDownProtocol().down(msg);
    }


    /* ------------------------------ End of Private Methods ------------------------------------ */

}
