
package org.jgroups.protocols.pbcast;

import org.jgroups.*;
import org.jgroups.util.Promise;
import org.jgroups.util.Digest;
import org.jgroups.util.MergeId;

import java.util.Vector;
import java.util.Collection;
import java.util.LinkedHashSet;


/**
 * @author Bela Ban
 * @version $Id: ParticipantGmsImpl.java,v 1.37 2009/09/28 07:16:55 belaban Exp $
 */
public class ParticipantGmsImpl extends ServerGmsImpl {
    private final Vector<Address>   suspected_mbrs=new Vector<Address>(11);
    private final Promise<Boolean>  leave_promise=new Promise<Boolean>();


    public ParticipantGmsImpl(GMS g) {
        super(g);
    }


    public void init() throws Exception {
        super.init();
        suspected_mbrs.removeAllElements();
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
        Address coord;
        int max_tries=3;
        Boolean result;

        leave_promise.reset();

        if(mbr.equals(gms.local_addr))
            leaving=true;

        while((coord=gms.determineCoordinator()) != null && max_tries-- > 0) {
            if(gms.local_addr.equals(coord)) {            // I'm the coordinator
                gms.becomeCoordinator();
                // gms.getImpl().handleLeave(mbr, false);    // regular leave
                gms.getImpl().leave(mbr);    // regular leave
                return;
            }

            if(log.isDebugEnabled()) log.debug("sending LEAVE request to " + coord + " (local_addr=" + gms.local_addr + ")");
            sendLeaveMessage(coord, mbr);
            result=leave_promise.getResult(gms.leave_timeout);
            if(result != null)
                break;
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
        ViewId tmp_vid=v != null? v.getVid() : null;
        if(tmp_vid != null && gms.view_id != null && tmp_vid.compareTo(gms.view_id) > 0) {
            gms.installView(v);
        }
    }

    public void handleLeaveResponse() {       
        leave_promise.setResult(true);  // unblocks thread waiting in leave()
    }


    public void suspect(Address mbr) {
        Collection<Request> suspected=new LinkedHashSet<Request>(1);
        suspected.add(new Request(Request.SUSPECT,mbr,true));
        handleMembershipChange(suspected);
    }


    /** Removes previously suspected member from list of currently suspected members */
    public void unsuspect(Address mbr) {
        if(mbr != null)
            suspected_mbrs.remove(mbr);
    }


    public void handleMembershipChange(Collection<Request> requests) {
        Collection<Address> suspectedMembers=new LinkedHashSet<Address>(requests.size());
        for(Request req: requests) {
            if(req.type == Request.SUSPECT)
                suspectedMembers.add(req.mbr);
        }
        
        if(suspectedMembers.isEmpty())
            return;

        for(Address mbr: suspectedMembers) {
            if(!suspected_mbrs.contains(mbr))
                suspected_mbrs.addElement(mbr);
        }

        if(log.isDebugEnabled())
            log.debug("suspected members=" + suspectedMembers + ", suspected_mbrs=" + suspected_mbrs);

        if(wouldIBeCoordinator()) {
            if(log.isDebugEnabled())
                log.debug("members are " + gms.members + ", coord=" + gms.local_addr + ": I'm the new coord !");

            suspected_mbrs.removeAllElements();
            gms.becomeCoordinator();
            for(Address mbr: suspectedMembers) {
                gms.getViewHandler().add(new Request(Request.SUSPECT, mbr, true));
                gms.ack_collector.suspect(mbr);
            }
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
        Vector<Address> mbrs=new_view.getMembers();
        suspected_mbrs.removeAllElements();
        if(leaving && !mbrs.contains(gms.local_addr)) { // received a view in which I'm not member: ignore
            return;
        }
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
        Address new_coord;
        Vector<Address> mbrs=gms.members.getMembers(); // getMembers() returns a *copy* of the membership vector
        mbrs.removeAll(suspected_mbrs);
        if(mbrs.size() < 1) return false;
        new_coord=mbrs.firstElement();
        return gms.local_addr.equals(new_coord);
    }


    void sendLeaveMessage(Address coord, Address mbr) {
        Message msg=new Message(coord, null, null);
        msg.setFlag(Message.OOB);
        GMS.GmsHeader hdr=new GMS.GmsHeader(GMS.GmsHeader.LEAVE_REQ, mbr);

        msg.putHeader(gms.getName(), hdr);
        gms.getDownProtocol().down(new Event(Event.MSG, msg));
    }


    /* ------------------------------ End of Private Methods ------------------------------------ */

}
