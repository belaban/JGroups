// $Id: ParticipantGmsImpl.java,v 1.17 2005/12/23 14:57:06 belaban Exp $

package org.jgroups.protocols.pbcast;

import org.jgroups.*;
import org.jgroups.util.Promise;

import java.util.Vector;


public class ParticipantGmsImpl extends GmsImpl {
    private final Vector     suspected_mbrs=new Vector(11);
    private final Promise    leave_promise=new Promise();


    public ParticipantGmsImpl(GMS g) {
        super(g);
    }


    public void init() throws Exception {
        super.init();
        suspected_mbrs.removeAllElements();
        leave_promise.reset();
    }

    public void join(Address mbr) {
        wrongMethod("join");
    }


    /**
     * Loop: determine coord. If coord is me --> handleLeave().
     * Else send handleLeave() to coord until success
     */
    public void leave(Address mbr) {
        Address coord;
        int max_tries=3;
        Object result;

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
            synchronized(leave_promise) {
                result=leave_promise.getResult(gms.leave_timeout);
                if(result != null)
                    break;
            }
        }
        gms.becomeClient();
    }


    public void handleJoinResponse(JoinRsp join_rsp) {
        // wrongMethod("handleJoinResponse");
    }

    public void handleLeaveResponse() {
        if(leave_promise == null) {
            if(log.isErrorEnabled()) log.error("leave_promise is null");
            return;
        }
        synchronized(leave_promise) {
            leave_promise.setResult(Boolean.TRUE);  // unblocks thread waiting in leave()
        }
    }


    public void suspect(Address mbr) {
        handleSuspect(mbr);
    }


    /** Removes previously suspected member from list of currently suspected members */
    public void unsuspect(Address mbr) {
        if(mbr != null)
            suspected_mbrs.remove(mbr);
    }


    public void handleJoin(Address mbr) {
    }


    public void handleLeave(Address mbr, boolean suspected) {
    }


    /**
     * If we are leaving, we have to wait for the view change (last msg in the current view) that
     * excludes us before we can leave.
     * @param new_view The view to be installed
     * @param digest   If view is a MergeView, digest contains the seqno digest of all members and has to
     *                 be set by GMS
     */
    public void handleViewChange(View new_view, Digest digest) {
        Vector mbrs=new_view.getMembers();
         if(log.isDebugEnabled()) log.debug("view=" + new_view);
        suspected_mbrs.removeAllElements();
        if(leaving && !mbrs.contains(gms.local_addr)) { // received a view in which I'm not member: ignore
            return;
        }
        gms.installView(new_view, digest);
    }


    public void handleSuspect(Address mbr) {
        if(mbr == null) return;
        if(!suspected_mbrs.contains(mbr))
            suspected_mbrs.addElement(mbr);
        
        if(log.isDebugEnabled()) log.debug("suspected mbr=" + mbr + ", suspected_mbrs=" + suspected_mbrs);

        if(wouldIBeCoordinator()) {
            if(log.isDebugEnabled()) log.debug("suspected mbr=" + mbr + "), members are " +
                    gms.members + ", coord=" + gms.local_addr + ": I'm the new coord !");

            suspected_mbrs.removeAllElements();
            gms.becomeCoordinator();
            // gms.getImpl().suspect(mbr);
            gms.view_handler.add(new GMS.Request(GMS.Request.SUSPECT, mbr, true, null));
            gms.ack_collector.suspect(mbr);
        }
    }

    public void handleMergeRequest(Address sender, ViewId merge_id) {
        // only coords handle this method; reject it if we're not coord
        sendMergeRejectedResponse(sender, merge_id);
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
        Vector mbrs=gms.members.getMembers(); // getMembers() returns a *copy* of the membership vector

        for(int i=0; i < suspected_mbrs.size(); i++)
            mbrs.removeElement(suspected_mbrs.elementAt(i));

        if(mbrs.size() < 1) return false;
        new_coord=(Address)mbrs.elementAt(0);
        return gms.local_addr.equals(new_coord);
    }


    void sendLeaveMessage(Address coord, Address mbr) {
        Message msg=new Message(coord, null, null);
        GMS.GmsHeader hdr=new GMS.GmsHeader(GMS.GmsHeader.LEAVE_REQ, mbr);

        msg.putHeader(gms.getName(), hdr);
        gms.passDown(new Event(Event.MSG, msg));
    }


    /* ------------------------------ End of Private Methods ------------------------------------ */

}
