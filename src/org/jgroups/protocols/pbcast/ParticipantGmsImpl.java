// $Id: ParticipantGmsImpl.java,v 1.3 2003/11/21 19:43:30 belaban Exp $

package org.jgroups.protocols.pbcast;

import java.util.Vector;

import org.jgroups.*;
import org.jgroups.util.*;
import org.jgroups.log.Trace;


public class ParticipantGmsImpl extends GmsImpl {
    boolean    leaving=false;
    Vector     suspected_mbrs=new Vector();
    Promise    leave_promise=new Promise();


    public ParticipantGmsImpl(GMS g) {
        gms=g;
        suspected_mbrs.removeAllElements();
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
                gms.getImpl().handleLeave(mbr, false);    // regular leave
                return;
            }
            if(Trace.trace)
                Trace.info("ParticipantGmsImpl.leave()", "sending LEAVE request to " + coord);
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
        wrongMethod("handleJoinResponse");
    }

    public void handleLeaveResponse() {
        if(leave_promise == null) {
            Trace.error("ParticipantGmsImpl.handleLeaveResponse()", "leave_promise is null");
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


    public JoinRsp handleJoin(Address mbr) {
        wrongMethod("handleJoin");
        return null;
    }


    public void handleLeave(Address mbr, boolean suspected) {
        wrongMethod("handleLeave");
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
        if(Trace.trace) Trace.info("ParticipantGmsImpl.handleViewChange()", "view=" + new_view);
        suspected_mbrs.removeAllElements();
        if(leaving && !mbrs.contains(gms.local_addr)) { // received a view in which I'm not member: ignore
            return;
        }
        gms.installView(new_view, digest);
    }


    public void handleSuspect(Address mbr) {
        Vector suspects=null;

        if(mbr == null) return;
        if(!suspected_mbrs.contains(mbr))
            suspected_mbrs.addElement(mbr);


        if(Trace.trace)
            Trace.info("ParticipantGmsImpl.handleSuspect()", "suspected mbr=" + mbr +
                                                             ", suspected_mbrs=" + suspected_mbrs);

        if(wouldIBeCoordinator()) {
            if(Trace.trace)
                Trace.info("ParticipantGmsImpl.handleSuspect()", "suspected mbr=" + mbr + "), members are " +
                                                                 gms.members + ", coord=" + gms.local_addr + ": I'm the new coord !");

            suspects=(Vector)suspected_mbrs.clone();
            suspected_mbrs.removeAllElements();
            gms.becomeCoordinator();
            gms.castViewChange(null, null, suspects);
        }
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
        Address new_coord=null;
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
