// $Id: ParticipantGmsImpl.java,v 1.6 2004/09/22 10:34:11 belaban Exp $

package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.View;
import org.jgroups.ViewId;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.blocks.MethodCall;

import java.util.Vector;


public class ParticipantGmsImpl extends GmsImpl {
    boolean leaving=false;
    boolean received_final_view=false;
    Object leave_mutex=new Object();
    Vector suspected_mbrs=new Vector(5);
    Vector new_mbrs=new Vector(11);


    public ParticipantGmsImpl(GMS g) {
        gms=g;
        init();
    }


    public void init() {
        leaving=false;
        received_final_view=false;
        suspected_mbrs.removeAllElements();
        new_mbrs.removeAllElements();
        if(gms.members != null)
            for(int i=0; i < gms.members.size(); i++)
                new_mbrs.addElement(gms.members.elementAt(i));
    }


    public void join(Address mbr) {
        wrongMethod("join");
    }


    /**
     Loop: determine coord. If coord is me --> handleLeave(). Else send handleLeave() to coord until
     success.
     */
    public void leave(Address mbr) {
        Address coord;
        Boolean retval=null;

        if(mbr.equals(gms.local_addr))
            leaving=true;


        while((coord=gms.determineCoordinator()) != null && !received_final_view) {
            synchronized(leave_mutex) {
                if(gms.local_addr.equals(coord)) {  // I'm the coordinator
                    gms.becomeCoordinator();
                    gms.handleLeave(mbr, false);    // regular leave
                }
                else {
                    try {

                            if(log.isInfoEnabled()) log.info("sending LEAVE request to " + coord);
                        MethodCall call = new MethodCall("handleLeave", new Object[] {mbr, Boolean.FALSE}, 
                            new String[] {Address.class.getName(), boolean.class.getName()});
                        gms.callRemoteMethod(coord, call, GroupRequest.GET_NONE, 0);  // asynchronous
                    }
                    catch(Exception e) {
                    }
                }

                try {
                    leave_mutex.wait(gms.leave_timeout);
                }
                catch(Exception e) {
                }
            }
        }
        gms.becomeClient();
    }


    public void suspect(Address mbr) {
        handleSuspect(mbr);
    }


    public void merge(Vector other_coords) {
        wrongMethod("merge");
    }


    public boolean handleJoin(Address mbr) {
        wrongMethod("handleJoin");
        return false;
    }


    public void handleLeave(Address mbr, boolean suspected) {
        wrongMethod("handleLeave");
    }


    /**
     If we are leaving, we have to wait for the view change (last msg in the current view) that
     excludes us before we can leave.
     */
    public void handleViewChange(ViewId new_view, Vector mbrs) {
         if(log.isInfoEnabled()) log.info("mbrs are " + mbrs);

        suspected_mbrs.removeAllElements();
        new_mbrs.removeAllElements();
        if(mbrs != null)
            for(int i=0; i < mbrs.size(); i++)
                new_mbrs.addElement(mbrs.elementAt(i));

        if(leaving) {
            if(mbrs.contains(gms.local_addr)) {
                if(log.isWarnEnabled()) log.warn("received view in " +
                                                                    "which I'm still a member, cannot quit yet");
                gms.installView(new_view, mbrs);
            }
            else {
                synchronized(leave_mutex) {
                    received_final_view=true;
                    leave_mutex.notifyAll();
                }
            }
            return;
        }
        gms.installView(new_view, mbrs);
    }


    // Only coordinators Handle Merge requests.
    public View handleMerge(ViewId other_view, Vector other_members) {
        wrongMethod("handleMerge");
        return null;
    }

    public void handleSuspect(Address mbr) {
        Vector suspects=null;

        if(mbr == null) return;
        if(!suspected_mbrs.contains(mbr))
            suspected_mbrs.addElement(mbr);
        new_mbrs.removeElement(mbr);


            if(log.isInfoEnabled()) log.info("suspected mbr=" + mbr +
                                                             "\nsuspected_mbrs=" + suspected_mbrs + "\nnew_mbrs=" + new_mbrs);

        if(new_mbrs.size() > 0 && new_mbrs.elementAt(0).equals(gms.local_addr)) {

                if(log.isInfoEnabled()) log.info("suspected mbr=" + mbr + ", members are " + gms.members +
                           ", coord=" + gms.local_addr + ": I'm the new coord !");

            suspects=(Vector) suspected_mbrs.clone();
            suspected_mbrs.removeAllElements();
            new_mbrs.removeAllElements();
            gms.becomeCoordinator();
            gms.castViewChange(null, null, suspects);
        }
    }


}
