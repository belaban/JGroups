// $Id: CoordGmsImpl.java,v 1.2 2004/01/08 02:39:56 belaban Exp $

package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.View;
import org.jgroups.ViewId;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.blocks.MethodCall;
import org.jgroups.log.Trace;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Vector;


public class CoordGmsImpl extends GmsImpl {
    boolean leaving=false;
    boolean received_last_view=false;
    Object leave_mutex=new Object();


    public CoordGmsImpl(GMS g) {
        gms=g;
    }


    public void init() {
        leaving=false;
        received_last_view=false;
    }


    public void join(Address mbr) {
        wrongMethod("join");
    }


    /**
     * The coordinator itself wants to leave the group
     */
    public void leave(Address mbr) {
        if(mbr.equals(gms.local_addr))
            leaving=true;

        handleLeave(mbr, false);    // regular leave
        synchronized(leave_mutex) {
            if(leaving && received_last_view)  // handleViewChange() has acquired leave_mutex before us...
                return;
            try {
                leave_mutex.wait(gms.leave_timeout); // will be notified by handleViewChange()
            }
            catch(Exception e) {
            }
        }
    }


    public void suspect(Address mbr) {
        handleSuspect(mbr);
    }


    /**
     * Invoked upon receiving a MERGE event from the MERGE layer. We have found a partition and
     * should merge with them, then I will become a Participant.
     * 
     * @param other_coords A list of other coordinators found. In the current implementation the list
     *                     only has a single element
     */
    public void merge(Vector other_coords) {
        View new_view=null;
        Address other_coord=other_coords != null? (Address)other_coords.elementAt(0) : null;

        if(Trace.trace) Trace.info("CoordGmsImpl.merge()", "other_coord = " + other_coord);
        try {
            MethodCall call=new MethodCall("handleMerge", new Object[]{gms.view_id, gms.members.getMembers()},
                    new String[]{ViewId.class.getName(), Vector.class.getName()});
            new_view=(View)gms.callRemoteMethod(other_coord, call, GroupRequest.GET_ALL, 0);
        }
        catch(Exception ex) {
            if(Trace.trace) Trace.error("CoordGmsImpl.merge()", "timed out or was suspected");
            return;
        }
        if(new_view == null) {
            if(Trace.trace) Trace.warn("CoordGmsImpl.merge()", "received a Merge Denied");
            gms.passDown(new Event(Event.MERGE_DENIED));
            return; //Merge denied
        }

        //Flushing my old view
        gms.flush(gms.members.getMembers(), null);
        MethodCall call=new MethodCall("handleViewChange", new Object[]{new_view.getVid(), new_view.getMembers()},
                new String[]{ViewId.class.getName(), Vector.class.getName()});
        gms.callRemoteMethods(gms.members.getMembers(), call, GroupRequest.GET_ALL, 0);
        gms.becomeParticipant();
        if(Trace.trace) Trace.info("CoordGmsImpl.merge()", "merge done");
    }


    public synchronized boolean handleJoin(Address mbr) {
        Vector new_mbrs=new Vector();

        if(Trace.trace)
            Trace.info("CoordGmsImpl.handleJoin()", "received JOIN request from " + mbr);

        if(gms.local_addr.equals(mbr)) {
            Trace.error("CoordGmsImpl.handleJoin()", "cannot join myself !");
            return false;
        }
        if(gms.members.contains(mbr)) {
            if(Trace.trace)
                Trace.warn("CoordGmsImpl.handleJoin()", "member " + mbr + " already present !");
            return true;  // already joined
        }

        new_mbrs.addElement(mbr);
        gms.castViewChange(new_mbrs, null, null);
        return true;
    }


    /**
     * Exclude <code>mbr</code> from the membership. If <code>suspected</code> is true, then
     * this member crashed and therefore is forced to leave, otherwise it is leaving voluntarily.
     */
    public synchronized void handleLeave(Address mbr, boolean suspected) {
        Vector v=new Vector();  // contains either leaving mbrs or suspected mbrs
        if(!gms.members.contains(mbr)) {
            if(Trace.trace) Trace.error("CoordGmsImpl.handleLeave()", "mbr " + mbr + " is not a member !");
            return;
        }
        v.addElement(mbr);
        if(suspected)
            gms.castViewChange(null, null, v);
        else
            gms.castViewChange(null, v, null);
    }


    public void handleViewChange(ViewId new_view, Vector mbrs) {
        if(leaving) {
            if(mbrs.contains(gms.local_addr)) {
                Trace.warn("CoordGmsImpl.handleViewChange()",
                        "received view in which I'm still a member, cannot quit yet");
                gms.installView(new_view, mbrs);  // +++ modify
            }
            else {
                synchronized(leave_mutex) {
                    received_last_view=true;
                    leave_mutex.notify();
                }
            }
            return;
        }
        gms.installView(new_view, mbrs);  // +++ modify
    }


    /**
     * Invoked by another coordinator that asks to merge its view with mine.
     * I 'll be the new coordinator.
     * We should flush our view, install a new view with all the members and
     * return the new view that will be installed by the other coordinator before
     * becoming a participant.
     */
    public synchronized View handleMerge(ViewId other_vid, Vector other_mbrs) {
        if(Trace.trace)
            Trace.info("CoordGmsImpl.handleMerge()",
                    "other_vid=" + other_vid + " , other_mbrs=" + other_mbrs);

        //Check that the views are disjoint otherwire return null (means MERGE_DENIED)
        for(Iterator i=other_mbrs.iterator(); i.hasNext();) {
            if(gms.members.contains((Address)i.next())) {
                gms.passDown(new Event(Event.MERGE_DENIED));
                return null;
            }
        }

        //Compute new View
        ViewId vid=new ViewId(gms.local_addr, Math.max(other_vid.getId() + 1, gms.ltime + 1));
        HashSet members=new HashSet(gms.members.getMembers());
        members.addAll(other_mbrs);
        Vector new_mbrs=new Vector(members);
        Collections.sort(new_mbrs);
        View new_view=new View(vid, new_mbrs);

        //Flush my view
        gms.flush(gms.members.getMembers(), null);

        //Install new view
        MethodCall call=new MethodCall("handleViewChange", new Object[]{vid, new_mbrs},
                new String[]{ViewId.class.getName(), Vector.class.getName()});
        gms.callRemoteMethods(gms.members.getMembers(), call, GroupRequest.GET_ALL, 0);
        return new_view;
    }


    public void handleSuspect(Address mbr) {
        if(mbr.equals(gms.local_addr)) {
            Trace.error("CoordGmsImpl.handleSuspect()", "I am the coord and am suspected: am not quitting !");
            return;
        }
        handleLeave(mbr, true); // irregular leave - forced
    }


}
