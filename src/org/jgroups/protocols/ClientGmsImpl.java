// $Id: ClientGmsImpl.java,v 1.1 2003/09/09 01:24:09 belaban Exp $

package org.jgroups.protocols;


import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;
import org.jgroups.*;
import org.jgroups.log.Trace;
import org.jgroups.blocks.*;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.util.*;



/**
 * Client part of GMS. Whenever a new member wants to join a group, it starts in the CLIENT role.
 * No multicasts to the group will be received and processed until the member has been joined and
 * turned into a SERVER (either coordinator or participant, mostly just participant). This class
 * only implements <code>Join</code> (called by clients who want to join a certain group, and
 * <code>ViewChange</code> which is called by the coordinator that was contacted by this client, to
 * tell the client what its initial membership is.
 * @author Bela Ban
 * @version $Revision: 1.1 $
 */
public class ClientGmsImpl extends GmsImpl {
    Vector                   initial_mbrs=new Vector();
    Object                   view_installation_mutex=new Object();
    boolean                  joined=false;




    public ClientGmsImpl(GMS g) {
	gms=g;
    }



    public void init() {
	initial_mbrs.removeAllElements();
	joined=false;
    }



    /**
     * Will generate a CONNECT_OK event. Determines the coordinator and sends a unicast
     * join() message to it. If successful, we wait for a ViewChange (can time out).
     * If view change is received, impl is changed to an instance of ParticipantGmsImpl.
     * Otherwise, we continue trying to send join() messages to	the coordinator,
     * until we succeed (or there is no member in the group. In this case, we create
     * our own singleton group).
     * <p>When GMS.disable_initial_coord is set to true, then we won't become coordinator on receiving an initial
     * membership of 0, but instead will retry (forever) until we get an initial membership of > 0.
     * @param mbr Our own address (assigned through SET_LOCAL_ADDRESS)
     */
    public void join(Address mbr) {
	Address   coord=null;
	Event     view_evt;

	while(!joined) {
	    findInitialMembers();
	    if(joined) {
		if(Trace.trace) Trace.info("ClientGmsImpl.join()", "joined successfully");
		return;
	    }
	    if(initial_mbrs.size() == 0) {
		if(gms.disable_initial_coord) {
		    if(Trace.trace)
			Trace.info("ClientGmsImpl.join()", "received an initial membership of 0, but " +
				   "cannot become coordinator (disable_initial_coord=" + gms.disable_initial_coord +
				   "), will retry fetching the initial membership");
		    continue;
		}
		joined=true;
		gms.view_id=new ViewId(mbr);       // create singleton view with mbr as only member
		gms.members.add(mbr);
		view_evt=new Event(Event.VIEW_CHANGE,
				   gms.makeView(gms.members.getMembers(), gms.view_id));
		gms.passDown(view_evt);
		gms.passUp(view_evt);
		gms.becomeCoordinator();

		gms.passUp(new Event(Event.BECOME_SERVER));
		gms.passDown(new Event(Event.BECOME_SERVER));
		if(Trace.trace) Trace.info("ClientGmsImpl.join()", "created group (first member)");
		break;
	    }

	    coord=determineCoord(initial_mbrs);
	    if(coord == null) {
		Trace.warn("ClientGmsImpl.join()", "could not determine coordinator " +
			   "from responses " + initial_mbrs);
		continue;
	    }

	    synchronized(view_installation_mutex) {
		try {
		    if(Trace.trace) Trace.info("ClientGmsImpl.join()", "sending handleJoin() to " + coord);
		    MethodCall call = new MethodCall("handleJoin", new Object[] {mbr}, new String[] {Address.class.getName()});
		    gms.callRemoteMethod(coord, call, GroupRequest.GET_NONE, 0);
		    view_installation_mutex.wait(gms.join_timeout);  // wait for view -> handleView()
		}
		catch(Exception e) {
		    Trace.error("ClientGmsImpl.join()", "exception is " + e);
		    continue;
		}
	    } // end synchronized

	    if(joined) {
		if(Trace.trace) Trace.info("ClientGmsImpl.join()", "joined successfully");
		return;  // --> SUCCESS
	    }
	    else {
		if(Trace.trace) Trace.info("ClientGmsImpl.join()", "failed, retrying");
		Util.sleep(gms.join_retry_timeout);
	    }

	}  // end while
    }





    public void leave(Address mbr) {
	wrongMethod("leave");
    }



    public void suspect(Address mbr) {
	// wrongMethod("suspect");
    }



    public void merge(Vector other_coords) {
	wrongMethod("merge");
    }



    public boolean handleJoin(Address mbr) {
	wrongMethod("handleJoin");
	return false;
    }



    /** Returns false. Clients don't handle leave() requests */
    public void handleLeave(Address mbr, boolean suspected) {
	wrongMethod("handleLeave");
    }



    /**
     * Install the first view in which we are a member. This is essentially a confirmation
     * of our JOIN request (see join() above).
     */
    public void handleViewChange(ViewId new_view, Vector mems) {
	if(gms.local_addr != null && mems != null && mems.contains(gms.local_addr)) {
	    synchronized(view_installation_mutex) {  // wait until JOIN is sent (above)
		joined=true;
		view_installation_mutex.notify();
		gms.installView(new_view, mems);
		gms.becomeParticipant();
		gms.passUp(new Event(Event.BECOME_SERVER));
		gms.passDown(new Event(Event.BECOME_SERVER));
	    }
	    synchronized(initial_mbrs) {    // in case findInitialMembers() is still running:
		initial_mbrs.notifyAll();   // this will unblock it
	    }
	}
	else
	    if(Trace.trace) Trace.warn("ClientGmsImpl.handleViewChange()", 
				       "am not member of " + mems + ", will not install view");
    }




    /** Returns immediately. Clients don't handle merge() requests */
    public View handleMerge(ViewId other_view,Vector other_members) {
	wrongMethod("handleMerge");
	return null;
    }



    /** Returns immediately. Clients don't handle suspect() requests */
    public void handleSuspect(Address mbr) {
	wrongMethod("handleSuspect");
	return;
    }



    public boolean handleUpEvent(Event evt) {
	Vector tmp;

	switch(evt.getType()) {

	case Event.FIND_INITIAL_MBRS_OK:
	    tmp=(Vector)evt.getArg();
	    synchronized(initial_mbrs) {
		if(tmp != null && tmp.size() > 0)
		    for(int i=0; i < tmp.size(); i++)
			initial_mbrs.addElement(tmp.elementAt(i));
		initial_mbrs.notify();
	    }
	    return false;  // don't pass up the stack
	}

	return true;
    }





    /* --------------------------- Private Methods ------------------------------------ */







    /**
       Pings initial members. Removes self before returning vector of initial members.
       Uses IP multicast or gossiping, depending on parameters.
     */
    void findInitialMembers() {
	PingRsp ping_rsp;

	synchronized(initial_mbrs) {
	    initial_mbrs.removeAllElements();
	    gms.passDown(new Event(Event.FIND_INITIAL_MBRS));
	    try {initial_mbrs.wait();}
	    catch(Exception e) {}

	    for(int i=0; i < initial_mbrs.size(); i++) {
		ping_rsp=(PingRsp)initial_mbrs.elementAt(i);
		if(ping_rsp.own_addr != null && gms.local_addr != null &&
		   ping_rsp.own_addr.equals(gms.local_addr)) {
		    initial_mbrs.removeElementAt(i);
		    break;
		}
	    }
	}
    }



    /**
       The coordinator is determined by a majority vote. If there are an equal number of votes for
       more than 1 candidate, we determine the winner randomly.
    */
    Address determineCoord(Vector mbrs) {
	PingRsp   mbr;
	Hashtable votes;
	int       count, most_votes;
	Address   winner=null, tmp;

	if(mbrs == null || mbrs.size() < 1)
	    return null;

	votes=new Hashtable();

	// count *all* the votes (unlike the 2000 election)
	for(int i=0; i < mbrs.size(); i++) {
	    mbr=(PingRsp)mbrs.elementAt(i);
	    if(mbr.coord_addr != null) {
		if(!votes.containsKey(mbr.coord_addr))
		    votes.put(mbr.coord_addr, new Integer(1));
		else {
		    count=((Integer)votes.get(mbr.coord_addr)).intValue();
		    votes.put(mbr.coord_addr, new Integer(count+1));
		}
	    }
	}

	if(Trace.trace) {
	    if(votes.size() > 1)
		Trace.warn("ClientGmsImpl.determineCoord()",
			   "there was more than 1 candidate for coordinator: " + votes);
	    else
		Trace.info("ClientGmsImpl.determineCoord()", "election results: " + votes);
	}
	

	// determine who got the most votes
	most_votes=0;
	for(Enumeration e=votes.keys(); e.hasMoreElements();) {
	    tmp=(Address)e.nextElement();
	    count=((Integer)votes.get(tmp)).intValue();
	    if(count > most_votes) {
		winner=tmp;
                // fixed July 15 2003 (patch submitted by Darren Hobbs, patch-id=771418)
		most_votes=count;
	    }
	}
	votes.clear();
	return winner;
    }



}
