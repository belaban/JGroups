// $Id: WANPING.java,v 1.1.1.1 2003/09/09 01:24:11 belaban Exp $

package org.jgroups.protocols;


import java.util.Vector;
import java.util.Properties;
import java.util.Enumeration;
import java.util.StringTokenizer;
import org.jgroups.*;
import org.jgroups.util.*;
import org.jgroups.stack.*;
import org.jgroups.log.Trace;




/**
   Similar to TCPPING, except that the initial host list is specified as a list of logical pipe names.
*/
public class WANPING extends Protocol {
    Vector          members=new Vector(), initial_members=new Vector();
    Address         local_addr=null;
    String          group_addr=null;
    String          groupname=null;
    long            timeout=3000;
    long            num_initial_members=2;
    int             port_range=5;        // number of ports to be probed for initial membership
    List            initial_hosts=null;  // hosts to be contacted for the initial membership
    boolean         is_server=false;



    public String  getName() {return "WANPING";}



    public Vector providedUpServices() {
	Vector ret=new Vector();
	ret.addElement(new Integer(Event.FIND_INITIAL_MBRS));
	return ret;
    }


    public boolean setProperties(Properties props) {
	String     str;

	str=props.getProperty("timeout");              // max time to wait for initial members
	if(str != null) {
	    timeout=new Long(str).longValue();
	    props.remove("timeout");
	}

	str=props.getProperty("port_range");           // if member cannot be contacted on base port,
	if(str != null) {                              // how many times can we increment the port
	    port_range=new Integer(str).intValue();
	    props.remove("port_range");
	}

	str=props.getProperty("num_initial_members");  // wait for at most n members
	if(str != null) {
	    num_initial_members=new Integer(str).intValue();
	    props.remove("num_initial_members");
	}

	str=props.getProperty("initial_hosts");
	if(str != null) {
	    props.remove("initial_hosts");
	    initial_hosts=createInitialHosts(str);
	    if(Trace.trace) Trace.info("WANPING.setProperties()", "initial_hosts: " + initial_hosts);
	}

	if(initial_hosts == null || initial_hosts.size() == 0) {
	    System.err.println("WANPING.setProperties(): hosts to contact for initial membership " +
			       "not specified. Cannot determine coordinator !");
	    return false;
	}

	if(props.size() > 0) {
	    System.err.println("WANPING.setProperties(): the following properties are not recognized:");
	    props.list(System.out);
	    return false;
	}
	return true;
    }





    public void up(Event evt) {
	Message      msg, rsp_msg;
	Object       obj;
	PingHeader   hdr, rsp_hdr;
	PingRsp      rsp;
	Address      coord;
	Address      sender;
	boolean      contains;
	Vector       tmp;


	switch(evt.getType()) {

	case Event.MSG:
	    msg=(Message)evt.getArg();
	    obj=msg.getHeader(getName());
	    	    if(obj == null || !(obj instanceof PingHeader)) {
		passUp(evt);
		return;
	    }
	    hdr=(PingHeader)msg.removeHeader(getName());

	    switch(hdr.type) {

	    case PingHeader.GET_MBRS_REQ:   // return Rsp(local_addr, coord)
		if(!is_server) {
		    //System.err.println("WANPING.up(GET_MBRS_REQ): did not return a response " +
		    //	       "as I'm not a server yet !");
		    return;
		}
		synchronized(members) {
		    coord=members.size() > 0 ? (Address)members.firstElement() : local_addr;
		}
		rsp_msg=new Message(msg.getSrc(), null, null);
		rsp_hdr=new PingHeader(PingHeader.GET_MBRS_RSP, new PingRsp(local_addr, coord));
		rsp_msg.putHeader(getName(), rsp_hdr);
		passDown(new Event(Event.MSG, rsp_msg));
		return;

	    case PingHeader.GET_MBRS_RSP:   // add response to vector and notify waiting thread
		rsp=(PingRsp)hdr.arg;
		synchronized(initial_members) {
		    initial_members.addElement(rsp);
		    initial_members.notify();
		}
		return;

	    default:
		System.err.println("WANPING.up(): got WANPING header with unknown type (" + 
				   hdr.type + ")");
		return;
	    }
	    

	case Event.SET_LOCAL_ADDRESS:
	    passUp(evt);
	    local_addr=(Address)evt.getArg();
	    break;

	default:
	    passUp(evt);            // Pass up to the layer above us
	    break;
	}
    }





    public void down(Event evt) {
	Message      msg, copy;
	PingHeader   hdr;
	long         time_to_wait, start_time;
	List         gossip_rsps=null;
	String       h;

	switch(evt.getType()) {

	case Event.FIND_INITIAL_MBRS:   // sent by GMS layer, pass up a GET_MBRS_OK event
	    
	    // 1. Mcast GET_MBRS_REQ message

	    System.out.println("WANPING.FIND_INITIAL_MBRS");
	    
	    hdr=new PingHeader(PingHeader.GET_MBRS_REQ, null);
	    msg=new Message(null, null, null);
	    msg.putHeader(getName(), hdr);

	    System.out.println("Sending PING to " + initial_hosts);
	    
	    for(Enumeration en=initial_hosts.elements(); en.hasMoreElements();) {
		h=(String)en.nextElement();
		copy=msg.copy();
		copy.setDest(new WanPipeAddress(h));
		if(Trace.trace)
		    System.out.println("WANPING.down(FIND_INITIAL_MBRS): sending PING request to " +
				       copy.getDest());
		passDown(new Event(Event.MSG, copy));
	    }
	    
	    
	    // 2. Wait 'timeout' ms or until 'num_initial_members' have been retrieved
	    synchronized(initial_members) {
		initial_members.removeAllElements();
		start_time=System.currentTimeMillis();
		time_to_wait=timeout;
		
		while(initial_members.size() < num_initial_members && time_to_wait > 0) {
		    try {initial_members.wait(time_to_wait);} catch(Exception e) {}
		    time_to_wait-=System.currentTimeMillis() - start_time;
		}
	    }

	    System.out.println("Initial members are " + initial_members);
	    
	    // 3. Send response
	    passUp(new Event(Event.FIND_INITIAL_MBRS_OK, initial_members));
	    break;
	   	    
	case Event.TMP_VIEW:
	case Event.VIEW_CHANGE:	    
	    Vector tmp;
	    if((tmp=((View)evt.getArg()).getMembers()) != null) {
		synchronized(members) {
		    members.removeAllElements();
		    for(int i=0; i < tmp.size(); i++)
			members.addElement(tmp.elementAt(i));
		}
	    }
	    passDown(evt);
	    break;

	case Event.BECOME_SERVER: // called after client has joined and is fully working group member
	    passDown(evt);
	    is_server=true;
	    break;

	case Event.CONNECT:
	    group_addr=(String)evt.getArg();
	    passDown(evt);
	    break;

	case Event.DISCONNECT:
	    passDown(evt);
	    break;
	    
	default:
	    passDown(evt);          // Pass on to the layer below us
	    break;
	}
    }



    /* -------------------------- Private methods ---------------------------- */


    private View makeView(Vector mbrs) {
	Address coord=null;
	long    id=0;
	ViewId  view_id=new ViewId(local_addr);

	coord=view_id.getCoordAddress();
	id=view_id.getId();

	return new View(coord, id, mbrs);
    }

    /** Input is "pipe1,pipe2". Return List of Strings */
    private List createInitialHosts(String l) {
	List            tmp=new List();
	StringTokenizer tok=new StringTokenizer(l, ",");
	String          t;

	while(tok.hasMoreTokens()) {
	    try {
		t=tok.nextToken();
		tmp.add(t.trim());
	    }
	    catch(NumberFormatException e) {
		System.err.println("WANPING.createInitialHosts(): " + e);
	    }
	}
	return tmp;
    }


}

