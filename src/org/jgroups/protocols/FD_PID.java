// $Id: FD_PID.java,v 1.1.1.1 2003/09/09 01:24:10 belaban Exp $

package org.jgroups.protocols;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.InetAddress;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;
import org.jgroups.*;
import org.jgroups.stack.*;
import org.jgroups.util.*;
import org.jgroups.log.Trace;



/**
   Process-ID based FD protocol. The existence of a process will be tested
   via the process ID instead of message based pinging. In order to probe a process' existence, the application (or
   some other protocol layer) has to send down a SET_PID event for the member. The addresses of all members will
   be associated with their respective PIDs. The PID will be used to probe for the existence of that process.<p>
   A cache of Addresses and PIDs is maintained in each member, which is adjusted upon reception of view changes.
   The population of the addr:pid cache is as follows:<br>
   When a new member joins, it requests the PID cache from the coordinator. Then it broadcasts its own addr:pid
   association, so all members can update their cache. When a member P is to be pinged by Q, and Q doesn't have
   P'd PID, Q will broadcast a WHO_HAS_PID message, to which all members who have that entry will respond. The
   latter case should actually never happen because all members should always have consistent caches. However,
   it is left in the code as a second line of defense.<p>
   Note that
   <em>1. The SET_PID has to be sent down after connecting to a channel !</em><p>
   <em>2. Note that if a process is shunned and subsequently reconnects, the SET_PID event has to be resent !</em><p>
   <em>3. This protocol only works for groups whose members are on the same host </em>. 'Host' actually means the
   same IP address (e.g. for multi-homed systems).
*/
public class FD_PID extends Protocol {
    Address       ping_dest=null;                 // address of the member we monitor
    int           ping_pid=0;                     // PID of the member we monitor
    Address       local_addr=null;                // our own address
    int           local_pid=0;                    // PID of this process
    long          timeout=3000;                   // msecs to wait for an are-you-alive msg
    long          get_pids_timeout=3000;          // msecs to wait for the PID cache from the coordinator
    long          get_pids_retry_timeout=500;     // msecs to wait until we retry fetching the cache from the coord
    int           num_tries=3;                    // attempts the coord is solicited for PID cache until we give up
    Vector        members=new Vector();           // list of group members (updated on VIEW_CHANGE)
    Hashtable     pids=new Hashtable();           // keys=Addresses, vals=Integer (PIDs)
    boolean       own_pid_sent=false;             // has own PID been broadcast yet ?
    Vector        pingable_mbrs=new Vector();     // mbrs from which we select ping_dest. possible subset of 'members'
    Promise       get_pids_promise=new Promise(); // used for rendezvous on GET_PIDS and GET_PIDS_RSP
    boolean       got_cache_from_coord=false;     // was cache already fetched ?
    TimeScheduler timer=null;                     // timer for recurring task of liveness pinging
    Monitor       monitor=null;                   // object that performs the actual monitoring




    public String  getName() {return "FD_PID";}



    public boolean setProperties(Properties props) {
	String     str;

	str=props.getProperty("timeout");
	if(str != null) {
	    timeout=new Long(str).longValue();
	    props.remove("timeout");
	}

	str=props.getProperty("get_pids_timeout");
	if(str != null) {
	    get_pids_timeout=new Long(str).longValue();
	    props.remove("get_pids_timeout");
	}

	str=props.getProperty("num_tries");
	if(str != null) {
	    num_tries=new Integer(str).intValue();
	    props.remove("num_tries");
	}

	if(props.size() > 0) {
	    System.err.println("FD_PID.setProperties(): the following properties are not recognized:");
	    props.list(System.out);
	    return false;
	}
	return true;
    }



    public void start() throws Exception {
	if(stack != null && stack.timer != null)
	    timer=stack.timer;
	else {
	    Trace.warn("FD_PID.start()", "TimeScheduler in protocol stack is null (or protocol stack is null)");
	    return;
	}

	if(monitor != null && monitor.started == false) {
	    monitor=null;
	}
	if(monitor == null) {
	    monitor=new Monitor();
	    timer.add(monitor, true);  // fixed-rate scheduling
	}
    }

    public void stop() {
	if(monitor != null) {
	    monitor.stop();
	    monitor=null;
	}
    }




    public void up(Event evt) {
	Message   msg;
	FdHeader  hdr=null;
	Object    sender, tmphdr;
	int       num_pings=0;
	Address   dst;

	switch(evt.getType()) {

	case Event.SET_LOCAL_ADDRESS:
	    local_addr=(Address)evt.getArg();
	    break;

	case Event.MSG:
	    msg=(Message)evt.getArg();
	    tmphdr=msg.getHeader(getName());
	    if(tmphdr == null || !(tmphdr instanceof FdHeader))
		break;  // message did not originate from FD_PID layer, just pass up

	    hdr=(FdHeader)msg.removeHeader(getName());
	    
	    switch(hdr.type) {

	    case FdHeader.SUSPECT:
		if(hdr.mbr != null) {
		    if(Trace.trace)
			Trace.info("FD_PID.up()", "[SUSPECT] hdr: "  + hdr);
		    passUp(new Event(Event.SUSPECT, hdr.mbr));
		    passDown(new Event(Event.SUSPECT, hdr.mbr));
		}
		break;

	    // If I have the PID for the address 'hdr.mbr', return it. Otherwise look it up in my cache and return it
	    case FdHeader.WHO_HAS_PID:
		if(local_addr != null && local_addr.equals(msg.getSrc()))
		    return; // don't reply to WHO_HAS bcasts sent by me !
		
		if(hdr.mbr == null) {
		    Trace.error("FD_PID.up()", "[WHO_HAS_PID] hdr.mbr is null");
		    return;
		}

		// 1. Try my own address, maybe it's me whose PID is wanted
		if(local_addr != null && local_addr.equals(hdr.mbr) && local_pid > 0) {
		    sendIHavePidMessage(msg.getSrc(), hdr.mbr, local_pid);  // unicast message to msg.getSrc()
		    return;
		}
		
		// 2. If I don't have it, maybe it is in the cache
		if(pids.containsKey(hdr.mbr))
		    sendIHavePidMessage(msg.getSrc(), hdr.mbr, ((Integer)pids.get(hdr.mbr)).intValue());  // ucast msg
		break;


	    // Update the cache with the add:pid entry (if on the same host)
	    case FdHeader.I_HAVE_PID:
		if(Trace.trace)
		    Trace.info("FD_PID.up()", "i-have pid: " + hdr.mbr + " --> " + hdr.pid);

		if(hdr.mbr == null || hdr.pid <= 0) {
		    Trace.error("FD_PID.up()", "[I_HAVE_PID] hdr.mbr is null or hdr.pid == 0");
		    return;
		}

		if(!sameHost(local_addr, hdr.mbr)) {
		    Trace.error("FD_PID.up()", hdr.mbr + " is not on the same host as I (" +
				local_addr + ", discarding I_HAVE_PID event");
		    return;
		}

		// if(!pids.containsKey(hdr.mbr))
		pids.put(hdr.mbr, new Integer(hdr.pid)); // update the cache
		if(Trace.trace)
		    Trace.info("FD_PID.up()", "[" + local_addr + "]: the cache is " + pids);

		if(ping_pid <= 0 && ping_dest != null && pids.containsKey(ping_dest)) {
		    ping_pid=((Integer)pids.get(ping_dest)).intValue();
                    try {
                        start();
                    }
                    catch(Exception ex) {
                        Trace.warn("FD_PID.up()", "exception when calling start(): " + ex);
                    }
		}
		break;

	    // Return the cache to the sender of this message
	    case FdHeader.GET_PIDS:
		if(hdr.mbr == null) {
		    if(Trace.trace)
			Trace.error("FD_PID.up()", "[GET_PIDS]: hdr.mbr == null");
		    return;
		}
		hdr=new FdHeader(FdHeader.GET_PIDS_RSP);
		hdr.pids=(Hashtable)pids.clone();
		msg=new Message(hdr.mbr, null, null);
		msg.putHeader(getName(), hdr);
		passDown(new Event(Event.MSG, msg));
		break;

	    case FdHeader.GET_PIDS_RSP:
		if(hdr.pids == null) {
		    if(Trace.trace)
			Trace.error("FD_PID.up()", "[GET_PIDS_RSP]: cache is null");
		    return;
		}
		get_pids_promise.setResult(hdr.pids);
		break;
	    }
	    return;
	}

	passUp(evt);                                        // pass up to the layer above us
    }



    public void down(Event evt) {
	Message  msg, tmpmsg;
	Integer  pid;
	FdHeader hdr, tmphdr;
	Address  mbr, tmp_ping_dest;
	Vector   old_mbrs=members;
	View     v;
	

	switch(evt.getType()) {

	case Event.SET_PID:
	    // 1. Set the PID for local_addr
	    pid=(Integer)evt.getArg();
	    if(pid == null) {
		Trace.error("FD_PID.down()", "SET_PID did not contain a pid !");
		return;
	    }
	    local_pid=pid.intValue();
	    if(Trace.trace)
		Trace.info("FD_PID.down(SET_PID)", "local_pid=" + local_pid);
	    break;

	case Event.VIEW_CHANGE:
	    synchronized(this) {
		v=(View)evt.getArg();
		members.removeAllElements();
		members.addAll(v.getMembers());
		pingable_mbrs.removeAllElements();
		pingable_mbrs.addAll(members);
		passDown(evt);


		// 1. Get the addr:pid cache from the coordinator (only if not already fetched)
		if(!got_cache_from_coord) {
		    getPidsFromCoordinator();
		    got_cache_from_coord=true;
		}


		// 2. Broadcast my own addr:pid to all members so they can update their cache
		if(!own_pid_sent) {
		    if(local_pid > 0) {
			sendIHavePidMessage(null, // send to all members
					    local_addr,
					    local_pid);
			own_pid_sent=true;
		    }
		    else
			Trace.warn("FD_PID.down()", "[VIEW_CHANGE]: local_pid == 0");
		}

		// 3. Remove all entries in 'pids' which are not in the new membership
		if(members != null) {
		    for(Enumeration e=pids.keys(); e.hasMoreElements();) {
			mbr=(Address)e.nextElement();
			if(!members.contains(mbr))
			    pids.remove(mbr);
		    }
		}
		tmp_ping_dest=determinePingDest();
		ping_pid=0;
		if(tmp_ping_dest == null) {
		    stop();
		    ping_dest=null;
		}
		else {
		    ping_dest=tmp_ping_dest;
                    try {
                        start();
                    }
                    catch(Exception ex) {
                        Trace.warn("FD_PID.down()", "exception when calling start(): " + ex);
                    }
		}
	    }
	    break;

	default:
	    passDown(evt);
	    break;
	}
    }







    /* ----------------------------------- Private Methods -------------------------------------- */



    /**
       Determines coordinator C. If C is null and we are the first member, return. Else loop: send GET_PIDS message
       to coordinator and wait for GET_PIDS_RSP response. Loop until valid response has been received.
    */
    void getPidsFromCoordinator() {
	Address   coord;
	int       attempts=num_tries;
	Message   msg;
	FdHeader  hdr;
	Hashtable result;
	
	get_pids_promise.reset();
	while(attempts > 0) {
	    if((coord=determineCoordinator()) != null) {
		if(coord.equals(local_addr)) { // we are the first member --> empty cache
		    if(Trace.trace)
			Trace.info("FD_PID.getPidsFromCoordinator()", "first member; cache is empty");
		    return;
		}
		hdr=new FdHeader(FdHeader.GET_PIDS);
		hdr.mbr=local_addr;
		msg=new Message(coord, null, null);
		msg.putHeader(getName(), hdr);
		passDown(new Event(Event.MSG, msg));
		result=(Hashtable)get_pids_promise.getResult(get_pids_timeout);
		if(result != null) {
		    pids.putAll(result); // replace all entries (there should be none !) in pids with the new values
		    if(Trace.trace)
			Trace.info("FD_PID.getPidsFromCoordinator()", "got cache from " +
				      coord + ": cache is " + pids);
		    return;
		}
		else {
		    if(Trace.trace)
			Trace.error("FD_PID.getPidsFromCoordinator()", "received null cache; retrying");
		}
	    }

	    Util.sleep(get_pids_retry_timeout);	    
	    --attempts;
	}
    }



    void broadcastSuspectMessage(Address suspected_mbr) {
	Message   suspect_msg;
	FdHeader  hdr;

	if(Trace.trace)
	    Trace.info("FD_PID.broadcastSuspectMessage()", "suspecting " + suspected_mbr + 
		       " (own address is " + local_addr + ")");

	hdr=new FdHeader(FdHeader.SUSPECT);
	hdr.mbr=suspected_mbr;
	suspect_msg=new Message();       // mcast SUSPECT to all members
	suspect_msg.putHeader(getName(), hdr);
	passDown(new Event(Event.MSG, suspect_msg));
    }


    void broadcastWhoHasPidMessage(Address mbr) {
	Message  msg;
	FdHeader hdr;

	if(Trace.trace && local_addr != null && mbr != null)
	    Trace.info("FD_PID.broadcastWhoHasPidMessage()", "[" + local_addr + "]: who-has " + mbr);

	msg=new Message();  // bcast msg
	hdr=new FdHeader(FdHeader.WHO_HAS_PID);
	hdr.mbr=mbr;
	msg.putHeader(getName(), hdr);
	passDown(new Event(Event.MSG, msg));
    }


    
    /**
       Sends or broadcasts a I_HAVE_PID response. If 'dst' is null, the reponse will be broadcast, otherwise
       it will be unicast back to the requester
    */
    void sendIHavePidMessage(Address dst, Address mbr, int pid) {
	Message  msg=new Message(dst, null, null);
	FdHeader hdr=new FdHeader(FdHeader.I_HAVE_PID);
	hdr.mbr=mbr;
	hdr.pid=pid;
	msg.putHeader(getName(), hdr);
	passDown(new Event(Event.MSG, msg));
    }




    /**
       Set ping_dest and ping_pid. If ping_pid is not known, broadcast a WHO_HAS_PID message.
    */
    Address determinePingDest() {
	Address tmp;

	if(pingable_mbrs == null || pingable_mbrs.size() < 2 || local_addr == null)
	    return null;
	for(int i=0; i < pingable_mbrs.size(); i++) {
	    tmp=(Address)pingable_mbrs.elementAt(i);
	    if(local_addr.equals(tmp)) {
		if(i + 1 >= pingable_mbrs.size())
		    return (Address)pingable_mbrs.elementAt(0);
		else
		    return (Address)pingable_mbrs.elementAt(i+1);
	    }
	}
	return null;
    }


    
    Address determineCoordinator() {
	return members.size() > 0 ? (Address)members.elementAt(0) : null;
    }


    /** Checks whether 2 Addresses are on the same host */
    boolean sameHost(Address one, Address two) {
	InetAddress a, b;
	String      host_a, host_b;

	if(one == null || two == null) return false;
	if(!(one instanceof IpAddress) || ! (two instanceof IpAddress)) {
	    Trace.error("FD_PID.sameHost()", "addresses have to be of type IpAddress to be compared");
	    return false;
	}
	
	a=((IpAddress)one).getIpAddress();
	b=((IpAddress)two).getIpAddress();
	if(a == null || b == null) return false;
	host_a=a.getHostAddress();
	host_b=b.getHostAddress();
	return host_a.equals(host_b);
    }



    /* ------------------------------- End of Private Methods ------------------------------------ */


    public static class FdHeader extends Header {
	static final int SUSPECT       = 10;
	static final int WHO_HAS_PID   = 11;
	static final int I_HAVE_PID    = 12;
	static final int GET_PIDS      = 13; // sent by joining member to coordinator
	static final int GET_PIDS_RSP  = 14; // sent by coordinator to joining member in response to GET_PIDS
		
	
	int       type=SUSPECT;
	Address   mbr=null;     // set on SUSPECT (suspected mbr), WHO_HAS_PID (requested mbr), I_HAVE_PID
	int       pid=0;        // set on I_HAVE_PID
	Hashtable pids=null;    // set on GET_PIDS_RSP
	

	public FdHeader() {} // used for externalization
	
	FdHeader(int type) {this.type=type;}
	
	
	public String toString() {
	    StringBuffer sb=new StringBuffer();
	    sb.append(type2String(type));
	    if(mbr != null)
		sb.append(", mbr=" + mbr);
	    if(pid > 0)
		sb.append(", pid=" +pid);
	    if(pids != null)
		sb.append(", pids=" + pids);
	    return sb.toString();
	}


	public static String type2String(int type) {
	    switch(type) {
	    case SUSPECT:      return "SUSPECT";
	    case WHO_HAS_PID:  return "WHO_HAS_PID";
	    case I_HAVE_PID:   return "I_HAVE_PID";
	    case GET_PIDS:     return "GET_PIDS";
	    case GET_PIDS_RSP: return "GET_PIDS_RSP";
	    default:           return "unknown type (" + type + ")";
	    }
	}

	public void writeExternal(ObjectOutput out) throws IOException {
	    out.writeInt(type);
	    out.writeObject(mbr);
	    out.writeInt(pid);
	    out.writeObject(pids);
	}
	
	
	
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
	    type=in.readInt();
	    mbr=(Address)in.readObject();
	    pid=in.readInt();
	    pids=(Hashtable)in.readObject();
	}

    }





    /**
       An instance of this class will be added to the TimeScheduler to be scheduled to be run every timeout
       msecs. When there is no ping_dest (e.g. only 1 member in the group), this task will be cancelled in
       TimeScheduler (and re-scheduled if ping_dest becomes available later).
     */
    private class Monitor implements TimeScheduler.Task {
	boolean started=true;



	void stop() {
	    started=false;
	}


	/* -------------------------------------- TimeScheduler.Task Interface -------------------------------- */

	public boolean cancelled()  {
	    return !started;
	}

    
	public long nextInterval() {
	    return timeout;
	}
    



	/**
	   Periodically probe for the destination process identified by ping_dest/ping_pid. Suspect the ping_dest
	   member if /prop/<ping_pid> process does not exist.
	*/
	public void run() {
	    if(ping_dest == null) {
		Trace.warn("FD_PID.Monitor.run()", "ping_dest is null, skipping ping");
		return;
	    }

	    if(Trace.trace)
		Trace.info("FD_PID.Monitor.run()", "ping_dest=" + ping_dest + ", ping_pid=" + ping_pid +
			   ", cache=" + pids);

	    // If the PID for ping_dest is not known, send a broadcast to solicit it
	    if(ping_pid <= 0) {
		if(ping_dest != null && pids.containsKey(ping_dest)) {
		    ping_pid=((Integer)pids.get(ping_dest)).intValue();
		    if(Trace.trace)
			Trace.info("FD_PID.Monitor.run()", "found PID for " + 
				   ping_dest + " in cache (pid=" + ping_pid + ")");
		}
		else {
		    if(Trace.trace)
			Trace.error("FD_PID.Monitor.run()", "PID for " + ping_dest + " not known" +
				    ", cache is " + pids);
		    broadcastWhoHasPidMessage(ping_dest);
		    return;
		}
	    }
		
	    if(!Util.fileExists("/proc/" + ping_pid)) {
		if(Trace.trace)
		    Trace.info("FD_PID.Monitor.run()", "process " + ping_pid + " does not exist");
		broadcastSuspectMessage(ping_dest);
		pingable_mbrs.removeElement(ping_dest);
		ping_dest=determinePingDest();
		if(ping_dest == null)
		    stop();
		ping_pid=0;
	    }
	    else {
		if(Trace.trace)
		    Trace.info("FD_PID.Monitor.run()", ping_dest + " is alive");		
	    }
	}

	/* ---------------------------------- End of TimeScheduler.Task Interface ---------------------------- */
	
    }

}
