// $Id: MERGE.java,v 1.1.1.1 2003/09/09 01:24:10 belaban Exp $

package org.jgroups.protocols;



import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Enumeration;
// import java.util.List;
import java.util.Properties;
import java.util.Vector;
import org.jgroups.*;
import org.jgroups.util.*;
import org.jgroups.stack.*;
import org.jgroups.log.Trace;



/**
 * Simple and stupid MERGE protocol (does not take into account state transfer).
 * Periodically mcasts a HELLO message with its own address. When a HELLO message is
 * received from a member that has the same group (UDP discards all messages with a group
 * name different that our own), but is not currently in the group, a MERGE event is sent
 * up the stack.  The protocol starts working upon receiving a View in which it is the coordinator.
 * @author Gianluca Collot, Jan 2001
 */
public class MERGE extends Protocol implements Runnable {
    Vector          members=new Vector();
    Address         local_addr=null;
    String          group_addr=null;
    String          groupname=null;
    Thread          hello_thread=null;       // thread that periodically mcasts HELLO messages
    long            timeout=5000;            // timeout between mcasting of HELLO messages

    String          router_host=null;
    int             router_port=0;

    RouterStub      client=null;
    boolean         is_server=false;
    boolean         is_coord = false;
    boolean         merging  = false;



    public String  getName() {return "MERGE";}


    public boolean setProperties(Properties props) {
	String     str;

	str=props.getProperty("timeout");          // max time to wait for initial members
	if(str != null) {
	    timeout=new Long(str).longValue();
	    props.remove("timeout");
	}

	str=props.getProperty("router_host");      // host to send gossip queries (if gossip enabled)
	if(str != null) {
	    router_host=new String(str);
	    props.remove("router_host");
	}

	str=props.getProperty("router_port");
	if(str != null) {
	    router_port=new Integer(str).intValue();
	    props.remove("router_port");
	}

	if(router_host != null && router_port != 0)
	    client=new RouterStub(router_host, router_port);

	if(props.size() > 0) {
	    System.err.println("MERGE.setProperties(): the following properties are not recognized:");
	    props.list(System.out);
	    return false;
	}
	return true;
    }


    public void start() throws Exception {
	if(hello_thread == null) {
	    hello_thread=new Thread(this, "MERGE Thread");
            hello_thread.setDaemon(true);
	    hello_thread.start();
	}
    }


    public void stop() {
	Thread tmp=null;
	if(hello_thread != null && hello_thread.isAlive()) {
	    tmp=hello_thread;
	    hello_thread=null;
	    tmp.interrupt();
	    try {tmp.join(1000);} catch(Exception ex) {}
	}
	hello_thread=null;
    }



    public void up(Event evt) {
	Message      msg, rsp_msg;
	Object       obj;
	MergeHeader   hdr, rsp_hdr;
	PingRsp      rsp;
	Object       coord;
	Address      sender;
	boolean      contains;
	Vector       tmp;


	switch(evt.getType()) {

	case Event.MSG:
	    msg=(Message)evt.getArg();
	    obj=msg.getHeader(getName());
		    if(obj == null || !(obj instanceof MergeHeader)) {
		passUp(evt);
		return;
	    }
	    hdr=(MergeHeader)msg.removeHeader(getName());

	    switch(hdr.type) {

	    case MergeHeader.HELLO:          // if coord: handle, else: discard
		if(!is_server || !is_coord) {
		    return;
		}
		if (merging) {
		  return;
		}
		sender=msg.getSrc();
		if((sender != null) && (members.size() >= 0)) {
		    synchronized(members) {
			contains=members.contains(sender);
		    }
		    //merge only with lower addresses :prevents cycles and ensures that the new coordinator is correct.
		    if(!contains && sender.compareTo(local_addr) < 0) {
			if(Trace.trace) Trace.info("MERGE.up()", "membership " + members +
						   " does not contain " + sender + "; merging it");
			tmp=new Vector();
			tmp.addElement(sender);
			merging = true;
			passUp(new Event(Event.MERGE, tmp));
		    }
		}
		return;

	    default:
		Trace.error("MERGE.up()", "got MERGE hdr with unknown type (" + hdr.type + ")");
		return;
	    }

	case Event.SET_LOCAL_ADDRESS:
	    local_addr=(Address)evt.getArg();
	    passUp(evt);
	    break;

	default:
	    passUp(evt);            // Pass up to the layer above us
	    break;
	}
    }





    public void down(Event evt) {
	Message      msg;
	MergeHeader   hdr;
	long         time_to_wait, start_time;
	Vector       gossip_rsps=new Vector();

	switch(evt.getType()) {

	case Event.TMP_VIEW:
	     passDown(evt);
	     break;

	case Event.MERGE_DENIED:
	     merging=false;
	     passDown(evt);
	     break;

	case Event.VIEW_CHANGE:
	    merging=false;
	    members =((View)evt.getArg()).getMembers();
	    if ((members == null) || (members.size() == 0)) {
	       Trace.fatal("MERGE.down()", "received VIEW_CHANGE with null or empty vector");
	       System.exit(6);
	    }
	    if (members.elementAt(0).equals(local_addr)) is_coord = true;
	    else is_coord = false;
	    passDown(evt);
	    if (is_coord) {
                if (Trace.trace) Trace.info("MERGE.down()", "start sending Hellos");
                try {
                    start();
                }
                catch(Exception ex) {
                    Trace.warn("MERGE.down()", "exception calling start(): " + ex);
                }
	    }
	    else {
                if (Trace.trace) Trace.info("MERGE.down()", "stop sending Hellos");
                stop();
	    }
	    break;

	case Event.BECOME_SERVER: // called after client has join and is fully working group member
	    passDown(evt);
            try {
                start();
                is_server=true;
            }
            catch(Exception ex) {
                Trace.warn("MERGE.down()", "exception calling start(): " + ex);
            }
	    break;

	case Event.CONNECT:
	    group_addr=(String)evt.getArg();
	    passDown(evt);
	    break;

	case Event.DISCONNECT:
	    if(local_addr != null && evt.getArg() != null && local_addr.equals(evt.getArg()))
		stop();
	    passDown(evt);
	    break;

	default:
	    passDown(evt);          // Pass on to the layer below us
	    break;
	}
    }




    /**
       If IP multicast: periodically mcast a HELLO message
       If gossiping: periodically retrieve the membership. Any members not part of our
       own membership are merged (passing MERGE event up).
     */
    public void run() {
	Message       hello_msg;
	MergeHeader   hdr;
	List          rsps;
	Vector        members_to_merge=new Vector(), tmp;
	Object        mbr;


	try {Thread.sleep(3000);} /// initial sleep; no premature merging
	catch(Exception e) {}


	while(hello_thread != null) {
	    Util.sleep(timeout);
	    if(hello_thread == null) break;

	    if(client == null) {                              // plain IP MCAST
		hello_msg=new Message(null, null, null);
		hdr=new MergeHeader(MergeHeader.HELLO);
		hello_msg.putHeader(getName(), hdr);
		passDown(new Event(Event.MSG, hello_msg));
	    }
	    else {                                           // gossiping; contact Router
		rsps=client.get(group_addr);

		synchronized(members) {
		    members_to_merge.removeAllElements();

		    for(Enumeration e=rsps.elements(); e.hasMoreElements();) {
			mbr=e.nextElement();
			if(!members.contains(mbr)) {

			    Trace.info("MERGE.run()", "membership " + members +
				       " does not contain " + mbr + "; merging it");

			    members_to_merge.addElement(mbr);
			}
		    }
		    if (members_to_merge.size() > 0) {
		       Membership new_membership = new Membership(members_to_merge);
		       new_membership.sort();
		       Address coord = (Address) new_membership.elementAt(0);
		       tmp=new Vector();
		       tmp.addElement(coord);
		       if (coord.compareTo(local_addr)<0)
			   passUp(new Event(Event.MERGE, tmp));
		    }
		}
	    }
	}
    }






    /* -------------------------- Private methods ---------------------------- */


    public static class MergeHeader extends Header {
	public static final int HELLO=1;          // arg = null
	
	public int    type=0;

	public MergeHeader() {} // used for externalization
	
	public MergeHeader(int type) {
	    this.type = type;
	}
	
	public String toString() {
	    return "[MERGE: type=" + type2Str(type) + "]";
	}
	
	String type2Str(int t) {
	    switch(t) {
	    case HELLO:        return "HELLO";
	    default:           return "<unkown type (" + t + ")>";
	    }
	}
	
	public void writeExternal(ObjectOutput out) throws IOException {
	    out.writeInt(type);
	}
	
	
	
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
	    type=in.readInt();
	}
    }

}
