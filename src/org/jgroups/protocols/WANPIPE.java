// $Id: WANPIPE.java,v 1.1.1.1 2003/09/09 01:24:11 belaban Exp $

package org.jgroups.protocols;


import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.InetAddress;
import java.util.Vector;
import java.util.Enumeration;
import java.util.Properties;
import java.util.StringTokenizer;
import org.jgroups.*;
import org.jgroups.util.*;
import org.jgroups.blocks.*;
import org.jgroups.stack.*;
import org.jgroups.log.Trace;



/**
   Logical point-to-point link. Uses multiple physical links to provide a reliable transport. For example,
   if there are 2 physical links over different networks, and one of them fails, the WAN pipe will still be
   able to send traffic over the other link. Currently traffic is sent over the physical links round-robin,
   but this will be made configurable in the future. Example: 70% over first link, 30% over second, or
   packets are split and sent across both links (increasing the available bandwidth).
 */
public class WANPIPE extends Protocol implements LogicalLink.Receiver {
    LogicalLink    pipe=null;
    String         name=null;         // logical name of WAN pipe
    List           links=new List();  // contains the parsed link descriptions

    Address        local_addr=null;
    String         group_addr=null;
    Properties     properties=null;
    Vector         members=new Vector();



    public WANPIPE() {
	pipe=new LogicalLink(this);
    }


    public String toString() {
	return "Protocol WANPIPE(local address: " + local_addr + ")";
    }


    public String getName() {return "WANPIPE";}






    /**
       Sent to destination(s) using the WAN pipe. Send local messages directly back up the stack
     */
    public void down(Event evt) {
	Message      msg, rsp, copy;
	Address      dest_addr;

	if(evt.getType() != Event.MSG) {
	    handleDownEvent(evt);
	    return;
	}

	msg=(Message)evt.getArg();
	dest_addr=msg.getDest();
	
	if(dest_addr == null) {                 // send both local and remote
	    for(int i=0; i < members.size(); i++) {
		dest_addr=(Address)members.elementAt(i);

		if(dest_addr.equals(local_addr)) {  // local or ...
		    returnLocal(msg);
		}
		else {                              // remote
		    copy=msg.copy();
		    copy.setDest(dest_addr);
		    copy.putHeader(getName(), new WanPipeHeader(group_addr));
		    sendUnicastMessage(copy);
		}
	    }
	}
	else {
	    if(dest_addr.equals(local_addr)) {  // destination can either be local ...
		returnLocal(msg);
	    }
	    else {                              // or remote
		msg.putHeader(getName(), new WanPipeHeader(group_addr));
		sendUnicastMessage(msg);
	    }
	}	
    }


    /** Make a response and send back up the same stack it came down */
    void returnLocal(Message msg) {
	Message rsp=msg.copy();
	rsp.setDest(local_addr);
	rsp.setSrc(local_addr);
	passUp(new Event(Event.MSG, rsp));
    }




    public void start() throws Exception {
	LinkInfo l;

        for(Enumeration e=links.elements(); e.hasMoreElements();) {
            l=(LinkInfo)e.nextElement();
            pipe.addLink(l.local_addr, l.local_port, l.remote_addr, l.remote_port);
        }
        pipe.start();
        local_addr=new WanPipeAddress(name);  // logical address for the WAN pipe
        passUp(new Event(Event.SET_LOCAL_ADDRESS, local_addr));
    }


    public void stop() {
	pipe.stop();
	pipe.removeAllLinks();
    }







    // LogicalLink.Receiver interface
    public void receive(byte[] buf) {
	WanPipeHeader    hdr=null;
	Message          msg=null;

	try {
	    msg=(Message)Util.objectFromByteBuffer(buf);
	}
	catch(Exception e) {
	    System.err.println("WANPIPE.receive(): " + e);
	    return;
	}
	
	if(Trace.trace) Trace.info("WANPIPE.receive()", "received msg " + msg);
	hdr=(WanPipeHeader)msg.removeHeader(getName());
	
	/* Discard all messages destined for a channel with a different name */
	String ch_name=null;

	if(hdr.group_addr != null)
	    ch_name=hdr.group_addr;

	if(group_addr == null) {
	    if(Trace.trace) System.err.println("WANPIPE.receive(): group address in header was null, discarded");
	    return;
	}

	if(ch_name != null && !group_addr.equals(ch_name))
	    return;

	passUp(new Event(Event.MSG, msg));	
    }





    public void linkDown(InetAddress local, int local_port, InetAddress remote, int remote_port) {
	Object p=getPeer();

	passUp(new Event(Event.SUSPECT, p));
    }


    public void linkUp(InetAddress local, int local_port, InetAddress remote, int remote_port) {
	
    }


    public void missedHeartbeat(InetAddress local, int local_port, InetAddress remote, int remote_port, int num_hbs) {
	
    }

    public void receivedHeartbeatAgain(InetAddress local, int local_port, InetAddress remote, int remote_port) {
	
    }



    /** Setup the Protocol instance acording to the configuration string */
    public boolean setProperties(Properties props) {
	String     str;

	str=props.getProperty("name");
	if(str != null) {
	    name=str;
	    props.remove("name");
	}

	str=props.getProperty("links");
	if(str != null) {

	    // parse links and put them in list (as LinkInfos)	    
	    if(parseLinks(str) == false)
		return false;
	    props.remove("links");
	}

	if(name == null || name.length() == 0) {
	    System.err.println("WANPIPE.setProperties(): 'name' must be set");
	    return false;
	}
	if(links.size() == 0) {
	    System.err.println("WANPIPE.setProperties(): no links specified (at least 1 link must be present)");
	    return false;
	}

	if(props.size() > 0) {
	    System.err.println("WANPIPE.setProperties(): the following properties are not recognized:");
	    props.list(System.out);
	    return false;
	}
	return true;
    }



    /** Parse link spec and put each link into 'links' (as LinkInfo) <br>
	Example: <pre> [daddy@6666,daddy@7777,daddy@7777,sindhu@6666] </pre>*/
    boolean parseLinks(String s) {
	LinkInfo        info;
	StringTokenizer tok;
	String          src, dst;
	int             index=0; // holds position of '@'

	s=s.replace('[', ' ');
	s=s.replace(']', ' ');
	s=s.trim();
	tok=new StringTokenizer(s, ",");
	while(tok.hasMoreElements()) {
	    src=tok.nextToken().trim();
	    dst=tok.nextToken().trim();
	    info=new LinkInfo();

	    index=src.indexOf('@');
	    if(index == -1) {
		System.err.println("WANPIPE.parseLinks(): local address " + src + " must have a @ separator");
		return false;
	    }
	    info.local_addr=src.substring(0, index);
	    info.local_port=new Integer(src.substring(index+1, src.length())).intValue();

	    index=dst.indexOf('@');
	    if(index == -1) {
		System.err.println("WANPIPE.parseLinks(): remote address " + dst + " must have a @ separator");
		return false;
	    }
	    info.remote_addr=dst.substring(0, index);
	    info.remote_port=new Integer(dst.substring(index+1, dst.length())).intValue();

	    links.add(info);
	}
	
	return true;
    }


    Object getPeer() {
	Object ret=null;
	if(members == null || members.size() == 0 || local_addr == null)
	    return null;
	for(int i=0; i < members.size(); i++)
	    if(!members.elementAt(i).equals(local_addr))
		return members.elementAt(i);
	return ret;
    }


    


    /**
       If the sender is null, set our own address. We cannot just go ahead and set the address
       anyway, as we might be sending a message on behalf of someone else ! E.g. in case of
       retransmission, when the original sender has crashed, or in a FLUSH protocol when we
       have to return all unstable messages with the FLUSH_OK response.
     */
    private void setSourceAddress(Message msg) {
	if(msg.getSrc() == null)
	    msg.setSrc(local_addr);
    }




    /** Send a message to the address specified in msg.dest */
    private void sendUnicastMessage(Message msg) {
	byte[] buf=null;

	setSourceAddress(msg);	
	try {
	    buf=Util.objectToByteBuffer(msg);
	}
	catch(Exception e) {
	    System.err.println("WANPIPE.sendUnicastMessage(): " + e);
	    return;
	}
	
	try { 
	    pipe.send(buf);
	}
	catch(LogicalLink.AllLinksDown links_down) {
	    System.err.println("WANPIPE.sendUnicastMessage(): WAN pipe has no currently operational " +
			       "link to send message. Discarding it.");
	}
	catch(LogicalLink.NoLinksAvailable no_links) {
	    System.err.println("WANPIPE.sendUnicastMessage(): WAN pipe has no physical links configured;" +
			       " cannot send message");
	}
	catch(Exception e) {
	    System.err.println("WANPIPE.sendUnicastMessage(): " + e);
	}
    }





    private void handleUpEvent(Event evt) {
	switch(evt.getType()) {
	    
	case Event.SUSPECT:
	    break;
	}
    }



    private void handleDownEvent(Event evt) {
	switch(evt.getType()) {

	case Event.TMP_VIEW:
	case Event.VIEW_CHANGE:
	    synchronized(members) {
		members.removeAllElements();
		Vector tmpvec=((View)evt.getArg()).getMembers();
		for(int i=0; i < tmpvec.size(); i++)
		    members.addElement(tmpvec.elementAt(i));
	    }
	    break;

	case Event.SUSPECT:
	    break;

	case Event.GET_LOCAL_ADDRESS:   // return local address -> Event(SET_LOCAL_ADDRESS, local)
	    passUp(new Event(Event.SET_LOCAL_ADDRESS, local_addr));
	    break;

	case Event.CONNECT:
	    group_addr=(String)evt.getArg();
	    passUp(new Event(Event.CONNECT_OK));
	    break;

	case Event.DISCONNECT:
	    passUp(new Event(Event.DISCONNECT_OK));
	    break;

	}
    }




    private static class LinkInfo {
	String local_addr=null, remote_addr=null;
	int    local_port=0, remote_port=0;
	
	public String toString() {
	    StringBuffer ret=new StringBuffer();
	    
	    ret.append("local_addr=" + (local_addr != null? local_addr : "null"));
	    ret.append(":" + local_port);
	    ret.append(", remote_addr=" + (remote_addr != null ? remote_addr : "null"));
	    ret.append(":" + remote_port);
	    return ret.toString();
	}
    }
    
    
    public class WanPipeHeader extends Header {
	public String group_addr=null;

	
	public WanPipeHeader() {} // used for externalization
	
	public WanPipeHeader(String n) {group_addr=n;}
	
	
	public long size() {
	    return Header.HDR_OVERHEAD;
	}
	
	public String toString() {
	    return "[WanPipe: group_addr=" + group_addr + "]";
	}
	
	public void writeExternal(ObjectOutput out) throws IOException {
	    out.writeObject(group_addr);
	}
	
	
	
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
	    group_addr=(String)in.readObject();
	}
	
    }
    
    
}



