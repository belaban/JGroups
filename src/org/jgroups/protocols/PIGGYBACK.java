// $Id: PIGGYBACK.java,v 1.1 2003/09/09 01:24:10 belaban Exp $

package org.jgroups.protocols;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Properties;
import java.util.Vector;
import org.jgroups.*;
import org.jgroups.util.*;
import org.jgroups.stack.*;
import org.jgroups.log.Trace;




/**
   Combines multiple messages into a single large one. As many messages as possible are combined into
   one, after a max timeout or when the msg size becomes too big the message is sent. On the receiving
   side, the large message is spliced into the smaller ones and delivered.
 */

public class PIGGYBACK extends Protocol {
    long     max_wait_time=20; // milliseconds: max. wait between consecutive msgs
    long     max_size=8192;    // don't piggyback if created msg would exceed this size (in bytes)
    Queue    msg_queue=new Queue();
    Packer   packer=null;
    boolean  packing=false;
    Address  local_addr=null;


    class Packer implements Runnable {
	Thread t=null;
	

	public void start() {
	    if(t == null) {
		t=new Thread(this, "Packer thread");
                t.setDaemon(true);
		t.start();
	    }
	}

	public void stop() {
	    t=null;
	}

	public void run() {
	    long     current_size=0;
	    long     start_time, time_to_wait=max_wait_time;
	    Message  m, new_msg;
	    Vector   msgs;

	    while(packer != null) {
		try {
		    m=(Message)msg_queue.remove();
		    m.setSrc(local_addr);
		    start_time=System.currentTimeMillis();
		    current_size=0;
		    new_msg=new Message();
		    msgs=new Vector();
		    msgs.addElement(m);
		    current_size+=m.size();

		    while(System.currentTimeMillis() - start_time <= max_wait_time &&
			  current_size <= max_size) {
			
			time_to_wait=max_wait_time - (System.currentTimeMillis() - start_time);
			if(time_to_wait <= 0)
			    break;

			try {
			    m=(Message)msg_queue.peek(time_to_wait);
			    m.setSrc(local_addr);
			}
			catch(TimeoutException timeout) {
			    break;
			}
			if(m == null || m.size() + current_size > max_size)
			    break;
			m=(Message)msg_queue.remove();
			current_size+=m.size();
			msgs.addElement(m);			
		    }

		    try {
			new_msg.putHeader(getName(), new PiggybackHeader());
			new_msg.setBuffer(Util.objectToByteBuffer(msgs));
			passDown(new Event(Event.MSG, new_msg));
			if(Trace.trace)
			    Trace.info("PIGGYBACK.run()", "combined " + msgs.size() +
				       " messages of a total size of " + current_size + " bytes");
		    }
		    catch(Exception e) {
			Trace.warn("PIGGYBACK.run()", "exception is " + e);
		    }
		}
		catch(QueueClosedException closed) {
		    if(Trace.trace) Trace.info("PIGGYBACK.run()", "packer stopped as queue is closed");
		    break;
		}
	    }
	}
    }



    /** All protocol names have to be unique ! */
    public String  getName() {return "PIGGYBACK";}


    public boolean setProperties(Properties props) {
	String     str;

	str=props.getProperty("max_wait_time");
	if(str != null) {
	    max_wait_time=new Long(str).longValue();
	    props.remove("max_wait_time");
	}
	str=props.getProperty("max_size");
	if(str != null) {
	    max_size=new Long(str).longValue();
	    props.remove("max_size");
	}

	if(props.size() > 0) {
	    System.err.println("PIGGYBACK.setProperties(): these properties are not recognized:");
	    props.list(System.out);
	    return false;
	}
	return true;
    }


    public void start() throws Exception {
        startPacker();
    }

    public void stop() {
         packing=false;
         msg_queue.close(true);  // flush pending messages, this should also stop the packer ...
         stopPacker();           // ... but for safety reasons, we stop it here again
    }


    public void up(Event evt) {
	Message          msg;
	PiggybackHeader  hdr;
	Object           obj;
	Vector           messages;

	switch(evt.getType()) {

	case Event.SET_LOCAL_ADDRESS:
	    local_addr=(Address)evt.getArg();
	    break;

	case Event.MSG:
	    msg=(Message)evt.getArg();
	    obj=msg.getHeader(getName());
	    if(obj == null || !(obj instanceof PiggybackHeader))
		break;
	    
	    msg.removeHeader(getName());
	    try {
		messages=(Vector)Util.objectFromByteBuffer(msg.getBuffer());
		if(Trace.trace) Trace.info("PIGGYBACK.up()", "unpacking " + messages.size() + " messages");
		for(int i=0; i < messages.size(); i++)
		    passUp(new Event(Event.MSG, messages.elementAt(i)));
	    }
	    catch(Exception e) {
		Trace.warn("PIGGYBACK.up()", "piggyback message does not contain a vector of " +
			   "piggybacked messages, discarding message ! Exception is " + e);
		return;
	    }

	    return;             // don't pass up !
	}

	passUp(evt);            // Pass up to the layer above us
    }





    public void down(Event evt) {
	Message          msg;

	switch(evt.getType()) {

	case Event.MSG:
	    msg=(Message)evt.getArg();

	    if(msg.getDest() != null && !msg.getDest().isMulticastAddress())
		break;  // unicast message, handle as usual

	    if(!packing)
		break;  // pass down as usual; we haven't started yet

	    try {
		msg_queue.add(msg);
	    }
	    catch(QueueClosedException closed) {
		break;  // pass down regularly
	    }
	    return;
	}

	passDown(evt);          // Pass on to the layer below us
    }




    void startPacker() {
	if(packer == null) {
	    packing=true;
	    packer=new Packer();
	    packer.start();
	}
    }


    void stopPacker() {
	if(packer != null) {
	    packer.stop();
	    packing=false;
	    msg_queue.close(false);
	    packer=null;
	}
    }


    public static class PiggybackHeader extends Header {
	
	public PiggybackHeader() {
	}
	
	public String toString() {
	    return "[PIGGYBACK: <variables> ]";
	}

	public void writeExternal(ObjectOutput out) throws IOException {
	}
	
	
	
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
	}

    }


}
