// $Id: AUTOCONF.java,v 1.1 2003/09/09 01:24:09 belaban Exp $

package org.jgroups.protocols;

import java.io.*;
import java.net.*;
import java.util.HashMap;
import java.util.Properties;
import org.jgroups.*;
import org.jgroups.stack.*;
import org.jgroups.log.Trace;



/**
 * Senses the network configuration when it is initialized (in init()) and sends a CONFIG event up
 * and down the stack. The CONFIG event contains a hashmap, with strings as keys (e.g. "frag_size")
 * and Objects as values. Certain protocols can set some of their properties when receiving the CONFIG
 * event.<br>
 * This protocol should be placed above the transport protocol (e.g. UDP). It is not needed for TCP.<br>
 * Example: senses the network send and receive buffers, plus the max size of a message to be sent and
 * generates a CONFIG event containing "frag_size", "send_buf_size" and "receive_buf_size" keys.
 * @author Bela Ban
 */
public class AUTOCONF extends Protocol {
    HashMap config=new HashMap();
    int     num_iterations=10; // to find optimal frag_size


    public String  getName() {return "AUTOCONF";}


    public void init() throws Exception {
	senseNetworkConfiguration();
	if(Trace.trace)
	    Trace.info("AUTOCONF.init()", "configuration is\n" + config);
    }

    public void start() throws Exception {
        if(config != null && config.size() > 0) {
            Event config_evt=new Event(Event.CONFIG, config);
            passDown(config_evt);
            passUp(config_evt);
        }
    }


    /** Setup the Protocol instance acording to the configuration string */
    public boolean setProperties(Properties props) {
        String     str;

	str=props.getProperty("num_iterations");
	if(str != null)
        {
            num_iterations=new Integer(str).intValue();
            props.remove("num_iterations");
        }


        if(props.size() > 0)
        {
            System.err.println("AUTOCONF.setProperties(): the following properties are not recognized:");
            props.list(System.out);
            return false;
        }
        return true;
    }


    /** Leave empty: no up_thread will be created, but the up_thread of the neighbor below us will be used */
    public void startUpHandler() {
	;
    }

    /** Leave empty: no down_thread will be created, but the down_thread of the neighbor above us will be used */
    public void startDownHandler() {
	;
    }



    /* -------------------------------------- Private metods ------------------------------------------- */
    void senseNetworkConfiguration() {
	senseMaxSendBufferSize(config);
	senseMaxReceiveBufferSize(config);
	senseMaxFragSize(config);
    }

    void senseMaxSendBufferSize(HashMap map) {
	DatagramSocket sock=null;
	int            max_size=4096, retval=32000;

	try {
	    sock=new DatagramSocket();
	    while(true) {
		sock.setSendBufferSize(max_size);
		if((retval=sock.getSendBufferSize()) < max_size)
		    return;
		max_size*=2;
	    }
	}
	catch(Throwable ex) {
	    retval=32000;
	    Trace.error("AUTOCONF.senseMaxSendBufferSize()", "failed getting the max send buffer size: " + ex +
			". Defaulting to " + retval);
	    return;
	}
	finally {
	    map.put("send_buf_size", new Integer(retval));
	}
    }

    void senseMaxReceiveBufferSize(HashMap map) {
	DatagramSocket sock=null;
	int            max_size=4096, retval=32000;

	try {
	    sock=new DatagramSocket();
	    while(true) {
		sock.setReceiveBufferSize(max_size);
		if((retval=sock.getReceiveBufferSize()) < max_size)
		    return;
		max_size*=2;
	    }
	}
	catch(Throwable ex) {
	    retval=32000;
	    Trace.error("AUTOCONF.senseMaxReceiveBufferSize()", "failed getting the max send buffer size: " + ex +
			". Defaulting to " + retval);
	    return;
	}
	finally {
	    map.put("recv_buf_size", new Integer(retval));
	}
    }


    /** Tries to find out the max number of bytes in a DatagramPacket we can send by sending increasingly
     * larger packets, until there is an exception (e.g. java.io.IOException: message too long)
     */
    void senseMaxFragSize(HashMap map) {
	int             max_send=32000;
	int             max_recv=32000;
	int             upper=8192;
	int             lower=upper;
	DatagramSocket  sock;
	byte[]          buf;
	DatagramPacket  packet;
	InetAddress     local_addr;

	if(config.containsKey("send_buf_size"))
	    max_send=((Integer)config.get("send_buf_size")).intValue();
	if(config.containsKey("recv_buf_size"))
	    max_recv=((Integer)config.get("recv_buf_size")).intValue();

	try {
	    sock=new DatagramSocket();
	    local_addr=InetAddress.getLocalHost();
	}
	catch(Exception ex) {
	    Trace.warn("AUTOCONF.senseMaxFragSize()", "failed creating DatagramSocket: " + ex);
	    return;
	}

	for(int i=0; i < num_iterations; i++) { // iterations to approximate frag_size
	    try {
		buf=new byte[upper];
		// System.out.println("** upper=" + upper + " (lower=" + lower + ")");
		packet=new DatagramPacket(buf, buf.length, local_addr, 9);
		sock.send(packet);
		lower=upper;
		upper*=2;
	    }
	    catch(IOException io_ex) {
		// System.err.println("upper (" + upper + ") was too big (lower=" + lower + ")");
		upper=(upper + lower) / 2;
	    }
	    catch(Throwable ex) {
		Trace.warn("AUTOCONF.senseMaxFragSize()", "exception=" + ex);
		break;
	    }
	}


	if(lower > max_send) {
	    Trace.warn("AUTOCONF.senseMaxFragSize()", "lower (" + lower + ") > max_send (" +
                       max_send + "): using max_send as frag_size");
	    lower=max_send;
	}
	map.put("frag_size", new Integer(lower));	
	if(Trace.trace) Trace.info("AUTOCONF.senseMaxFragSize()", "frag_size=" + lower);
    }
    /* ----------------------------------- End of Private metods --------------------------------------- */

}
