// $Id: MessageSerializationTest.java,v 1.1 2003/09/09 01:24:13 belaban Exp $

package org.jgroups.tests;

/**
 * Title:        Java Groups Communications
 * Description:  Contact me at <a href="mailto:mail@filip.net">mail@filip.net</a>
 * Copyright:    Copyright (c) 2002
 * Company:      www.filip.net
 * @author Filip Hanik
 * @version 1.0
 */
import org.jgroups.Message;
import org.jgroups.stack.IpAddress;
import java.net.InetAddress;
import org.jgroups.protocols.*;
import org.jgroups.log.Trace;
import org.jgroups.conf.ClassConfigurator;


public class MessageSerializationTest {

    public MessageSerializationTest() {}

    
    public static void main(String[] args) throws Exception {
	boolean     add_headers=false;
        InetAddress addr = InetAddress.getLocalHost();
	int         num=10000;

	for(int i=0; i < args.length; i++) {
	    if(args[i].equals("-help")) {
		help();
		return;
	    }
	    if(args[i].equals("-add_headers")) {
		add_headers=true;
		continue;
	    }
	    if(args[i].equals("-num")) {
		num=Integer.parseInt(args[++i]);
		continue;
	    }
	}
	
        
	Trace.init();
	ClassConfigurator.getInstance();
        long start = System.currentTimeMillis();
        for (int i=0; i < num; i++ ) {
            Message m = new Message(new IpAddress(addr,5555),new IpAddress(addr,6666),new byte[256]);
	    if(add_headers)
		addHeaders(m);
            java.io.ByteArrayOutputStream msg_data = new java.io.ByteArrayOutputStream();
            java.io.ObjectOutputStream msg_out = new java.io.ObjectOutputStream(msg_data);
            m.writeExternal(msg_out);
            msg_out.flush();
            msg_out.close();
            byte[] data = msg_data.toByteArray();
            java.io.ByteArrayInputStream msg_in_data = new java.io.ByteArrayInputStream(data);
            java.io.ObjectInputStream msg_in = new java.io.ObjectInputStream(msg_in_data);
            Message m2 = (Message)Message.class.newInstance();
            m2.readExternal(msg_in);
        }
        
        long stop = System.currentTimeMillis();
        System.out.println("Serializing and deserializing a message " + num + " times took "+(stop-start)+"ms.");
    }

    /** Adds some dummy headers to the message */
    static void addHeaders(Message msg) {
	msg.putHeader("UDP", new UdpHeader("MyGroup"));
	msg.putHeader("PING", new PingHeader(PingHeader.GET_MBRS_REQ, null));
	msg.putHeader("FD_SOCK", new FD_SOCK.FdHeader());
	msg.putHeader("VERIFY_SUSPECT", new VERIFY_SUSPECT.VerifyHeader());
	msg.putHeader("STABLE", new org.jgroups.protocols.pbcast.STABLE.StableHeader());
	msg.putHeader("NAKACK", new org.jgroups.protocols.pbcast.NakAckHeader());
	msg.putHeader("UNICAST", new UNICAST.UnicastHeader());
	msg.putHeader("FRAG", new FRAG.FragHeader());
	msg.putHeader("GMS", new org.jgroups.protocols.pbcast.GMS.GmsHeader());
    }


    static void help() {
	System.out.println("MessageSerializationTest [-help] [-add_headers] [-num <iterations>]");
    }
}
