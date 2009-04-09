// $Id: FragTestUnicast.java,v 1.9 2009/04/09 09:11:20 belaban Exp $


package org.jgroups.tests;


import org.jgroups.*;
import org.jgroups.util.Util;

import java.util.Vector;




/**
 * Tests the fragmentation protocol (FRAG) for large unicast messages. The first member needs to be started as
 * receiver. The second member needs to be started as sender (-sender). The latter then starts sending unicast
 * messages to the receiver, which will be fragmented. If the relation between message size and fragmentation
 * size results in many fragments to be sent, then the receive and send buffer sizes in UDP have to be
 * adjusted accordingly, otherwise packets will be dropped and retransmitted.
 * @author Bela Ban April 19 2001
 */
public class FragTestUnicast {
    int         mode=0;  // 0=receiver, 1=sender
    Channel     channel;
    String      props;
    int         i=1;
    Message     msg;
    Object      obj;
    int         MSG_SIZE;  // bytes
    String      groupname="FragTestUnicastGroup";
    char        sendingChar;
    int         num_msgs=10;
    long        timeout=3000;
    int         frag_size=20000;
    Address     local_addr;



    public FragTestUnicast(char character , int mode, int msg_size, int num_msgs, long timeout, int frag_size) {
	this.sendingChar = character;
	this.mode=mode;
	this.MSG_SIZE=msg_size;
	this.num_msgs=num_msgs;
	this.timeout=timeout;
	this.frag_size=frag_size;


	
	props="UDP(mcast_addr=224.10.20.3;mcast_port=45566;ip_ttl=32;" +
            "ucast_send_buf_size=" + (frag_size*2) + ";ucast_recv_buf_size=" + (frag_size*4) + "):" +
            "PING(timeout=3000;num_initial_members=2):" +
            "FD(timeout=5000):" +
            "VERIFY_SUSPECT(timeout=1500):" +
            "pbcast.NAKACK(gc_lag=20;retransmit_timeout=2000):" +
            "UNICAST(timeout=4000):" +
            "pbcast.STABLE(desired_avg_gossip=10000):" +
            "FRAG(frag_size=" + frag_size + "):" +
            "pbcast.GMS(join_timeout=5000;" +
            "shun=false;print_local_addr=true)";
    }
    
    


    public void start() throws Exception {
	Vector   mbrs;
	Object   obj;
	View     view;
	Address  receiver=null;


	channel=new JChannel(props);
	// if(mode == 1) channel.setOpt(Channel.LOCAL, Boolean.FALSE);
	channel.connect(groupname);
	local_addr=channel.getAddress();

	if(mode == 1) {

	    // waiting for receiver
	    mbrs=channel.getView().getMembers();
	    if(mbrs.size() < 2) {
		// loop until a new member has joined
		System.out.println("Waiting for receiver to join group");
		while(true) {
		    obj=channel.receive(0);
		    if(obj instanceof View) {
			view=(View)obj;
			mbrs=view.getMembers();
			if(mbrs.size() >= 2)
			    break;
		    }
		}
	    }
	    for(int i=0; i < mbrs.size(); i++) {
		if(mbrs.elementAt(i).equals(local_addr))
		    continue;
		else {
		    receiver=(Address)mbrs.elementAt(i);
		    break;
		}
	    }
	    System.out.println("Receiver joined group: I'm starting to send messages to " + receiver);

	    for(int j=0; j < num_msgs; j++) {
		msg=createBigMessage(MSG_SIZE);
		msg.setDest(receiver);
		System.out.println("Sending msg (" + MSG_SIZE + " bytes) to " + receiver);
		channel.send(msg);
		System.out.println("Done Sending msg (" + MSG_SIZE + " bytes)");
		Util.sleep(timeout);
	    }
	    System.out.println("Press [return] to exit");
	    System.in.read();
	}
	else {
	    System.out.println("Waiting for messages:");

	    while(true) {
		try {
		    obj=channel.receive(0);
		    if(obj instanceof Message)
			{
			    System.out.println("Received message: " + obj);
			    Message tmp = (Message)obj;
			    byte[] buf = tmp.getBuffer();
			    for (int i=0; i<(10 < MSG_SIZE ? 10 : MSG_SIZE); i++)
				{
				    System.out.print((char)buf[i]);
				}
			    System.out.println();

			}
		}
		catch(Exception e) {
		    System.err.println(e);
		}
	    }
	
	}

	channel.close();
    }



    Message createBigMessage(int size) {
	byte[] buf=new byte[size];
	for(int i=0; i < buf.length; i++) buf[i]=(byte)sendingChar;
	return new Message(null, null, buf);
    }



    public static void main(String[] args) {
	char defaultChar = 'A';
	int  default_mode=0; // receiver
	int  MSG_SIZE=30000;
	int  num_msgs=5;
	long timeout=1000;
	int  frag_size=8195;

	for(int i=0; i < args.length; i++) {
	    if("-help".equals(args[i])) {
		usage();
		return;
	    }
	    if("-sender".equals(args[i])) {
		default_mode=1;
		continue;
	    }
	    if("-size".equals(args[i])) {
		MSG_SIZE=Integer.parseInt(args[++i]);
		continue;
	    }
	    if("-num_msgs".equals(args[i])) {
		num_msgs=Integer.parseInt(args[++i]);
		continue;
	    }
	    if("-frag_size".equals(args[i])) {
		frag_size=Integer.parseInt(args[++i]);
		continue;
	    }
	    if("-timeout".equals(args[i])) {
		timeout=Long.parseLong(args[++i]);
		continue;
	    }
	    if("-char".equals(args[i])) {
		defaultChar=args[++i].charAt(0);
		continue;
	    }
	    usage();
	    return;
	}

	try {
	    new FragTestUnicast(defaultChar , default_mode, MSG_SIZE, num_msgs, timeout, frag_size).start();
	}
	catch(Exception e) {
	    System.err.println(e);
	}
    }


    static void usage() {
	System.out.println("FragTestUnicast [-sender] [-size <message size (in bytes)>] [-timeout <msecs>]"+
			   " [-num_msgs <number of messages>] [-char <frag character>] " + 
			   "[-frag_size <fragmentation size>] [-help]");
    }

}

