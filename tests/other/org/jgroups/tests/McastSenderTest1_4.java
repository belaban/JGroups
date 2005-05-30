// $Id: McastSenderTest1_4.java,v 1.5 2005/05/30 14:31:37 belaban Exp $

package org.jgroups.tests;

import java.io.DataInputStream;
import java.net.*;
import java.util.Enumeration;
import java.util.Vector;



/**
   Same as McastSenderTest, but uses all available interfaces (including loopback) to send the packets
   @see McastReceiverTest
   @author Bela Ban
   @version $Revision: 1.5 $
 */
public class McastSenderTest1_4 {

    public static void main(String args[]) {	
	MulticastSocket[] sockets=null;
	MulticastSocket   sock;
	InetAddress       mcast_addr=null, bind_addr=null;
	DatagramPacket    packet;
	byte[]            buf=new byte[0];
	String            tmp;
	int               ttl=32;
	String            line;
	DataInputStream   in;
	AckReceiver       ack_receiver=null;
	boolean           use_all_interfaces=false;
	int               port=5555;


	try {
	    for(int i=0; i < args.length; i++) {
		tmp=args[i];
		if("-help".equals(tmp)) {
		    help();
		    return;
		}
		if("-bind_addr".equals(tmp)) {
		    bind_addr=InetAddress.getByName(args[++i]);
		    continue;
		}
		if("-mcast_addr".equals(tmp)) {
		    mcast_addr=InetAddress.getByName(args[++i]);
		    continue;
		}
		if("-ttl".equals(tmp)) {
		    ttl=Integer.parseInt(args[++i]);
		    continue;
		}
		if("-port".equals(tmp)) {
		    port=Integer.parseInt(args[++i]);
		    continue;
		}
		if("-use_all_interfaces".equals(tmp)) {
		    use_all_interfaces=true;
		    continue;
		}
		help();
		return;
	    }
	    if(mcast_addr == null)
		mcast_addr=InetAddress.getByName("224.0.0.150");

	    if(use_all_interfaces) {
		if(!is1_4()) {
		    log.error("-use_all_interfaces flag requires JDK 1.4 or greater");
		    return;
		}
	    }	    
	}
	catch(Exception ex) {
	    log.error(ex);
	    return;
	}


	try {
	    if(use_all_interfaces) {
		Vector v=new Vector();
		
		for(Enumeration en=NetworkInterface.getNetworkInterfaces(); en.hasMoreElements();) {
		    NetworkInterface intf=(NetworkInterface)en.nextElement();
		    for(Enumeration e2=intf.getInetAddresses(); e2.hasMoreElements();) {
			bind_addr=(InetAddress)e2.nextElement();
			v.addElement(bind_addr);
		    }
		}
		sockets=new MulticastSocket[v.size()];
		for(int i=0; i < v.size(); i++) {
		    sock=new MulticastSocket(port);
		    sock.setTimeToLive(ttl);
		    sock.setInterface((InetAddress)v.elementAt(i));
		    sockets[i]=sock;
		    ack_receiver=new AckReceiver(sock);
		    ack_receiver.start();
		}
	    }
	    else {
		sockets=new MulticastSocket[1];
		sockets[0]=new MulticastSocket(port);
		sockets[0].setTimeToLive(ttl);
		if(bind_addr != null)
		    sockets[0].setInterface(bind_addr);
		ack_receiver=new AckReceiver(sockets[0]);
		ack_receiver.start();
	    }

	    for(int i=0; i < sockets.length; i++) {
		sock=sockets[i];
		if(sock == null) continue;
		System.out.println("Socket #" + (i+1) + '=' + sock.getLocalAddress() + ':' + sock.getLocalPort() +
				   ", ttl=" + sock.getTimeToLive() + ", bind interface=" + sock.getInterface());
	    }
	    
	    in=new DataInputStream(System.in);
	    while(true) {
		System.out.print("> ");
		line=in.readLine();
		if(line.startsWith("quit") || line.startsWith("exit"))
		    System.exit(0);
		buf=line.getBytes();
		packet=new DatagramPacket(buf, buf.length, mcast_addr, port);
		send(packet, sockets); // send on all interfaces
	    }
	}
	catch(Exception e) {
	    log.error(e);
	}
    }
    

	
    static void send(DatagramPacket packet, MulticastSocket[] sockets) {
	if(packet == null || sockets == null) return;
	for(int i=0; i < sockets.length; i++) {
	    try {
		if(sockets[i] != null)
		    sockets[i].send(packet);
	    }
	    catch(Exception ex) {
		log.error("McastSenderTest1_4.send(): " + ex);
	    }
	}
    }


    static boolean is1_4() {
	Class cl;
	
	try {
	    cl=Thread.currentThread().getContextClassLoader().loadClass("java.net.NetworkInterface");
	    return true;
	}
	catch(Throwable ex) {
	    return false;
	}
    }


    static void help() {
	System.out.println("McastSenderTest1_4 [-bind_addr <bind address>] [-help] [-mcast_addr <multicast address>] " +
			   "[-port <multicast port that receivers are listening on>] "  +
			   "[-ttl <time to live for mcast packets>] [-use_all_interfaces]");
    }
    




    private static class AckReceiver implements Runnable {
	DatagramSocket sock;
	DatagramPacket packet;
	byte[]         buf;
	Thread         t=null;

	AckReceiver(DatagramSocket sock) {
	    this.sock=sock;
	}

	public void run() {
	    while(t != null) {
		try {
		    buf=new byte[256];
		    packet=new DatagramPacket(buf, buf.length);
		    sock.receive(packet);
		    System.out.println("<< Received packet from " + 
				       packet.getAddress().getHostAddress() + ':' +
				       packet.getPort() + ": " + new String(packet.getData()));
		}
		catch(Exception e) {
		    log.error(e);
		    break;
		}
	    }
	    t=null;
	}

	void start() {
	    t=new Thread(this, "McastSenderTest1_4.AckReceiver thread");
	    t.start();
	}

	void stop() {
	    if(t != null && t.isAlive()) {
		t=null;
		try {
		    sock.close();
		}
		catch(Exception e) {
		}
	    }
	}
    }



}
