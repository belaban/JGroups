// $Id: UcastTest.java,v 1.5.14.1 2008/01/22 10:01:30 belaban Exp $

package org.jgroups.tests;

import java.io.DataInputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.StringTokenizer;



/**
   @author Bela Ban
   @version $Revision: 1.5.14.1 $
 */
public class UcastTest {

    public static void main(String args[]) {	
	DatagramSocket  sock;
	DatagramPacket  packet;
	byte[]          buf=new byte[0];
	boolean         receiver=true;
	String          val;
	InetAddress     receiver_addr=null, sender_addr=null;
	int             receiver_port=0, sender_port=0;

	
	
	for(int i=0; i < args.length; i++) {
	    val=args[i];
	    if("-help".equals(val)) {
		help();
		return;
	    }
	    if("-sender".equals(val)) {
		receiver=false;
		continue;
	    }
	    if("-sender_addr".equals(val)) {
		try {
		    sender_addr=InetAddress.getByName(args[++i]);
		    sender_port=Integer.parseInt(args[++i]);
		    continue;
		}
		catch(Exception e) {
		    System.err.println(e);
		    help();
		    return;
		}
	    }
	    if("-receiver".equals(val)) {
		receiver=true;
		try {
		    receiver_addr=InetAddress.getByName(args[++i]);
		    receiver_port=Integer.parseInt(args[++i]);
		}
		catch(Exception e) {
		    System.err.println(e);
		    help();
		    return;
		}
		continue;
	    }
	    help();
	}


	try {
	    if(receiver) {
		sock=new DatagramSocket(receiver_port, receiver_addr);
		System.out.println("Socket=" + sock.getLocalAddress() + ':' + sock.getLocalPort());
		System.out.println("starting as receiver");
		receiverLoop(sock);
	    }
	    else {
		if(sender_addr != null && sender_port > 0)
		    sock=new DatagramSocket(sender_port, sender_addr);
		else
		    sock=new DatagramSocket();
		System.out.println("Socket=" + sock.getLocalAddress() + ':' + sock.getLocalPort());
		System.out.println("starting as sender");
		senderLoop(sock);
	    }
	}
	catch(Exception e) {
	    System.err.println(e);
	}

    }


    static void receiverLoop(DatagramSocket sock) {
	DatagramPacket packet;
	byte[]         buf;

	while(true) {
	    buf=new byte[256];
	    packet=new DatagramPacket(buf, buf.length);
	    try {
		sock.receive(packet);
		System.out.println("Receive packet from " + packet.getAddress().getHostAddress() +
                           ':' + packet.getPort() + ": " + new String(packet.getData()));

		buf="ACK".getBytes();
		packet=new DatagramPacket(buf, buf.length, packet.getAddress(), packet.getPort());
		sock.send(packet);
	    }
	    catch(Exception e) {
		System.err.println(e);
		break;
	    }
	}
    }


    
    static void senderLoop(DatagramSocket sock) {
	DataInputStream in;
	String          line;
	DatagramPacket  packet;
	InetAddress     dst;
	int             dst_port;
	StringTokenizer tok;
	StringBuilder    sb;
	String          tmp, buf;
	byte[]          bbuf;
	AckReceiver     ack_receiver=new AckReceiver(sock);

	ack_receiver.start();
	in=new DataInputStream(System.in);
	while(true) {
	    try {
		System.out.print("> ");
		line=in.readLine();
		if(line.startsWith("quit") || line.startsWith("exit"))
		    break;
		tok=new StringTokenizer(line);
		try {
		    dst=InetAddress.getByName(tok.nextToken());
		    dst_port=Integer.parseInt(tok.nextToken());
		}
		catch(Exception e) {
		    System.err.println(e);
		    continue;
		}
		sb=new StringBuilder();
		while(tok.hasMoreTokens()) {
		    tmp=tok.nextToken();
		    sb.append(tmp + ' ');
		}
		buf=sb.toString();
		System.out.println("sending " + buf);
		bbuf=buf.getBytes();
		packet=new DatagramPacket(bbuf, bbuf.length, dst, dst_port);
		sock.send(packet);
	    }
	    catch(Exception e) {
		System.err.println(e);
		break;
	    }
	}
	ack_receiver.stop();
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
		    System.err.println(e);
		    break;
		}
	    }
	    t=null;
	}

	void start() {
	    t=new Thread(this, "UcastTest.AckReceiver thread");
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


    static void help() {
	System.out.println("UcastTest [-help] [-sender | -receiver <local addr> <local port>] " +
			   "[-sender_addr <sender's bind address> <sender' port>]");
    }

    
}
