// $Id: McastTest.java,v 1.1.1.1 2003/09/09 01:24:13 belaban Exp $

package org.jgroups.tests;

import java.io.DataInputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.StringTokenizer;



/**
   @author Bela Ban
   @version $Revision: 1.1.1.1 $
 */
public class McastTest {

    public static void main(String args[]) {	
	DatagramSocket  sock;
	DatagramPacket  packet;
	byte[]          buf=new byte[0];
	boolean         receiver=true;
	String          val;
	InetAddress     addr=null;
	int             port=0;

	
	for(int i=0; i < args.length; i++) {
	    val=args[i];
	    if(val.equals("-help")) {
		help();
		return;
	    }
	    if(val.equals("-sender")) {
		receiver=false;
		continue;
	    }
	    if(val.equals("-receiver")) {
		receiver=true;
		try {
		    addr=InetAddress.getByName(args[++i]);
		    port=Integer.parseInt(args[++i]);
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
		sock=new DatagramSocket(port, addr);
		System.out.println("Socket=" + sock.getLocalAddress() + ":" + sock.getLocalPort());
		System.out.println("starting as receiver");
		receiverLoop(sock);
	    }
	    else {
		sock=new DatagramSocket();
		System.out.println("Socket=" + sock.getLocalAddress() + ":" + sock.getLocalPort());
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
				   ":" + packet.getPort() + ": " + new String(packet.getData()));
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
	StringBuffer    sb;
	String          tmp, buf;
	byte[]          bbuf;
	
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
		sb=new StringBuffer();
		while(tok.hasMoreTokens()) {
		    tmp=tok.nextToken();
		    sb.append(tmp + " ");
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
    }


    static void help() {
	System.out.println("McastTest [-help] [-sender | -receiver <local addr> <local port>]");
    }

    
}
