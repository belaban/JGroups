// $Id: McastReceiverTest1_4.java,v 1.1 2003/09/09 01:24:13 belaban Exp $

package org.jgroups.tests;

import java.net.DatagramPacket;
import java.net.*;
import java.util.Enumeration;



/**
   Tests IP multicast. Start one or more instances of McastReceiverTest1_4 which listen for IP mcast packets
   and then start McastSenderTest, which sends IP mcast packets (all have to have the same IPMCAST address and port).
   A TTL of 0 for the McastSenderTest means that packets will only be sent to receivers on the same host. If TTL > 0,
   other hosts will receive the packets too. Since many routers are dropping IPMCAST traffic, this is a good way to
   test whether IPMCAST works between different subnets.
   This class compiles and runs only under JDK 1.4 or higher
   @see McastSenderTest
   @author Bela Ban
   @version $Revision: 1.1 $
 */
public class McastReceiverTest1_4 {

    public static void main(String args[]) {	
	InetAddress     mcast_addr=null, bind_addr=null;
	String          tmp;
	int             port=5555;
	boolean         use_all_interfaces=false;

	try {
	    for(int i=0; i < args.length; i++) {
		tmp=args[i];
		if(tmp.equals("-help")) {
		    help();
		    return;
		}
		if(tmp.equals("-bind_addr")) {
		    bind_addr=InetAddress.getByName(args[++i]);
		    continue;
		}
		if(tmp.equals("-mcast_addr")) {
		    mcast_addr=InetAddress.getByName(args[++i]);
		    continue;
		}
		if(tmp.equals("-port")) {
		    port=Integer.parseInt(args[++i]);
		    continue;
		}
		if(tmp.equals("-use_all_interfaces")) {
		    use_all_interfaces=true;
		    continue;
		}
		help();
		return;
	    }
	    if(mcast_addr == null)
		mcast_addr=InetAddress.getByName("224.0.0.150");
	}
	catch(Exception ex) {
	    System.err.println(ex);
	    return;
	}

	if(use_all_interfaces) {
	    if(!is1_4()) {
		System.err.println("-use_all_interfaces flag requires JDK 1.4 or greater");
		return;
	    }
	}


	try {
	    if(use_all_interfaces) {
		for(Enumeration en=NetworkInterface.getNetworkInterfaces(); en.hasMoreElements();) {
		    NetworkInterface intf=(NetworkInterface)en.nextElement();
		    for(Enumeration e2=intf.getInetAddresses(); e2.hasMoreElements();) {
			bind_addr=(InetAddress)e2.nextElement();
			// System.out.println("Binding multicast socket to " + bind_addr);
			Receiver r=new Receiver(mcast_addr, bind_addr, port);
			r.start();
		    }
		}
	    }
	    else {
		Receiver r=new Receiver(mcast_addr, bind_addr, port);
		r.start();
	    }
	}
	catch(Exception e) {
	    System.err.println(e);
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
	System.out.println("McastSenderTest [-bind_addr <bind address>] [-help] [-mcast_addr <multicast address>] " +
			   "[-port <port for multicast socket>] [-use_all_interfaces (JDK 1.4 only)]");
    }



    static class Receiver extends Thread {
	MulticastSocket sock=null;
	DatagramPacket  packet;
	byte            buf[]=null;
	byte[]          recv_buf;

	Receiver(InetAddress mcast_addr, InetAddress bind_interface, int port) throws Exception {
	    sock=new MulticastSocket(port);
	    if(bind_interface != null)
		sock.setInterface(bind_interface);
	    sock.joinGroup(mcast_addr);	    
	    System.out.println("Socket=" + sock.getLocalAddress() + ":" + sock.getLocalPort() + ", bind interface=" +
			       sock.getInterface());
	}
	

	public void run() {
	    while(true) {
		try {
		    buf=new byte[256];
		    packet=new DatagramPacket(buf, buf.length);
		    sock.receive(packet);
		    recv_buf=packet.getData();
		    System.out.println(new String(recv_buf) + " [sender=" + packet.getAddress().getHostAddress() +
				       ":" + packet.getPort() + "]");
		}
		catch(Exception ex) {
		    System.err.println("Receiver terminated: " + ex);
		    break;
		}
	    }
	}
    }

    
}
