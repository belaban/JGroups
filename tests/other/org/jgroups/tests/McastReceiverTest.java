// $Id: McastReceiverTest.java,v 1.1 2003/09/09 01:24:13 belaban Exp $

package org.jgroups.tests;


import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;




/**
 Tests IP multicast. Start one or more instances of McastReceiverTest which listen for IP mcast packets
 and then start McastSenderTest, which sends IP mcast packets (all have to have the same IPMCAST address and port).
 A TTL of 0 for the McastSenderTest means that packets will only be sent to receivers on the same host. If TTL > 0,
 other hosts will receive the packets too. Since many routers are dropping IPMCAST traffic, this is a good way to
 test whether IPMCAST works between different subnets.
 @see McastSenderTest
 @author Bela Ban
 @version $Revision: 1.1 $
 */
public class McastReceiverTest {

    public static void main(String args[]) {
        MulticastSocket sock;
        InetAddress mcast_addr=null, bind_addr=null;
        DatagramPacket packet;
        byte buf[]=null;
        byte[] recv_buf;
        String tmp;
        int port=5555;

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


        try {
            sock=new MulticastSocket(port);
            if(bind_addr != null)
                sock.setInterface(bind_addr);
            sock.joinGroup(mcast_addr);
            System.out.println("Socket=" + sock.getLocalAddress() + ":" + sock.getLocalPort() + ", bind interface=" +
                               sock.getInterface());

            while(true) {
                buf=new byte[256];
                packet=new DatagramPacket(buf, buf.length);
                sock.receive(packet);
                recv_buf=packet.getData();
                System.out.println(new String(recv_buf) + " [sender=" + packet.getAddress().getHostAddress() +
                                   ":" + packet.getPort() + "]");
            }

        }
        catch(Exception e) {
            System.err.println(e);
        }

    }


    static void help() {
        System.out.println("McastSenderTest [-bind_addr <bind address>] [-help] [-mcast_addr <multicast address>] " +
                           "[-port <port for multicast socket>]");
    }


}
