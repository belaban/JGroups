// $Id: McastReceiverTest.java,v 1.8 2008/07/17 17:48:32 belaban Exp $

package org.jgroups.tests;


import java.net.*;
import java.util.Enumeration;


/**
 Tests IP multicast. Start one or more instances of McastReceiverTest which listen for IP mcast packets
 and then start McastSenderTest, which sends IP mcast packets (all have to have the same IPMCAST address and port).
 A TTL of 0 for the McastSenderTest means that packets will only be sent to receivers on the same host. If TTL > 0,
 other hosts will receive the packets too. Since many routers are dropping IPMCAST traffic, this is a good way to
 test whether IPMCAST works between different subnets.
 @see McastSenderTest
 @author Bela Ban
 @version $Revision: 1.8 $
 */
public class McastReceiverTest {

    public static void main(String args[]) {
        MulticastSocket sock;
        InetAddress bind_addr=null, mcast_addr=null;
        DatagramPacket packet;
        byte buf[]=null;
        byte[] recv_buf;
        String tmp;
        int port=5555;
        boolean receive_on_all_interfaces=false;

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
                if("-port".equals(tmp)) {
                    port=Integer.parseInt(args[++i]);
                    continue;
                }
                if(("-receive_on_all_interfaces".equals(args[i]))) {
                    receive_on_all_interfaces=true;
                    continue;
                }
                help();
                return;
            }
            if(mcast_addr == null)
                mcast_addr=InetAddress.getByName("224.10.10.150");
        }
        catch(Exception ex) {
            System.err.println(ex);
            return;
        }


        try {
            sock=new MulticastSocket(port);
            SocketAddress join_addr=new InetSocketAddress(mcast_addr, port);


            if(receive_on_all_interfaces) {
                NetworkInterface intf;
                for(Enumeration en=NetworkInterface.getNetworkInterfaces(); en.hasMoreElements();) {
                    intf=(NetworkInterface)en.nextElement();
                    sock.joinGroup(join_addr, intf);
                    System.out.println("joined " + join_addr + " on " + intf.getName());
                }
            }
            else {
                if(bind_addr != null)
                    sock.setInterface(bind_addr);
                sock.joinGroup(join_addr, null);
            }


            System.out.println("Socket=" + sock.getLocalAddress() + ':' + sock.getLocalPort() + ", bind interface=" +
                               sock.getInterface());

            int length;
            while(true) {
                buf=new byte[256];
                packet=new DatagramPacket(buf, buf.length);
                sock.receive(packet);
                recv_buf=packet.getData();
                length=packet.getLength();
                System.out.println(new String(recv_buf, 0, length) + " [sender=" + packet.getAddress().getHostAddress() +
                                   ':' + packet.getPort() + ']');
                byte[] buf2="Hello from Bela".getBytes();
                DatagramPacket rsp=new DatagramPacket(buf2, buf2.length, packet.getAddress(), packet.getPort());
                sock.send(rsp);
            }

        }
        catch(Exception e) {
            System.err.println(e);
        }

    }


    static void help() {
        System.out.println("McastSenderTest [-bind_addr <bind address>] [-help] [-mcast_addr <multicast address>] " +
                           "[-port <port for multicast socket>] [-receive_on_all_interfaces]");
    }


}
