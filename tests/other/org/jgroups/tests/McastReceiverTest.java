
package org.jgroups.tests;

import java.net.*;
import java.util.Enumeration;


/**
 * Tests IP multicast. Start one or more instances of McastReceiverTest which listen for IP mcast packets
 * and then start McastSenderTest, which sends IP mcast packets (all have to have the same IPMCAST address and port).
 * A TTL of 0 for the McastSenderTest means that packets will only be sent to receivers on the same host. If TTL > 0,
 * other hosts will receive the packets too. Since many routers are dropping IPMCAST traffic, this is a good way to
 * test whether IPMCAST works between different subnets.
 *
 * @author Bela Ban
 * @version $Revision: 1.7 $
 * @see McastSenderTest
 */
public class McastReceiverTest {

    public static void main(String[] args) {
        InetAddress mcast_addr=null, bind_addr=null;
        String tmp;
        int port=5555;

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
            if (bind_addr != null) {
                Receiver r=new Receiver(mcast_addr, bind_addr, port);
                r.start();
            }
            else {
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
        }
        catch(Exception e) {
            System.err.println(e);
        }

    }


    static void help() {
        System.out.println("McastReceiverTest [-bind_addr <bind address>] [-help] [-mcast_addr <multicast address>] " +
                "[-port <port for multicast socket>]");
    }


    static class Receiver extends Thread {
        MulticastSocket sock=null;
        DatagramPacket packet;
        byte[] buf=null;
        byte[] recv_buf;
        int recv_len=0;

        Receiver(InetAddress mcast_addr, InetAddress bind_interface, int port) throws Exception {
            sock=new MulticastSocket(port);
            if(bind_interface != null)
                sock.setNetworkInterface(NetworkInterface.getByInetAddress(bind_interface));
            try {
                sock.joinGroup(new InetSocketAddress(mcast_addr, port),
                               bind_interface == null? null : NetworkInterface.getByInetAddress(bind_interface));
                System.out.println("Socket=" + sock.getLocalAddress() + ':' + sock.getLocalPort() + ", bind interface=" +
                                     sock.getNetworkInterface());
            }
            catch(Exception ex) {
                System.err.printf("failed joining interface %s: %s\n", bind_interface, ex);
            }
        }


        public void run() {
            while(true) {
                try {
                    buf=new byte[256];
                    packet=new DatagramPacket(buf, buf.length);
                    sock.receive(packet);
                    recv_buf=packet.getData();
                    recv_len=packet.getLength();
                    System.out.println(new String(recv_buf,0,recv_len) + " [sender=" + packet.getAddress().getHostAddress() +
                            ':' + packet.getPort() + ']');
                }
                catch(Exception ex) {
                    System.err.println("Receiver terminated: " + ex);
                    break;
                }
            }
        }
    }


}
