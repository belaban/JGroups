// $Id: McastSenderTest.java,v 1.6 2005/05/30 16:15:11 belaban Exp $

package org.jgroups.tests;


import java.io.DataInputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.MulticastSocket;




/**
 Tests IP multicast. Start one or more instances of McastReceiverTest which listen for IP mcast packets
 and then start McastSenderTest, which sends IP mcast packets (all have to have the same IPMCAST address and port).
 A TTL of 0 for the McastSenderTest means that packets will only be sent to receivers on the same host. If TTL > 0,
 other hosts will receive the packets too. Since many routers are dropping IPMCAST traffic, this is a good way to
 test whether IPMCAST works between different subnets.
 @see McastReceiverTest
 @author Bela Ban
 @version $Revision: 1.6 $
 */
public class McastSenderTest {

    public static void main(String args[]) {
        MulticastSocket sock;
        InetAddress mcast_addr=null, bind_addr=null;
        DatagramPacket packet;
        byte[] buf=new byte[0];
        String tmp;
        int ttl=32;
        String line;
        DataInputStream in;
        AckReceiver ack_receiver=null;
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
                if("-ttl".equals(tmp)) {
                    ttl=Integer.parseInt(args[++i]);
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
            sock=new MulticastSocket(port);
            sock.setTimeToLive(ttl);
            if(bind_addr != null)
                sock.setInterface(bind_addr);

            System.out.println("Socket=" + sock.getLocalAddress() + ':' + sock.getLocalPort() +
                               ", ttl=" + sock.getTimeToLive() + ", bind interface=" + sock.getInterface());

            ack_receiver=new AckReceiver(sock);
            ack_receiver.start();
            in=new DataInputStream(System.in);
            while(true) {
                System.out.print("> ");
                line=in.readLine();
                if(line.startsWith("quit") || line.startsWith("exit")) {
                    if(ack_receiver != null)
                        ack_receiver.stop();
                    break;
                }
                buf=line.getBytes();
                packet=new DatagramPacket(buf, buf.length, mcast_addr, port);
                sock.send(packet);
            }
        }
        catch(Exception e) {
            System.err.println(e);
        }

    }


    static void help() {
        System.out.println("McastSenderTest [-bind_addr <bind address>] [-help] [-mcast_addr <multicast address>] " +
                           "[-port <multicast port that receivers are listening on>] [-ttl <time to live for mcast packets>]");
    }


    private static class AckReceiver implements Runnable {
        DatagramSocket sock;
        DatagramPacket packet;
        byte[] buf;
        Thread t=null;

        AckReceiver(DatagramSocket sock) {
            this.sock=sock;
        }

        public void run() {
            while(t != null) {
                try {
                    buf=new byte[256];
                    packet=new DatagramPacket(buf, buf.length);
                    sock.receive(packet);
                    System.out.println("<< Received response from " +
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
            t=new Thread(this, "McastSenderTest.AckReceiver thread");
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
