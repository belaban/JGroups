
package org.jgroups.tests;

import java.io.DataInputStream;
import java.net.*;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;


/**
 * Same as McastSenderTest, but uses all available interfaces (including loopback) to send the packets
 *
 * @author Bela Ban
 * @version $Revision: 1.7 $
 * @see McastReceiverTest
 */
public class McastSenderTest {

    public static void main(String args[]) {
        MulticastSocket[] sockets=null;
        MulticastSocket sock;
        InetAddress mcast_addr=null, bind_addr=null;
        DatagramPacket packet;
        byte[]            buf=new byte[0];
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
            List<InetAddress> v=new ArrayList<>();

            if (bind_addr != null) {
                v.add(bind_addr);
            }
            else {
                for(Enumeration en=NetworkInterface.getNetworkInterfaces(); en.hasMoreElements();) {
                    NetworkInterface intf=(NetworkInterface)en.nextElement();
                    for(Enumeration e2=intf.getInetAddresses(); e2.hasMoreElements();) {
                        bind_addr=(InetAddress)e2.nextElement();
                        v.add(bind_addr);
                    }
                }
            }

            sockets=new MulticastSocket[v.size()];
            for(int i=0; i < v.size(); i++) {
                sock=new MulticastSocket(port);
                sock.setTimeToLive(ttl);
                sock.setInterface(v.get(i));
                sockets[i]=sock;
                ack_receiver=new AckReceiver(sock);
                ack_receiver.start();
            }


            for(int i=0; i < sockets.length; i++) {
                sock=sockets[i];
                if(sock == null) continue;
                System.out.println("Socket #" + (i + 1) + '=' + sock.getLocalAddress() + ':' + sock.getLocalPort() +
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
            System.err.println(e);
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
                System.err.println("McastSenderTest.send(): " + ex);
            }
        }
    }




    static void help() {
        System.out.println("McastSenderTest [-bind_addr <bind address>] [-help] [-mcast_addr <multicast address>] " +
                "[-port <multicast port that receivers are listening on>] " +
                "[-ttl <time to live for mcast packets>]");
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
