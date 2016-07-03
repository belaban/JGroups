package org.jgroups.tests;

import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.util.Util;

import java.io.IOException;
import java.net.*;

/**
 * Simple protocol to test round trip times. Requests are [PING], responses are [PONG]. Start multiple instances
 * and press <return> to get the round trip times for all nodes in the cluster, This program doesn't use JGroups at all,
 * see {@link org.jgroups.tests.PingPong} for a comparison.
 * @author Bela Ban
 */
public class PingPongDatagram {
    MulticastSocket mcast_sock;

    static final SocketAddress MCAST_GROUP;

    Log log=LogFactory.getLog(PingPongDatagram.class);


    static final byte PING = 1;
    static final byte PONG = 2;

    static final byte[] PING_REQ={PING};
    static final byte[] PONG_RSP={PONG};

    long start=0;


    static {
        MCAST_GROUP=new InetSocketAddress("239.5.5.5", 7500);
    }


    public void start() throws Exception {
        mcast_sock=new MulticastSocket(7500);
        mcast_sock.joinGroup(MCAST_GROUP, NetworkInterface.getByName("192.168.1.5"));

        Receiver receiver=new Receiver();
        receiver.start();

        while(true) {
            Util.keyPress("enter to ping");
            DatagramPacket packet=new DatagramPacket(PING_REQ, 0, PING_REQ.length, MCAST_GROUP);
            start=System.nanoTime();
            mcast_sock.send(packet);
        }
    }




    class Receiver extends Thread {
        byte[] buf=new byte[1];

        public void run() {
            while(true) {
                DatagramPacket packet=new DatagramPacket(buf, 0, buf.length);
                try {
                    mcast_sock.receive(packet);
                    SocketAddress sender=packet.getSocketAddress();
                    byte type=packet.getData()[0];

                    switch(type) {
                        case PING:
                            DatagramPacket rsp=new DatagramPacket(PONG_RSP, 0, PONG_RSP.length, MCAST_GROUP);
                            mcast_sock.send(rsp);
                            break;
                        case PONG:
                            long rtt=System.nanoTime() - start;
                            double ms=rtt / 1000.0 / 1000.0;
                            System.out.printf("RTT for %s: %.2f ms\n", sender, ms);
                            break;
                        default:
                            System.err.println("type " + type + " unknown");
                    }
                }
                catch(IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }




    public static void main(String[] args) throws Exception {
        new PingPongDatagram().start();
    }
}