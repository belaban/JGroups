// $Id: NetworkUtilization.java,v 1.4 2004/07/05 14:15:22 belaban Exp $

package org.jgroups.tests;

import org.jgroups.util.Util;

import java.net.*;

/**
 * @author Bela Ban
 */
public class NetworkUtilization {
    boolean sender=false;
    InetAddress mcast_addr;
    int mcast_port=7500;
    long start, stop;
    long num_received=0;
    int received_packet_size=0;
    MulticastSocket mcast_sock=null;


    class MyTimer extends Thread {

        MyTimer() {
        }

        public void run() {

            if(mcast_sock != null)
                mcast_sock.close();

            long stop=System.currentTimeMillis();
            long diff=stop-start;
            double secs=diff/1000.0;
            double num_kb_received=num_received * received_packet_size / 1000.0;

            Util.sleep(200);
            System.out.println("** took " + secs + " secs to receive " + num_received + " msgs (" +
                    (num_received / secs) + " msgs/sec)\n" +
                    "** throughput: " + num_kb_received / secs + " KB/sec");
        }
    }


    byte[] createBuffer(int size) {
        byte[] buf=new byte[size];
        for(int i=0; i < buf.length; i++) {
            buf[i]='.';
        }
        return buf;
    }

    public void start(boolean sender, int packet_size) throws Exception {
        this.sender=sender;

        mcast_addr=InetAddress.getByName("228.8.8.8");

        if(sender) {
            DatagramSocket sock=new DatagramSocket();
            byte[] buf=createBuffer(packet_size);

            System.out.println("-- starting to send packets");
            while(true) {
                DatagramPacket p=new DatagramPacket(buf, buf.length, mcast_addr, mcast_port);
                sock.send(p);
            }
        }
        else {
            Runtime.getRuntime().addShutdownHook(new MyTimer());
            byte[] buf=new byte[1000000];
            boolean first=true;
            DatagramPacket p=new DatagramPacket(buf, buf.length);
            mcast_sock=new MulticastSocket(mcast_port);
            // sock.setLoopbackMode(true); // disable reception of own mcasts
            mcast_sock.joinGroup(mcast_addr);
            System.out.println("-- joined group " + mcast_addr + ':' + mcast_port + ", waiting for packets\n" +
                    "(press ctrl-c to kill)");
            while(true) {
                p.setData(buf);
                try {
                    mcast_sock.receive(p);
                }
                catch(SocketException ex) {
                    break;
                }
                if(first) {
                    first=false;
                    start=System.currentTimeMillis();
                    received_packet_size=p.getLength();
                }
                num_received++;
                //System.out.println("-- received " + p.getLength() + " bytes from " + p.getAddress() + ":" + p.getPort());
                if(num_received % 1000 == 0)
                    System.out.println(num_received);
            }
        }

    }



    public static void main(String[] args) {
        boolean sender=false;
        int     packet_size=10;

        for(int i=0; i < args.length; i++) {
            if("-help".equals(args[i])) {
                help();
                return;
            }
            if("-sender".equals(args[i])) {
                sender=true;
                continue;
            }
            if("-size".equals(args[i])) {
                packet_size=Integer.parseInt(args[++i]);
                continue;
            }
            help();
            return;
        }

        try {
            new NetworkUtilization().start(sender, packet_size);
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    static void help() {
        System.out.println("NetworkUtilization [-help] [-sender] [-size <packet size in bytes>]");
    }


}
