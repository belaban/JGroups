// $Id: NetworkUtilization.java,v 1.1 2004/01/05 18:04:24 belaban Exp $

package org.jgroups.tests;

import java.net.MulticastSocket;
import java.net.InetAddress;
import java.net.DatagramSocket;
import java.net.DatagramPacket;

/**
 * @author Bela Ban
 */
public class NetworkUtilization {
    boolean sender=false;
    InetAddress mcast_addr;
    int mcast_port=7500;
    long start, stop;
    long num_received=0;
    final int NUM_BYTES=1;


    class MyTimer extends Thread {

        MyTimer() {
        }

        public void run() {
            long stop=System.currentTimeMillis();
            long diff=stop-start;
            System.out.println("-- took " + diff/1000.0 + " secs to receive " + num_received + " msgs (" +
                    (num_received / (diff/1000.0)) + " msgs/sec)");
        }
    }


    byte[] createBuffer(int size) {
        byte[] buf=new byte[size];
        for(int i=0; i < buf.length; i++) {
            buf[i]='.';
        }
        return buf;
    }

    public void start(boolean sender) throws Exception {
        this.sender=sender;

        mcast_addr=InetAddress.getByName("228.8.8.8");

        if(sender) {
            DatagramSocket sock=new DatagramSocket();
            byte[] buf=createBuffer(NUM_BYTES);

            System.out.println("-- starting to send packets");
            while(true) {
                DatagramPacket p=new DatagramPacket(buf, buf.length, mcast_addr, mcast_port);
                sock.send(p);
            }
        }
        else {
            Runtime.getRuntime().addShutdownHook(new MyTimer());
            byte[] buf=new byte[1024];
            boolean first=true;
            DatagramPacket p=new DatagramPacket(buf, buf.length);
            MulticastSocket sock=new MulticastSocket(mcast_port);
            //sock.setLoopbackMode(true); // disable reception of own mcasts
            sock.joinGroup(mcast_addr);
            System.out.println("-- joined group " + mcast_addr + ":" + mcast_port + ", waiting for packets");
            while(true) {
                p.setData(buf);
                sock.receive(p);
                if(first) {
                    first=false;
                    start=System.currentTimeMillis();
                }
                num_received++;
                System.out.println("-- received " + p.getLength() + " bytes from " + p.getAddress() + ":" + p.getPort());
            }
        }

    }



    public static void main(String[] args) {
        boolean sender=false;

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-help")) {
                help();
                return;
            }
            if(args[i].equals("-sender")) {
                sender=true;
                continue;
            }
            help();
            return;
        }

        try {
            new NetworkUtilization().start(sender);
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    static void help() {
        System.out.println("NetworkUtilization [-help] [-sender]");
    }


}
