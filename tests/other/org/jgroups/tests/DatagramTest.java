package org.jgroups.tests;

import org.jgroups.util.Util;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * Tests loss rate of UDP datagrams
 * @author Bela Ban
 * @version $Id: DatagramTest.java,v 1.1 2007/08/16 08:47:24 belaban Exp $
 */
public class DatagramTest {
    DatagramSocket sock;


    private void start(boolean sender, InetAddress host, int port, int num_packets, int size, long sleep,
                       int buffer_size) throws Exception {
        if(sender) {
            sock=new DatagramSocket();
            if(buffer_size > 0)
                sock.setSendBufferSize(buffer_size);
            System.out.println("local socket is " + sock.getLocalSocketAddress() + ", send buffer size=" + sock.getSendBufferSize());
            sendPackets(num_packets, size, host, port, sleep);
        }
        else {
            sock=new DatagramSocket(new InetSocketAddress(host, port));
            if(buffer_size > 0)
            sock.setReceiveBufferSize(buffer_size);
            System.out.println("receive buffer size=" + sock.getReceiveBufferSize());
            System.out.println("listening on " + sock.getLocalSocketAddress());
            loop();
        }
    }

    private void loop() throws IOException {
        byte[] buf=new byte[70000];
        DatagramPacket packet;
        int count=0;
        while(true) {
            packet=new DatagramPacket(buf, buf.length);
            sock.receive(packet);
            count++;
            System.out.print("received " + count + " packets\r");
        }
    }

    private void sendPackets(int num_packets, int size, InetAddress host, int port, long sleep) throws Exception {
        byte[] buf=new byte[size];
        DatagramPacket packet;
        int print=num_packets / 10;
        for(int i=0; i < num_packets; i++) {
            packet=new DatagramPacket(buf, buf.length, host, port);
            sock.send(packet);
            if(print == 0 || i % print == 0)
                System.out.print("sent " + i + " messages\r");
            if(sleep> 0) {
                Util.sleep(sleep);
            }
        }
    }


    public static void main(String[] args) throws Exception {
        boolean sender=false;
        InetAddress host=null;
        int port=5000;
        int num_packets=10000;
        int size=1000;
        long sleep=0;
        int  buf=0;


        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-sender")) {
                sender=true;
                continue;
            }
            if(args[i].equals("-host")) {
                host=InetAddress.getByName(args[++i]);
                continue;
            }
            if(args[i].equals("-port")) {
                port=Integer.parseInt(args[++i]);
                continue;
            }
            if(args[i].equals("-num_packets")) {
                num_packets=Integer.parseInt(args[++i]);
                continue;
            }
            if(args[i].equals("-size")) {
                size=Integer.parseInt(args[++i]);
                continue;
            }
            if(args[i].equals("-sleep")) {
                sleep=Long.parseLong(args[++i]);
                continue;
            }
            if(args[i].equals("-buf")) {
                buf=Integer.parseInt(args[++i]);
                continue;
            }
            help();
            return;
        }

        if(host == null)
            host=InetAddress.getByName("localhost");
        new DatagramTest().start(sender, host, port, num_packets, size, sleep, buf);
    }


    private static void help() {
        System.out.println("DatagramTest [-help] [-sender] [-host] [-port] [-num_packets <num>] [-size <size>] " +
                "[-sleep <ms>] [-buf <buf in bytes>]");
    }

}
