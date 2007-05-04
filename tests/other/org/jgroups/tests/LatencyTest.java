package org.jgroups.tests;

import org.jgroups.util.Util;

import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;

/**
 * @author Bela Ban
 * @version $Id: LatencyTest.java,v 1.3 2007/05/04 12:49:38 belaban Exp $
 */
public class LatencyTest {
    InetAddress GROUP=null;
    int PORT=7500;

    private void start(boolean sender, boolean local) throws Exception {
        GROUP=InetAddress.getByName("228.1.2.3");
        long start;
        DatagramPacket send_packet, recv_packet;
        byte[] send_buf;
        byte[] recv_buf=new byte[2100];

        if(local) {
            MulticastSocket send_sock=new MulticastSocket(PORT);
            send_sock.setTrafficClass(8);
            MulticastSocket recv_sock=new MulticastSocket(PORT);
            recv_sock.joinGroup(GROUP);
            recv_packet=new DatagramPacket(recv_buf, 0, recv_buf.length);
            for(int i=0; i < 10; i++) {
                start=System.currentTimeMillis();
                send_buf=Util.objectToByteBuffer(start);
                send_packet=new DatagramPacket(send_buf, 0, send_buf.length, GROUP, PORT);
                send_sock.send(send_packet);
                recv_sock.receive(recv_packet);
                start=((Long)Util.objectFromByteBuffer(recv_buf, recv_packet.getOffset(), recv_packet.getLength())).longValue();
                System.out.println("took " + (System.currentTimeMillis() - start) + " ms");
                Util.sleep(1000);
            }
            return;
        }

        if(sender) {
            MulticastSocket send_sock=new MulticastSocket(PORT);
            send_sock.setTrafficClass(8);
            for(int i=0; i < 10; i++) {
                start=System.currentTimeMillis();
                send_buf=Util.objectToByteBuffer(start);
                send_packet=new DatagramPacket(send_buf, 0, send_buf.length, GROUP, PORT);
                send_sock.send(send_packet);
                Util.sleep(1000);
            }
        }
        else {
            MulticastSocket recv_sock=new MulticastSocket(PORT);
            recv_sock.joinGroup(GROUP);
            recv_packet=new DatagramPacket(recv_buf, 0, recv_buf.length);
            
            for(;;) {
                recv_sock.receive(recv_packet);
                start=((Long)Util.objectFromByteBuffer(recv_buf, recv_packet.getOffset(), recv_packet.getLength())).longValue();
                System.out.println("took " + (System.currentTimeMillis() - start) + " ms");
            }
        }
    }





    public static void main(String[] args) throws Exception {
        boolean sender=false;
        boolean local=true;
        for(int i=0; i < args.length; i++) {
            if(args[i].equalsIgnoreCase("-sender")) {
                sender=true;
                continue;
            }
            if(args[i].equalsIgnoreCase("-local")) {
                local=true;
                continue;
            }
            help();
            return;
        }
        new LatencyTest().start(sender, local);
    }

    private static void help() {
        System.out.println("LatencyTest [-sender] [-local (overrides -sender)]");
    }


}
