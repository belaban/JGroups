package org.jgroups.tests;

// $Id: SpeedTest_NIO.java,v 1.6 2009/06/17 16:28:59 belaban Exp $


import org.jgroups.Channel;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.util.Util;

import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.nio.ByteBuffer;


/**
 * Same test as SpeedTest, but using NIO ByteBuffer rather than serialization. For 10000 messages, this took
 * 25% of SpeedTest !
 * Test time taken for multicasting n local messages (messages sent to self). Uses simple MulticastSocket.
 * Note that packets might get dropped if Util.sleep(1) is commented out (on certain systems this has
 * to be increased even further). If running with -jg option and Util.sleep() is commented out, there will
 * probably be packet loss, which will be repaired (by means of retransmission) by JGroups. To see the
 * retransmit messages, enable tracing (trace=true) in jgroups.properties and add the following lines:
 * <pre>
 * trace0=NAKACK.retransmit DEBUG STDOUT
 * trace1=UNICAST.retransmit DEBUG STDOUT
 * </pre>
 * 
 * @author Bela Ban
 */
public class SpeedTest_NIO {
    static long start=0, stop=0;


    public static void main(String[] args) {
        MulticastSocket sock=null;
        Receiver receiver=null;
        int num_msgs=1000;
        byte[] buf;
        DatagramPacket packet;
        InetAddress group_addr=null;
        int[][] matrix;
        boolean jg=false; // use JGroups channel instead of UDP MulticastSocket
        JChannel channel=null;
        String props=null, loopback_props;
        String group_name="SpeedTest-Group";
        Message send_msg;
        long sleep_time=1; // sleep in msecs between msg sends
        boolean busy_sleep=false;
        boolean yield=false;
        int num_yields=0;
        boolean loopback=false;


        props="UDP(mcast_addr=224.0.0.36;mcast_port=55566;ip_ttl=32;" +
                "ucast_send_buf_size=32000;ucast_recv_buf_size=64000;" +
                "mcast_send_buf_size=32000;mcast_recv_buf_size=64000):" +
                "PING(timeout=2000;num_initial_members=3):" +
                "MERGE2(min_interval=5000;max_interval=10000):" +
                "FD_SOCK:" +
                "VERIFY_SUSPECT(timeout=1500):" +
                "pbcast.NAKACK(gc_lag=50;retransmit_timeout=600,800,1200,2400,4800):" +
                "UNICAST(timeout=1200):" +
                "pbcast.STABLE(desired_avg_gossip=10000):" +
                "FRAG(frag_size=8192;down_thread=false;up_thread=false):" +
// "PIGGYBACK(max_size=16000;max_wait_time=500):" +
                "pbcast.GMS(join_timeout=5000;" +
                "print_local_addr=true):" +
                "pbcast.STATE_TRANSFER";
        // "PERF(details=true)";


        loopback_props="LOOPBACK:" +
                "PING(timeout=2000;num_initial_members=3):" +
                "MERGE2(min_interval=5000;max_interval=10000):" +
                "FD_SOCK:" +
                "VERIFY_SUSPECT(timeout=1500):" +
                "pbcast.NAKACK(gc_lag=50;retransmit_timeout=600,800,1200,2400,4800):" +
                "UNICAST(timeout=5000):" +
                "pbcast.STABLE(desired_avg_gossip=20000):" +
                "FRAG(frag_size=16000;down_thread=false;up_thread=false):" +
                "pbcast.GMS(join_timeout=5000;" +
                "print_local_addr=true):" +
                "pbcast.STATE_TRANSFER";


        for(int i=0; i < args.length; i++) {
            if("-help".equals(args[i])) {
                help();
                return;
            }
            if("-jg".equals(args[i])) {
                jg=true;
                continue;
            }
            if("-loopback".equals(args[i])) {
                loopback=true;
                props=loopback_props;
                continue;
            }
            if("-props".equals(args[i])) {
                props=args[++i];
                continue;
            }
            if("-busy_sleep".equals(args[i])) {
                busy_sleep=true;
                continue;
            }
            if("-yield".equals(args[i])) {
                yield=true;
                num_yields++;
                continue;
            }
            if("-sleep".equals(args[i])) {
                sleep_time=Long.parseLong(args[++i]);
                continue;
            }
            if("-num_msgs".equals(args[i])) {
                num_msgs=Integer.parseInt(args[++i]);
                continue;
            }
            help();
            return;
        }

        System.out.println("jg       = " + jg +
                "\nloopback = " + loopback +
                "\nsleep    = " + sleep_time +
                "\nbusy_sleep=" + busy_sleep +
                "\nyield=" + yield +
                "\nnum_yields=" + num_yields +
                "\nnum_msgs = " + num_msgs +
                           '\n');



        try {
            matrix=new int[num_msgs][2];
            for(int i=0; i < num_msgs; i++) {
                for(int j=0; j < matrix[i].length; j++)
                    matrix[i][j]=0;
            }

            if(jg) {
                channel=new JChannel(props);
                channel.connect(group_name);
            }
            else {
                group_addr=InetAddress.getByName("224.0.0.36");
                sock=new MulticastSocket(7777);
                sock.joinGroup(group_addr);
            }

            receiver=new Receiver(sock, channel, matrix, jg);
            receiver.start();

            ByteBuffer bb=ByteBuffer.allocate(16);
            bb.mark();

            start=System.currentTimeMillis();
            for(int i=0; i < num_msgs; i++) {
                bb.reset();
                bb.putInt(i);
                buf=(byte[])(bb.array()).clone();

                if(jg) {
                    send_msg=new Message(null, null, buf);
                    channel.send(send_msg);
                }
                else {
                    packet=new DatagramPacket(buf, buf.length, group_addr, 7777);
                    sock.send(packet);
                }
                if(i % 1000 == 0)
                    System.out.println("-- sent " + i);

                matrix[i][0]=1;
                if(yield) {
                    for(int k=0; k < num_yields; k++) {
                        Thread.yield();
                    }
                }
                else {
                    if(sleep_time > 0) {
                        sleep(sleep_time, busy_sleep);
                    }
                }
            }
            while(true) {
                System.in.read();
                printMatrix(matrix);
            }
        }
        catch(Exception ex) {
            System.err.println(ex);
        }
    }


    /**
     * On most UNIX systems, the minimum sleep time is 10-20ms. Even if we specify sleep(1), the thread will
     * sleep for at least 10-20ms. On Windows, sleep() seems to be implemented as a busy sleep, that is the
     * thread never relinquishes control and therefore the sleep(x) is exactly x ms long.
     */
    static void sleep(long msecs, boolean busy_sleep) {
        if(!busy_sleep) {
            Util.sleep(msecs);
            return;
        }

        long start=System.currentTimeMillis();
        long stop=start + msecs;

        while(stop > start) {
            start=System.currentTimeMillis();
        }
    }

    static void printMatrix(int[][] m) {
        int tmp=0;
        System.out.print("not sent: ");
        for(int i=0; i < m.length; i++) {
            if(m[i][0] == 0) {
                System.out.print(i + " ");
                tmp++;
            }
        }
        System.out.println("\ntotal not sent: " + tmp);

        tmp=0;
        System.out.print("not received: ");
        for(int i=0; i < m.length; i++) {
            if(m[i][1] == 0) {
                System.out.print(i + " ");
                tmp++;
            }
        }
        System.out.println("\ntotal not received: " + tmp);
        System.out.println("Press CTRL-C to kill this test");
    }


    static void help() {
        System.out.println("SpeedTest [-help] [-num_msgs <num>] [-sleep <sleeptime in msecs between messages>] " +
                "[-busy_sleep] [-yield] [-jg] [-loopback] [-props <channel properties>]");
        System.out.println("Options -props are only valid if -jg is used");
    }


    static class Receiver implements Runnable {
        Thread t=null;
        byte[] buf=new byte[1024];
        MulticastSocket sock;
        Channel channel;
        int num_msgs=1000;
        int[][] matrix=null;
        boolean jg=false;


        Receiver(MulticastSocket sock, Channel channel, int[][] matrix, boolean jg) {
            this.sock=sock;
            this.channel=channel;
            this.matrix=matrix;
            this.jg=jg;
            num_msgs=matrix.length;
        }

        public void start() {
            if(t == null) {
                t=new Thread(this, "receiver thread");
                t.start();
            }
        }

        public void run() {
            int num_received=0;
            int number;
            DatagramPacket packet;
            Object obj;
            Message msg;
            byte[] msg_data=null;
            long total_time;
            double msgs_per_sec=0;
            ByteBuffer rb=ByteBuffer.allocate(16);
            rb.mark();

            packet=new DatagramPacket(buf, buf.length);
            while(num_received <= num_msgs) {
                try {
                    if(jg) {
                        obj=channel.receive(0);
                        if(obj instanceof Message) {
                            msg=(Message)obj;
                            msg_data=msg.getBuffer();
                        }
                        else {
                            System.out.println("received non-msg: " + obj.getClass());
                            continue;
                        }
                    }
                    else {
                        sock.receive(packet);
                        msg_data=packet.getData();
                    }

                    rb.rewind();
                    rb.put(msg_data);
                    rb.rewind();
                    number=rb.getInt();

                    matrix[number][1]=1;
                    // System.out.println("#set " + number);
                    num_received++;
                    if(num_received % 1000 == 0)
                        System.out.println("received " + num_received + " packets");
                    if(num_received >= num_msgs)
                        break;
                }
                catch(Exception ex) {
                    System.err.println("receiver: " + ex);
                }
            }
            stop=System.currentTimeMillis();
            total_time=stop - start;
            msgs_per_sec=(num_received / (total_time / 1000.0));
            System.out.println("\n** Sending and receiving " + num_received + " took " +
                    total_time + " msecs (" + msgs_per_sec + " msgs/sec) **");
            System.exit(1);
        }
    }

}
