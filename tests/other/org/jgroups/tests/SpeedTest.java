// $Id: SpeedTest.java,v 1.23 2008/07/21 14:01:01 belaban Exp $


package org.jgroups.tests;


import org.jgroups.Channel;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.conf.ConfiguratorFactory;
import org.jgroups.conf.ProtocolStackConfigurator;
import org.jgroups.util.ExposedByteArrayOutputStream;
import org.jgroups.util.Util;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.MulticastSocket;


/**
 * Test time taken for multicasting n local messages (messages sent to self). Uses simple MulticastSocket.
 * Note that packets might get dropped if Util.sleep(1) is commented out (on certain systems this has
 * to be increased even further). If running with -jg option and Util.sleep() is commented out, there will
 * probably be packet loss, which will be repaired (by means of retransmission) by JGroups.
 * @author Bela Ban
 * @version $Id: SpeedTest.java,v 1.23 2008/07/21 14:01:01 belaban Exp $
 */
public class SpeedTest {
    static long start=0, stop=0;
    private static final String LOOPBACK="LOOPBACK(down_thread=false;up_thread=false)";


    public static void main(String[] args) {
        DatagramSocket sock=null;
        Receiver receiver;
        int num_msgs=1000, num_sent=0, group_port=7500, num_yields=0;
        DatagramPacket packet;
        InetAddress group_addr=null;
        int[][] matrix;
        boolean jg=false; // use JGroups channel instead of UDP MulticastSocket
        JChannel channel=null;
        String group_name="SpeedTest-Group";
        Message send_msg;
        boolean busy_sleep=false, yield=false, loopback=false;
        long sleep_time=1; // sleep in msecs between msg sends
        String props;


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
                "pbcast.GMS(join_timeout=5000;" +
                "shun=false;print_local_addr=true):" +
                "pbcast.STATE_TRANSFER";
                //  "PERF(details=true)";



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
                if(loopback) {
                    ProtocolStackConfigurator conf=ConfiguratorFactory.getStackConfigurator(props);
                    String tmp=conf.getProtocolStackString();
                    int index=tmp.indexOf(':');
                    props=LOOPBACK + tmp.substring(index);
                }
                channel=new JChannel(props);
                // System.out.println("props:\n" + channel.getProperties());
                channel.connect(group_name);
            }
            else {
                group_addr=InetAddress.getByName("224.0.0.36");
                sock=new DatagramSocket();
            }

            receiver=new Receiver(group_addr, group_port, channel, matrix, jg);
            receiver.start();

            byte[] buf;
            DataOutputStream out;

            start=System.currentTimeMillis();
            for(int i=0; i < num_msgs; i++) {
                ExposedByteArrayOutputStream output=new ExposedByteArrayOutputStream(64);
                out=new DataOutputStream(output);
                out.writeInt(i);
                out.flush();
                buf=output.getRawBuffer();
                out.close();

                if(jg) {
                    send_msg=new Message(null, null, buf, 0, buf.length);
                    channel.send(send_msg);
                }
                else {
                    packet=new DatagramPacket(buf, buf.length, group_addr, group_port);
                    sock.send(packet);
                }
                num_sent++;
                if(num_sent % 1000 == 0)
                    System.out.println("-- sent " + num_sent);

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
            ex.printStackTrace();
            System.exit(-1);
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
        System.out.println("Options -props and are only valid if -jg is used");
    }


    static class Receiver implements Runnable {
        Thread t=null;
        byte[] buf=new byte[1024];
        MulticastSocket sock;
        Channel channel;
        int num_msgs=1000;
        int[][] matrix=null;
        boolean jg=false;

        Receiver(InetAddress group_addr, int group_port, Channel channel, int[][] matrix, boolean jg) throws IOException {
            this.channel=channel;
            this.matrix=matrix;
            this.jg=jg;
            num_msgs=matrix.length;
            if(group_addr != null) {
                sock=new MulticastSocket(group_port);
                sock.joinGroup(group_addr);
            }
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
            long total_time;
            double msgs_per_sec=0;
            DataInputStream in;

            packet=new DatagramPacket(buf, buf.length);
            while(num_received <= num_msgs) {
                try {
                    if(jg) {
                        obj=channel.receive(0);
                        if(obj instanceof Message) {
                            // msg=(Message)obj;
                        }
                        else {
                            System.out.println("received non-msg: " + obj.getClass());
                            continue;
                        }
                    }
                    else {
                        sock.receive(packet);
                    }

                    // number=((Integer)Util.objectFromByteBuffer(msg_data)).intValue();
                    in=new DataInputStream(new ByteArrayInputStream(buf));
                    number=in.readInt();
                    matrix[number][1]=1;
                    // System.out.println("-- received " + number);
                    num_received++;
                    if(num_received % 1000 == 0)
                        System.out.println("received " + num_received + " packets");
                    if(num_received >= num_msgs)
                        break;
                }
                catch(Exception ex) {
                    System.err.println(ex);
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
