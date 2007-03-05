package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.util.Util;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simple and experimental performance program for the Multiplexer. Runs 6 MuxChannels (2 clusters) inside the
 * same VM. Not optimal, but the main reason was to see whether the Multiplexer falls apart when we stress test it
 * (seems not to be the case).
 * @author Bela Ban
 * @version $Id: MultiplexerStressTest.java,v 1.1 2007/03/05 16:21:22 belaban Exp $
 */
public class MultiplexerStressTest {
    Channel c11, c12, c21, c22, c31, c32;
    ChannelFactory f1, f2, f3;
    private MyReceiver r11, r12, r21, r22, r31, r32;

    static final int NUM_MSGS=100000;
    static final int SIZE=1000;


    public MultiplexerStressTest() {

    }

    private void start() throws Exception {
        CyclicBarrier barrier=new CyclicBarrier(7);
        f1=new JChannelFactory();
        f2=new JChannelFactory();
        f3=new JChannelFactory();

        f1.setMultiplexerConfig("stacks.xml");
        f2.setMultiplexerConfig("stacks.xml");
        f3.setMultiplexerConfig("stacks.xml");

        c11=f1.createMultiplexerChannel("udp", "A");
        c11.connect("X");
        r11=new MyReceiver(barrier);
        c11.setReceiver(r11);

        c12=f1.createMultiplexerChannel("udp", "B");
        c12.connect("X");
        r12=new MyReceiver(barrier);
        c12.setReceiver(r12);

        c21=f2.createMultiplexerChannel("udp", "A");
        c21.connect("X");
        r21=new MyReceiver(barrier);
        c21.setReceiver(r21);

        c22=f2.createMultiplexerChannel("udp", "B");
        c22.connect("X");
        r22=new MyReceiver(barrier);
        c22.setReceiver(r22);

        c31=f3.createMultiplexerChannel("udp", "A");
        c31.connect("X");
        r31=new MyReceiver(barrier);
        c31.setReceiver(r31);

        c32=f3.createMultiplexerChannel("udp", "B");
        c32.connect("X");
        r32=new MyReceiver(barrier);
        c32.setReceiver(r32);

        long start, stop;

        new MySender(barrier, c11).start();
        new MySender(barrier, c12).start();
        new MySender(barrier, c21).start();
        new MySender(barrier, c22).start();
        new MySender(barrier, c31).start();
        new MySender(barrier, c32).start();

        barrier.await(); // start the 6 sender threads
        start=System.currentTimeMillis();

        barrier.await(); // results from the 6 receivers
        stop=System.currentTimeMillis();

        System.out.println("Cluster A:\n" + printStats(stop-start,new MyReceiver[]{r11,r21,r31}));
        System.out.println("Cluster B:\n" + printStats(stop-start,new MyReceiver[]{r12,r22,r32}));

        c32.close();
        c31.close();
        c21.close();
        c22.close();
        c12.close();
        c11.close();
    }

    private String printStats(long total_time, MyReceiver[] cluster) {
        int num_msgs=0;
        long num_bytes=0;
        int cluster_size=cluster.length;

        for(int i=0; i < cluster_size; i++) {
            num_msgs+=cluster[i].getNumMessages();
            num_bytes+=cluster[i].getNumBytes();
        }

        double msgs_per_sec=num_msgs / (total_time / 1000.00);
        double bytes_per_sec=num_bytes * SIZE / (total_time / 1000.00);

        StringBuilder sb=new StringBuilder();
        sb.append("total msgs=").append(num_msgs).append(", msg rate=").append(msgs_per_sec);
        sb.append(", total time=").append(total_time / 1000.00);
        sb.append(", throughput=").append(Util.printBytes(bytes_per_sec));
        return sb.toString();
    }


    private static class MyReceiver extends ReceiverAdapter {
        AtomicLong received_msgs=new AtomicLong(0);
        AtomicLong received_bytes=new AtomicLong(0);
        CyclicBarrier barrier;
        int print=NUM_MSGS / 10;


        public MyReceiver(CyclicBarrier barrier) {
            this.barrier=barrier;
        }

        public long getNumMessages() {
            return received_msgs.get();
        }

        public long getNumBytes() {
            return received_bytes.get();
        }


        public void receive(Message msg) {
            int length=msg.getLength();
            if(length > 0) {
                received_msgs.incrementAndGet();
                received_bytes.addAndGet(length);

                if(received_msgs.get() % print == 0)
                    System.out.println("received " + received_msgs.get() + " msgs");

                if(received_msgs.get() >= NUM_MSGS) {
                    try {
                        barrier.await();
                    }
                    catch(Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }



    private static class MySender extends Thread {
        CyclicBarrier barrier;
        Channel ch;


        public MySender(CyclicBarrier barrier, Channel ch) {
            this.barrier=barrier;
            this.ch=ch;
        }

        public void run() {
            byte[] buf=new byte[SIZE];
            Message msg;
            int print=NUM_MSGS / 10;
            try {
                barrier.await();
                for(int i=1; i <= NUM_MSGS; i++) {
                    msg=new Message(null, null, buf, 0, buf.length);
                    ch.send(msg);
                    if(i % print == 0)
                        System.out.println(getName() + ": sent " + i + " msgs");
                }
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
    }


    public static void main(String[] args) throws Exception {
        new MultiplexerStressTest().start();
    }


}
