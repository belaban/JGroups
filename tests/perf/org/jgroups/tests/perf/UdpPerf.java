package org.jgroups.tests.perf;


import org.jgroups.util.Util;

import java.io.IOException;
import java.net.*;
import java.util.Date;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

/**
 * Dynamic tool to measure multicast performance of JGroups; every member sends N messages and we measure how long it
 * takes for all receivers to receive them. MPerf is <em>dynamic</em> because it doesn't accept any configuration
 * parameters (besides the channel config file and name); all configuration is done at runtime, and will be broadcast
 * to all cluster members.
 * @author Bela Ban (belaban@yahoo.com)
 * @since 3.1
 */
public class UdpPerf {
    protected MulticastSocket mcast_sock;
    protected SocketAddress   sock_addr;
    protected Receiver        receiver;

    protected int             num_msgs=1000 * 1000;
    protected int             msg_size=1000;
    protected int             num_threads=1;
    protected int             log_interval=num_msgs / 10; // log every 10%
    protected int             receive_log_interval=num_msgs / 10;


    /** Maintains stats per sender, will be sent to perf originator when all messages have been received */
    protected final LongAdder total_received_msgs=new LongAdder();
    protected boolean         looping=true;
    protected long            last_interval;



    public void start() throws Exception {
        StringBuilder sb=new StringBuilder();
        sb.append("\n\n----------------------- MPerf -----------------------\n");
        sb.append("Date: ").append(new Date()).append('\n');
        sb.append("Run by: ").append(System.getProperty("user.name")).append("\n");
        System.out.println(sb);

        mcast_sock=new MulticastSocket(7500);
        sock_addr=new InetSocketAddress(InetAddress.getByName("232.5.5.5"), 7500);
        mcast_sock.joinGroup(sock_addr, null);
        mcast_sock.setReceiveBufferSize(10 * 1000 * 1000);
        mcast_sock.setSendBufferSize(5 * 1000 * 1000);
        mcast_sock.setTrafficClass(8);

        receiver=new Receiver();
        receiver.start();
    }


    protected void loop() {
        int c;

        final String INPUT="[1] Send [x] Exit";

        while(looping) {
            try {
                c=Util.keyPress(String.format(INPUT));
                switch(c) {
                    case '1':
                        sendMessages();
                        break;
                    case 'x':
                    case -1:
                        looping=false;
                        break;
                }
            }
            catch(Throwable t) {
                System.err.println(t);
            }
        }
        stop();
    }




    protected void send(byte[] payload) throws Exception {
        DatagramPacket packet=new DatagramPacket(payload, 0, payload.length, sock_addr);
        mcast_sock.send(packet);
    }



    public void stop() {
        looping=false;
        mcast_sock.close();
    }



    protected void handleData() {
        if(last_interval == 0)
            last_interval=System.currentTimeMillis();

        total_received_msgs.increment();
        long received_so_far=total_received_msgs.sum();
        if(received_so_far % receive_log_interval == 0) {
            long curr_time=System.currentTimeMillis();
            long diff=curr_time - last_interval;
            double msgs_sec=receive_log_interval / (diff / 1000.0);
            double throughput=msgs_sec * msg_size;
            last_interval=curr_time;
            System.out.println(String.format("-- received %d msgs %d ms, %.2f msgs/sec, %s / sec)",
                                             received_so_far, diff, msgs_sec, Util.printBytes(throughput)));
        }
    }


    void reset() {
        total_received_msgs.reset();
        last_interval=0;
    }


    protected void sendMessages() {
        final AtomicInteger num_msgs_sent=new AtomicInteger(0); // all threads will increment this
        final Sender[]      senders=new Sender[num_threads];
        final CyclicBarrier barrier=new CyclicBarrier(num_threads +1);
        final byte[]        payload=new byte[msg_size];

        reset();

        for(int i=0; i < num_threads; i++) {
            senders[i]=new Sender(barrier, num_msgs_sent, payload);
            senders[i].setName("sender-" + i);
            senders[i].start();
        }
        try {
            System.out.println("-- sending " + num_msgs + " msgs");
            barrier.await();
        }
        catch(Exception e) {
            System.err.println("failed triggering send threads: " + e);
        }
    }



    
    protected class Sender extends Thread {
        protected final CyclicBarrier barrier;
        protected final AtomicInteger num_msgs_sent;
        protected final byte[]        payload;

        protected Sender(CyclicBarrier barrier, AtomicInteger num_msgs_sent, byte[] payload) {
            this.barrier=barrier;
            this.num_msgs_sent=num_msgs_sent;
            this.payload=payload;
        }

        public void run() {
            try {
                barrier.await();
            }
            catch(Exception e) {
                e.printStackTrace();
                return;
            }

            for(;;) {
                try {
                    int tmp=num_msgs_sent.incrementAndGet();
                    if(tmp > num_msgs)
                        break;
                    send(payload);
                    if(tmp % log_interval == 0)
                        System.out.println("++ sent " + tmp);
                    if(tmp == num_msgs) // last message, send SENDING_DONE message
                        break;
                }
                catch(Exception e) {
                }
            }
        }
    }


    protected class Receiver extends Thread {
        byte[] buf=new byte[10000];

        public void run() {
            while(!mcast_sock.isClosed()) {

                DatagramPacket packet=new DatagramPacket(buf, 0, buf.length);
                try {
                    mcast_sock.receive(packet);
                    handleData();
                }
                catch(IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }




    public static void main(String[] args) {
        final UdpPerf test=new UdpPerf();
        try {
            test.start();
            test.loop();
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

}
