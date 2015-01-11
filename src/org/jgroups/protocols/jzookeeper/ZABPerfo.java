package org.jgroups.protocols.jzookeeper;



import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.Version;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.protocols.jzookeeper.ZABPerf.MPerfHeader;
import org.jgroups.protocols.jzookeeper.ZABPerf.Stats;
import org.jgroups.util.AckCollector;
import org.jgroups.util.Util;

import java.io.IOException;
import java.net.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Dynamic tool to measure multicast performance of JGroups; every member sends N messages and we measure how long it
 * takes for all receivers to receive them. MPerf is <em>dynamic</em> because it doesn't accept any configuration
 * parameters (besides the channel config file and name); all configuration is done at runtime, and will be broadcast
 * to all cluster members.
 * @author Bela Ban (belaban@yahoo.com)
 * @since 3.1
 */
public class ZABPerfo extends ReceiverAdapter{
	
	protected String                props;
    protected JChannel              channel;
    protected String                name;
    protected MulticastSocket mcast_sock;
    protected SocketAddress   sock_addr;
    protected Receiver        receiver;

    protected int             num_msgs=1000 * 1000;
    protected int             msg_size=1000;
    protected int             num_threads=1;
    protected int             log_interval=num_msgs / 10; // log every 10%
    protected int             receive_log_interval=num_msgs / 10;
    protected Data 					payload= new Data();
    protected String 				data = null;
    protected Address               local_addr=null;
    private final List<Address> members=new ArrayList<Address>();
    protected final List<Message>  received_msgs=new ArrayList<Message>();





    /** Maintains stats per sender, will be sent to perf originator when all messages have been received */
    protected final AtomicLong                    total_received_msgs=new AtomicLong(0);
    protected boolean                             looping=true;
    protected long                                last_interval=0;



    public void start(String props, String name) throws Exception {
        this.props=props;
        this.name=name;
        StringBuilder sb=new StringBuilder();
        sb.append("\n\n----------------------- MPerf -----------------------\n");
        sb.append("Date: ").append(new Date()).append('\n');
        sb.append("Run by: ").append(System.getProperty("user.name")).append("\n");
        sb.append("JGroups version: ").append(Version.description).append('\n');
        System.out.println(sb);

        channel=new JChannel(props);
        channel.setName(name);
        channel.setReceiver(this);
        channel.connect("zabperf");
        local_addr=channel.getAddress();
        String data = payload.createBytes(100);
        JmxConfigurator.registerChannel(channel, Util.getMBeanServer(), "jgroups", "zabperf", true);

    }


    protected void loop() {
        int c;

        final String INPUT="[1] Send\n [2] print stat\n [3] Exit";

        while(looping) {
            try {
                c=Util.keyPress(String.format(INPUT));
                switch(c) {
                    case '1':
                        sendMessages();
                        break;
                    case '2':
                    	System.out.println("Received Messages = " + received_msgs);
                    case 'x':
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

    public void receive(Message msg) {
    	received_msgs.add(msg);
        String line="[" + msg.getSrc() + "]: " + msg.getObject();
        System.out.println(line);
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

        long received_so_far=total_received_msgs.incrementAndGet();
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
        total_received_msgs.set(0);
        last_interval=0;
    }


    protected void sendMessages() {
        final AtomicInteger num_msgs_sent=new AtomicInteger(0); // all threads will increment this
        final Sender[]      senders=new Sender[num_threads];
        final CyclicBarrier barrier=new CyclicBarrier(num_threads +1);
        //final byte[]        payload=new byte[msg_size];
        String data = payload.createBytes(100);


        reset();

        for(int i=0; i < num_threads; i++) {
            senders[i]=new Sender(barrier, num_msgs_sent, data);
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
        protected final String        payload;

        protected Sender(CyclicBarrier barrier, AtomicInteger num_msgs_sent, String payload) {
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
                    Message msg=new Message(null, payload);
                    channel.send(msg);
                    if(tmp % log_interval == 0)
                        System.out.println("++ sent " + tmp);

                    // if we used num_msgs_sent, we might have thread T3 which reaches the condition below, but
                    // actually didn't send the *last* message !
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
    	
    	String props=null, name=null;

        for(int i=0; i < args.length; i++) {
            if("-props".equals(args[i])) {
                props=args[++i];
                continue;
            }
            if("-name".equals(args[i])) {
                name=args[++i];
                continue;
            }
            System.out.println("MPerf [-props <stack config>] [-name <logical name>]");
            return;
        }
        final ZABPerfo test=new ZABPerfo();
        try {
            test.start(props, name);
            test.loop();
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

}
