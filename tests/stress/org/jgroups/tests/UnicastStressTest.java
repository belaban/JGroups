package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.View;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.util.Util;

import java.util.Vector;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tests UNICAST by sending anycast messages via an RpcDispatcher
 * @author Bela Ban
 * @version $Id: UnicastStressTest.java,v 1.4 2009/04/09 09:11:35 belaban Exp $
 */
public class UnicastStressTest {
    int num_channels=6;
    int num_threads=1; // number of threads per channel
    int num_msgs=1000; // number of messages sent by 1 thread
    int msg_size=4096;  // number of bytes / message
    String props=null;
    int buddies=1;

    private JChannel[]      channels;
    private RpcDispatcher[] dispatchers;
    private Receiver[]      receivers;

    final AtomicInteger msgs_received=new AtomicInteger(0);
    final AtomicLong bytes_received=new AtomicLong(0);

    final CyclicBarrier start_barrier;
    final CyclicBarrier terminate_barrier;


    public UnicastStressTest(String props, int num_channels, int num_threads, int num_msgs, int msg_size, int buddies) {
        this.props=props;
        this.num_channels=num_channels;
        this.num_threads=num_threads;
        this.num_msgs=num_msgs;
        this.msg_size=msg_size;
        this.buddies=buddies;
        start_barrier=new CyclicBarrier(num_channels * num_threads +1);
        terminate_barrier=new CyclicBarrier(num_channels +1);
        if(buddies > num_channels)
            throw new IllegalArgumentException("buddies needs to be smaller than number of channels");
    }


    private void start() throws Exception {
        channels=new JChannel[num_channels];
        receivers=new Receiver[num_channels];
        dispatchers=new RpcDispatcher[num_channels];
        long start, stop;

        int num_expected_msgs=num_threads * num_msgs * buddies;
        int num_total_msgs=num_channels * num_threads * num_msgs; // over all channels
        for(int i=0; i < channels.length; i++) {
            channels[i]=new JChannel(props);
            receivers[i]=new Receiver(terminate_barrier, bytes_received, msgs_received, num_expected_msgs, num_total_msgs);
            dispatchers[i]=new RpcDispatcher(channels[i], null, null, receivers[i]);
            channels[i].connect("x");
        }

        // start the senders
        for(int i=0; i < channels.length; i++) {
            JChannel channel=channels[i];
            View view=channel.getView();
            Vector<Address> members=view.getMembers();
            if(members.size() != num_channels) {
                throw new Exception("cluster has not formed correctly, expected " + num_channels + " channels, found" +
                        " only " + members.size() + " (view: " + view + ")");
            }
            Vector<Address> tmp=pickBuddies(members, channel.getAddress());

            for(int j=0; j < num_threads; j++) {
                Sender sender=new Sender(start_barrier, msg_size, num_msgs, dispatchers[i], channel.getAddress(), tmp);
                sender.start(); // will wait on barrier
            }
        }

        System.out.println("sending " + num_total_msgs + " msgs with " + num_threads + " threads over " + num_channels + " channels");

        start_barrier.await(); // signals all senders to start
        start=System.currentTimeMillis();


        terminate_barrier.await(); // when all receivers have received all messages
        stop=System.currentTimeMillis();

        for(int i=0; i < dispatchers.length; i++) {
            dispatchers[i].stop();
        }
        for(int i=channels.length -1; i >= 0; i--) {
            channels[i].close();
        }

        printStats(stop - start);
    }

    private void printStats(long time) {
        for(int i=0; i < receivers.length; i++) {
            System.out.println("receiver #" + (i+1) + ": " + receivers[i].getNumReceivedMessages());
        }
        System.out.println("total received messages for " + num_channels + " channels: " + msgs_received.get());
        System.out.println("total bytes received by " + num_channels + " channels: " + Util.printBytes(bytes_received.get()));
        System.out.println("time: " + time + " ms");
        double msgs_per_sec=msgs_received.get() / (time /1000.0);
        double throughput=bytes_received.get() / (time / 1000.0);
        System.out.println("Message rate: " + msgs_per_sec + " msgs/sec");
        System.out.println("Throughput: " + Util.printBytes(throughput) + " / sec");
    }

    private Vector<Address> pickBuddies(Vector<Address> members, Address local_addr) {
        Vector<Address> retval=new Vector<Address>();
        int index=members.indexOf(local_addr);
        if(index < 0)
            return null;
        for(int i=index +1; i <= index + buddies; i++) {
            int real_index=i % members.size();
            Address buddy=members.get(real_index);
            retval.add(buddy);
        }
        return retval;
    }


    public static class Receiver {
        final AtomicInteger msgs;
        final AtomicLong bytes;
        final int num_expected_msgs, num_total_msgs, print;
        final CyclicBarrier barrier;
        final AtomicInteger num_received_msgs=new AtomicInteger(0);


        public Receiver(CyclicBarrier barrier, AtomicLong bytes, AtomicInteger msgs, int num_expected_msgs, int num_total_msgs) {
            this.barrier=barrier;
            this.bytes=bytes;
            this.msgs=msgs;
            this.num_expected_msgs=num_expected_msgs;
            this.num_total_msgs=num_total_msgs;
            print=num_total_msgs / 10;
        }

        public int getNumReceivedMessages() {return num_received_msgs.get();}

        public void receive(byte[] data) {
            msgs.incrementAndGet();
            bytes.addAndGet(data.length);

            int count=num_received_msgs.incrementAndGet();
            if(count % print == 0) {
                System.out.println("received " + count + " msgs");
            }
            
            if((count=num_received_msgs.get()) >= num_expected_msgs) {
                try {
                    barrier.await();
                }
                catch(Exception e) {
                }
            }

        }
    }

    private static class Sender extends Thread {
        private final CyclicBarrier barrier;
        private final int num_msgs;
        private final int msg_size;
        private final RpcDispatcher disp;
        private final Vector buddies;


        public Sender(CyclicBarrier barrier, int msg_size, int num_msgs, RpcDispatcher disp, Address local_addr, Vector buddies) {
            this.barrier=barrier;
            this.msg_size=msg_size;
            this.num_msgs=num_msgs;
            this.disp=disp;
            this.buddies=buddies;
            setName("Sender (" + local_addr + " --> " + buddies + ")");
        }

        public void run() {
            final byte[] data=new byte[msg_size];
            final Object[] arg=new Object[]{data};
            final Class[] types=new Class[]{byte[].class};

            try {
                barrier.await();
            }
            catch(Exception e) {
            }

            for(int i=0; i < num_msgs; i++) {
                disp.callRemoteMethods(buddies, "receive", arg, types, GroupRequest.GET_NONE, 5000, true);
            }
        }
    }



    public static void main(String[] args) throws Exception {
        int num_channels=6;
        int num_threads=10; // number of threads per channel
        int num_msgs=10000; // number of messages sent by 1 thread
        int msg_size=4096;  // number of bytes / message
        int buddies=1;
        String props=null;

        for(int i=0; i < args.length; i++) {
            if(args[i].equalsIgnoreCase("-props")) {
                props=args[++i];
                continue;
            }
            if(args[i].equalsIgnoreCase("-num_channels")) {
                num_channels=Integer.parseInt(args[++i]);
                continue;
            }
            if(args[i].equalsIgnoreCase("-num_threads")) {
                num_threads=Integer.parseInt(args[++i]);
                continue;
            }
            if(args[i].equalsIgnoreCase("-num_msgs")) {
                num_msgs=Integer.parseInt(args[++i]);
                continue;
            }
            if(args[i].equalsIgnoreCase("-msg_size")) {
                msg_size=Integer.parseInt(args[++i]);
                continue;
            }
            if(args[i].equalsIgnoreCase("-buddies")) {
                buddies=Integer.parseInt(args[++i]);
                continue;
            }
            help();
            return;
        }

        new UnicastStressTest(props, num_channels, num_threads, num_msgs, msg_size, buddies).start();
    }




    private static void help() {
        System.out.println("UnicastStressTest [-help] [-props <props>] [-num_channels <num>] " +
                "[-num_threads <threads per channel>] [-num_msgs <number of msgs per thread>] [-msg_size <size in bytes>] " +
                "[-buddies <num>]");
    }
}
