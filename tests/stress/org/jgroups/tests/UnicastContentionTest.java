package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.util.Util;

import java.util.concurrent.CountDownLatch;
import java.util.Vector;
import java.util.Map;

/**
 * Tests contention of locks in UNICAST, by concurrently sending and receiving unicast messages. The contention is
 * in the 'connections' hashmap, and results in a lot of retransmissions. Run 2 instances with
 * java org.jgroups.tests.UnicastContentionTest -props udp.xml -num_msgs 100 -num_threads 200
 * and the UNICAST.num_xmits value will be high
 * @author Bela Ban
 * @version $Id: UnicastContentionTest.java,v 1.1.2.1 2009/09/11 10:59:15 belaban Exp $
 */
public class UnicastContentionTest {
    static final String GROUP="UnicastContentionTest-Cluster";

    int num_msgs=10000;
    int size=1000; // bytes
    int num_mbrs=2;
    int num_threads=1;
    int MOD=1000;


    private void start(String props, int num_msgs, int size, int num_mbrs, int num_threads) throws Exception {
        this.num_msgs=num_msgs;
        this.size=size;
        this.num_mbrs=num_mbrs;
        this.num_threads=num_threads;
        this.MOD=num_threads * num_msgs / 10;

        MySender[] senders=new MySender[num_threads];

        JChannel ch=new JChannel(props);
        JmxConfigurator.registerChannel(ch, Util.getMBeanServer(), "jgroups", GROUP, true);
        final CountDownLatch latch=new CountDownLatch(1);
        MyReceiver receiver=new MyReceiver(latch);
        ch.setReceiver(receiver);
        ch.connect(GROUP);

        System.out.println("Waiting for " + num_mbrs + " members");
        latch.await();
        View view=ch.getView();
        Address local_addr=ch.getLocalAddress();
        Address dest=pickNextMember(view, local_addr);
        System.out.println("View is " + view + "\n" + num_threads + " are sending " + num_msgs + " messages (of " + size + " bytes) to " + dest);

        for(int i=0; i < senders.length; i++)
            senders[i]=new MySender(dest, ch);

        for(MySender sender: senders)
            sender.start();
        for(MySender sender: senders)
            sender.join();

        Util.keyPress("enter to dump stats and close channel");
        System.out.println("stats:\n" + printStats(ch.dumpStats()));
        Util.close(ch);
    }

    @SuppressWarnings("unchecked")
    private static String printStats(Map<String,Object> map) {
        StringBuilder sb=new StringBuilder();
        for(Map.Entry<String,Object> entry: map.entrySet()) {
            sb.append(entry.getKey()).append("\n");
            Map<Object,Object> val=(Map)entry.getValue();
            for(Map.Entry<Object,Object> tmp: val.entrySet()) {
                sb.append(tmp.getKey()).append("=").append(tmp.getValue()).append("\n");
            }
            sb.append("\n");
        }
        return sb.toString();
    }

    private static Address pickNextMember(View view, Address local_addr) {
        Vector<Address> mbrs=view.getMembers();
        for(Address mbr: mbrs) {
            if(!mbr.equals(local_addr))
                return mbr;
        }
        return null;
    }


    private class MySender extends Thread {
        final byte[] buf=new byte[size];
        final Address dest;
        final JChannel ch;

        public MySender(Address dest, JChannel ch) {
            this.dest=dest;
            this.ch=ch;
        }

        public void run() {
            for(int i=0; i < num_msgs; i++) {
                Message msg=new Message(dest, null, buf);
                try {
                    ch.send(msg);
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private class MyReceiver extends ReceiverAdapter {
        private final CountDownLatch latch;
        private int msgs=0;

        public MyReceiver(CountDownLatch latch) {
            this.latch=latch;
        }

        public void receive(Message msg) {
            msgs++;
            // bytes+=msg.getLength();
            if(msgs % MOD == 0)
                System.out.println("-- " + msgs + " received");
        }

        public void viewAccepted(View new_view) {
            if(new_view.size() >= num_mbrs)
                latch.countDown();
        }
    }


    public static void main(String[] args) throws Exception {
        int num_msgs=10000;
        int size=1000; //bytes
        int num_mbrs=2;
        int num_threads=1;
        String props=null;

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-num_msgs")) {
                num_msgs=Integer.parseInt(args[++i]);
                continue;
            }
            if(args[i].equals("-size")) {
                size=Integer.parseInt(args[++i]);
                continue;
            }
            if(args[i].equals("-num_mbrs")) {
                num_mbrs=Integer.parseInt(args[++i]);
                continue;
            }
            if(args[i].equals("-num_threads")) {
                num_threads=Integer.parseInt(args[++i]);
                continue;
            }
            if(args[i].equals("-props")) {
                props=args[++i];
                continue;
            }
            help();
            return;
        }

        new UnicastContentionTest().start(props, num_msgs, size, num_mbrs, num_threads);
    }



    private static void help() {
        System.out.println("UnicastStressTest2 [-props properties] [-num_msgs <number of messages to send>]" +
                " [-size bytes] [-num_mbrs members]");
    }
}
