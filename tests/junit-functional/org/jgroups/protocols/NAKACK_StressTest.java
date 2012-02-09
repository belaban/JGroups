package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.NakAckHeader2;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MutableDigest;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Tests time for N threads to deliver M messages to NAKACK
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL, sequential=true)
public class NAKACK_StressTest {
    static final int   NUM_MSGS=1000000;
    static final int   NUM_THREADS=50;
    static final short NAKACK_ID=ClassConfigurator.getProtocolId(NAKACK2.class);


    public static void testStress() {
        start(NUM_THREADS, NUM_MSGS, false);
    }

    public static void testStressOOB() {
        start(NUM_THREADS, NUM_MSGS, true);
    }

    private static void start(final int num_threads, final int num_msgs, boolean oob) {
        final NAKACK2 nak=new NAKACK2();
        final AtomicInteger counter=new AtomicInteger(num_msgs);
        final AtomicLong seqno=new AtomicLong(1);
        final AtomicInteger delivered_msgs=new AtomicInteger(0);
        final Lock lock=new ReentrantLock();
        final Condition all_msgs_delivered=lock.newCondition();
        final ConcurrentLinkedQueue<Long> delivered_msg_list=new ConcurrentLinkedQueue<Long>();
        final Address local_addr=Util.createRandomAddress("A");
        final Address sender=Util.createRandomAddress("B");


        nak.setDownProtocol(new Protocol() {public Object down(Event evt) {return null;}});

        nak.setUpProtocol(new Protocol() {
            public Object up(Event evt) {
                if(evt.getType() == Event.MSG) {
                    delivered_msgs.incrementAndGet();
                    NakAckHeader2 hdr=(NakAckHeader2)((Message)evt.getArg()).getHeader(NAKACK_ID);
                    if(hdr != null)
                        delivered_msg_list.add(hdr.getSeqno());

                    if(delivered_msgs.get() >= num_msgs) {
                        lock.lock();
                        try {
                            all_msgs_delivered.signalAll();
                        }
                        finally {
                            lock.unlock();
                        }
                    }
                }
                return null;
            }
        });

        nak.setDiscardDeliveredMsgs(true);
        nak.down(new Event(Event.SET_LOCAL_ADDRESS, local_addr));
        nak.down(new Event(Event.BECOME_SERVER));
        View view=new View(local_addr, 1, Arrays.asList(local_addr, sender));
        nak.down(new Event(Event.VIEW_CHANGE, view));

        MutableDigest digest=new MutableDigest(10);
        digest.add(local_addr, 0, 0);
        digest.add(sender, 0, 0);
        nak.down(new Event(Event.SET_DIGEST, digest));

        final CountDownLatch latch=new CountDownLatch(1);
        Sender[] adders=new Sender[num_threads];
        for(int i=0; i < adders.length; i++) {
            adders[i]=new Sender(nak, latch, counter, seqno, oob, sender);
            adders[i].start();
        }

        long start=System.currentTimeMillis();
        latch.countDown(); // starts all adders

        lock.lock();
        try {
            while(delivered_msgs.get() < num_msgs) {
                try {
                    all_msgs_delivered.await(1000, TimeUnit.MILLISECONDS);
                    System.out.println("received " + delivered_msgs.get() + " msgs");
                }
                catch(InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        finally {
            lock.unlock();
        }

        long time=System.currentTimeMillis() - start;
        double requests_sec=num_msgs / (time / 1000.0);
        System.out.println("\nTime: " + time + " ms, " + Util.format(requests_sec) + " requests / sec\n");
        System.out.println("Delivered messages: " + delivered_msg_list.size());
        if(delivered_msg_list.size() < 100)
            System.out.println("Elements: " + delivered_msg_list);

        nak.stop();

        List<Long> results=new ArrayList<Long>(delivered_msg_list);

        if(oob)
            Collections.sort(results);

        assert results.size() == num_msgs : "expected " + num_msgs + ", but got " + results.size();

        System.out.println("Checking results consistency");
        int i=1;
        for(Long num: results) {
            if(num.longValue() != i) {
                assert i == num : "expected " + i + " but got " + num;
                return;
            }
            i++;
        }
        System.out.println("OK");
    }

    private static Message createMessage(Address dest, Address src, long seqno, boolean oob) {
        Message msg=new Message(dest, src, "hello world");
        NakAckHeader2 hdr=NakAckHeader2.createMessageHeader(seqno) ;
        msg.putHeader(NAKACK_ID, hdr);
        if(oob)
            msg.setFlag(Message.OOB);
        return msg;
    }


    static class Sender extends Thread {
        final NAKACK2 nak;
        final CountDownLatch latch;
        final AtomicInteger num_msgs;
        final AtomicLong current_seqno;
        final boolean oob;
        final Address sender;

        public Sender(NAKACK2 nak, CountDownLatch latch, AtomicInteger num_msgs, AtomicLong current_seqno,
                      boolean oob, final Address sender) {
            this.nak=nak;
            this.latch=latch;
            this.num_msgs=num_msgs;
            this.current_seqno=current_seqno;
            this.oob=oob;
            this.sender=sender;
            setName("Adder");
        }


        public void run() {
            try {
                latch.await();
            }
            catch(InterruptedException e) {
                e.printStackTrace();
                return;
            }

            while(num_msgs.getAndDecrement() > 0) {
                long seqno=current_seqno.getAndIncrement();
                Message msg=createMessage(null, sender, seqno, oob);
                nak.up(new Event(Event.MSG, msg));
            }
        }
    }


    /*@Test(enabled=false)
    public static void main(String[] args) {
        int num_threads=10;
        int num_msgs=1000000;
        boolean oob=false;

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-num_threads")) {
                num_threads=Integer.parseInt(args[++i]);
                continue;
            }
            if(args[i].equals("-num_msgs")) {
                num_msgs=Integer.parseInt(args[++i]);
                continue;
            }
            if(args[i].equals("-oob")) {
                oob=Boolean.parseBoolean(args[++i]);
                continue;
            }
            System.out.println("NAKACK_StressTest [-num_msgs msgs] [-num_threads threads] [-oob <true | false>]");
            return;
        }
        start(num_threads, num_msgs, oob);
    }*/
}