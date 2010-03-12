package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Global;
import org.jgroups.Message;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;
import org.jgroups.util.TimeScheduler;
import org.testng.annotations.Test;

import java.util.ArrayList;
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
 * Tests time for N threads to deliver M messages to UNICAST2
 * @author Bela Ban
 * @version $Id: UNICAST2_StressTest.java,v 1.3 2010/03/12 15:41:18 belaban Exp $
 */
@Test(groups=Global.FUNCTIONAL, sequential=true)
public class UNICAST2_StressTest {
    static final int NUM_MSGS=1000000;
    static final int NUM_THREADS=50;
    static final int MAX_MSG_BATCH_SIZE=50000;

    static final short UNICAST_ID=ClassConfigurator.getProtocolId(UNICAST2.class);

    @Test
    public static void stressTest() {
        start(NUM_THREADS, NUM_MSGS, false, MAX_MSG_BATCH_SIZE);
    }

    @Test
    public static void stressTestOOB() {
        start(NUM_THREADS, NUM_MSGS, true, MAX_MSG_BATCH_SIZE);
    }

    private static void start(final int num_threads, final int num_msgs, boolean oob, int max_msg_batch_size) {
        final UNICAST2 unicast=new UNICAST2();
        final AtomicInteger counter=new AtomicInteger(num_msgs);
        final AtomicLong seqno=new AtomicLong(1);
        final AtomicInteger delivered_msgs=new AtomicInteger(0);
        final Lock lock=new ReentrantLock();
        final Condition all_msgs_delivered=lock.newCondition();
        final ConcurrentLinkedQueue<Long> delivered_msg_list=new ConcurrentLinkedQueue<Long>();
        final Address local_addr=Util.createRandomAddress();
        final Address sender=Util.createRandomAddress();

//        Runtime.getRuntime().addShutdownHook(new Thread() {
//            public void run() {
//                System.out.println("\ndelivered_msgs=" + delivered_msgs);
//                System.out.println("stats:\n" + unicast.dumpStats());
//            }
//        });

        TimeScheduler timer=new TimeScheduler(10);
        unicast.setTimer(timer);

        unicast.setDownProtocol(new Protocol() {
            public Object down(Event evt) {
                return null;
            }
        });

        unicast.setUpProtocol(new Protocol() {
            public Object up(Event evt) {
                if(evt.getType() == Event.MSG) {
                    delivered_msgs.incrementAndGet();
                    UNICAST2.Unicast2Header hdr=(UNICAST2.Unicast2Header)((Message)evt.getArg()).getHeader(UNICAST_ID);
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

        unicast.down(new Event(Event.SET_LOCAL_ADDRESS, local_addr));

        unicast.setMaxMessageBatchSize(max_msg_batch_size);

        // send the first message manually, to initialize the AckReceiverWindow tables
        Message msg=createMessage(local_addr, sender, 1L, oob, true);
        unicast.up(new Event(Event.MSG, msg));
        Util.sleep(500);


        final CountDownLatch latch=new CountDownLatch(1);
        Adder[] adders=new Adder[num_threads];
        for(int i=0; i < adders.length; i++) {
            adders[i]=new Adder(unicast, latch, counter, seqno, oob, local_addr, sender);
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

                    // send a spurious message to trigger removal of pending messages in AckReceiverWindow
                    msg=createMessage(local_addr, sender, 1L, oob, false);
                    unicast.up(new Event(Event.MSG, msg));
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

        unicast.stop();
        try {
            timer.stop();
        }
        catch(InterruptedException e) {
        }

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

    private static Message createMessage(Address dest, Address src, long seqno, boolean oob, boolean first) {
        Message msg=new Message(dest, src, "hello world");
        UNICAST2.Unicast2Header hdr=UNICAST2.Unicast2Header.createDataHeader(seqno, (short)1, first);
        msg.putHeader(UNICAST_ID, hdr);
        if(oob)
            msg.setFlag(Message.OOB);
        return msg;
    }


    static class Adder extends Thread {
        final UNICAST2 unicast;
        final CountDownLatch latch;
        final AtomicInteger num_msgs;
        final AtomicLong current_seqno;
        final boolean oob;
        final Address dest;
        final Address sender;

        public Adder(UNICAST2 unicast, CountDownLatch latch, AtomicInteger num_msgs, AtomicLong current_seqno,
                     boolean oob, final Address dest, final Address sender) {
            this.unicast=unicast;
            this.latch=latch;
            this.num_msgs=num_msgs;
            this.current_seqno=current_seqno;
            this.oob=oob;
            this.dest=dest;
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
                Message msg=createMessage(dest, sender, seqno, oob, false);
                unicast.up(new Event(Event.MSG, msg));
            }
        }
    }


    @Test(enabled=false)
    public static void main(String[] args) {
        int num_threads=10;
        int num_msgs=1000000;
        int max=20000;
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
            if(args[i].equals("-max")) {
                max=Integer.parseInt(args[++i]);
                continue;
            }
            System.out.println("UNICAST2_StressTest [-num_msgs msgs] [-num_threads threads] " +
                    "[-oob <true | false>] [-max <batch size>]");
            return;
        }
        start(num_threads, num_msgs, oob, max);
    }
}