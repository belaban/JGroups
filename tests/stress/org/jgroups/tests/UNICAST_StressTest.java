package org.jgroups.tests;

import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.protocols.UNICAST;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Tests time for N threads to deliver M messages to UNICAST
 * @author Bela Ban
 * @version $Id: UNICAST_StressTest.java,v 1.1 2010/02/24 15:31:16 belaban Exp $
 */
public class UNICAST_StressTest {

    static void start(final int num_threads, final int num_msgs, boolean oob) {
        final UNICAST unicast=new UNICAST();
        final AtomicInteger counter=new AtomicInteger(num_msgs);
        final AtomicLong seqno=new AtomicLong(1);
        final AtomicInteger delivered_msgs=new AtomicInteger(0);
        final Lock lock=new ReentrantLock();
        final Condition all_msgs_delivered=lock.newCondition();

        
        unicast.setDownProtocol(new Protocol() {
            public Object down(Event evt) {
                return null;
            }
        });

        unicast.setUpProtocol(new Protocol() {
            public Object up(Event evt) {
                if(evt.getType() == Event.MSG) {
                    delivered_msgs.incrementAndGet();
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


        final CountDownLatch latch=new CountDownLatch(1);
        Adder[] adders=new Adder[num_threads];
        for(int i=0; i < adders.length; i++) {
            adders[i]=new Adder(unicast, latch, counter, seqno, oob);
            adders[i].start();
        }

        long start=System.currentTimeMillis();
        latch.countDown(); // starts all adders

        lock.lock();
        try {
            while(delivered_msgs.get() < num_msgs) {
                try {
                    all_msgs_delivered.await();
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
    }


    static class Adder extends Thread {
        final UNICAST unicast;
        final CountDownLatch latch;
        final AtomicInteger num_msgs;
        final AtomicLong current_seqno;
        final boolean oob;

        public Adder(UNICAST unicast, CountDownLatch latch, AtomicInteger num_msgs, AtomicLong current_seqno, boolean oob) {
            this.unicast=unicast;
            this.latch=latch;
            this.num_msgs=num_msgs;
            this.current_seqno=current_seqno;
            this.oob=oob;
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
                Message msg=new Message(true);
                UNICAST.UnicastHeader hdr=UNICAST.UnicastHeader.createDataHeader(seqno, (short)1, seqno == UNICAST.DEFAULT_FIRST_SEQNO);
                msg.putHeader("UNICAST", hdr);
                if(oob)
                    msg.setFlag(Message.OOB);
                unicast.up(new Event(Event.MSG, msg));
            }
        }
    }


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
            System.out.println("UNICAST_StressTest [-num_msgs msgs] [-num_threads threads] [-oob <true | false>]");
            return;
        }
        start(num_threads, num_msgs, oob);
    }
}