package org.jgroups.tests;

import org.jgroups.stack.AckReceiverWindow;
import org.jgroups.Message;
import org.jgroups.util.Util;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.List;


/**
 * Tests time for N threads to insert and remove M messages into an AckReceiverWindow
 * @author Bela Ban
 * @version $Id: AckReceiverWindowStressTest.java,v 1.2 2010/02/13 16:21:57 belaban Exp $
 */
public class AckReceiverWindowStressTest {

    static void start(int num_threads, int num_msgs) {
        final AckReceiverWindow win=new AckReceiverWindow(1);
        final AtomicInteger counter=new AtomicInteger(num_msgs);
        final AtomicLong seqno=new AtomicLong(1);

        final CountDownLatch latch=new CountDownLatch(1);
        Adder[] adders=new Adder[num_threads];
        for(int i=0; i < adders.length; i++) {
            adders[i]=new Adder(win, latch, counter, seqno);
            adders[i].start();
        }

        long start=System.currentTimeMillis();
        latch.countDown(); // starts all adders

        for(Adder adder: adders) {
            try {
                adder.join();
            }
            catch(InterruptedException e) {
                e.printStackTrace();
            }
        }

        long time=System.currentTimeMillis() - start;
        double requests_sec=num_msgs / (time / 1000.0);
        System.out.println("\nTime: " + time + " ms, " + Util.format(requests_sec) + " requests / sec\n");

        int total=0;
        for(Adder adder: adders) {
            int num_removed=adder.getRemovedMessages();
            // System.out.println(adder + ": " + num_removed + " removed messages");
            total+=num_removed;
        }
        System.out.println("Total removed messages: " + total);

    }


    static class Adder extends Thread {
        final AckReceiverWindow win;
        final CountDownLatch latch;
        final AtomicInteger num_msgs;
        final AtomicLong current_seqno;
        int removed_msgs=0;

        public Adder(AckReceiverWindow win, CountDownLatch latch, AtomicInteger num_msgs, AtomicLong current_seqno) {
            this.win=win;
            this.latch=latch;
            this.num_msgs=num_msgs;
            this.current_seqno=current_seqno;
        }

        public int getRemovedMessages() {
            return removed_msgs;
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
                Message msg=new Message();
                int result=win.add2(seqno, msg);
                if(result != 1)
                    System.err.println("seqno " + seqno + " not added correctly");
                List<Message> msgs=win.removeMany();
                removed_msgs+=msgs.size();
            }
        }
    }


    public static void main(String[] args) {
        int num_threads=10;
        int num_msgs=1000000;

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-num_threads")) {
                num_threads=Integer.parseInt(args[++i]);
                continue;
            }
            if(args[i].equals("-num_msgs")) {
                num_msgs=Integer.parseInt(args[++i]);
                continue;
            }
            System.out.println("AckReceiverWindowStressTest [-num_msgs msgs] [-num_threads threads]");
            return;
        }
        start(num_threads, num_msgs);
    }
}
