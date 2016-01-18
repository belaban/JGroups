package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.stack.NakReceiverWindow;
import org.jgroups.stack.AbstractRetransmitter;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.TimeScheduler3;
import org.jgroups.util.Util;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class NakReceiverWindowStressTest2 {
    static int NUM_THREADS=10;
    static int NUM_MSGS=1000000;

    static final Message MSG=new Message(false);

    static final AtomicInteger added=new AtomicInteger(0);
    static final AtomicInteger removed=new AtomicInteger(0);

    public static void main(String[] args) throws InterruptedException {
        for(int i=0; i < args.length; i++) {
            if(args[i].startsWith("-h")) {
                System.out.println("NakReceiverWindowStressTest2 [-num messages] [-adders <number of adder threads>]");
                return;
            }
            if(args[i].equals("-num")) {
                NUM_MSGS=Integer.parseInt(args[++i]);
                continue;
            }
            if(args[i].equals("-adders")) {
                NUM_THREADS=Integer.parseInt(args[++i]);
            }
        }

        Address sender=Util.createRandomAddress("A");
        TimeScheduler timer=new TimeScheduler3();

        NakReceiverWindow win=new NakReceiverWindow(sender, new AbstractRetransmitter.RetransmitCommand() {

            public void retransmit(long first_seqno, long last_seqno, Address sender) {
                System.out.println("-- retransmit(" + first_seqno + "-" + last_seqno);
            }
        }, 0, timer, true);

        // win.setRetransmitTimeouts(new ExponentialInterval(1000));

        final CountDownLatch latch=new CountDownLatch(1);

        Remover remover=new Remover(win, latch);
        remover.start();

        Adder[] adders=new Adder[NUM_THREADS];
        for(int i=0; i < adders.length; i++) {
            adders[i]=new Adder(win, latch, added);
            adders[i].start();
        }

        Util.sleep(1000);

        long start=System.currentTimeMillis();
        latch.countDown();
        while(remover.isAlive()) {
            System.out.println("added messages: " + added + ", removed messages: " + removed);
            remover.join(2000);
        }
        long diff=System.currentTimeMillis() - start;

        System.out.println("added messages: " + added + ", removed messages: " + removed);
        System.out.println("took " + diff + " ms to insert and remove " + NUM_MSGS + " messages");
        win.destroy();
        timer.stop();
    }


    protected static class Adder extends Thread {
        protected final NakReceiverWindow win;
        protected final AtomicInteger num;
        protected final CountDownLatch latch;

        public Adder(NakReceiverWindow win, CountDownLatch latch, AtomicInteger num) {
            this.win=win;
            this.num=num;
            this.latch=latch;
            setName("Adder");
        }

        public void run() {
            try {
                latch.await();
            }
            catch(InterruptedException e) {
                e.printStackTrace();
            }

            while(true) {
                int seqno=num.incrementAndGet();
                if(seqno > NUM_MSGS) {
                    num.decrementAndGet();
                    break;
                }
                win.add(seqno, MSG);
            }
        }
    }

    protected static class Remover extends Thread {
        protected final NakReceiverWindow win;
        protected final CountDownLatch latch;

        public Remover(NakReceiverWindow win, CountDownLatch latch) {
            this.win=win;
            this.latch=latch;
            setName("Remover");
        }

        public void run() {
            try {
                latch.await();
            }
            catch(InterruptedException e) {
                e.printStackTrace();
            }
            int cnt=0;
            final AtomicBoolean processing=new AtomicBoolean(false);
            for(;;) {
                List<Message> msgs=win.removeMany(processing, true, 100);
                if(msgs != null) {
                    for(Message msg: msgs) {
                        cnt++;
                        removed.incrementAndGet();
                    }
                    continue;
                }
                if(cnt >= NUM_MSGS)
                    break;
                Util.sleep(500);
            }
            System.out.println("-- removed " + cnt + " messages");
        }
    }

}
