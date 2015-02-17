package org.jgroups.tests;

import org.jgroups.util.Table;
import org.jgroups.util.Util;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test which uses 1 or more adder threads to add numbers to a table, and 1 remover thread to remove all of them. Test
 * concurrency: when all threads are done, exactly N numbers have to have been added and the remover needs to
 * ensure that it gets the numbers in the correct sequence, that there are no gaps and every number has been
 * received only once.
 */
public class TableStressTest {
    static int NUM_THREADS=10;
    static int NUM=1000000;


    static final AtomicInteger added=new AtomicInteger(0);
    static final AtomicInteger removed=new AtomicInteger(0);

    public static void main(String[] args) throws InterruptedException {
        for(int i=0; i < args.length; i++) {
            if(args[i].startsWith("-h")) {
                System.out.println("TableStressTest [-num numbers] [-adders <number of adder threads>]");
                return;
            }
            if(args[i].equals("-num")) {
                NUM=Integer.parseInt(args[++i]);
                continue;
            }
            if(args[i].equals("-adders")) {
                NUM_THREADS=Integer.parseInt(args[++i]);
            }
        }

        Table<Integer> buf=new Table<>(10000, 10240, 0);

        final CountDownLatch latch=new CountDownLatch(1);

        Remover remover=new Remover(buf,latch);
        remover.start();

        Adder[] adders=new Adder[NUM_THREADS];
        for(int i=0; i < adders.length; i++) {
            adders[i]=new Adder(buf,latch, added);
            adders[i].start();
        }

        long start=System.currentTimeMillis();
        latch.countDown();
        while(remover.isAlive()) {
            System.out.println("added messages: " + added + ", removed messages: " + removed);
            remover.join(2000);
        }
        long diff=System.currentTimeMillis() - start;

        System.out.println("added messages: " + added + ", removed messages: " + removed);
        System.out.println("took " + diff + " ms to insert and remove " + NUM + " messages");
    }


    protected static class Adder extends Thread {
        protected final Table<Integer> buf;
        protected final AtomicInteger  num;
        protected final CountDownLatch latch;

        public Adder(Table<Integer> buf, CountDownLatch latch, AtomicInteger num) {
            this.buf=buf;
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
                if(seqno > NUM) {
                    num.decrementAndGet();
                    break;
                }
                buf.add(seqno, seqno);
            }
        }
    }

    protected static class Remover extends Thread {
        protected final Table<Integer> buf;
        protected final CountDownLatch latch;

        public Remover(Table<Integer> buf, CountDownLatch latch) {
            this.buf=buf;
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
            int expected=1, unexpected=0;

            for(;;) {
                List<Integer> nums=buf.removeMany(true, 100);
                if(nums != null) {
                    for(Integer num: nums) {
                        cnt++;
                        removed.incrementAndGet();
                        if(num.intValue() != expected) {
                            unexpected++;
                            System.err.println("***** expected " + expected + ", but got " + num);
                        }
                        expected++;
                    }
                    continue;
                }
                if(cnt >= NUM)
                    break;
                Util.sleep(500);
            }
            System.out.println("-- removed " + cnt + " numbers");
            if(unexpected > 0)
                System.err.println("****** got " + unexpected + " numbers ! *******");
        }
    }
}
