package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.FIFOMessageQueue;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.LinkedList;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

/**
 * @author Bela Ban
 * @version $Id: FIFOMessageQueueTest.java,v 1.2 2008/03/10 15:39:20 belaban Exp $
 */
@Test(groups=Global.FUNCTIONAL)
public class FIFOMessageQueueTest {
    String s1="s1", s2="s2", s3="s3";
    private static final Address a1, a2;

    static {
        a1=new IpAddress(5000);
        a2=new IpAddress(6000);
    }



    public static void testPollFromEmptyQueue() throws InterruptedException {
        FIFOMessageQueue<String,Integer> queue=new FIFOMessageQueue<String,Integer>();
        Assert.assertEquals(0, queue.size());
        Integer ret=queue.poll(5);
        assert ret == null;
        Assert.assertEquals(queue.size(), 0, "queue.size() should be 0, but is " + queue.size());
    }



    public void testPutTwoTakeTwo() throws InterruptedException {
        FIFOMessageQueue<String,Integer> queue=new FIFOMessageQueue<String,Integer>();
        queue.put(a1, s1, 1); // 1 is available immediately
        queue.put(a1, s1, 2); // 2 is queued
        Integer ret=queue.poll(5);
        assert ret != null;
        queue.done(a1, s1); // 2 is made available (moved into 'queue')
        queue.done(a1, s1); // done() by the first putter
        ret=queue.poll(5);
        assert ret != null;
        Assert.assertEquals(0, queue.size());
        queue.put(a1, s1, 3);
        Assert.assertEquals(1, queue.size());
        ret=queue.poll(5); // 3 should be available because queue for a1/s1 was empty
        assert ret != null;
    }



    public void testTakeFollowedByPut() throws InterruptedException {
        final FIFOMessageQueue<String,Integer> queue=new FIFOMessageQueue<String,Integer>();
        Assert.assertEquals(0, queue.size());

        new Thread() {

            public void run() {
                Util.sleep(1000);
                try {
                    queue.put(a1, s1, 1);
                }
                catch(InterruptedException e) {

                }
            }
        }.start();

        Integer ret=queue.take();
        assert ret != null;
        Assert.assertEquals(1, ret.intValue());
        Assert.assertEquals(queue.size(), 0, "queue.size() should be 0, but is " + queue.size());
    }


    public void testMultipleTakersOnePutter() throws Exception {
        final FIFOMessageQueue<String,Integer> queue=new FIFOMessageQueue<String,Integer>();
        final CyclicBarrier barrier=new CyclicBarrier(11);
        for(int i=0; i < 10; i++) {
            new Thread() {
                public void run() {
                    try {
                        barrier.await();
                        queue.take();

                    }
                    catch(Exception e) {
                    }
                }
            }.start();
        }
        barrier.await();
        for(int i=0; i < 10; i++) {
            queue.put(a1, s1, i);
            queue.done(a1, s1);
        }
        Util.sleep(100);
        Assert.assertEquals(0, queue.size());
    }


    public void testConcurrentPutsAndTakes() throws InterruptedException {
        final FIFOMessageQueue<String,Integer> queue=new FIFOMessageQueue<String,Integer>();
        final int NUM=10000;
        final int print=NUM / 10;

        Thread putter=new Thread() {

            public void run() {
                setName("Putter");
                int cnt=0;
                for(int i=0; i < NUM; i++) {
                    try {
                        queue.put(a1, s1, i);
                        cnt++;
                        if(cnt % print == 0) {
                            System.out.println("Putter: " + cnt);
                        }
                        queue.done(a1, s1);
                    }
                    catch(InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        };

        Thread taker=new Thread() {

            public void run() {
                setName("Taker");
                int cnt=0;
                for(int i=0; i < NUM; i++) {
                    try {
                        queue.take();
                        cnt++;
                        if(cnt % print == 0) {
                            System.out.println("Taker: " + cnt);
                        }
                    }
                    catch(InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        };

        System.out.println("starting threads");
        taker.start();
        putter.start();

        new Thread() {

            public void run() {
                Util.sleep(3000);
                System.out.println("queue:\n" + queue);
            }
        }.start();

        putter.join();
        taker.join();

        Assert.assertEquals(0, queue.size());
    }



    public void testNullAddress() throws InterruptedException {
        FIFOMessageQueue<String,Integer> queue=new FIFOMessageQueue<String,Integer>();
        queue.put(null, s1, 1);
        queue.put(a1, s1, 2);
        queue.put(a1, s1, 3);
        queue.put(null, s1, 4);
        System.out.println("queue:\n" + queue);

        Integer ret=queue.poll(5);
        assert ret != null;
        Assert.assertEquals(1, ret.intValue());

        ret=queue.poll(5);
        assert ret != null;
        Assert.assertEquals(2, ret.intValue());

        ret=queue.poll(5);
        assert ret != null;
        Assert.assertEquals(4, ret.intValue());

        ret=queue.poll(5);
        assert ret == null;

        queue.done(a1, s1);
        ret=queue.poll(5);
        assert ret != null;
        Assert.assertEquals(3, ret.intValue());

        ret=queue.poll(5);
        assert ret == null;
        Assert.assertEquals(0, queue.size());
    }



    public void testSimplePutAndTake() throws InterruptedException {
        FIFOMessageQueue<String,Integer> queue=new FIFOMessageQueue<String,Integer>();
        queue.put(a1, s1, 1);
        Assert.assertEquals(1, queue.size());
        int ret=queue.take();
        Assert.assertEquals(1, ret);
        Assert.assertEquals(0, queue.size());
    }


    public void testSimplePutAndTakeMultipleSenders() throws InterruptedException {
        FIFOMessageQueue<String,Integer> queue=new FIFOMessageQueue<String,Integer>();
        queue.put(a1, s1, 1);
        queue.put(a2, s1, 2);
        System.out.println("queue is:\n" + queue);
        Assert.assertEquals(2, queue.size());
        int ret=queue.take();
        Assert.assertEquals(1, ret);
        Assert.assertEquals(1, queue.size());
        ret=queue.take();
        Assert.assertEquals(2, ret);
        Assert.assertEquals(0, queue.size());
    }


    public void testMultiplePutsAndTakes() throws InterruptedException {
        FIFOMessageQueue<String,Integer> queue=new FIFOMessageQueue<String,Integer>();
        for(int i=1; i <= 5; i++)
            queue.put(a1, s1, i);
        System.out.println("queue is " + queue);
        Assert.assertEquals(5, queue.size());
        for(int i=1; i <= 5; i++) {
            int ret=queue.take();
            Assert.assertEquals(i, ret);
            Assert.assertEquals(5 - i, queue.size());
            queue.done(a1, s1);
        }
        Assert.assertEquals(0, queue.size());
    }


    /**
     * Sender A sends M1 to S1 and M2 to S1. M2 should wait until M1 is done
     */

    public void testSameSenderSameDestination() throws InterruptedException {
        FIFOMessageQueue<String,Integer> queue=new FIFOMessageQueue<String,Integer>();
        queue.put(a1, s1, 1);
        queue.put(a1, s1, 2);
        queue.put(a1, s1, 3);
        System.out.println("queue:\n" + queue);

        Assert.assertEquals(3, queue.size());
        int ret=queue.take();

        Assert.assertEquals(1, ret);
        Integer retval=queue.poll(100);
        assert retval == null;
        queue.done(a1, s1);
        System.out.println("queue:\n" + queue);
        ret=queue.take();
        Assert.assertEquals(2, ret);
        queue.done(a1, s1);
        System.out.println("queue:\n" + queue);
        ret=queue.take();
        System.out.println("queue:\n" + queue);
        Assert.assertEquals(3, ret);
    }



    /**
     * Sender A sends M1 to S1 and M2 to S2. M2 should get processed immediately and not have
     * to wait for M1 to complete
     */

    public void testSameSenderMultipleDestinations() throws InterruptedException {
        FIFOMessageQueue<String,Integer> queue=new FIFOMessageQueue<String,Integer>();
        queue.put(a1, s1, 10);
        queue.put(a1, s1, 11);
        queue.put(a1, s1, 12);

        queue.put(a1, s2, 20);
        queue.put(a1, s2, 21);
        queue.put(a1, s2, 22);

        queue.put(a1, s3, 30);
        queue.put(a1, s3, 31);
        queue.put(a1, s3, 32);
        System.out.println("queue:\n" + queue);
        Integer ret=queue.poll(5);
        assert ret != null;
        Assert.assertEquals(10, ret.intValue());

        ret=queue.poll(5);
        assert ret != null;
        Assert.assertEquals(20, ret.intValue());

        ret=queue.poll(5);
        assert ret != null;
        Assert.assertEquals(30, ret.intValue());

        ret=queue.poll(5);
        assert ret == null;

        queue.done(a1, s3);
        queue.done(a1, s1);
        queue.done(a1, s2);

        ret=queue.poll(5);
        assert ret != null;
        Assert.assertEquals(31, ret.intValue());

        ret=queue.poll(5);
        assert ret != null;
        Assert.assertEquals(11, ret.intValue());

        ret=queue.poll(5);
        assert ret != null;
        Assert.assertEquals(21, ret.intValue());

        ret=queue.poll(5);
        assert ret == null;

        Assert.assertEquals(3, queue.size());

        ret=queue.poll(5);
        assert ret == null;

        queue.done(a1, s1);
        queue.done(a1, s3);
        queue.done(a1, s2);

        ret=queue.poll(5);
        assert ret != null;
        Assert.assertEquals(12, ret.intValue());

        ret=queue.poll(5);
        assert ret != null;
        Assert.assertEquals(32, ret.intValue());

        ret=queue.poll(5);
        assert ret != null;
        Assert.assertEquals(22, ret.intValue());

        ret=queue.poll(5);
        assert ret == null;

        Assert.assertEquals(0, queue.size());
    }


    /**
     * Sender A sends M1 to S1 and sender B sends M2 to S1. M2 should get processed concurrently to M1 and
     * should not have to wait for M1's completion
     */

    public void testDifferentSendersSameDestination() throws InterruptedException {
        FIFOMessageQueue<String,Integer> queue=new FIFOMessageQueue<String,Integer>();
        queue.put(a1, s1, 10);
        queue.put(a2, s1, 20);
        queue.put(a1, s1, 11);
        queue.put(a2, s1, 21);
        System.out.println("queue:\n" + queue);
        Assert.assertEquals(4, queue.size());

        Integer ret=queue.poll(5);
        assert ret != null;
        Assert.assertEquals(10, ret.intValue());

        ret=queue.poll(5);
        assert ret != null;
        Assert.assertEquals(20, ret.intValue());

        queue.done(a1, s1);
        ret=queue.poll(5);
        assert ret != null;
        Assert.assertEquals(11, ret.intValue());

        queue.done(a2, s1);
        ret=queue.poll(5);
        assert ret != null;
        Assert.assertEquals(21, ret.intValue());

        ret=queue.poll(5);
        assert ret == null;
        Assert.assertEquals(0, queue.size());
    }



    /**
     * Sender A sends M1 to S1 and sender B sends M2 to S2. M1 and M2 should get processed concurrently 
     */

    public void testDifferentSendersDifferentDestinations() throws Exception {
        FIFOMessageQueue<String,Integer> queue=new FIFOMessageQueue<String,Integer>();
        queue.put(a1, s1, 1);
        queue.put(a2, s2, 2);
        queue.put(a1, s2, 3);
        queue.put(a2, s1, 4);
        System.out.println("queue:\n" + queue);
        Assert.assertEquals(4, queue.size());

        Integer ret=queue.poll(5);
        assert ret != null;
        Assert.assertEquals(1, ret.intValue());

        ret=queue.poll(5);
        assert ret != null;
        Assert.assertEquals(2, ret.intValue());

        ret=queue.poll(5);
        assert ret != null;
        Assert.assertEquals(3, ret.intValue());

        ret=queue.poll(5);
        assert ret != null;
        Assert.assertEquals(4, ret.intValue());

        ret=queue.poll(5);
        assert ret == null;
        Assert.assertEquals(0, queue.size());

    }




    public void testDifferentSendersDifferentDestinationsMultipleMessages() throws Exception {
        FIFOMessageQueue<String,Integer> queue=new FIFOMessageQueue<String,Integer>();
        queue.put(a1, s1, 1);
        queue.put(a2, s2, 2);
        queue.put(a1, s2, 3);
        queue.put(a2, s1, 4);

        queue.put(a1, s1, 5);
        queue.put(a2, s2, 6);
        queue.put(a1, s2, 7);
        queue.put(a2, s1, 8);

        System.out.println("queue:\n" + queue);
        Assert.assertEquals(8, queue.size());

        Integer ret=queue.poll(5);
        assert ret != null;
        Assert.assertEquals(1, ret.intValue());

        ret=queue.poll(5);
        assert ret != null;
        Assert.assertEquals(2, ret.intValue());

        ret=queue.poll(5);
        assert ret != null;
        Assert.assertEquals(3, ret.intValue());

        ret=queue.poll(5);
        assert ret != null;
        Assert.assertEquals(4, ret.intValue());


        queue.done(a1, s1);
        ret=queue.poll(5);
        assert ret != null;
        Assert.assertEquals(5, ret.intValue());

        queue.done(a2, s2);
        ret=queue.poll(5);
        assert ret != null;
        Assert.assertEquals(6, ret.intValue());

        queue.done(a1, s2);
        ret=queue.poll(5);
        assert ret != null;
        Assert.assertEquals(7, ret.intValue());

        queue.done(a2, s1);
        ret=queue.poll(5);
        assert ret != null;
        Assert.assertEquals(8, ret.intValue());
    }
    



    public void testOrdering() throws InterruptedException {
        FIFOMessageQueue<String,Integer> queue=new FIFOMessageQueue<String,Integer>();
        for(int i=1; i <= 3; i++)
            queue.put(a1, s1, i);
        Assert.assertEquals(3, queue.size());

        int ret=queue.take();
        Assert.assertEquals(1, ret);
        Assert.assertEquals(2, queue.size());

        queue.done(a1, s1);
        queue.put(a1, s1, 4);
        queue.put(a1, s1, 5);
        System.out.println("queue: " + queue);

        for(int i=2; i <= 5; i++) {
            ret=queue.take();
            Assert.assertEquals(i, ret);
            Assert.assertEquals(5 - i, queue.size());
            queue.done(a1, s1);
        }
        Assert.assertEquals(0, queue.size());
    }



    public static void testOrderingMultipleThreads() throws BrokenBarrierException, InterruptedException {
        FIFOMessageQueue<String,Integer> queue=new FIFOMessageQueue<String,Integer>();
        CyclicBarrier barrier=new CyclicBarrier(4);
        int NUM=500;
        Producer p1=new Producer(queue, "s1",    1, NUM, barrier);
        Producer p2=new Producer(queue, "s2", 1001, NUM, barrier);
        Producer p3=new Producer(queue, "s3", 2001, NUM, barrier);

        p1.start();
        p2.start();
        p3.start();
        Util.sleep(100);
        barrier.await(); // starts all 3 threads

        p1.join();
        p2.join();
        p3.join();
        System.out.println("queue: " + queue.size() + " elements");
        Assert.assertEquals(NUM * 3, queue.size());
    }


    public static void testOrderingMultipleThreadsWithTakes() throws BrokenBarrierException, InterruptedException {
        testOrderingMultipleThreads();
        int ret;
        LinkedList<Integer> list=new LinkedList<Integer>();
        FIFOMessageQueue<String,Integer> queue=new FIFOMessageQueue<String,Integer>();

        int size=queue.size();
        for(int i=0; i < size; i++) {
            ret=queue.take();
            list.add(ret);
            queue.done(a1, "s1");
            queue.done(a1, "s2");
            queue.done(a1, "s3");
        }

        System.out.println("analyzing returned values for correct ordering");
        LinkedList<Integer> one=new LinkedList<Integer>(), two=new LinkedList<Integer>(), three=new LinkedList<Integer>();
        for(int val: list) {
            if(val < 1000) {
                one.add(val);
                continue;
            }
            if(val > 1000 && val <= 2000) {
                two.add(val);
                continue;
            }
            if(val > 2000) {
                three.add(val);
            }
        }

        int len=one.size();
        Assert.assertEquals(len, two.size());
        Assert.assertEquals(len, three.size());


        LinkedList<Integer> sorted_one=new LinkedList<Integer>(one);
        Collections.sort(sorted_one);
        Assert.assertEquals(sorted_one, one, "one: " + one + ", sorted: " + sorted_one);

        LinkedList<Integer> sorted_two=new LinkedList<Integer>(two);
        Collections.sort(sorted_two);
        Assert.assertEquals(sorted_two, two, "two: " + two + ", sorted: " + sorted_two);

        LinkedList<Integer> sorted_three=new LinkedList<Integer>(three);
        Collections.sort(sorted_three);
        Assert.assertEquals(sorted_three, three, "three: " + three + ", sorted: " + sorted_three);

        System.out.println("OK - all 3 collections are ordered");
    }



    private static class Producer extends Thread {
        private FIFOMessageQueue<String,Integer> queue;
        private String key;
        private int num_msgs;
        private CyclicBarrier barrier;
        private int start_num;

        private Producer(FIFOMessageQueue<String,Integer> queue, String key, int start_num, int num_msgs, CyclicBarrier barrier) {
            this.queue=queue;
            this.key=key;
            this.start_num=start_num;
            this.num_msgs=num_msgs;
            this.barrier=barrier;
        }


        public void run() {
            try {
                barrier.await();
            }
            catch(Exception e) {
                e.printStackTrace();
            }
            for(int i=start_num; i <= num_msgs+start_num-1; i++) {
                try {
                    // Util.sleepRandom(50);
                    queue.put(a1, key, i);
                }
                catch(InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }


}
