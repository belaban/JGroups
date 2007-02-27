package org.jgroups.tests;

import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.util.FIFOMessageQueue;
import org.jgroups.util.Util;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.LinkedList;
import java.util.Collections;

/**
 * @author Bela Ban
 * @version $Id: FIFOMessageQueueTest.java,v 1.2 2007/02/27 17:32:27 belaban Exp $
 */
public class FIFOMessageQueueTest extends TestCase {
    FIFOMessageQueue<String,Integer> queue;
    String s1="s1", s2="s2", s3="s3";

    public void setUp() throws Exception {
        super.setUp();
        queue=new FIFOMessageQueue<String,Integer>();
    }

    public void tearDown() throws Exception {
        super.tearDown();
    }


    public void testSimplePutAndTake() throws InterruptedException {
        queue.put(s1, 1);
        assertEquals(1, queue.size());
        int ret=queue.take();
        assertEquals(1, ret);
        assertEquals(0, queue.size());
    }

    public void testMultiplePutsAndTakes() throws InterruptedException {
        for(int i=1; i <= 5; i++)
            queue.put(s1, i);
        System.out.println("queue is " + queue);
        assertEquals(5, queue.size());
        for(int i=1; i <= 5; i++) {
            int ret=queue.take();
            assertEquals(i, ret);
            assertEquals(5-i, queue.size());
            queue.done(s1);
        }
        assertEquals(0, queue.size());
    }


    public void testOrdering() throws InterruptedException {
        for(int i=1; i <= 3; i++)
            queue.put(s1, i);
        assertEquals(3, queue.size());

        int ret=queue.take();
        assertEquals(1, ret);
        assertEquals(2, queue.size());

        queue.done(s1);
        queue.put(s1, 4);
        queue.put(s1, 5);
        System.out.println("queue: " + queue);

        for(int i=2; i <= 5; i++) {
            ret=queue.take();
            assertEquals(i, ret);
            assertEquals(5-i, queue.size());
            queue.done(s1);
        }
        assertEquals(0, queue.size());
    }


    public void testOrderingMultipleThreads() throws BrokenBarrierException, InterruptedException {
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
        assertEquals(NUM * 3, queue.size());
    }

    public void testOrderingMultipleThreadsWithTakes() throws BrokenBarrierException, InterruptedException {
        testOrderingMultipleThreads();
        int ret;
        LinkedList<Integer> list=new LinkedList<Integer>();

        int size=queue.size();
        for(int i=0; i < size; i++) {
            ret=queue.take();
            list.add(ret);
            queue.done("s1");
            queue.done("s2");
            queue.done("s3");
        }

        System.out.println("analyzing returned values for correct ordering");
        LinkedList one=new LinkedList(), two=new LinkedList(), three=new LinkedList();
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
        assertEquals(len, two.size());
        assertEquals(len, three.size());


        LinkedList sorted_one=new LinkedList(one);
        Collections.sort(sorted_one);
        assertEquals("one: " + one + ", sorted: " + sorted_one, one, sorted_one);

        LinkedList sorted_two=new LinkedList(two);
        Collections.sort(sorted_two);
        assertEquals("two: " + two + ", sorted: " + sorted_two, two, sorted_two);

        LinkedList sorted_three=new LinkedList(three);
        Collections.sort(sorted_three);
        assertEquals("three: " + three + ", sorted: " + sorted_three, three, sorted_three);

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
                    queue.put(key, i);
                }
                catch(InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    public static junit.framework.Test suite() {
        return new TestSuite(FIFOMessageQueueTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }
}
