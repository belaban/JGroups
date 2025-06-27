package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.util.ConcurrentLinkedBlockingQueue;
import org.jgroups.util.FastArray;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Tests {@link org.jgroups.util.ConcurrentLinkedBlockingQueue}
 * @author Bela Ban
 * @since  5.4.9
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class ConcurrentLinkedBlockingQueueTest {
    protected BlockingQueue<Integer> q;


    public void testConstructor() {
        q=new ConcurrentLinkedBlockingQueue<>(10, true, true);
        assert q.isEmpty();
    }

    public void testOffer() {
        q=new ConcurrentLinkedBlockingQueue<>(10, false, false);
        for(int i=1; i <= 15; i++) {
            boolean added=q.offer(i);
            assert (i <= 10) == added;
        }
        assert !q.isEmpty();
        assert q.size() == 10;
    }

    public void testPoll() {
        q=new ConcurrentLinkedBlockingQueue<>(10, false, false);
        Integer el=q.poll();
        assert el == null && q.isEmpty();
        add(q, 1, 10);
        assert q.size() == 10;
        for(int i=1; i <= 10; i++) {
            el=q.poll();
            assert el == i;
        }
    }

    public void testBlockingTake() throws InterruptedException {
        q=new ConcurrentLinkedBlockingQueue<>(10, true, false);
        q.offer(1);
        assert q.size() == 1;
        Integer el=q.take();
        assert el == 1 && q.isEmpty();

        Taker taker=new Taker();
        taker.start();
        Util.sleep(500);
        q.offer(1);
        taker.join();
        el=taker.get();
        assert el == 1 && q.isEmpty();
    }

    public void testBlockingTake2() {
        q=new ConcurrentLinkedBlockingQueue<>(10, true, false);
        Taker2 taker=new Taker2();
        taker.start();
        assert q.isEmpty();
        Util.sleep(500);
        add(q,1,10);
        Util.sleep(500);
        taker.stopIt();
        taker.interrupt();
        List<Integer> list=taker.list;
        Collections.sort(list);
        List<Integer> expected=IntStream.rangeClosed(1,10).boxed().collect(Collectors.toList());
        assert list.equals(expected);
    }

    public void testBlockingTakeInterrupted() throws InterruptedException {
        q=new ConcurrentLinkedBlockingQueue<>(10, true, false);

        Thread t=new Thread(() -> {
            try {
                // Integer ignored=
                q.take();
                assert false : "take() should throw an InterruptedException";
            }
            catch(InterruptedException ex) {
                System.out.printf("thread %d got an exception (expected): %s\n", Thread.currentThread().getId(), ex);
            }
        });
        t.start();

        Util.sleep(500);
        t.interrupt();
        t.join();
        assert q.isEmpty();
    }

    public void testTakeOnNonBlockingQueue() throws InterruptedException {
        q=new ConcurrentLinkedBlockingQueue<>(10, false, false);
        try {
            // Integer ignored=
            q.take();
            assert false : "take() cannot be called on non-blocking queue";
        }
        catch(IllegalStateException ex) {
            System.out.printf("received exception as expected: %s", ex);
        }
    }

    public void testDrainTo() {
        q=new ConcurrentLinkedBlockingQueue<>(10, false, false);
        List<Integer> l=new FastArray<>(10);
        int num=q.drainTo(l);
        assert num == 0;
        add(q,1,10);
        num=q.drainTo(l);
        assert num == 10;
        assert q.isEmpty();
        assert l.size() == 10;
        assert l.equals(IntStream.rangeClosed(1,10).boxed().collect(Collectors.toList()));

        add(q,1,10);
        assert q.size() == 10;
        l=new FastArray<>(5);
        num=q.drainTo(l,5);
        assert num == 5;
        assert q.size() == 5 && l.size() == 5;
        assert l.equals(IntStream.rangeClosed(1,5).boxed().collect(Collectors.toList()));

        q.clear();
        add(q,1,10);
        l=new FastArray<>(15);
        num=q.drainTo(l, 15);
        assert num == 10;
        assert q.isEmpty() && l.size() == 10;
        assert l.equals(IntStream.rangeClosed(1,10).boxed().collect(Collectors.toList()));
    }

    public void testClear() {
        q=new ConcurrentLinkedBlockingQueue<>(100, false, false);
        add(q,1,100);
        assert q.size() == 100;
        q.clear();
        assert q.isEmpty();
    }

    public void testRemainingCapacity() {
        q=new ConcurrentLinkedBlockingQueue<>(10, false, false);
        add(q,1,10);
        for(int i=0; i < 10; i++) {
            int rem=q.remainingCapacity();
            assert rem == i;
            q.poll();
        }
    }

    public void testPut() throws InterruptedException, TimeoutException {
        q=new ConcurrentLinkedBlockingQueue<>(10, true, true);
        for(int i=1; i <= 5; i++)
            q.put(i);
        assert q.size() == 5;
        Thread putter=new Thread(() -> {
            for(int i=6; i <= 15; i++) {
                try {
                    q.put(i);
                }
                catch(InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        putter.start();
        Util.sleep(500);
        for(int i=0; i < 5; i++)
            q.poll();
        Util.waitUntil(1000, 100, () -> !putter.isAlive());
        putter.join();
        List<Integer> l=new FastArray<>(15);
        int num=q.drainTo(l);
        assert num == 10;
        assert l.equals(IntStream.rangeClosed(6,15).boxed().collect(Collectors.toList()));
    }

    public void testPutOnNonBlockingQueue() throws InterruptedException {
        q=new ConcurrentLinkedBlockingQueue<>(10, false, false);
        try {
            q.put(1);
            assert false : "put() cannot be called on non-blocking queue";
        }
        catch(IllegalStateException ex) {
            System.out.printf("received exception as expected: %s", ex);
        }
    }

    protected static void add(Queue<Integer> q, int from, int to) {
        for(int i=from; i <= to; i++)
            q.add(i);
    }

    protected class Taker extends Thread {
        protected Integer el;

        Integer get() {
            return el;
        }

        @Override
        public void run() {
            try {
                el=q.take();
            }
            catch(InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    protected class Taker2 extends Thread {
        protected volatile boolean running=true;
        protected final List<Integer> list=new FastArray<>(10);

        protected void stopIt() {running=false;}
        protected List<Integer> list() {return list;}

        @Override
        public void run() {
            while(running) {
                try {
                    Integer el=q.take();
                    list.add(el);
                }
                catch(InterruptedException e) {
                }
            }
        }
    }
}
