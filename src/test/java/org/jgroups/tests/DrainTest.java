package org.jgroups.tests;

import org.jgroups.EmptyMessage;
import org.jgroups.Global;
import org.jgroups.Message;
import org.jgroups.util.AverageMinMax;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

/**
 * Tests an algorithm that has multiple threads adding to a queue but only one thread at a time consuming the queue's
 * elements, using a simple CAS counter and a (lock-synchronized) queue. The Pluscal code is at
 * https://github.com/belaban/pluscal/blob/master/add.tla
 * @author Bela Ban
 * @since  4.0
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class DrainTest {
    protected final Queue<Message> queue=new ArrayBlockingQueue<>(50000);
    protected final AtomicInteger  counter=new AtomicInteger(0);
    protected final LongAdder      added=new LongAdder();
    protected final LongAdder      removed=new LongAdder();
    protected final LongAdder      num_removers=new LongAdder();
    protected final AverageMinMax  avg_removed=new AverageMinMax();

    public void testDraining() throws InterruptedException {
        MyThread[] threads=new MyThread[10];
        final CountDownLatch latch=new CountDownLatch(1);
        for(int i=0; i < threads.length; i++) {
            threads[i]=new MyThread(latch);
            threads[i].start();
        }
        latch.countDown();

        Util.sleep(5000);

        System.out.print("\nStopping threads\n");
        for(MyThread thread: threads)
            thread.cancel();
        System.out.print("done, joining threads\n");
        for(MyThread thread: threads)
            thread.join();

        System.out.printf("\ncounter=%d, added=%d, removed=%d, avg_removed=%s (removers=%d)\n",
                          counter.get(), added.sum(), removed.sum(), avg_removed, num_removers.sum());

        assert added.sum() == removed.sum();
        assert counter.get() == 0;
        assert this.queue.isEmpty();
    }

    protected void add(Message msg) {
        if(queue.offer(msg))
            added.increment();
        drain();
    }

    protected void drain() {
        if(counter.getAndIncrement() == 0) {
            num_removers.increment();
            int cnt=0;
            do {
                Message msg=queue.poll();
                if(msg == null)
                    continue; // System.err.printf("got empty message");
                removed.increment();
                cnt++;
            } while(counter.decrementAndGet() != 0);
            avg_removed.add(cnt);
        }
    }

    protected class MyThread extends Thread {
        protected final CountDownLatch latch;
        protected volatile boolean     running=true;

        public MyThread(CountDownLatch latch) {
            this.latch=latch;
        }

        protected void cancel() {running=false;}

        public void run() {
            try {
                latch.await();
            }
            catch(InterruptedException e) {
                e.printStackTrace();
            }
            while(running) {
                Message msg=new EmptyMessage();
                add(msg);
            }
        }
    }


}
