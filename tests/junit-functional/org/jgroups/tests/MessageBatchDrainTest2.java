package org.jgroups.tests;

import org.jgroups.EmptyMessage;
import org.jgroups.Global;
import org.jgroups.Message;
import org.jgroups.util.AverageMinMax;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Tests multiple producers adding message batches to a queue and one of them becoming the (single) consumer which
 * removes and delivers messages.
 * https://github.com/belaban/pluscal/blob/master/MessageBatchDrainTest3.tla
 * @author Bela Ban
 * @since  4.0
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class MessageBatchDrainTest2 {
    protected final Lock           lock=new ReentrantLock();
    protected final MessageBatch   batch=new MessageBatch(BATCH_SIZE);
    protected final AtomicInteger  adders=new AtomicInteger(0);
    protected final LongAdder      added=new LongAdder();
    protected final LongAdder      removed=new LongAdder();
    protected final LongAdder      num_removers=new LongAdder();
    protected final AverageMinMax  avg_removed=new AverageMinMax();
    protected final AverageMinMax  avg_remove_loops=new AverageMinMax();

    protected static final boolean RESIZE=false;
    protected static final int     BATCH_SIZE=500;

    public void testDraining() throws InterruptedException {
        MyThread[] threads=new MyThread[10];
        final CountDownLatch latch=new CountDownLatch(1);
        for(int i=0; i < threads.length; i++) {
            threads[i]=new MyThread(latch);
            threads[i].start();
        }
        latch.countDown();

        Util.sleep(5000);

        System.out.printf("\nStopping threads\n");
        for(MyThread thread: threads)
            thread.cancel();
        System.out.printf("done, joining threads\n");
        for(MyThread thread: threads)
            thread.join();

        System.out.printf("\ncounter=%d, added=%d, removed=%d, avg_removed=%s, avg_remove_loops=%s (removers=%d)\n",
                          adders.get(), added.sum(), removed.sum(), avg_removed, avg_remove_loops, num_removers.sum());

        assert added.sum() == removed.sum();
        assert adders.get() == 0;
        assert this.batch.isEmpty();
    }

    protected void add(Message msg) {
        int size=_add(msg);
        if(size > 0) {
            added.add(size);
            drain(size);
        }
    }


    protected void add(MessageBatch mb) {
        int size=_add(mb);
        if(size > 0) {
            added.add(size);
            drain(size);
        }
    }


    protected void drain(int num) {
        if(adders.getAndIncrement() == 0) {
            num_removers.increment();
            int cnt=0, removed_msgs, total_removed=0;
            final MessageBatch delivery_batch=new MessageBatch(num);
            do {
                delivery_batch.reset();
                removed_msgs=_transfer(delivery_batch);
                if(removed_msgs > 0) {
                    total_removed+=removed_msgs;
                    removed.add(removed_msgs);
                }
                cnt++;
                //   LockSupport.parkNanos(4_000);
            } while(adders.decrementAndGet() != 0);
            avg_remove_loops.add(cnt);
            avg_removed.add(total_removed);
        }
    }

    protected int _transfer(MessageBatch to) {
        lock.lock();
        try {
            return to.transferFrom(this.batch, true);
        }
        finally {
            lock.unlock();
        }
    }

    protected int _add(Message msg) {
        lock.lock();
        try {
            return this.batch.add(msg, RESIZE);
        }
        finally {
            lock.unlock();
        }
    }

    protected int _add(MessageBatch b) {
        lock.lock();
        try {
            return this.batch.add(b, RESIZE);
        }
        finally {
            lock.unlock();
        }
    }

    protected int _clear() {
        lock.lock();
        try {
            int size=batch.size();
            batch.clear();
            return size;
        }
        finally {
            lock.unlock();
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
                if(Util.tossWeightedCoin(.3))
                    add(new EmptyMessage());
                else {
                    Message[] msgs=create(10);
                    MessageBatch mb=new MessageBatch(Arrays.asList(msgs));
                    add(mb);
                }
            }
        }
    }

    protected static Message[] create(int max) {
        int num=(int)Util.random(max);
        Message[] msgs=new Message[num];
        for(int i=0; i < msgs.length; i++)
            msgs[i]=new EmptyMessage();
        return msgs;
    }
}
