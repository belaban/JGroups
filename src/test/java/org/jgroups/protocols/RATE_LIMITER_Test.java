package org.jgroups.protocols;

import org.jgroups.BytesMessage;
import org.jgroups.Global;
import org.jgroups.Message;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;


/**
 * Tests RATE_LIMITER
 * @author Bela Ban
 */
@Test(groups={Global.TIME_SENSITIVE,Global.EAP_EXCLUDED}, singleThreaded=true)
public class RATE_LIMITER_Test {
    final byte[] buffer=new byte[1000];

    public void testThroughputSingleThreaded() throws Exception {
        _testThroughput(1);
    }

    public void testThroughputMultiThreaded() throws Exception {
        _testThroughput(10);
    }

    protected void _testThroughput(int num_threads) throws Exception {
        RATE_LIMITER limiter=create(10, 100000); // 100K in 10 ms --> 10MB in 1 s
        long target_throughput=10000000;         // 10MB/s
        final CountDownLatch latch=new CountDownLatch(1);
        Throughput throughput=new Throughput(latch);
        limiter.setDownProtocol(throughput);
        throughput.init();
        throughput.start();
        latch.countDown();

        System.out.println("Measuring throughput for 10 seconds:");
        sendMessages(limiter, TimeUnit.MILLISECONDS.convert(10, TimeUnit.SECONDS), num_threads);
        throughput.stop();

        List<Long> list=new ArrayList<>(throughput.getMeasurements());
        if(list.size()  > 10)
            list.remove(list.size()-1);
        int second=1;
        System.out.println("Measurements:");
        for(long value: list) {
            System.out.println("sec " + second++ + ": " + Util.printBytes(value));
        }

        // 50% deviation below is ok: e.g. max_bytes=100000, and sending 50001 bytes !
        // In the real setup, there a warning if the system is not configured for max_bytes to be a multiple of frag_size
        long min_value=(long)(target_throughput * 0.45), max_value=(long)(target_throughput * 1.4);
        for(long value: list) {
            assert value >= min_value && value <= max_value: "value=" + value + " (min_value=" + min_value + ",max_value=" + max_value + ")";
        }
    }


    protected void sendMessages(RATE_LIMITER limiter, long duration, int num_threads) {
        System.out.println("Measuring throughput for 10 seconds (" + num_threads + " threads):");
        long target_time=TimeUnit.NANOSECONDS.convert(duration, TimeUnit.MILLISECONDS) + System.nanoTime();
        Sender[] senders=new Sender[num_threads];
        for(int i=0; i < senders.length; i++) {
            senders[i]=new Sender(limiter, target_time);
            senders[i].start();
        }
        for(Sender sender: senders) {
            try {
                sender.join();
            }
            catch(InterruptedException e) {
            }
        }
    }


    protected static RATE_LIMITER create(long time_period, long max_bytes) throws Exception {
        RATE_LIMITER prot=new RATE_LIMITER();
        prot.setTimePeriod(time_period);
        prot.setMaxBytes(max_bytes);
        prot.init();
        prot.start();
        return prot;
    }


    protected class Sender extends Thread {
        protected final RATE_LIMITER limiter;
        protected final long         target_time;

        public Sender(RATE_LIMITER limiter, long target_time) {
            this.limiter=limiter;
            this.target_time=target_time;
        }

        public void run() {
            do {
                Message msg=new BytesMessage().setArray(buffer, 0, buffer.length);
                limiter.down(msg);
            }
            while(System.nanoTime() <= target_time);
        }
    }


    protected static class Throughput extends Protocol implements Runnable {
        protected final AtomicInteger    bytes_in_period=new AtomicInteger(0);
        protected final Collection<Long> measurements=new ConcurrentLinkedQueue<>(); // measurement taken every second
        protected Thread                 runner;
        protected volatile boolean       running=true;
        protected final CountDownLatch   latch;
        protected final long             SLEEP_TIME=TimeUnit.NANOSECONDS.convert(1, TimeUnit.SECONDS);

        final AtomicInteger num_msgs_in_period=new AtomicInteger(0);

        public Throughput(CountDownLatch latch) {
            this.latch=latch;
        }

        public Collection<Long> getMeasurements() {
            return measurements;
        }

        public Object down(Message msg) {
            int length=msg.getLength();
            num_msgs_in_period.incrementAndGet();
            bytes_in_period.addAndGet(length);
            return null;
        }

        public void init() throws Exception {
            runner=new Thread(this);
            runner.start();
        }

        public void start() throws Exception {
            running=true;
        }

        public void stop() {
            running=false;
            Thread.currentThread().interrupt();
        }

        public void run() {
            try {
                latch.await();
            }
            catch(InterruptedException e) {
                e.printStackTrace();
                return;
            }

            while(running) {
                LockSupport.parkNanos(SLEEP_TIME);
                long tmp=0;

                // below we can lose a few messages between doing the get and the set, but that's ok as we only
                // need an approximation of throughput
                tmp=bytes_in_period.get();
                bytes_in_period.set(0);
                System.out.println("num_msgs_in_period=" + num_msgs_in_period.get());
                num_msgs_in_period.set(0);
                measurements.add(tmp);
            }
        }
    }

    @Test(enabled=false)
    public static void main(String[] args) throws Exception {
        RATE_LIMITER_Test test=new RATE_LIMITER_Test();
        test.testThroughputMultiThreaded();
    }
}