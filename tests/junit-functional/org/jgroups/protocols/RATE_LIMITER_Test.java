package org.jgroups.protocols;

import org.jgroups.Event;
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
@Test(groups=Global.TIME_SENSITIVE, sequential=true)
public class RATE_LIMITER_Test {

    public void testThroughput() throws Exception {
        byte[] buffer=new byte[1000];
        RATE_LIMITER limiter=create(10, 100000);
        long target_throughput=10000000; // 10MB/s
        final CountDownLatch latch=new CountDownLatch(1);
        Throughput throughput=new Throughput(latch);
        limiter.setDownProtocol(throughput);
        throughput.init();
        throughput.start();
        latch.countDown();

        System.out.println("Measuring throughput for 10 seconds:");
        long target=TimeUnit.NANOSECONDS.convert(10, TimeUnit.SECONDS) + System.nanoTime();
        for(;;) {
            Message msg=new Message(false);
            msg.setBuffer(buffer);
            limiter.down(new Event(Event.MSG,msg));
            if(System.nanoTime() > target)
                break;
        }
        throughput.stop();

        List<Long> list=new ArrayList<Long>(throughput.getMeasurements());
        if(list.size()  > 10)
            list.remove(list.size()-1);
        int second=1;
        System.out.println("Measurements:");
        for(long value: list) {
            System.out.println("sec " + second++ + ": " + Util.printBytes(value));
        }

        // 50% deviation below is ok: e.g. max_bytes=100000, and sending 50001 bytes !
        // In the real setup, there a warning if the system is not configured for max_bytes to be a multiple of frag_size
        long min_value=(long)(target_throughput * 0.45), max_value=(long)(target_throughput * 1.1);
        for(long value: list) {
            assert value >= min_value && value <= max_value;
        }
    }


    protected RATE_LIMITER create(long time_period, long max_bytes) throws Exception {
        RATE_LIMITER prot=new RATE_LIMITER();
        prot.setTimePeriod(time_period);
        prot.setMaxBytes(max_bytes);
        prot.init();
        prot.start();
        return prot;
    }


    protected static class Throughput extends Protocol implements Runnable {
        protected final AtomicInteger    bytes_in_period=new AtomicInteger(0);
        protected final Collection<Long> measurements=new ConcurrentLinkedQueue<Long>(); // measurement taken every second
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

        public Object down(Event evt) {
            if(evt.getType() == Event.MSG) {
                Message msg=(Message)evt.getArg();
                int length=msg.getLength();
                num_msgs_in_period.incrementAndGet();
                bytes_in_period.addAndGet(length);
            }
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


    public static void main(String[] args) throws Exception {
        RATE_LIMITER_Test test=new RATE_LIMITER_Test();
        test.testThroughput();
    }
}