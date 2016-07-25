package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.*;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests the BARRIER protocol
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class BARRIERTest {
    protected JChannel  ch;
    protected Discovery discovery_prot;
    protected BARRIER   barrier_prot;
    protected TP        tp;


    @BeforeMethod void setUp() throws Exception {
        ch=new JChannel(tp=new SHARED_LOOPBACK(), discovery_prot=new SHARED_LOOPBACK_PING(), barrier_prot=new BARRIER()).name("A");
        ch.connect("BARRIERTest");
    }

    @AfterMethod void destroy() {Util.close(ch);}

    public void testBlocking() {
        assert !barrier_prot.isClosed();
        ch.down(new Event(Event.CLOSE_BARRIER));
        assert barrier_prot.isClosed();
        ch.down(new Event(Event.OPEN_BARRIER));
        assert !barrier_prot.isClosed();
    }

    public void testThreadsBlockedOnBarrier() {
        MyReceiver receiver=new MyReceiver();
        ch.setReceiver(receiver);
        ch.down(new Event(Event.CLOSE_BARRIER)); // BARRIER starts discarding messages from now on
        for(int i=0; i < 5; i++) {
            new Thread() {public void run() {
                discovery_prot.up(createMessage());}}.start();
        }

        Util.sleep(2000);
        int num_in_flight_threads=barrier_prot.getNumberOfInFlightThreads();
        assert num_in_flight_threads == 0;

        ch.down(new Event(Event.OPEN_BARRIER));
        Util.sleep(2000);

        num_in_flight_threads=barrier_prot.getNumberOfInFlightThreads();
        assert num_in_flight_threads == 0;
        int received_msgs=receiver.getNumberOfReceivedMessages();
        assert received_msgs == 0 : "expected " + 0 + " messages but got " + received_msgs;
    }


    public void testThreadsBlockedOnMutex() throws Exception {
        final CyclicBarrier barrier=new CyclicBarrier(3);
        BlockingReceiver receiver=new BlockingReceiver(barrier);
        ch.setReceiver(receiver);

        Thread[] threads=new Thread[2];
        for(int i=1; i <= threads.length; i++) {
            Thread thread=new Thread() {
                public void run() {
                    discovery_prot.up(createMessage());}
            };
            thread.setName("blocker-" + i);
            thread.start();
        }

        waitUntilNumThreadsAreBlocked(2, 10000, 500);
        assert barrier_prot.getNumberOfInFlightThreads() == 2;
        barrier.await(); // starts the threads

        waitUntilNumThreadsAreBlocked(0, 10000, 500);
        assert barrier_prot.getNumberOfInFlightThreads() == 0;
    }


    public void testThreadFlushTimeout() throws Exception {
        final CyclicBarrier barrier=new CyclicBarrier(3);
        BlockingReceiver receiver=new BlockingReceiver(barrier);
        ch.setReceiver(receiver);
        barrier_prot.setValue("flush_timeout", 2000);

        Thread[] threads=new Thread[2];
        for(int i=1; i <= threads.length; i++) {
            Thread thread=new Thread() {
                public void run() {
                    discovery_prot.up(createMessage());}
            };
            thread.setName("blocker-" + i);
            thread.start();
        }

        // wait until all threads are blocked
        waitUntilNumThreadsAreBlocked(2, 10000, 500);
        assert barrier_prot.getNumberOfInFlightThreads() == 2;

        try {
            ch.down(new Event(Event.CLOSE_BARRIER));
            assert false : "closing BARRIER should have thrown an exception as threads couldn't be flushed";
        }
        catch(Exception ex) {
            System.out.println("got exception as expected: " + ex);
        }
        barrier.await();
    }


    protected Message createMessage() {
        return new Message(null).src(ch.getAddress()).putHeader(tp.getId(),new TpHeader("BARRIERTest"));
    }

    protected void waitUntilNumThreadsAreBlocked(int expected, long timeout, long interval) {
        long target_time=System.currentTimeMillis() + timeout;
        while(System.currentTimeMillis() < target_time) {
            if(barrier_prot.getNumberOfInFlightThreads() == expected)
                break;
            Util.sleep(interval);
        }
    }


    protected static class MyReceiver extends ReceiverAdapter {
        protected final AtomicInteger num_mgs_received=new AtomicInteger(0);

        public void receive(Message msg) {
            if(num_mgs_received.incrementAndGet() % 1000 == 0)
                System.out.println("<== " + num_mgs_received.get());
        }

        public int getNumberOfReceivedMessages() {
            return num_mgs_received.get();
        }
    }


    protected static class BlockingReceiver extends ReceiverAdapter {
        protected final CyclicBarrier barrier;

        BlockingReceiver(CyclicBarrier barrier) {
            this.barrier=barrier;
        }

        public void receive(Message msg) {
            try {
                barrier.await();
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
    }



}
