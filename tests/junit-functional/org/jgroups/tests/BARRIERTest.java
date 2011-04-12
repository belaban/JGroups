package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.BARRIER;
import org.jgroups.protocols.EXAMPLE;
import org.jgroups.protocols.PING;
import org.jgroups.protocols.SHARED_LOOPBACK;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests the BARRIER protocol
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL,sequential=true)
public class BARRIERTest {
    JChannel ch;
    PING     ping_prot;
    BARRIER  barrier_prot;
    EXAMPLE example_prot;


    @BeforeMethod
    public void setUp() throws Exception {
        ping_prot=new PING();
        example_prot=new EXAMPLE();
        barrier_prot=new BARRIER();
        ch=Util.createChannel(new SHARED_LOOPBACK(), ping_prot, barrier_prot, example_prot);
        ch.connect("BARRIERTest");
    }

    @AfterMethod
    public void destroy() {
        Util.close(ch);
    }

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
        ch.down(new Event(Event.CLOSE_BARRIER));
        for(int i=0; i < 5; i++) {
            new Thread() {
                public void run() {
                    ping_prot.up(new Event(Event.MSG, new Message(null, null, null)));
                }
            }.start();
        }

        Util.sleep(2000);
        int num_in_flight_threads=barrier_prot.getNumberOfInFlightThreads();
        assert num_in_flight_threads == 0;

        ch.down(new Event(Event.OPEN_BARRIER));
        Util.sleep(2000);

        num_in_flight_threads=barrier_prot.getNumberOfInFlightThreads();
        assert num_in_flight_threads == 0;
        int received_msgs=receiver.getNumberOfReceivedMessages();
        assert received_msgs == 5 : "expected " + 5 + " messages but got " + received_msgs;
    }


    public void testThreadsBlockedOnMutex() throws InterruptedException {
        BlockingReceiver receiver=new BlockingReceiver();
        ch.setReceiver(receiver);

        Thread thread=new Thread() {
            public void run() {
                ping_prot.up(new Event(Event.MSG, new Message()));}
        };

        Thread thread2=new Thread() {
            public void run() {
                ping_prot.up(new Event(Event.MSG, new Message()));}
        };

        thread.start();
        thread2.start();

        thread.join();
        thread2.join();
    }




    static class MyReceiver extends ReceiverAdapter {
        AtomicInteger num_mgs_received=new AtomicInteger(0);

        public void receive(Message msg) {
            if(num_mgs_received.incrementAndGet() % 1000 == 0)
                System.out.println("<== " + num_mgs_received.get());
        }

        public int getNumberOfReceivedMessages() {
            return num_mgs_received.get();
        }
    }


    class BlockingReceiver extends ReceiverAdapter {

        public void receive(Message msg) {
            System.out.println("Thread " + Thread.currentThread().getId() + " receive() called - about to enter mutex");
            synchronized(this) {
                System.out.println("Thread " + Thread.currentThread().getId() + " entered mutex");
                Util.sleep(2000);
                System.out.println("Thread " + Thread.currentThread().getId() + " closing barrier");
                ch.down(new Event(Event.CLOSE_BARRIER));
                System.out.println("Thread " + Thread.currentThread().getId() + " closed barrier");
            }
        }
    }



}
