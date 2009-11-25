package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.debug.Simulator;
import org.jgroups.protocols.BARRIER;
import org.jgroups.protocols.PING;
import org.jgroups.protocols.VIEW_SYNC;
import org.jgroups.stack.Protocol;
import org.jgroups.util.UUID;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests the BARRIER protocol
 * @author Bela Ban
 * @version $Id: BARRIERTest.java,v 1.8 2009/11/25 10:25:05 belaban Exp $
 */
@Test(groups=Global.FUNCTIONAL, sequential=true)
public class BARRIERTest {
    Address a1;
    Vector<Address> members;
    View v;
    Simulator s;
    BARRIER barrier_prot=new BARRIER();
    PING bottom_prot;


    @BeforeMethod
    public void setUp() throws Exception {
        a1=UUID.randomUUID();
        members=new Vector<Address>();
        members.add(a1);
        v=new View(a1, 1, members);
        s=new Simulator();
        s.setLocalAddress(a1);
        s.setView(v);
        s.addMember(a1);
        bottom_prot=new PING();
        Protocol[] stack=new Protocol[]{new VIEW_SYNC(), barrier_prot, bottom_prot};
        s.setProtocolStack(stack);
        s.start();
    }

    @AfterMethod
    public void tearDown() throws Exception {
        s.stop();
    }


    public void testBlocking() {
        assert !barrier_prot.isClosed();
        s.send(new Event(Event.CLOSE_BARRIER));
        assert barrier_prot.isClosed();
        s.send(new Event(Event.OPEN_BARRIER));
        assert !barrier_prot.isClosed();
    }


    public void testThreadsBlockedOnBarrier() {
        MyReceiver receiver=new MyReceiver();
        s.setReceiver(receiver);
        s.send(new Event(Event.CLOSE_BARRIER));
        for(int i=0; i < 5; i++) {
            new Thread() {
                public void run() {
                    bottom_prot.up(new Event(Event.MSG, new Message(null, null, null)));
                }
            }.start();
        }

        Util.sleep(2000);
        int num_in_flight_threads=barrier_prot.getNumberOfInFlightThreads();
        assert num_in_flight_threads == 0;

        s.send(new Event(Event.OPEN_BARRIER));
        Util.sleep(2000);

        num_in_flight_threads=barrier_prot.getNumberOfInFlightThreads();
        assert num_in_flight_threads == 0;
        int received_msgs=receiver.getNumberOfReceivedMessages();
        assert received_msgs == 5 : "expected " + 5 + " messages but got " + received_msgs;
    }


    public void testThreadsBlockedOnMutex() throws InterruptedException {
        BlockingReceiver receiver=new BlockingReceiver();
        s.setReceiver(receiver);

        Thread thread=new Thread() {
            public void run() {bottom_prot.up(new Event(Event.MSG, new Message()));}
        };

        Thread thread2=new Thread() {
            public void run() {bottom_prot.up(new Event(Event.MSG, new Message()));}
        };

        thread.start();
        thread2.start();

        thread.join();
        thread2.join();
    }




    static class MyReceiver implements Simulator.Receiver {
        AtomicInteger num_mgs_received=new AtomicInteger(0);

        public void receive(Event evt) {
            if(evt.getType() == Event.MSG) {
                if(num_mgs_received.incrementAndGet() % 1000 == 0)
                    System.out.println("<== " + num_mgs_received.get());
            }
        }

        public int getNumberOfReceivedMessages() {
            return num_mgs_received.get();
        }
    }


    class BlockingReceiver implements Simulator.Receiver {

        public void receive(Event evt) {
            System.out.println("Thread " + Thread.currentThread().getId() + " receive() called - about to enter mutex");
            synchronized(this) {
                System.out.println("Thread " + Thread.currentThread().getId() + " entered mutex");
                Util.sleep(2000);
                System.out.println("Thread " + Thread.currentThread().getId() + " closing barrier");
                s.send(new Event(Event.CLOSE_BARRIER));
                System.out.println("Thread " + Thread.currentThread().getId() + " closed barrier");
}
        }
    }



}
