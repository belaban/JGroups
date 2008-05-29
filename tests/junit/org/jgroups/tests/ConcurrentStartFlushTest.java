package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.util.Util;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeClass;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;

/**
 * Tests flush phases started concurrently by different members
 * @author Bela Ban
 * @version $Id: ConcurrentStartFlushTest.java,v 1.9 2008/05/29 11:13:10 belaban Exp $
 */
@Test(groups={Global.FLUSH},sequential=true)
public class ConcurrentStartFlushTest extends ChannelTestBase {
    private Receiver r1, r2, r3;
    JChannel c1,c2,c3;
    private static final long TIMEOUT=10000L;


    @BeforeClass
    void init() throws Exception {
        c1 = createChannel(true);
        r1=new Receiver("C1", c1);

        c2 = createChannel(c1);
        r2=new Receiver("C2", c2);

        c3 = createChannel(c1);
        r3=new Receiver("C3", c3);
        c1.setReceiver(r1);
        c2.setReceiver(r2);
        c3.setReceiver(r3);

        final String GROUP=getUniqueClusterName("ConcurrentStartFlushTest");
        c1.connect(GROUP);
        c2.connect(GROUP);
        c3.connect(GROUP);
    }


    @AfterClass
    protected void tearDown() throws Exception {
        Util.close(c3,c2,c1);
    }
    
    public void testSimpleFlush() throws Exception {
        CyclicBarrier barrier=new CyclicBarrier(2);
        Flusher flusher_one=new Flusher(c1, barrier);

        flusher_one.start();
        Util.sleep(1000);

        System.out.println("starting flush at C1");
        barrier.await();
        flusher_one.join();

        //let async events propagate up
        Util.sleep(500);
        
        System.out.println("events for C1: " + r1.getEvents());
        System.out.println("events for C2: " + r2.getEvents());
        System.out.println("events for C3: " + r3.getEvents());
        
        checkEventStateTransferSequence(r1);
        checkEventStateTransferSequence(r2);
        checkEventStateTransferSequence(r3);
    }


    public void testConcurrentFlush() throws Exception {
        CyclicBarrier barrier=new CyclicBarrier(3);

        Flusher flusher_one=new Flusher(c1, barrier);
        Flusher flusher_three=new Flusher(c3, barrier);

        flusher_one.start();
        flusher_three.start();
        Util.sleep(1000);

        System.out.println("starting concurrent flush at C1 and C3");
        barrier.await();
        flusher_one.join();
        flusher_three.join();

        //let async events propagate up
        Util.sleep(500);
        
        System.out.println("events for C1: " + r1.getEvents());
        System.out.println("events for C2: " + r2.getEvents());
        System.out.println("events for C3: " + r3.getEvents());
        
        checkEventStateTransferSequence(r1);
        checkEventStateTransferSequence(r2);
        checkEventStateTransferSequence(r3);
    }


    public void testFlushStartedByOneButCompletedByOther() throws Exception {
        System.out.println("starting flush at C1");
        boolean rc=c1.startFlush(TIMEOUT, false);
        assertTrue(rc);
        Util.sleep(500);
        
        Util.sleep(1000);
        System.out.println("Stopping flush at C2");
        c2.stopFlush();
        
        System.out.println("starting flush at C2");
        rc=c2.startFlush(TIMEOUT, false);
        assertTrue(rc);
        

        Util.sleep(1000);
        System.out.println("Stopping flush at C1");
        c1.stopFlush();

        //let async events propagate up
        Util.sleep(500);
        System.out.println("events for C1: " + r1.getEvents());
        System.out.println("events for C2: " + r2.getEvents());
        System.out.println("events for C3: " + r3.getEvents());
        
        checkEventStateTransferSequence(r1);
        checkEventStateTransferSequence(r2);
        checkEventStateTransferSequence(r3);
    }  

    private static class Flusher extends Thread {
        final Channel channel;
        final CyclicBarrier barrier;

        public Flusher(Channel channel, CyclicBarrier barrier) {
            this.channel=channel;
            this.barrier=barrier;
        }

        public void run() {
            try {
                barrier.await();
                System.out.println("Flusher " + channel.getLocalAddress() + ": starting flush");
                boolean rc=channel.startFlush(TIMEOUT, false);
                System.out.println("flush was " + (rc? "successful" : "unsuccessful"));
                Util.sleep(500);
                System.out.println("Flusher " + channel.getLocalAddress() + ": stopping flush");
                channel.stopFlush();
            }
            catch(Throwable t) {
                t.printStackTrace();
            }
        }
    }

    private static class Receiver extends ExtendedReceiverAdapter implements EventSequence {
        final String name;
        final Channel channel;
        final List<Object> events;

        public Receiver(String name, Channel channel) {
            this.name=name;
            this.channel=channel;
            this.events=new LinkedList<Object>();
        }

        public List<Object> getEvents() {
            return events;
        }

        public void block() {
            System.out.println("[" + name + ", " + channel.getLocalAddress() + "] block()");
            events.add(new BlockEvent());
        }

        public void unblock() {
            System.out.println("[" + name + ", " + channel.getLocalAddress() + "] unblock()");
            events.add(new UnblockEvent());
        }

        public void viewAccepted(View new_view) {
            System.out.println("[" + name + ", " + channel.getLocalAddress() + "] view=" + new_view);
            events.add(new_view);
        }

        public String getName() {
            return name;
        }

    }

}
