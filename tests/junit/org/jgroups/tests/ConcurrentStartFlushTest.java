package org.jgroups.tests;

import junit.framework.TestCase;
import org.jgroups.*;
import org.jgroups.util.Util;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;

/**
 * Tests flush phases started concurrently by different members
 * @author Bela Ban
 * @version $Id: ConcurrentStartFlushTest.java,v 1.1 2007/12/03 13:12:26 belaban Exp $
 */
public class ConcurrentStartFlushTest extends TestCase {
    private JChannel c1, c2, c3;
    private Receiver r1, r2, r3;
    private static final long TIMEOUT=10000L;


    protected void setUp() throws Exception {
        super.setUp();
        c1=new JChannel("flush-udp.xml"); c1.setOpt(Channel.BLOCK, true);
        c2=new JChannel("flush-udp.xml"); c2.setOpt(Channel.BLOCK, true);
        c3=new JChannel("flush-udp.xml"); c3.setOpt(Channel.BLOCK, true);

        c1.connect("x");
        c2.connect("x");
        c3.connect("x");
    }

    protected void tearDown() throws Exception {
        if(c3 != null)
            c3.close();
        if(c2 != null)
            c2.close();
        if(c1 != null)
            c1.close();
        super.tearDown();
    }


    public void testSimpleFlush() throws Exception {
        CyclicBarrier barrier=new CyclicBarrier(2);
        r1=new Receiver("C1", c1);
        r2=new Receiver("C2", c2);
        r3=new Receiver("C3", c3);
        c1.setReceiver(r1);
        c2.setReceiver(r2);
        c3.setReceiver(r3);

        Flusher flusher_one=new Flusher(c1, barrier);

        flusher_one.start();
        Util.sleep(1000);

        System.out.println("starting flush at C1");
        barrier.await();
        flusher_one.join();

        System.out.println("events for C1: " + r1.getEvents());
        System.out.println("events for C2: " + r2.getEvents());
        System.out.println("events for C3: " + r3.getEvents());
        assertEquals(2, r1.getEvents().size());
        assertEquals(2, r2.getEvents().size());
        assertEquals(2, r3.getEvents().size());
        ensureOrdering(r1.getEvents(), BlockEvent.class, UnblockEvent.class);
        ensureOrdering(r2.getEvents(), BlockEvent.class, UnblockEvent.class);
        ensureOrdering(r3.getEvents(), BlockEvent.class, UnblockEvent.class);
    }


    public void testConcurrentFlush() throws Exception {
        CyclicBarrier barrier=new CyclicBarrier(3);
        r1=new Receiver("C1", c1);
        r2=new Receiver("C2", c2);
        r3=new Receiver("C3", c3);
        c1.setReceiver(r1);
        c2.setReceiver(r2);
        c3.setReceiver(r3);

        Flusher flusher_one=new Flusher(c1, barrier);
        Flusher flusher_three=new Flusher(c3, barrier);

        flusher_one.start();
        flusher_three.start();
        Util.sleep(1000);

        System.out.println("starting concurrent flush at C1 and C3");
        barrier.await();
        flusher_one.join();
        flusher_three.join();

        System.out.println("events for C1: " + r1.getEvents());
        System.out.println("events for C2: " + r2.getEvents());
        System.out.println("events for C3: " + r3.getEvents());
        assertEquals(4, r1.getEvents().size());
        assertEquals(4, r2.getEvents().size());
        assertEquals(4, r3.getEvents().size());
        ensureOrdering(r1.getEvents(), BlockEvent.class, UnblockEvent.class, BlockEvent.class, UnblockEvent.class);
        ensureOrdering(r2.getEvents(), BlockEvent.class, UnblockEvent.class, BlockEvent.class, UnblockEvent.class);
        ensureOrdering(r3.getEvents(), BlockEvent.class, UnblockEvent.class, BlockEvent.class, UnblockEvent.class);
    }


    public void testFlushStartedByOneButCompletedByOther() throws Exception {
        r1=new Receiver("C1", c1);
        r2=new Receiver("C2", c2);
        r3=new Receiver("C3", c3);
        c1.setReceiver(r1);
        c2.setReceiver(r2);
        c3.setReceiver(r3);

        Util.sleep(1000);

        System.out.println("starting flush at C1");
        boolean rc=c1.startFlush(TIMEOUT, false);
        assertTrue(rc);
        Util.sleep(500);
        System.out.println("starting flush at C2");
        rc=c2.startFlush(TIMEOUT, false);
        assertTrue(rc);

        Util.sleep(1000);
        System.out.println("Stopping flush at C2");
        c2.stopFlush();

        Util.sleep(1000);
        System.out.println("Stopping flush at C1");
        c1.stopFlush();

        System.out.println("events for C1: " + r1.getEvents());
        System.out.println("events for C2: " + r2.getEvents());
        System.out.println("events for C3: " + r3.getEvents());

        assertEquals(4, r1.getEvents().size());
        assertEquals(4, r2.getEvents().size());
        assertEquals(4, r3.getEvents().size());
        ensureOrdering(r1.getEvents(), BlockEvent.class, UnblockEvent.class, BlockEvent.class, UnblockEvent.class);
        ensureOrdering(r2.getEvents(), BlockEvent.class, UnblockEvent.class, BlockEvent.class, UnblockEvent.class);
        ensureOrdering(r3.getEvents(), BlockEvent.class, UnblockEvent.class, BlockEvent.class, UnblockEvent.class);
    }


    private static void ensureOrdering(List<Object> events, Class... classes) {
        for(Class cl: classes) {
            Object element=events.remove(0);
            Class clazz=element.getClass();
            assertEquals(clazz, cl);
        }
    }


    private static class Flusher extends Thread {
        final JChannel channel;
        final CyclicBarrier barrier;

        public Flusher(JChannel channel, CyclicBarrier barrier) {
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

    private static class Receiver extends ExtendedReceiverAdapter {
        final String name;
        final JChannel channel;
        final List<Object> events=new LinkedList<Object>();

        public Receiver(String name, JChannel channel) {
            this.name=name;
            this.channel=channel;
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

    }

}
