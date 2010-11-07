package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Tests concurrent and partial flushes
 * @author Manik Surtani
 * @author Bela Ban
 * @version $Id: ConcurrentFlushTest.java,v 1.14 2009/08/20 12:32:36 belaban Exp $
 */
@Test(groups=Global.FLUSH, sequential=true)
public class ConcurrentFlushTest extends ChannelTestBase {

    JChannel c1, c2, c3;

    @AfterMethod
    public void tearDown() throws Exception {
        Util.close(c3, c2, c1);
    }

    public boolean useBlocking() {
        return true;
    }



    /**
     * Tests A.startFlush(), followed by another A.startFlush()
     */
    @Test
    public void testTwoStartFlushesOnSameMemberWithTotalFlush() throws Exception {
        c1=createChannel(true, 3);
        c1.connect("testTwoStartFlushes");
        c2=createChannel(c1);
        c2.connect("testTwoStartFlushes");
        assertViewsReceived(c1, c2);

        boolean rc=startFlush(c1, true);
        assert rc;

        rc=startFlush(c1, false);
        assert rc;

        rc=startFlush(c1, 1, 500, false);
        assert !rc;
        stopFlush(c1);

        rc=startFlush(c1, true);
        assert rc;
    }

    /**
     * Tests A.startFlush(), followed by another A.startFlush()
     */
    public void testTwoStartFlushesOnDifferentMembersWithTotalFlush() throws Exception {
        c1=createChannel(true, 3);
        c1.connect("testTwoStartFlushesOnDifferentMembersWithTotalFlush");
        c2=createChannel(c1);
        c2.connect("testTwoStartFlushesOnDifferentMembersWithTotalFlush");
        assertViewsReceived(c1, c2);

        boolean rc=startFlush(c1, false);
        assert rc;

        rc=startFlush(c2, 1, 500, false);
        assert !rc;

        stopFlush(c1);

        rc=startFlush(c2, false);
        assert rc;
        stopFlush(c2);

        rc=startFlush(c1, false);
        assert rc;
        stopFlush(c2); // c2 can actually stop a flush started by c1

        rc=startFlush(c2, true);
        assert rc;
    }

    /**
     * Tests 2 channels calling FLUSH simultaneously
     */
    @Test
    public void testConcurrentFlush() throws Exception {
        c1=createChannel(true, 2);
        c1.connect("testConcurrentFlush");
        c2=createChannel(c1);
        c2.connect("testConcurrentFlush");

        assertViewsReceived(c1, c2);

        final CountDownLatch startFlushLatch=new CountDownLatch(1);
        final CountDownLatch stopFlushLatch=new CountDownLatch(1);
        final CountDownLatch flushStartReceived=new CountDownLatch(2);
        final CountDownLatch flushStopReceived=new CountDownLatch(2);

        Thread t1=new Thread() {
            public void run() {
                try {
                    startFlushLatch.await();
                    boolean rc=Util.startFlush(c1);
                    System.out.println("t1: rc=" + rc);
                }
                catch(InterruptedException e) {
                    interrupt();
                }

                try {
                    stopFlushLatch.await();
                }
                catch(InterruptedException e) {
                    interrupt();
                }
                finally {
                    c1.stopFlush();
                }
            }
        };

        Thread t2=new Thread() {
            public void run() {
                try {
                    startFlushLatch.await();
                    boolean rc=Util.startFlush(c2);
                    System.out.println("t2: rc=" + rc);
                }
                catch(InterruptedException e) {
                    interrupt();
                }

                try {
                    stopFlushLatch.await();
                }
                catch(InterruptedException e) {
                    interrupt();
                }
                finally {
                    c2.stopFlush();
                }
            }
        };

        Listener l1=new Listener("c1", c1, flushStartReceived, flushStopReceived);
        Listener l2=new Listener("c2", c2, flushStartReceived, flushStopReceived);
        t1.start();
        t2.start();

        startFlushLatch.countDown();

        assertTrue(flushStartReceived.await(60, TimeUnit.SECONDS));

        // at this stage both channels should have started a flush
        stopFlushLatch.countDown();

        t1.join();
        t2.join();

        assertTrue(flushStopReceived.await(60, TimeUnit.SECONDS));


        assert l1.blockReceived;
        assert l1.unblockReceived;
        assert l2.blockReceived;
        assert l2.unblockReceived;
    }

    /**
     * Tests 2 channels calling partial FLUSHes and one calling FLUSH simultaneously
     */
    @Test
    public void testConcurrentFlushAndPartialFlush() throws Exception {
        c1=createChannel(true, 3);
        c1.connect("testConcurrentFlushAndPartialFlush");

        c2=createChannel(c1);
        c2.connect("testConcurrentFlushAndPartialFlush");

        c3=createChannel(c1);
        c3.connect("testConcurrentFlushAndPartialFlush");
        assertViewsReceived(c1, c2, c3);

        final CountDownLatch startFlushLatch=new CountDownLatch(1);
        final CountDownLatch stopFlushLatch=new CountDownLatch(1);

        // 2 because either total or partial has to finish first
        final CountDownLatch flushStartReceived=new CountDownLatch(2);

        // 5 because we have total and partial flush
        final CountDownLatch flushStopReceived=new CountDownLatch(5);

        Thread t1=new Thread() {
            public void run() {
                try {
                    startFlushLatch.await();
                    boolean rc=Util.startFlush(c1);
                    System.out.println("t1: rc=" + rc);
                }
                catch(InterruptedException e) {
                    interrupt();
                }

                try {
                    stopFlushLatch.await();
                }
                catch(InterruptedException e) {
                    interrupt();
                }
                finally {
                    c1.stopFlush();
                }
            }
        };

        Thread t2=new Thread() {
            public void run() {
                try {
                    startFlushLatch.await();
                    // partial, only between c2 and c3
                    boolean rc=Util.startFlush(c2, (Arrays.asList(c2.getLocalAddress(), c3.getLocalAddress())));
                    System.out.println("t2: partial flush rc=" + rc);
                }
                catch(InterruptedException e) {
                    interrupt();
                }

                try {
                    stopFlushLatch.await();
                }
                catch(InterruptedException e) {
                    interrupt();
                }
                finally {
                    c2.stopFlush(Arrays.asList(c2.getLocalAddress(), c3.getLocalAddress()));
                }
            }
        };

        Listener l1=new Listener("c1", c1, flushStartReceived, flushStopReceived);
        Listener l2=new Listener("c2", c2, flushStartReceived, flushStopReceived);
        Listener l3=new Listener("c3", c3, flushStartReceived, flushStopReceived);

        t1.start();
        t2.start();

        startFlushLatch.countDown();

        assertTrue(flushStartReceived.await(60, TimeUnit.SECONDS));

        // at this stage both channels should have started a flush?
        stopFlushLatch.countDown();

        t1.join();
        t2.join();

        assertTrue(flushStopReceived.await(60, TimeUnit.SECONDS));

        assert l1.blockReceived;
        assert l1.unblockReceived;
        assert l2.blockReceived;
        assert l2.unblockReceived;
        assert l3.blockReceived;
        assert l3.unblockReceived;
    }

    private boolean startFlush(Channel ch, boolean automatic_resume) {
        log.debug("starting flush on " + ch.getAddress() + " with automatic resume=" + automatic_resume);
        boolean result=Util.startFlush(ch);
        if(automatic_resume) {
            ch.stopFlush();
        }
        return result;
    }

    private boolean startFlush(Channel ch, int num_attempts, long timeout, boolean automatic_resume) {
        log.debug("starting flush on " + ch.getAddress() + " with automatic resume=" + automatic_resume);
        boolean result=Util.startFlush(ch, num_attempts, 10, timeout);
        if(automatic_resume) {
            ch.stopFlush();
        }
        return result;
    }

    private void stopFlush(Channel ch) {
        log.debug("calling stopFlush()");
        ch.stopFlush();
    }

    private static void assertViewsReceived(JChannel... channels) {
        for(JChannel c : channels)
            assertEquals(c.getView().getMembers().size(), channels.length);
    }

    private static class Listener extends ExtendedReceiverAdapter implements EventSequence {
        final String name;
        boolean  blockReceived, unblockReceived;
        JChannel channel;
        CountDownLatch flushStartReceived, flushStopReceived;
        final List<Object> events=new LinkedList<Object>();


        Listener(String name, JChannel channel, CountDownLatch flushStartReceived, CountDownLatch flushStopReceived) {
            this.name=name;
            this.channel=channel;
            this.flushStartReceived=flushStartReceived;
            this.flushStopReceived=flushStopReceived;
            this.channel.setReceiver(this);
        }

        public void unblock() {
            unblockReceived=true;
            if(flushStopReceived != null)
                flushStopReceived.countDown();
            events.add(new UnblockEvent());
        }

        public void block() {
            blockReceived=true;
            if(flushStartReceived != null)
                flushStartReceived.countDown();
            events.add(new BlockEvent());
        }

        public List<Object> getEvents() {
            return events;
        }

        public void viewAccepted(View new_view) {
            events.add(new_view);
        }

        public String getName() {
            return name;
        }

    }

    
}
