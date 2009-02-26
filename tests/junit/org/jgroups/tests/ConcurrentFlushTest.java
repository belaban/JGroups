package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.blocks.MessageDispatcher;
import org.jgroups.util.Util;
import org.testng.annotations.Test;
import org.testng.annotations.AfterTest;
import org.testng.annotations.AfterMethod;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Tests concurrent FLUSH and partial FLUSHes
 * @author Manik Surtani
 * @version $Id: ConcurrentFlushTest.java,v 1.1 2009/02/26 11:23:28 belaban Exp $
 */
@Test(groups=Global.FLUSH, sequential=true)
public class ConcurrentFlushTest extends ChannelTestBase {

    JChannel c1, c2, c3;

    @AfterMethod
    public void tearDown() throws Exception {
        Util.close(c1, c2, c3);
    }

    public boolean useBlocking() {
        return true;
    }

    /**
     * Tests 2 channels calling FLUSH simultaneously
     */
    public void testConcurrentFlush() throws Exception {
        c1=createChannel(true, 2);
        c1.connect("testConcurrentFlush");
        c2=createChannel(c1);
        c2.connect("testConcurrentFlush");

        assertViewsReceived(c1, c2);

        final CountDownLatch startFlushLatch=new CountDownLatch(1);
        final CountDownLatch stopFlushLatch=new CountDownLatch(1);
        final CountDownLatch flushStopReceived=new CountDownLatch(2);

        Thread t1=new Thread() {
            public void run() {
                try {
                    startFlushLatch.await();
                }
                catch(InterruptedException e) {
                    interrupt();
                }
                c1.startFlush(false);
            }
        };

        Thread t2=new Thread() {
            public void run() {
                try {
                    startFlushLatch.await();
                }
                catch(InterruptedException e) {
                    interrupt();
                }
                c2.startFlush(false);
            }
        };

        Listener l1=new Listener(c1, stopFlushLatch, flushStopReceived);
        Listener l2=new Listener(c2, stopFlushLatch, flushStopReceived);
        t1.start();
        t2.start();

        startFlushLatch.countDown();

        t1.join();
        t2.join();

        // at this stage both channels should have started a flush?
        stopFlushLatch.countDown();

        assertTrue(flushStopReceived.await(60, TimeUnit.SECONDS));

        assert l1.blockReceived;
        assert l1.unblockReceived;
        assert l2.blockReceived;
        assert l2.unblockReceived;
    }

    /**
     * Tests 2 channels calling partial FLUSHes and one calling FLUSH simultaneously
     */
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

        // TODO: Would c2 and c3 receive *2* block and unblock messages, one from the total flush and one from the partial?
        final CountDownLatch flushStopReceived=new CountDownLatch(3);

        Thread t1=new Thread() {
            public void run() {
                try {
                    startFlushLatch.await();
                }
                catch(InterruptedException e) {
                    interrupt();
                }
                c1.startFlush(false);
            }
        };

        Thread t2=new Thread() {
            public void run() {
                try {
                    startFlushLatch.await();
                }
                catch(InterruptedException e) {
                    interrupt();
                }
                // partial, only between c2 and c3
                c2.startFlush(Arrays.asList(c2.getLocalAddress(), c3.getLocalAddress()), false);
            }
        };

        Listener l1=new Listener(c1, stopFlushLatch, flushStopReceived);
        Listener l2=new Listener(c2, stopFlushLatch, flushStopReceived);
        Listener l3=new Listener(c3, stopFlushLatch, flushStopReceived);

        t1.start();
        t2.start();

        startFlushLatch.countDown();

        t1.join();
        t2.join();

        // at this stage both channels should have started a flush?
        stopFlushLatch.countDown();

        assertTrue(flushStopReceived.await(60, TimeUnit.SECONDS));

        assertTrue(l1.blockReceived);
        assertTrue(l1.unblockReceived);
        assertTrue(l2.blockReceived);
        assertTrue(l2.unblockReceived);
        assertTrue(l3.blockReceived);
        assertTrue(l3.unblockReceived);
    }



    private static void assertViewsReceived(JChannel... channels) {
        for(JChannel c : channels) assertEquals(c.getView().getMembers().size(), channels.length);
    }

    private static class Listener extends ReceiverAdapter {
        boolean blockReceived, unblockReceived;
        JChannel channel;
        CountDownLatch stopFlushLatch, flushStopReceived;

        Listener(JChannel channel, CountDownLatch stopFlushLatch, CountDownLatch flushStopReceived) {
            this.channel=channel;
            this.stopFlushLatch=stopFlushLatch;
            this.flushStopReceived=flushStopReceived;
            this.channel.setReceiver(this);
        }

        public void unblock() {
            unblockReceived=true;
            flushStopReceived.countDown();
        }

        public void block() {
            blockReceived=true;
            try {
                if(stopFlushLatch.await(60, TimeUnit.SECONDS)) channel.stopFlush();
            }
            catch(InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
