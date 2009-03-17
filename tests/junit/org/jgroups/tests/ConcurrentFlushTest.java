package org.jgroups.tests;

import org.jgroups.ExtendedReceiverAdapter;
import org.jgroups.JChannel;
import org.jgroups.Channel;
import org.jgroups.util.Util;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Tests concurrent FLUSH and partial FLUSHes
 *
 * @author Manik Surtani
 * @version $Id: ConcurrentFlushTest.java,v 1.10.2.2 2009/03/17 14:45:38 vlada Exp $
 */
public class ConcurrentFlushTest extends ChannelTestBase {

    JChannel c1, c2, c3;

    public void setUp() throws Exception {
        super.setUp();
        CHANNEL_CONFIG = System.getProperty("channel.conf.flush", "flush-udp.xml");
    }

    public void tearDown() throws Exception {
    	if(c3 != null){
            c3.close();
            assertFalse(c3.isOpen());
            assertFalse(c3.isConnected());
            c3 = null;
        }
    	
    	if(c2 != null){
            c2.close();
            assertFalse(c2.isOpen());
            assertFalse(c2.isConnected());
            c2 = null;
        }

        if(c1 != null){
            c1.close();
            assertFalse(c1.isOpen());
            assertFalse(c1.isConnected());
            c1 = null;
        }

        Util.sleep(500);
        super.tearDown();
    }

    public boolean useBlocking() {
        return true;
    }
    
    /**
     * Tests A.startFlush(), followed by another A.startFlush()
     */
    public void testTwoStartFlushesOnSameMemberWithTotalFlush() throws Exception {
        c1=createChannel("c1");
        c1.connect("testTwoStartFlushes");
        c2=createChannel("c2");
        c2.connect("testTwoStartFlushes");
        assertViewsReceived(c1, c2);

        startFlush(c1, false);

        new Thread() {
            public void run() {
                stopFlush(c1);
            }
        }.start();

        boolean rc=startFlush(c1, false);
        assert rc;
        stopFlush(c1);

        rc=startFlush(c1, false);
        assert rc;
        stopFlush(c1);

        rc=startFlush(c1, true);
        assert rc;

        rc=startFlush(c1, true);

        rc=startFlush(c2, true);
    }

    /**
     * Tests A.startFlush(), followed by another A.startFlush()
     */
    public void testTwoStartFlushesOnDifferentMembersWithTotalFlush() throws Exception {
        c1=createChannel("c1");
        c1.connect("testTwoStartFlushesOnDifferentMembersWithTotalFlush");
        c2=createChannel("c2");
        c2.connect("testTwoStartFlushesOnDifferentMembersWithTotalFlush");
        assertViewsReceived(c1, c2);

        boolean rc=startFlush(c1, false);
        assert rc;

        rc=startFlush(c2, false);
        assert !rc;

        new Thread() {
            public void run() {
                stopFlush(c1);
            }
        }.start();

        rc=startFlush(c2, false);
        assert rc;
        stopFlush(c2);

        rc=startFlush(c1, false);
        assert rc;
        stopFlush(c1);

        rc=startFlush(c2, true);
        assert rc;

        rc=startFlush(c1, true);

        rc=startFlush(c2, true);
    }

    /**
     * Tests 2 channels calling FLUSH simultaneously
     */
    public void testConcurrentFlush() throws Exception {
        c1=createChannel("c1");
        c1.connect("testConcurrentFlush");
        c2=createChannel("c2");
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
                }
                catch (InterruptedException e) {
                    interrupt();
                }
                Util.startFlush(c1);

                try {
                    stopFlushLatch.await();
                } catch (InterruptedException e) {
                    interrupt();
                }

                c1.stopFlush();
            }
        };

        Thread t2=new Thread() {
            public void run() {
                try {
                    startFlushLatch.await();
                }
                catch (InterruptedException e) {
                    interrupt();
                }
                Util.startFlush(c2);

                try {
                    stopFlushLatch.await();
                } catch (InterruptedException e) {
                    interrupt();
                }

                c2.stopFlush();
            }
        };

        Listener l1=new Listener(c1, flushStartReceived, flushStopReceived);
        Listener l2=new Listener(c2, flushStartReceived, flushStopReceived);
        t1.start();
        t2.start();

        startFlushLatch.countDown();

        assertTrue(flushStartReceived.await(60, TimeUnit.SECONDS));

        // at this stage both channels should have started a flush
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
        c1=createChannel("c1");
        c1.connect("testConcurrentFlushAndPartialFlush");

        c2=createChannel("c2");
        c2.connect("testConcurrentFlushAndPartialFlush");

        c3=createChannel("c3");
        c3.connect("testConcurrentFlushAndPartialFlush");
        assertViewsReceived(c1, c2, c3);

        final CountDownLatch startFlushLatch=new CountDownLatch(1);
        final CountDownLatch stopFlushLatch=new CountDownLatch(1);

        //2 because either total or partial has to finish first
        final CountDownLatch flushStartReceived=new CountDownLatch(2);
        
        //5 because we have total and partial flush
        final CountDownLatch flushStopReceived=new CountDownLatch(5);

        Thread t1=new Thread() {
            public void run() {
                try {
                    startFlushLatch.await();
                }
                catch (InterruptedException e) {
                    interrupt();
                }
                Util.startFlush(c1);

                try {
                    stopFlushLatch.await();
                } catch (InterruptedException e) {
                    interrupt();
                }

                c1.stopFlush();

            }
        };

        Thread t2=new Thread() {
            public void run() {
                try {
                    startFlushLatch.await();
                }
                catch (InterruptedException e) {
                    interrupt();
                }
                // partial, only between c2 and c3
                Util.startFlush(c2, (Arrays.asList(c2.getLocalAddress(), c3.getLocalAddress())));
                
                try {
                    stopFlushLatch.await();
                } catch (InterruptedException e) {
                    interrupt();
                }

                c2.stopFlush(Arrays.asList(c2.getLocalAddress(), c3.getLocalAddress()));
            }
        };

        Listener l1=new Listener(c1, flushStartReceived, flushStopReceived);
        Listener l2=new Listener(c2, flushStartReceived, flushStopReceived);
        Listener l3=new Listener(c3, flushStartReceived, flushStopReceived);

        t1.start();
        t2.start();

        startFlushLatch.countDown();

        assertTrue(flushStartReceived.await(60, TimeUnit.SECONDS));

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

    private static boolean startFlush(Channel ch, boolean automatic_resume) {
        System.out.println("starting flush on " + ch.getLocalAddress() + " with automatic resume=" + automatic_resume);
        boolean result = Util.startFlush(ch);
        if(automatic_resume){
        	ch.stopFlush();
        }
        return result;
    }

    private static void stopFlush(Channel ch) {
        System.out.println("calling stopFlush()");
        ch.stopFlush();
    }


    private static void assertViewsReceived(JChannel... channels) {
        for (JChannel c : channels) assertEquals(c.getView().getMembers().size(), channels.length);
    }

    private static class Listener extends ExtendedReceiverAdapter {
        boolean blockReceived, unblockReceived;
        JChannel channel;
        CountDownLatch flushStartReceived, flushStopReceived;

        Listener(JChannel channel, CountDownLatch flushStartReceived, CountDownLatch flushStopReceived) {
            this.channel=channel;
            this.flushStartReceived=flushStartReceived;
            this.flushStopReceived=flushStopReceived;
            this.channel.setReceiver(this);
        }

        public void unblock() {
            unblockReceived=true;
            flushStopReceived.countDown();
        }

        public void block() {
            blockReceived=true;
            flushStartReceived.countDown();
        }
    }
}
