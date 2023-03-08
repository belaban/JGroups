package org.jgroups.protocols;

import org.jgroups.Event;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.MyReceiver;
import org.jgroups.util.ThreadPool;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.*;

/**
 * Tests NAKACK2 with retransmission disabled: https://issues.redhat.com/browse/JGRP-2675
 * @author Bela Ban
 * @since  5.2.13
 */
@Test(groups= Global.FUNCTIONAL,singleThreaded=true)
public class NAKACK2_NoRetransmissionTest {
    protected JChannel         a,b,c;
    protected static final int BECOME_SERVER_QUEUE_SIZE=10;

    @BeforeMethod public void init() throws Exception {
        a=create("A").connect(NAKACK2_NoRetransmissionTest.class.getSimpleName());
        b=create("B").connect(NAKACK2_NoRetransmissionTest.class.getSimpleName());
        Util.waitUntilAllChannelsHaveSameView(5000, 200, a,b);
    }

    @AfterMethod
    public void destroy() {
        Util.close(c,b,a);
    }

    /**
     * C joins, but before the view {A,B,C} is received, B sends N messages that are queued on C
     * (NAKACK2.become_server_queue). C should deliver the queued and then the new messages from B in the right order,
     * and without gaps.
     */
    public void testJoinerGettingCorrectDigest() throws Exception {
        c=create("C");
        MyReceiver<Integer> receiver=new MyReceiver<>();
        c.setReceiver(receiver);
        CountDownLatch latch=new CountDownLatch(1);
        DelayViewChangeEvent delay=new DelayViewChangeEvent(latch);
        ProtocolStack stack=c.getProtocolStack();
        NAKACK2 nak=stack.findProtocol(NAKACK2.class);
        stack.insertProtocol(delay, ProtocolStack.Position.ABOVE, NAKACK2.class);

        new Thread(() -> {
            try {
                c.connect(NAKACK2_NoRetransmissionTest.class.getSimpleName());
            }
            catch(Exception e) {
                System.err.printf("failed connecting C: %s\n", e);
            }
        }).start();

        Util.sleep(1000);
        List<Integer> list=receiver.list();
        for(int i=1; i <= 20; i++)
            b.send(null, i);

        // VIEW from A and 20 messages from B
        Util.waitUntil(5000, 200, () -> nak.getBecomeServerQueueSizeActual() >= BECOME_SERVER_QUEUE_SIZE);

        latch.countDown(); // causes the become_server_queue to be flushed
        Util.waitUntilAllChannelsHaveSameView(10000, 500, a,b,c);

        Util.waitUntil(5000, 100, () -> list.size() == 20, () -> String.format("list.size()=%d", list.size()));

        for(int i=21; i <= 50; i++)
            b.send(null, i);
        Util.waitUntil(5000, 100, () -> list.size() == 50);
    }

    /**
     * C joins, view is {A,B,C}. C's thread pool discards every 2nd message by throwing a
     * {@link java.util.concurrent.RejectedExecutionException}. Because there's no retransmission, a correct
     * {@link java.util.concurrent.RejectedExecutionHandler} needs to pass the message up on the same thread, bypassing
     * the thread pool
     */
    public void testThreadPoolDiscardingMessages() throws Exception {
        c=create("C");
        TP transport=c.getProtocolStack().getTransport();
        MyReceiver<Integer> receiver=new MyReceiver<>();
        c.setReceiver(receiver);
        c.connect(NAKACK2_NoRetransmissionTest.class.getSimpleName());
        Util.waitUntilAllChannelsHaveSameView(10000, 500, a,b,c);
        Util.sleep(1000);
        List<Integer> list=receiver.list();
        ThreadPool pool=transport.getThreadPool().setThreadDumpsThreshold(50).setMaxThreads(3);

        // fill the thread pool
        for(int i=1; i <= pool.getMaxThreads(); i++)
            pool.execute(new LongTakingTask());

        for(int i=1; i <= 20; i++)
            b.send(null, i);
        Util.waitUntil(5000, 100, () -> list.size() == 20, () -> String.format("list.size()=%d", list.size()));
    }


    protected static JChannel create(String name) throws Exception {
        TCP tcp=new TCP().setBindAddr(Util.getLoopback());
        Protocol[] prots={tcp,
          new LOCAL_PING(),
          new NAKACK2().setXmitInterval(0).useMcastXmit(false).setBecomeServerQueueSize(BECOME_SERVER_QUEUE_SIZE),
          new UNICAST3(), new STABLE(), new GMS()};
        JChannel ret=new JChannel(prots).name(name);
        tcp.getThreadPool().setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        return ret;
    }

    protected static class DelayViewChangeEvent extends Protocol {
        protected final CountDownLatch latch;

        protected DelayViewChangeEvent(CountDownLatch latch) {
            this.latch=latch;
        }

        @Override
        public Object down(Event evt) {
            if(evt.type() == Event.VIEW_CHANGE) {
                try {
                    latch.await(30, TimeUnit.SECONDS);
                }
                catch(InterruptedException e) {
                    System.out.printf("failed waiting on latch: %s\n", e);
                }
            }
            return down_prot.down(evt);
        }
    }

    // Fills up the thread pool
    protected static class LongTakingTask implements Runnable {
        @Override public void run() {
            Util.sleep(60000);
        }
    }

}
