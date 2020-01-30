package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.pbcast.FLUSH;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Tests concurrent and partial flushes
 * @author Manik Surtani
 * @author Bela Ban
 */
@Test(groups={Global.FLUSH,Global.EAP_EXCLUDED}, singleThreaded=true)
public class ConcurrentFlushTest {
    protected JChannel a, b, c;

    @AfterMethod void tearDown() throws Exception {Util.close(c,b,a);}


    /** Tests A.startFlush(), followed by another A.startFlush() */
    public void testTwoStartFlushesOnSameMemberWithTotalFlush() throws Exception {
        a=createChannel("A");
        a.connect("testTwoStartFlushes");
        b=createChannel("B");
        b.connect("testTwoStartFlushes");
        Util.waitUntilAllChannelsHaveSameView(10000, 500, a, b);

        assert startFlush(a, true);
        assert startFlush(a, false);
        assert !startFlush(a, 1, 500, false);
        a.stopFlush();
        assert startFlush(a, true);
    }

    /** Tests A.startFlush(), followed by another A.startFlush() */
    public void testTwoStartFlushesOnDifferentMembersWithTotalFlush() throws Exception {
        a=createChannel("A");
        a.connect("testTwoStartFlushesOnDifferentMembersWithTotalFlush");
        b=createChannel("B");
        b.connect("testTwoStartFlushesOnDifferentMembersWithTotalFlush");
        Util.waitUntilAllChannelsHaveSameView(10000, 500, a, b);

        assert startFlush(a, false);
        assert !startFlush(b, 1, 500, false);
        a.stopFlush();
        assert startFlush(b, false);
        b.stopFlush();
        assert startFlush(a, false);
        b.stopFlush(); // c2 can actually stop a flush started by c1
        assert startFlush(b, true);
    }

    /** Tests 2 channels calling FLUSH simultaneously */
    public void testConcurrentFlush() throws Exception {
        a=createChannel("A");
        a.connect("testConcurrentFlush");
        b=createChannel("B");
        b.connect("testConcurrentFlush");
        Util.waitUntilAllChannelsHaveSameView(10000, 500, a, b);

        final CountDownLatch startFlushLatch=new CountDownLatch(1);
        final CountDownLatch stopFlushLatch=new CountDownLatch(1);
        final CountDownLatch flushStartReceived=new CountDownLatch(2);
        final CountDownLatch flushStopReceived=new CountDownLatch(2);

        Thread t1=new Flusher(startFlushLatch, stopFlushLatch, a, null);
        Thread t2=new Flusher(startFlushLatch, stopFlushLatch, b, null);
        Listener l1=new Listener("c1",a, flushStartReceived, flushStopReceived);
        Listener l2=new Listener("c2",b, flushStartReceived, flushStopReceived);
        t1.start();
        t2.start();

        startFlushLatch.countDown();

        assert flushStartReceived.await(60, TimeUnit.SECONDS);

        // at this stage both channels should have started a flush
        stopFlushLatch.countDown();
        t1.join();
        t2.join();
        assert flushStopReceived.await(60, TimeUnit.SECONDS);

        assert l1.blockReceived;
        assert l1.unblockReceived;
        assert l2.blockReceived;
        assert l2.unblockReceived;
    }

    /** Tests 2 channels calling partial FLUSHes and one calling FLUSH simultaneously */
    public void testConcurrentFlushAndPartialFlush() throws Exception {
        a=createChannel("A");
        a.connect("testConcurrentFlushAndPartialFlush");

        b=createChannel("B");
        b.connect("testConcurrentFlushAndPartialFlush");

        c=createChannel("C");
        c.connect("testConcurrentFlushAndPartialFlush");
        Util.waitUntilAllChannelsHaveSameView(10000, 500, a, b, c);

        final CountDownLatch startFlushLatch=new CountDownLatch(1);
        final CountDownLatch stopFlushLatch=new CountDownLatch(1);

        // 2 because either total or partial has to finish first
        final CountDownLatch flushStartReceived=new CountDownLatch(2);

        // 5 because we have total and partial flush
        final CountDownLatch flushStopReceived=new CountDownLatch(5);

        Thread t1=new Flusher(startFlushLatch, stopFlushLatch, a, null);
        Thread t2=new Flusher(startFlushLatch, stopFlushLatch, b, Arrays.asList(b.getAddress(),c.getAddress()));
        Listener l1=new Listener("c1",a, flushStartReceived, flushStopReceived);
        Listener l2=new Listener("c2",b, flushStartReceived, flushStopReceived);
        Listener l3=new Listener("c3",c, flushStartReceived, flushStopReceived);

        t1.start();
        t2.start();

        startFlushLatch.countDown();

        assert flushStartReceived.await(60, TimeUnit.SECONDS);

        // at this stage both channels should have started a flush?
        stopFlushLatch.countDown();

        t1.join();
        t2.join();

        assert flushStopReceived.await(60, TimeUnit.SECONDS);

        assert l1.blockReceived;
        assert l1.unblockReceived;
        assert l2.blockReceived;
        assert l2.unblockReceived;
        assert l3.blockReceived;
        assert l3.unblockReceived;
    }

    protected static boolean startFlush(JChannel ch, boolean automatic_resume) {
        boolean result=Util.startFlush(ch);
        if(automatic_resume)
            ch.stopFlush();
        return result;
    }

    protected static boolean startFlush(JChannel ch, int num_attempts, long timeout, boolean automatic_resume) {
        boolean result=Util.startFlush(ch, num_attempts, 10, timeout);
        if(automatic_resume)
            ch.stopFlush();
        return result;
    }


    protected static JChannel createChannel(String name) throws Exception {
        return new JChannel(Util.getTestStack(new FLUSH())).name(name);
    }

    protected interface EventSequence {
        /** Return an event string. Events are translated as follows: get state='g', set state='s',
         *  block='b', unlock='u', view='v' */
        String getEventSequence();
        String getName();
    }

    protected static class Flusher extends Thread {
        protected final CountDownLatch startFlushLatch, stopFlushLatch;
        protected final JChannel       ch;
        protected final List<Address>  flushParticipants;

        public Flusher(CountDownLatch startFlushLatch, CountDownLatch stopFlushLatch, JChannel ch, List<Address> flushParticipants) {
            this.startFlushLatch=startFlushLatch;
            this.stopFlushLatch=stopFlushLatch;
            this.ch=ch;
            this.flushParticipants=flushParticipants;
        }

        public void run() {
            try {
                startFlushLatch.await();
                boolean rc=flushParticipants != null && !flushParticipants.isEmpty()?
                  Util.startFlush(ch, flushParticipants) : Util.startFlush(ch);
                System.out.println("Flusher " + Thread.currentThread().getId() + ": rc=" + rc);
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
                ch.stopFlush(flushParticipants);
            }
        }
    }

    protected static class Listener implements Receiver, EventSequence {
        final String name;
        boolean  blockReceived, unblockReceived;
        JChannel channel;
        CountDownLatch flushStartReceived, flushStopReceived;
        final StringBuilder events=new StringBuilder();


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
            events.append('u');
        }

        public void block() {
            blockReceived=true;
            if(flushStartReceived != null)
                flushStartReceived.countDown();
            events.append('b');
        }

        public String getEventSequence() {return events.toString();}

        public void viewAccepted(View new_view) {events.append('v');}

        public String getName() {return name;}

    }

    
}
