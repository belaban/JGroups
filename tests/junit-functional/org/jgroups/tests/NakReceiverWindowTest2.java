package org.jgroups.tests;

import org.testng.annotations.Test;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.AfterMethod;
import org.jgroups.Global;
import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.protocols.pbcast.NakAckHeader;
import org.jgroups.util.Util;
import org.jgroups.util.TimeScheduler;
import org.jgroups.stack.NakReceiverWindow;
import org.jgroups.stack.Retransmitter;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

/**
 * Stresses the NakreceiverWindow in isolation(https://jira.jboss.org/jira/browse/JGRP-1103)
 * @author Bela Ban
 * @version $Id: NakReceiverWindowTest2.java,v 1.8 2010/06/14 08:11:26 belaban Exp $
 */
@Test(groups=Global.FUNCTIONAL, sequential=true)
public class NakReceiverWindowTest2 {
    final static int NUM_THREADS=200;
    final static int NUM_MSGS=5000;

    static final short NAKACK_ID=100;

    final Address self=Util.createRandomAddress();
    final Address sender=Util.createRandomAddress();
    final CyclicBarrier barrier=new CyclicBarrier(NUM_THREADS +1);

    NakReceiverWindow win;

    @BeforeMethod
    void init() {
        win=new NakReceiverWindow(self, new Retransmitter.RetransmitCommand() {
            public void retransmit(long first_seqno, long last_seqno, Address sender) {
            }
        }, 0, 0, new TimeScheduler(2));
    }

    @AfterMethod
    void cleanup() {
        win.destroy();
    }


    /**
     * Has NUM_THREAD threads insert NUM_MSGS messages concurrently, checks whether messages are added only once
     * @throws BrokenBarrierException
     * @throws InterruptedException
     */
    @Test(invocationCount=10)
    public void testConcurrentInsertions() throws BrokenBarrierException, InterruptedException {
        Sender[] senders=new Sender[NUM_THREADS];
        ConcurrentMap<Long,AtomicInteger> successful_adds=new ConcurrentHashMap<Long,AtomicInteger>();
        for(int i=1; i <= NUM_MSGS; i++)
            successful_adds.put((long)i, new AtomicInteger(0));

        for(int i=0; i < senders.length; i++) {
            senders[i]=new Sender(NUM_MSGS, win, sender, barrier, successful_adds);
            senders[i].start();
        }

        Util.sleep(2000);
        System.out.println("Concurrently inserting " + NUM_MSGS + " messages with " + NUM_THREADS + " threads");
        barrier.await();

        for(int i=0; i < senders.length; i++)
            senders[i].join(20000);
        System.out.println("OK: " + NUM_MSGS + " were added to the NakReceiverWindow concurrently by " + NUM_THREADS + " threads");

        Set<Long> keys=successful_adds.keySet();

        System.out.println("checking for missing or duplicate seqnos in " + keys.size() + " seqnos:");
        for(int i=1; i <= NUM_MSGS; i++) {
            AtomicInteger val=successful_adds.get((long)i);
            if(val.get() != 1)
                System.err.println(i + " was not added exactly once (successful insertions=" + val.get() + ")");
        }
        for(int i=1; i <= NUM_MSGS; i++) {
            AtomicInteger val=successful_adds.get((long)i);
            assert val != null : i + " is missing in " + successful_adds.keySet();
            assert val.get() == 1 : i + " was not added exactly once (successful insertions=" + val.get() + ")";
        }

        System.out.println("OK: " + keys.size() + " seqnos were added exactly once");
    }


    @Test(invocationCount=5)
    public void testConcurrentRandomInsertions() throws BrokenBarrierException, InterruptedException {
        Sender[] senders=new RandomSender[NUM_THREADS];
        ConcurrentMap<Long,AtomicInteger> successful_adds=new ConcurrentHashMap<Long,AtomicInteger>();
        for(int i=1; i <= NUM_MSGS; i++)
            successful_adds.put((long)i, new AtomicInteger(0));

        for(int i=0; i < senders.length; i++) {
            senders[i]=new RandomSender(NUM_MSGS, win, sender, barrier, successful_adds);
            senders[i].start();
        }

        Util.sleep(2000);
        System.out.println("Concurrently inserting " + NUM_MSGS + " messages with " + NUM_THREADS + " threads");
        barrier.await();

        for(int i=0; i < senders.length; i++)
            senders[i].join(20000);
        System.out.println("OK: " + NUM_MSGS + " were added to the NakReceiverWindow concurrently by " + NUM_THREADS + " threads");

        Set<Long> keys=successful_adds.keySet();

        System.out.println("checking for missing or duplicate seqnos in " + keys.size() + " seqnos:");
        for(int i=1; i <= NUM_MSGS; i++) {
            AtomicInteger val=successful_adds.get((long)i);
            if(val.get() != 1)
                System.err.println(i + " was not added exactly once (successful insertions=" + val.get() + ")");
        }
        for(int i=1; i <= NUM_MSGS; i++) {
            AtomicInteger val=successful_adds.get((long)i);
            assert val != null : i + " is missing in " + successful_adds.keySet();
            assert val.get() == 1 : i + " was not added exactly once (successful insertions=" + val.get() + ")";
        }

        System.out.println("OK: " + keys.size() + " seqnos were added exactly once");
    }


    @Test(invocationCount=5)
    public void testConcurrentInsertionOfSameSeqno() throws BrokenBarrierException, InterruptedException {
        Sender[] senders=new SameSeqnoSender[NUM_THREADS];
        ConcurrentMap<Long,AtomicInteger> successful_adds=new ConcurrentHashMap<Long,AtomicInteger>();
        for(int i=1; i <= NUM_MSGS; i++)
            successful_adds.put((long)i, new AtomicInteger(0));

        for(int i=0; i < senders.length; i++) {
            senders[i]=new SameSeqnoSender(NUM_MSGS, win, sender, barrier, successful_adds);
            senders[i].start();
        }

        Util.sleep(2000);
        System.out.println("Concurrently inserting 1 message with " + NUM_THREADS + " threads");
        barrier.await();

        for(int i=0; i < senders.length; i++)
            senders[i].join(20000);
        System.out.println("OK: 1 message was added to the NakReceiverWindow concurrently by " + NUM_THREADS + " threads");

        Set<Long> keys=successful_adds.keySet();

        System.out.println("checking for missing or duplicate seqnos in " + keys.size() + " seqnos:");
        AtomicInteger val=successful_adds.get(1L);
        if(val.get() != 1)
            System.err.println("1 was not added exactly once (successful insertions=" + val.get() + ")");
        assert val.get() == 1 : "1 was not added exactly once (successful insertions=" + val.get() + ")";

        System.out.println("OK: 1 seqno was added exactly once");
    }



    static class Sender extends Thread {
        final int num;
        final NakReceiverWindow win;
        final Address sender;
        final CyclicBarrier barrier;
        final ConcurrentMap<Long,AtomicInteger> map;

        public Sender(int num, NakReceiverWindow win, Address sender, CyclicBarrier barrier, ConcurrentMap<Long, AtomicInteger> map) {
            this.num=num;
            this.win=win;
            this.sender=sender;
            this.barrier=barrier;
            this.map=map;
        }

        public void run() {
            waitForBarrier();

            for(int i=1; i <= num; i++)
                add(i);
        }

        protected void add(long seqno) {
            NakAckHeader hdr=NakAckHeader.createMessageHeader(seqno);
            Message msg=new Message(null, sender, "hello");
            msg.putHeader(NAKACK_ID, hdr);
            boolean added=win.add(seqno, msg);

            if(added) {
                AtomicInteger val=map.get((long)seqno);
                val.incrementAndGet();
            }
        }

        protected void waitForBarrier() {
            try {
                barrier.await();
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
    }

    static class RandomSender extends Sender {

        public RandomSender(int num, NakReceiverWindow win, Address sender, CyclicBarrier barrier, ConcurrentMap<Long, AtomicInteger> map) {
            super(num, win, sender, barrier, map);
        }

        public void run() {
            final List<Long> seqnos;
            seqnos=new ArrayList<Long>(num);
            for(long i=1; i <= num; i++)
                seqnos.add(i);

            // now randomize the seqnos:
            Collections.shuffle(seqnos);

            waitForBarrier();
            for(long seqno: seqnos)
                add(seqno);
        }
    }

    /**
     * Inserts seqno 1 NUM_MSGS times
     */
    static class SameSeqnoSender extends Sender {

        public SameSeqnoSender(int num, NakReceiverWindow win, Address sender, CyclicBarrier barrier, ConcurrentMap<Long, AtomicInteger> map) {
            super(num, win, sender, barrier, map);
        }

        public void run() {
            waitForBarrier();
            for(int i=1; i <= num; i++)
                add(1L);
        }
    }

}
