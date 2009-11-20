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

/**
 * Stresses the NakreceiverWindow in isolation(https://jira.jboss.org/jira/browse/JGRP-1103)
 * @author Bela Ban
 * @version $Id: NakReceiverWindowTest2.java,v 1.2 2009/11/20 15:30:43 belaban Exp $
 */
@Test(groups=Global.FUNCTIONAL, sequential=true)
public class NakReceiverWindowTest2 {
    final static int NUM_THREADS=200;
    final static int NUM_MSGS=20;

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
        win.reset();
    }


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
        barrier.await();

        for(int i=0; i < senders.length; i++)
            senders[i].join(20000);

        Set<Long> keys=successful_adds.keySet();

        System.out.println("checking for missing or duplicate seqnos in " + keys.size() + " seqnos:");
        for(int i=1; i <= NUM_MSGS; i++) {
            AtomicInteger val=successful_adds.get((long)i);
            assert val != null : i + " is missing in " + successful_adds.keySet();
            assert val.get() == 1 : i + " was not added exactly once (successful insertions=" + val.get() + ")";
        }

        System.out.println("OK: " + keys.size() + " seqnos were added exactly once");
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
            try {
                barrier.await();
            }
            catch(Exception e) {
                e.printStackTrace();
            }
            for(int i=1; i <= num; i++) {
                NakAckHeader hdr=new NakAckHeader(NakAckHeader.MSG, i);
                Message msg=new Message(null, sender, "hello");
                msg.putHeader("NAKAC", hdr);
                boolean added=win.add(i, msg);
                if(added) {
                    AtomicInteger val=map.get((long)i);
                    val.incrementAndGet();
                    System.out.println(Thread.currentThread().getId() + ": added " + i);
                }
            }
        }
    }

}
