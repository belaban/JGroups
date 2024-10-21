package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.NakAckHeader2;
import org.jgroups.stack.Protocol;
import org.jgroups.util.*;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Tests time for N threads to deliver M messages to NAKACK
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class NAKACK_StressTest {
    final AtomicInteger               received=new AtomicInteger();
    final AtomicLong                  seqno=new AtomicLong(1);
    final ConcurrentLinkedQueue<Long> delivered_msg_list=new ConcurrentLinkedQueue<>();
    final static Address A=Util.createRandomAddress("A");
    final static Address B=Util.createRandomAddress("B");

    static final int   NUM_MSGS=1_000_000;
    static final int   NUM_THREADS=50;
    static final short NAK2=ClassConfigurator.getProtocolId(NAKACK2.class),
                       NAK4=ClassConfigurator.getProtocolId(NAKACK4.class);


    public void testStressNak2() {
        start(new NAKACK2(), false);
    }

    public void testStressOOBNak2() {
        start(new NAKACK2(), true);
    }

    public void testStressNak4() {
        start(new NAKACK4().capacity(100_000), false);
    }

    public void testStressOOBNak4() {
        start(new NAKACK4().capacity(100000), true);
    }

    protected Object handleMessage(Message msg) {
        long seq=getSeqno(msg);
        if(seq >= 0) {
            received.incrementAndGet();
            delivered_msg_list.add(seq);
        }
        return null;
    }

    protected void start(final Protocol prot, boolean oob) {
        seqno.set(1);
        delivered_msg_list.clear();
        received.set(0);

        prot.setDownProtocol(new MockTransport());

        prot.setUpProtocol(new Protocol() {
            public Object up(Message msg) {
                return handleMessage(msg);
            }

            public void up(MessageBatch batch) {
                for(Message msg: batch) {
                    handleMessage(msg);
                }
            }
        });

        for(Protocol p=prot; p != null; p=p.getDownProtocol())
            p.setAddress(A);
        prot.down(new Event(Event.BECOME_SERVER));
        View view=View.create(A, 1, A, B);
        prot.down(new Event(Event.VIEW_CHANGE, view));

        MutableDigest digest=new MutableDigest(view.getMembersRaw());
        digest.set(A, 0, 0);
        digest.set(B, 0, 0);
        prot.down(new Event(Event.SET_DIGEST, digest));

        final CountDownLatch latch=new CountDownLatch(1);
        Sender[] adders=new Sender[NUM_THREADS];
        for(int i=0; i < adders.length; i++) {
            adders[i]=new Sender(prot, latch, oob, B);
            adders[i].start();
        }

        long start=System.currentTimeMillis();
        latch.countDown(); // starts all adders

        for(int i=0; i < 30; i++) {
            int received_msgs=received.get();
            long dropped_msgs=getDroppedMessages(prot);
            System.out.printf("-- seqno: %d | received: %d dropped: %d \n", seqno.get(), received_msgs, dropped_msgs);
            if(received_msgs + dropped_msgs >= NUM_MSGS)
                break;
            Util.sleep(1000);
        }

        long time=System.currentTimeMillis() - start;
        double requests_sec=NUM_MSGS / (time / 1000.0);
        System.out.printf("\nTime: %d ms, %.2f requests / sec\n", time, requests_sec);
        System.out.println("Delivered messages: " + delivered_msg_list.size());
        if(delivered_msg_list.size() < 100)
            System.out.println("Elements: " + delivered_msg_list);

        long dropped=getDroppedMessages(prot);
        prot.stop();

        if(dropped > 0) {
            System.out.printf("%d msgs were dropped; skipping ordering check\n", dropped);
            return;
        }

        List<Long> results=new ArrayList<>(delivered_msg_list);
        if(oob)
            Collections.sort(results);

        assert results.size() == NUM_MSGS : "expected " + NUM_MSGS + ", but got " + results.size();

        System.out.println("Checking results consistency");
        int i=1;
        for(Long num: results) {
            if(num != i) {
                assert i == num : "expected " + i + " but got " + num;
                return;
            }
            i++;
        }
        System.out.println("OK");
        delivered_msg_list.clear();
    }

    protected static long getDroppedMessages(Protocol p) {
        return p instanceof NAKACK4? ((NAKACK4)p).getNumDroppedMessages() : 0;
    }

    protected static long getSeqno(Message msg) {
        NakAckHeader2 hdr2=msg.getHeader(NAK2);
        if(hdr2 != null)
            return hdr2.getSeqno();
        NakAckHeader hdr4=msg.getHeader(NAK4);
        if(hdr4 != null)
            return hdr4.getSeqno();
        return -1;
    }

    private static Message createMessage(Class<? extends Protocol> cl, Address dest, Address src, long seqno, boolean oob) {
        Message msg=new BytesMessage(dest, "hello world").setSrc(src);
        if(cl.equals(NAKACK2.class)) {
            NakAckHeader2 hdr=NakAckHeader2.createMessageHeader(seqno);
            msg.putHeader(NAK2, hdr);
        }
        else {
            NakAckHeader hdr4=NakAckHeader.createMessageHeader(seqno);
            msg.putHeader(NAK4, hdr4);
        }
        if(oob)
            msg.setFlag(Message.Flag.OOB);
        return msg;
    }

    protected static class MockTransport extends TP {
        @Override public Object down(Event evt)   {return null;}
        @Override public Object down(Message msg) {return null;}
        @Override public AsciiString getClusterNameAscii() {return new AsciiString("cluster");}
        @Override public boolean supportsMulticasting() {return false;}

        @Override
        public void sendUnicast(PhysicalAddress dest, byte[] data, int offset, int length) throws Exception {}
        @Override public String getInfo() {return "n/a";}
        @Override protected PhysicalAddress getPhysicalAddress() {return null;}
    }

    protected class Sender extends Thread {
        final Protocol       prot;
        final CountDownLatch latch;
        final boolean        oob;
        final Address        sender;

        public Sender(Protocol prot, CountDownLatch latch, boolean oob, final Address sender) {
            this.prot=prot;
            this.latch=latch;
            this.oob=oob;
            this.sender=sender;
            setName("Adder");
        }


        public void run() {
            try {
                latch.await();
            }
            catch(InterruptedException e) {
                e.printStackTrace();
                return;
            }
            while(true) {
                long seq=seqno.getAndIncrement();
                if(seq > NUM_MSGS) {
                    seqno.decrementAndGet();
                    break;
                }
                Message msg=createMessage(prot.getClass(), null, sender, seq, oob);
                prot.up(msg);
            }
        }
    }

}