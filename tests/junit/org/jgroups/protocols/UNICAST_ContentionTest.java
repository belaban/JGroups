package org.jgroups.protocols;


import junit.framework.TestCase;
import org.jgroups.*;
import org.jgroups.util.Util;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests for contention on UNICAST, measured by the number of retransmissions in UNICAST 
 * @author Bela Ban
 * @version $Id: UNICAST_ContentionTest.java,v 1.1.2.2 2009/09/14 08:25:30 belaban Exp $
 */
public class UNICAST_ContentionTest extends TestCase {
    JChannel c1, c2;
    static final String props="SHARED_LOOPBACK:UNICAST";
    static final int NUM_THREADS=200;
    static final int NUM_MSGS=100;
    static final int SIZE=1000; // default size of a message in bytes

    protected void tearDown() throws Exception {
        Util.close(c2, c1);
        super.tearDown();
    }


    public static void testSimpleMessageReception() throws Exception {
        JChannel c1=new JChannel(props);
        JChannel c2=new JChannel(props);
        MyReceiver r1=new MyReceiver("c1"), r2=new MyReceiver("c2");
        c1.setReceiver(r1);
        c2.setReceiver(r2);
        c1.connect("testSimpleMessageReception");
        c2.connect("testSimpleMessageReception");

        int NUM=100;
        Address c1_addr=c1.getLocalAddress(), c2_addr=c2.getLocalAddress();
        for(int i=1; i <= NUM; i++) {
            c1.send(c1_addr, null, "bla");
            c1.send(c2_addr, null, "bla");
            c2.send(c2_addr, null, "bla");
            c2.send(c1_addr, null, "bla");
        }

        for(int i=0; i < 10; i++) {
            if(r1.getNum() == NUM * 2 && r2.getNum() == NUM * 2)
                break;
            Util.sleep(500);
        }

        System.out.println("c1 received " + r1.getNum() + " msgs, " + getNumberOfRetransmissions(c1) + " retransmissions");
        System.out.println("c2 received " + r2.getNum() + " msgs, " + getNumberOfRetransmissions(c2) + " retransmissions");

        assertEquals("expected " + NUM *2 + ", but got " + r1.getNum(), NUM * 2, r1.getNum());
        assertEquals("expected " + NUM *2 + ", but got " + r2.getNum(), NUM * 2, r2.getNum());
    }


    /**
     * Multiple threads (NUM_THREADS) send messages (NUM_MSGS)
     * @throws Exception
     */
    public static void testMessageReceptionUnderHighLoad() throws Exception {
        CountDownLatch latch=new CountDownLatch(1);
        JChannel c1=new JChannel(props);
        JChannel c2=new JChannel(props);
        MyReceiver r1=new MyReceiver("c1"), r2=new MyReceiver("c2");
        c1.setReceiver(r1);
        c2.setReceiver(r2);
        c1.connect("testSimpleMessageReception");
        c2.connect("testSimpleMessageReception");

        Address c1_addr=c1.getLocalAddress(), c2_addr=c2.getLocalAddress();
        MySender[] c1_senders=new MySender[NUM_THREADS];
        for(int i=0; i < c1_senders.length; i++) {
            c1_senders[i]=new MySender(c1, c2_addr, latch);
            c1_senders[i].start();
        }
        MySender[] c2_senders=new MySender[NUM_THREADS];
        for(int i=0; i < c2_senders.length; i++) {
            c2_senders[i]=new MySender(c2, c1_addr, latch);
            c2_senders[i].start();
        }

        Util.sleep(500);
        latch.countDown(); // starts all threads

        long NUM_EXPECTED_MSGS=NUM_THREADS * NUM_MSGS;

        for(int i=0; i < 100; i++) {
            if(r1.getNum() == NUM_EXPECTED_MSGS && r2.getNum() == NUM_EXPECTED_MSGS)
                break;
            Util.sleep(500);
        }

        System.out.println("c1 received " + r1.getNum() + " msgs, " + getNumberOfRetransmissions(c1) + " retransmissions");
        System.out.println("c2 received " + r2.getNum() + " msgs, " + getNumberOfRetransmissions(c2) + " retransmissions");

        assertEquals("expected " + NUM_EXPECTED_MSGS + ", but got " + r1.getNum(), NUM_EXPECTED_MSGS, r1.getNum());
        assertEquals("expected " + NUM_EXPECTED_MSGS + ", but got " + r2.getNum(), NUM_EXPECTED_MSGS, r2.getNum()); 
    }




    private static long getNumberOfRetransmissions(JChannel ch) {
        UNICAST unicast=(UNICAST)ch.getProtocolStack().findProtocol(UNICAST.class);
        return unicast.getNumberOfRetransmissions();
    }



    private static class MySender extends Thread {
        private final JChannel ch;
        private final Address dest;
        private final CountDownLatch latch;
        private final byte[] buf=new byte[SIZE];

        public MySender(JChannel ch, Address dest, CountDownLatch latch) {
            this.ch=ch;
            this.dest=dest;
            this.latch=latch;
        }


        public void run() {
            try {
                latch.await();
            }
            catch(InterruptedException e) {
                e.printStackTrace();
            }
            for(int i=0; i < NUM_MSGS; i++) {
                try {
                    ch.send(dest, null, buf);
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }



    private static class MyReceiver extends ReceiverAdapter {
        final String name;
        final AtomicInteger num=new AtomicInteger(0);
        static final long MOD=NUM_MSGS * NUM_THREADS / 10;

        public MyReceiver(String name) {
            this.name=name;
        }

        public void receive(Message msg) {
            if(num.incrementAndGet() % MOD == 0) {
                System.out.println("[" + name + "] received " + getNum() + " msgs");
            }
        }

        public int getNum() {
            return num.get();
        }
    }




}