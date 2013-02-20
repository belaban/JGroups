package org.jgroups.protocols;


import org.jgroups.*;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests for contention on UNICAST, measured by the number of retransmissions in UNICAST 
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL, sequential=true)
public class UNICAST_ContentionTest {
    JChannel c1, c2;
    static final String unicast_props="SHARED_LOOPBACK(thread_pool.queue_max_size=5000;" +
            "thread_pool.rejection_policy=discard;thread_pool.min_threads=20;thread_pool.max_threads=20;" +
            "oob_thread_pool.rejection_policy=discard)"+
            ":UNICAST(xmit_interval=500)";
    static final String unicast2_props=unicast_props.replace("UNICAST", "UNICAST2");
    static final String unicast3_props=unicast_props.replace("UNICAST", "UNICAST3");
    static final int NUM_THREADS=100;
    static final int NUM_MSGS=100;
    static final int SIZE=1000; // default size of a message in bytes

    @AfterMethod
    protected void tearDown() throws Exception {
        Util.close(c2, c1);
    }

    @DataProvider
    static Object[][] provider() {
        return new Object[][] {
          {unicast_props},
          {unicast2_props},
          {unicast3_props}
        };
    }


    @Test(dataProvider="provider")
    public void testSimpleMessageReception(String props) throws Exception {
        c1=new JChannel(props); c1.setName("A");
        c2=new JChannel(props); c2.setName("B");
        MyReceiver r1=new MyReceiver("c1"), r2=new MyReceiver("c2");
        c1.setReceiver(r1);
        c2.setReceiver(r2);
        c1.connect("testSimpleMessageReception");
        c2.connect("testSimpleMessageReception");

        int NUM=100;
        Address c1_addr=c1.getAddress(), c2_addr=c2.getAddress();
        for(int i=1; i <= NUM; i++) {
            c1.send(c1_addr, "bla");
            c1.send(c2_addr, "bla");
            c2.send(c2_addr, "bla");
            c2.send(c1_addr, "bla");
        }

        for(int i=0; i < 10; i++) {
            if(r1.getNum() == NUM * 2 && r2.getNum() == NUM * 2)
                break;
            Util.sleep(500);
        }

        System.out.println("c1 received " + r1.getNum() + " msgs, " + getNumberOfRetransmissions(c1) + " retransmissions");
        System.out.println("c2 received " + r2.getNum() + " msgs, " + getNumberOfRetransmissions(c2) + " retransmissions");

        assert r1.getNum() == NUM * 2: "expected " + NUM *2 + ", but got " + r1.getNum();
        assert r2.getNum() == NUM * 2: "expected " + NUM *2 + ", but got " + r2.getNum();
    }


    /**
     * Multiple threads (NUM_THREADS) send messages (NUM_MSGS)
     * @throws Exception
     */
    @Test(dataProvider="provider")
    public void testMessageReceptionUnderHighLoad(String props) throws Exception {
        CountDownLatch latch=new CountDownLatch(1);
        c1=new JChannel(props); c1.setName("A");
        c2=new JChannel(props); c2.setName("B");
        MyReceiver r1=new MyReceiver("c1"), r2=new MyReceiver("c2");
        c1.setReceiver(r1);
        c2.setReceiver(r2);
        c1.connect("testSimpleMessageReception");
        c2.connect("testSimpleMessageReception");

        Address c1_addr=c1.getAddress(), c2_addr=c2.getAddress();
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

        latch.countDown(); // starts all threads

        for(MySender sender: c1_senders)
            sender.join();
        for(MySender sender: c2_senders)
            sender.join();
        System.out.println("Senders are done, waiting for all messages to be received");

        long NUM_EXPECTED_MSGS=NUM_THREADS * NUM_MSGS;

        for(int i=0; i < 20; i++) {
            if(r1.getNum() == NUM_EXPECTED_MSGS && r2.getNum() == NUM_EXPECTED_MSGS)
                break;
            Util.sleep(2000);
            UNICAST2 unicast2=(UNICAST2)c1.getProtocolStack().findProtocol(UNICAST2.class);
            if(unicast2 != null)
                unicast2.sendStableMessages();
            unicast2=(UNICAST2)c2.getProtocolStack().findProtocol(UNICAST2.class);
            if(unicast2 != null)
                unicast2.sendStableMessages();
        }

        System.out.println("c1 received " + r1.getNum() + " msgs, " + getNumberOfRetransmissions(c1) + " retransmissions");
        System.out.println("c2 received " + r2.getNum() + " msgs, " + getNumberOfRetransmissions(c2) + " retransmissions");

        assert r1.getNum() == NUM_EXPECTED_MSGS : "expected " + NUM_EXPECTED_MSGS + ", but got " + r1.getNum();
        assert r2.getNum() == NUM_EXPECTED_MSGS : "expected " + NUM_EXPECTED_MSGS + ", but got " + r2.getNum();
    }



    private static long getNumberOfRetransmissions(JChannel ch) {
        Protocol prot=ch.getProtocolStack().findProtocol(Util.getUnicastProtocols());
        if(prot instanceof UNICAST)
            return ((UNICAST)prot).getNumXmits();
        return -1;
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
                    Message msg=new Message(dest, null, buf);
                    ch.send(msg);
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