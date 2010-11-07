package org.jgroups.tests;


import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests contention in TP.Bundler
 * @author Bela Ban
 * @version $Id: BundlerTest.java,v 1.6 2010/02/10 17:28:03 belaban Exp $
 */
@Test(groups=Global.STACK_INDEPENDENT,sequential=true)
public class BundlerTest {
    static final String props="SHARED_LOOPBACK(thread_pool.queue_max_size=5000;" +
            "thread_pool.rejection_policy=run;thread_pool.min_threads=20;thread_pool.max_threads=20;" +
            "oob_thread_pool.rejection_policy=run;enable_bundling=true)";
    static final int NUM_THREADS=200;
    static final int NUM_MSGS=1000;
    static final int SIZE=1000; // default size of a message in bytes



    public static void testSimpleMessageReception() throws Exception {
        JChannel c1=new JChannel(props);
        JChannel c2=new JChannel(props);
        MyReceiver r1=new MyReceiver("c1"), r2=new MyReceiver("c2");
        c1.setReceiver(r1);
        c2.setReceiver(r2);
        c1.connect("testSimpleMessageReception");
        c2.connect("testSimpleMessageReception");

        int NUM=100;
        for(int i=1; i <= NUM; i++) {
            c1.send(null, null, "bla");
            c2.send(null, null, "bla");
        }

        for(int i=0; i < 10; i++) {
            if(r1.getNum() == NUM * 2 && r2.getNum() == NUM * 2)
                break;
            Util.sleep(500);
        }

        System.out.println("c1 received " + r1.getNum() + " msgs");
        System.out.println("c2 received " + r2.getNum() + " msgs");

        Util.close(c2, c1);

        assert r1.getNum() == NUM * 2: "expected " + NUM *2 + ", but got " + r1.getNum();
        assert r2.getNum() == NUM * 2: "expected " + NUM *2 + ", but got " + r2.getNum();
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

        System.out.println("starting to send messages");
        MySender[] c1_senders=new MySender[NUM_THREADS];
        for(int i=0; i < c1_senders.length; i++) {
            c1_senders[i]=new MySender(c1, latch);
            c1_senders[i].start();
        }
        MySender[] c2_senders=new MySender[NUM_THREADS];
        for(int i=0; i < c2_senders.length; i++) {
            c2_senders[i]=new MySender(c2, latch);
            c2_senders[i].start();
        }

        Util.sleep(500);
        long start=System.currentTimeMillis();
        latch.countDown(); // starts all threads

        long NUM_EXPECTED_MSGS=NUM_THREADS * NUM_MSGS * 2;

        for(int i=0; i < 1000; i++) {
            if(r1.getNum() >= NUM_EXPECTED_MSGS && r2.getNum() >= NUM_EXPECTED_MSGS)
                break;
            Util.sleep(2000);
        }

        System.out.println("c1 received " + r1.getNum() + " msgs");
        System.out.println("c2 received " + r2.getNum() + " msgs");

        long diff=System.currentTimeMillis() - start;
        Util.close(c2, c1);

        assert r1.getNum() == NUM_EXPECTED_MSGS : "expected " + NUM_EXPECTED_MSGS + ", but got " + r1.getNum();
        assert r2.getNum() == NUM_EXPECTED_MSGS : "expected " + NUM_EXPECTED_MSGS + ", but got " + r2.getNum();
        System.out.println("sending and receiving of " + NUM_EXPECTED_MSGS + " took " + diff + " ms");
    }




    private static class MySender extends Thread {
        private final JChannel ch;
        private final CountDownLatch latch;
        private final byte[] buf=new byte[SIZE];

        public MySender(JChannel ch, CountDownLatch latch) {
            this.ch=ch;
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
                    Message msg=new Message(null, null, buf);
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
            int count=num.getAndIncrement();
            if(count > 0 && count % MOD == 0) {
                System.out.println("[" + name + "] received " + count + " msgs");
            }
        }

        public int getNum() {
            return num.get();
        }
    }




}