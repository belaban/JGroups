package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.TP;
import org.jgroups.util.Util;

import java.util.*;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests the concurrent stack
 * @author Bela Ban
 * @version $Id: ConcurrentStackTest.java,v 1.1.4.2 2008/05/26 11:55:07 belaban Exp $
 */
public class ConcurrentStackTest extends ChannelTestBase {    
    JChannel ch1, ch2, ch3;
    final static int NUM=10, EXPECTED=NUM * 3;
    final static long SLEEPTIME=500;
    CyclicBarrier barrier;

    public ConcurrentStackTest(String name) {
        super(name);
    }

    public void setUp() throws Exception {
        super.setUp();
        barrier=new CyclicBarrier(4);
        ch1=createChannel();
        ch2=createChannel();
        ch3=createChannel();
    }

    protected void tearDown() throws Exception {
        Util.close(ch3, ch2, ch1);
        barrier.reset();
        super.tearDown();
    }



    public void testSequentialDelivery() throws Exception {
        doIt(false);
    }

    public void testConcurrentDelivery() throws Exception {
        doIt(true);
    }


    private void doIt(boolean use_concurrent_stack) throws Exception {
        long start, stop, diff;
        setConcurrentStack(ch1, use_concurrent_stack);
        setConcurrentStack(ch2, use_concurrent_stack);
        setConcurrentStack(ch3, use_concurrent_stack);

        MyReceiver r1=new MyReceiver("R1"), r2=new MyReceiver("R2"), r3=new MyReceiver("R3");
        ch1.setReceiver(r1); ch2.setReceiver(r2); ch3.setReceiver(r3);

        ch1.connect("test");
        ch2.connect("test");
        ch3.connect("test");
        View v=ch3.getView();
        assertEquals(3, v.size());

        new Thread(new Sender(ch1)) {}.start();
        new Thread(new Sender(ch2)) {}.start();
        new Thread(new Sender(ch3)) {}.start();
        barrier.await(); // start senders
        start=System.currentTimeMillis();

        Exception ex=null;

        try {
            barrier.await((long)(EXPECTED * SLEEPTIME * 1.3), TimeUnit.MILLISECONDS); // wait for all receivers
        }
        catch(java.util.concurrent.TimeoutException e) {
            ex=e;
        }

        stop=System.currentTimeMillis();
        diff=stop - start;

        System.out.println("Total time: " + diff + " ms\n");

        checkFIFO(r1);
        checkFIFO(r2);
        checkFIFO(r3);

        if(ex != null)
            throw ex;
    }

    private void checkFIFO(MyReceiver r) {
        List<Pair<Address,Integer>> msgs=r.getMessages();
        Map<Address,List<Integer>> map=new HashMap();
        for(Pair<Address,Integer> p: msgs) {
            Address sender=p.key;
            List<Integer> list=map.get(sender);
            if(list == null) {
                list=new LinkedList();
                map.put(sender, list);
            }
            list.add(p.val);
        }

        boolean fifo=true;
        List<Address> incorrect_receivers=new LinkedList();
        System.out.println("Checking FIFO for " + r.getName() + ":");
        for(Address addr: map.keySet()) {
            List<Integer> list=map.get(addr);
            print(addr, list);
            if(!verifyFIFO(list)) {
                fifo=false;
                incorrect_receivers.add(addr);
            }
        }
        System.out.print("\n");

        if(!fifo)
            fail("The following receivers didn't receive all messages in FIFO order: " + incorrect_receivers);
    }


    private static boolean verifyFIFO(List<Integer> list) {
        List<Integer> list2=new LinkedList(list);
        Collections.sort(list2);
        return list.equals(list2);
    }

    private static void print(Address addr, List<Integer> list) {
        StringBuilder sb=new StringBuilder();
        sb.append(addr).append(": ");
        for(Integer i: list)
            sb.append(i).append(" ");
        System.out.println(sb);
    }




    private static void setConcurrentStack(JChannel ch1, boolean use_concurrent_stack) {
        TP tp=ch1.getProtocolStack().getTransport();
        if(tp == null)
            throw new IllegalStateException("Transport not found");
        Properties p=new Properties();
        p.setProperty("use_concurrent_stack", String.valueOf(use_concurrent_stack));
        p.setProperty("thread_pool.min_threads", "1");
        p.setProperty("thread_pool.max_threads", "100");
        p.setProperty("thread_pool.queue_enabled", "false");
        // p.setProperty("loopback", "true");
        tp.setProperties(p);
    }


    private class Sender implements Runnable {
        Channel ch;
        Address local_addr;

        public Sender(Channel ch) {
            this.ch=ch;
            local_addr=ch.getLocalAddress();
        }

        public void run() {
            Message msg;
            try {
                barrier.await();
            }
            catch(Throwable t) {
                return;
            }

            for(int i=1; i <= NUM; i++) {
                msg=new Message(null, null, new Integer(i));
                try {
                    // System.out.println(local_addr + ": sending " + i);
                    ch.send(msg);
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }


    private class Pair<K,V> {
        K key;
        V val;

        public Pair(K key, V val) {
            this.key=key;
            this.val=val;
        }

        public String toString() {
            return key + "::" + val;
        }
    }

    private class MyReceiver extends ReceiverAdapter {
        String name;
        final List<Pair<Address,Integer>> msgs=new LinkedList();
        AtomicInteger count=new AtomicInteger(0);

        public MyReceiver(String name) {
            this.name=name;
        }

        public void receive(Message msg) {
            Util.sleep(SLEEPTIME);
            Pair pair=new Pair<Address,Integer>(msg.getSrc(), (Integer)msg.getObject());
            // System.out.println(name + ": received " + pair);
            synchronized(msgs) {
                msgs.add(pair);
            }
            if(count.incrementAndGet() >= EXPECTED) {
                try {
                    barrier.await();
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            }
        }

        public List getMessages() {return msgs;}

        public String getName() {
            return name;
        }
    }


    public static void main(String[] args) {
        String[] testCaseName={ConcurrentStackTest.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }

}
