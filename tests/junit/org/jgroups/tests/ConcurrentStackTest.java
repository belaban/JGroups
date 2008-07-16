package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.TP;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.*;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests the TLS
 * @author Bela Ban
 * @version $Id: ConcurrentStackTest.java,v 1.10 2008/07/16 14:08:40 vlada Exp $
 */
@Test(groups="temp",sequential=true)
public class ConcurrentStackTest extends ChannelTestBase {    
    JChannel ch1, ch2, ch3;
    final static int NUM=25, EXPECTED=NUM * 3;
    final static long SLEEPTIME=100;
    CyclicBarrier barrier;


    @BeforeMethod
    void setUp() throws Exception {
        barrier=new CyclicBarrier(4);
        ch1=createChannel(true,3);
        ch2=createChannel(ch1);
        ch3=createChannel(ch1);
    }

    @AfterMethod
    protected void tearDown() throws Exception {
        Util.close(ch3, ch2, ch1);
        barrier.reset();
    }



    @Test
    public void testSequentialDelivery() throws Exception {
        doIt(false);
    }

    @Test
    public void testConcurrentDelivery() throws Exception {
        doIt(true);
    }


    private void doIt(boolean concurrent) throws Exception {
        long start, stop, diff;
        setConcurrent(ch1, concurrent);
        setConcurrent(ch2, concurrent);
        setConcurrent(ch3, concurrent);

        MyReceiver r1=new MyReceiver("R1"), r2=new MyReceiver("R2"), r3=new MyReceiver("R3");
        ch1.setReceiver(r1); ch2.setReceiver(r2); ch3.setReceiver(r3);

        ch1.connect("ConcurrentStackTest");
        ch2.connect("ConcurrentStackTest");
        ch3.connect("ConcurrentStackTest");
        View v=ch3.getView();
        assert v.size() == 3 : "view is " + v;

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
        /*
         * TODO Vladimir Feb 15th 2007
         * Re-enable once acceptable bounds are known for all stacks
         * checkTime(diff, threadless);
         * 
         * */

        if(ex != null)
            throw ex;
    }

    private void checkFIFO(MyReceiver r) {
        List<Pair<Address,Integer>> msgs=r.getMessages();
        Map<Address,List<Integer>> map=new HashMap<Address,List<Integer>>();
        for(Pair<Address,Integer> p: msgs) {
            Address sender=p.key;
            List<Integer> list=map.get(sender);
            if(list == null) {
                list=new LinkedList<Integer>();
                map.put(sender, list);
            }
            list.add(p.val);
        }

        boolean fifo=true;
        List<Address> incorrect_receivers=new LinkedList<Address>();
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
            assert false : "The following receivers didn't receive all messages in FIFO order: " + incorrect_receivers;
    }


    private static boolean verifyFIFO(List<Integer> list) {
        List<Integer> list2=new LinkedList<Integer>(list);
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



    private static void setConcurrent(JChannel ch1, boolean concurrent) {
        TP transport=ch1.getProtocolStack().getTransport();
        transport.setUseConcurrentStack(concurrent);
        ThreadPoolExecutor default_pool=(ThreadPoolExecutor)transport.getDefaultThreadPool();
        if(default_pool != null) {
            default_pool.setCorePoolSize(1);
            default_pool.setMaximumPoolSize(100);
        }
        transport.setThreadPoolQueueEnabled(false);
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
        final List<Pair<Address,Integer>> msgs=new LinkedList<Pair<Address,Integer>>();
        AtomicInteger count=new AtomicInteger(0);

        public MyReceiver(String name) {
            this.name=name;
        }

        public void receive(Message msg) {
            Util.sleep(SLEEPTIME);
            Pair<Address,Integer> pair=new Pair<Address,Integer>(msg.getSrc(), (Integer)msg.getObject());           
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

        public List<Pair<Address,Integer>> getMessages() {return msgs;}

        public String getName() {
            return name;
        }
    }


}
