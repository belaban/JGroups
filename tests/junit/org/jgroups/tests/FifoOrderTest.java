package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Verifies that messages sent by P are delivered in the order in which P sent them
 * @author Bela Ban
 */
@Test(groups=Global.STACK_DEPENDENT,singleThreaded=true)
public class FifoOrderTest extends ChannelTestBase {    
    JChannel       a, b, c;
    CountDownLatch latch;
    final static int NUM=25, EXPECTED=NUM * 3;
    final static long SLEEPTIME=50;


    @BeforeMethod void setUp() throws Exception {
        latch=new CountDownLatch(1);
        a=createChannel(true,3, "A");
        b=createChannel(a,    "B");
        c=createChannel(a,    "C");
    }

    @AfterMethod protected void tearDown() throws Exception {Util.close(c,b,a);}


    public void testFifoDelivery() throws Exception {
        long start, stop, diff;

        MyReceiver r1=new MyReceiver("R1"), r2=new MyReceiver("R2"), r3=new MyReceiver("R3");
        a.setReceiver(r1); b.setReceiver(r2); c.setReceiver(r3);

        a.connect("FifoOrderTest");
        b.connect("FifoOrderTest");
        c.connect("FifoOrderTest");
        Util.waitUntilAllChannelsHaveSameView(10000, 1000, a, b, c);

        new Thread(new Sender(a)) {}.start();
        new Thread(new Sender(b)) {}.start();
        new Thread(new Sender(c)) {}.start();
        Util.sleep(500);
        latch.countDown(); // start senders

        start=System.currentTimeMillis();
        for(int i=0; i < 60; i++) {
            System.out.println("r1: " + r1.size() + ", r2: " + r2.size() + ", r3: " + r3.size());
            if(r1.size() == EXPECTED && r2.size() == EXPECTED && r3.size() == EXPECTED)
                break;
            Util.sleep(500);
        }
        stop=System.currentTimeMillis();
        diff=stop - start;

        System.out.println("Total time: " + diff + " ms\n");
        assert r1.size() == EXPECTED;
        assert r2.size() == EXPECTED;
        assert r3.size() == EXPECTED;

        checkFIFO(r1);
        checkFIFO(r2);
        checkFIFO(r3);
    }

    private static void checkFIFO(MyReceiver r) {
        Map<Address,List<Integer>> map=r.getMessages();

        boolean fifo=true;
        List<Address> incorrect_receivers=new LinkedList<>();
        System.out.println("Checking FIFO for " + r.getName() + ":");
        for(Map.Entry<Address,List<Integer>> addressListEntry : map.entrySet()) {
            List<Integer> list=addressListEntry.getValue();
            print(addressListEntry.getKey(), list);
            if(!verifyFIFO(list)) {
                fifo=false;
                incorrect_receivers.add(addressListEntry.getKey());
            }
        }
        System.out.print("\n");

        if(!fifo)
            assert false : "the following receivers didn't receive all messages in FIFO order: " + incorrect_receivers;
    }


    private static boolean verifyFIFO(List<Integer> list) {
        List<Integer> list2=new LinkedList<>(list);
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



    private class Sender implements Runnable {
        final JChannel ch;
        final Address local_addr;

        public Sender(JChannel ch) {
            this.ch=ch;
            local_addr=ch.getAddress();
        }

        public void run() {
            Message msg;
            try {
                latch.await();
            }
            catch(Throwable t) {
                return;
            }

            for(int i=1; i <= NUM; i++) {
                msg=new BytesMessage(null, i);
                try {                    
                    ch.send(msg);
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }


    protected static class MyReceiver extends ReceiverAdapter {
        final String name;
        final ConcurrentMap<Address,List<Integer>> msgs=new ConcurrentHashMap<>();
        AtomicInteger count=new AtomicInteger(0);

        public MyReceiver(String name) {
            this.name=name;
        }

        public void receive(Message msg) {
            Util.sleep(SLEEPTIME);

            Address sender=msg.getSrc();
            List<Integer> list=msgs.get(sender);
            if(list == null) {
                list=new LinkedList<>();
                List<Integer> tmp=msgs.putIfAbsent(sender, list);
                if(tmp != null)
                    list=tmp;
            }
            Integer num=msg.getObject();
            list.add(num); // no concurrent access: FIFO per sender ! (No need to synchronize on list)
            count.incrementAndGet();
        }

        public ConcurrentMap<Address,List<Integer>> getMessages() {return msgs;}
        public String getName() {
            return name;
        }
        public int size() {return count.get();}
    }


}
