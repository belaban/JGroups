package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests message ordering
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL,sequential=true)
public class OrderingTest {
    protected static final int NUM_MSGS=200000;
    protected static final int NUM_SENDERS=2;
    protected static final int TOTAL_NUM_MSGS=NUM_MSGS * NUM_SENDERS;

    protected JChannel[] channels=new JChannel[NUM_SENDERS];
    protected MySender[] senders=new MySender[NUM_SENDERS];


    @BeforeMethod
    void init() throws Exception {
        System.out.println("creating " + NUM_SENDERS + " channels");
        for(int i=0; i < channels.length; i++) {
            channels[i]=createChannel();
            channels[i].setReceiver(new MyReceiver());
            senders[i]=new MySender(channels[i]);
            channels[i].connect("OrderingTest.testFIFOOrder");
        }
        System.out.println("done");

        System.out.println("\nwaiting for a cluster of " + NUM_SENDERS + " to form:");
        boolean done=true;
        for(int i=0; i < 20; i++) {
            for(JChannel ch: channels) {
                if(ch.getView().size() != NUM_SENDERS) {
                    done=false;
                    break;
                }
            }
            if(!done)
                Util.sleep(1000);
            else
                break;
        }
    }

    @AfterMethod
    void destroy() {
        for(int i=channels.length-1; i >= 0; i--) {
            Util.close(channels[i]);
        }
    }


    protected static JChannel createChannel() throws Exception {
        JChannel ch=new JChannel(false);
        ProtocolStack stack=new ProtocolStack();
        ch.setProtocolStack(stack);
        stack.addProtocol(new SHARED_LOOPBACK().setValue("oob_thread_pool_rejection_policy", "run")
                            .setValue("thread_pool_rejection_policy", "run")
                            .setValue("thread_pool_queue_max_size", 100000))
          .addProtocol(new PING())
          .addProtocol(new MERGE2())
          .addProtocol(new FD_SOCK())
          .addProtocol(new VERIFY_SUSPECT())
          .addProtocol(new BARRIER())
          .addProtocol(new NAKACK().setValue("use_mcast_xmit", false).setValue("discard_delivered_msgs", true))
          .addProtocol(new UNICAST2().setValue("stable_interval", 10000).setValue("max_bytes", 50000))
          .addProtocol(new STABLE().setValue("max_bytes", 50000))
          .addProtocol(new GMS().setValue("print_local_addr", false))
          .addProtocol(new UFC().setValue("max_credits", 2000000))
          .addProtocol(new MFC().setValue("max_credits", 2000000))
          .addProtocol(new FRAG2());
        stack.init();
        return ch;
    }

    /*protected static JChannel createChannel() throws Exception {
        return new JChannel("/home/bela/fast.xml");
    }*/



    public void testFIFOOrdering() throws Exception {
        assert channels[0].getView().size() == NUM_SENDERS : "view[0] is " + channels[0].getView().size();
        System.out.println("done, view is " + channels[0].getView());

        System.out.println("\nstarting to send " + NUM_MSGS + " messages");
        for(int i=0; i < senders.length; i++)
            senders[i].start();
        for(int i=0; i < senders.length; i++) {
            MySender sender=senders[i];
            sender.join();
        }
        System.out.println("senders done");

        System.out.println("\nwaiting for message reception by all receivers:");
        boolean done;
        for(int i=0; i < 50; i++) {
            done=true;
            for(JChannel ch: channels) {
                MyReceiver receiver=(MyReceiver)ch.getReceiver();
                int received=receiver.getReceived();
                System.out.println(ch.getAddress() + ": " + received);
                STABLE stable=(STABLE)ch.getProtocolStack().findProtocol(STABLE.class);
                stable.runMessageGarbageCollection(); 
                if(received != TOTAL_NUM_MSGS) {
                    done=false;
                    break;
                }
            }
            if(!done)
                Util.sleep(1000);
            else
                break;
        }
        for(JChannel ch: channels) {
            MyReceiver receiver=(MyReceiver)ch.getReceiver();
            System.out.println(ch.getAddress() + ": " + receiver.getReceived());
        }

        for(JChannel ch: channels) {
            MyReceiver receiver=(MyReceiver)ch.getReceiver();
            assert receiver.getReceived() == TOTAL_NUM_MSGS : "receiver had " + receiver.getReceived() +
              " messages (expected=" + TOTAL_NUM_MSGS + ")";
        }
        System.out.println("done");

        System.out.println("\nchecking message order");

        for(JChannel ch: channels) {
            MyReceiver receiver=(MyReceiver)ch.getReceiver();
            System.out.print(ch.getAddress() + ": ");
            boolean ok=receiver.getNumberOfErrors() == 0;
            System.out.println(ok? "OK" : "FAIL (" + receiver.getNumberOfErrors() + " errors)");
            assert ok : receiver.getNumberOfErrors() + " errors";
        }

        System.out.println("done");
    }


   /* private static boolean checkOrder(ConcurrentMap<Address,List<Integer>> map, boolean print_incorrect_elements) {
        boolean retval=true;
        for(Map.Entry<Address,List<Integer>> entry: map.entrySet()) {
            Address sender=entry.getKey();
            List<Integer> list=entry.getValue();
            int curr=1;
            for(Integer num: list) {
                if(!num.equals(curr)) {
                    retval=false;
                    if(!print_incorrect_elements)
                        return false;
                    System.err.println("element " + num + " != " + curr);
                }
                curr++;
            }
        }

        return retval;
    }*/


    protected static class MySender extends Thread {
        protected final JChannel ch;

        public MySender(JChannel ch) {
            this.ch=ch;
        }

        public void run() {
            for(int i=1; i <= NUM_MSGS; i++) {
                try {
                    Message msg=new Message(null, null, new Integer(i));
                    ch.send(msg);
                    if(i % 100000 == 0)
                        System.out.println(Thread.currentThread().getId() + ": " + i + " sent");
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    protected static class MyReceiver extends ReceiverAdapter {
        protected final ConcurrentMap<Address,Integer> map=new ConcurrentHashMap<Address,Integer>();
        final AtomicInteger received=new AtomicInteger(0);
        protected int num_errors=0;

        public int getNumberOfErrors() {
            return num_errors;
        }

        public int getReceived() {
            return received.intValue();
        }

        public void receive(Message msg) {
            Integer num=(Integer)msg.getObject();
            Address sender=msg.getSrc();

            Integer current_seqno=map.get(sender);
            if(current_seqno == null) {
                current_seqno=new Integer(1);
                Integer tmp=map.putIfAbsent(sender, current_seqno);
                if(tmp != null)
                    current_seqno=tmp;
            }

            if(current_seqno.intValue() == num)
                map.put(sender, current_seqno + 1);
            else
                num_errors++;

            if(received.incrementAndGet() % 100000 == 0)
                System.out.println("received " + received);
        }
    }


    /*public static void main(String[] args) throws Exception {
        OrderingTest test=new OrderingTest();
        test.init();
        test.testFIFOOrdering();
        test.destroy();
    }*/
}
