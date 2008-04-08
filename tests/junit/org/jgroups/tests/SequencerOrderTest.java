

package org.jgroups.tests;


import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.LinkedList;
import java.util.List;


/**
 * Tests a SEQUENCER based stack: demonstrates race condition where thread#1
 * gets seqno, thread#2 gets seqno, thread#2 sends, thread#1 tries to send but
 * is out of order.
 */
public class SequencerOrderTest {
    private JChannel ch1, ch2;
    private MyReceiver r1, r2;
    static final String GROUP="demo-group";
    static final int NUM_MSGS=1000;


    String props="UDP(mcast_addr=228.8.8.8;mcast_port=45566;ip_ttl=2;" +
            "mcast_send_buf_size=25000000;mcast_recv_buf_size=640000;" +
            "enable_bundling=true;use_incoming_packet_handler=true;loopback=true):" +
            "PING(timeout=2000;num_initial_members=3):" +
            "MERGE2(min_interval=5000;max_interval=10000):" +
            "FD(timeout=2000;max_tries=2):" +
            "VERIFY_SUSPECT(timeout=1500):" +
            "pbcast.NAKACK(gc_lag=50;retransmit_timeout=600,1200,2400,4800):" +
            "UNICAST(timeout=600,1200,2400):" +
            "pbcast.STABLE(desired_avg_gossip=5000):" +
            "pbcast.GMS(join_timeout=5000;" +
            "shun=true;print_local_addr=true;view_ack_collection_timeout=2000):" +
            "SEQUENCER";



    public SequencerOrderTest(String name) {
    }

    @BeforeMethod
    public void setUp() throws Exception {
        ;
        ch1=new JChannel(props);
        ch1.connect(GROUP);

        ch2=new JChannel(props);
        ch2.connect(GROUP);
    }

    @AfterMethod
    public void tearDown() throws Exception {
        ;
        if(ch2 != null) {
            ch2.close();
            ch2 = null;
        }
        if(ch1 != null) {
            ch1.close();
            ch1 = null;
        }
    }

    @Test
    public void testBroadcastSequence() throws Exception {
        r1=new MyReceiver(ch1.getLocalAddress());
        ch1.setReceiver(r1);
        r2=new MyReceiver(ch2.getLocalAddress());
        ch2.setReceiver(r2);
        
        Thread thread1 = new Thread() {
        	public void run() {
        		Util.sleep(300);
                for(int i=1; i <= NUM_MSGS; i++) {
                    try {
                    	ch1.send(new Message(null, null, new Integer(i)));
                    } catch (Exception e) {
                    	throw new RuntimeException(e);
                    }
                    System.out.print("-- messages sent thread 1: " + i + "/" + NUM_MSGS + "\r");
                }

        	}
        };
        
    	Thread thread2 = new Thread() {
        	public void run() {
        		Util.sleep(300);
                for(int i=1; i <= NUM_MSGS; i++) {
                    try {
                    	ch1.send(new Message(null, null, new Integer(i)));
                    } catch (Exception e) {
                    	throw new RuntimeException(e);
                    }
                    System.out.print("-- messages sent thread 2: " + i + "/" + NUM_MSGS + "\r");
                }

        	}
        };
        
        thread1.start();
        thread2.start();
        thread1.join();
        thread2.join();
        
        System.out.println("");
        System.out.println("-- verifying messages on ch1 and ch2");
        verifyNumberOfMessages(NUM_MSGS * 2);
        verifyMessageOrder(r1.getMsgs());
        verifyMessageOrder(r2.getMsgs());
        verifySameOrder();
    }

    private void verifyNumberOfMessages(int num_msgs) throws Exception {
        List<Integer> l1=r1.getMsgs();
        List<Integer> l2=r2.getMsgs();

        long end_time=System.currentTimeMillis() + 10000;
        while(System.currentTimeMillis() < end_time) {
            if(l1.size() >= num_msgs && l2.size() >= num_msgs)
                break;
            Util.sleep(500);
        }

        System.out.println("l1.size()=" + l1.size() + ", l2.size()=" + l2.size());
        Assert.assertEquals(l1.size(), num_msgs, "list 1 should have " + num_msgs + " elements");
        Assert.assertEquals(l2.size(), num_msgs, "list 2 should have " + num_msgs + " elements");
    }

    private void verifyMessageOrder(List<Integer> list) throws Exception {
        List<Integer> l1=r1.getMsgs();
        List<Integer> l2=r2.getMsgs();
        System.out.println("l1: " + l1);
        System.out.println("l2: " + l2);
        int i=1,j=1;
        for(int count: list) {
            if(count == i)
                i++;
            else if(count == j)
                j++;
            else
                throw new Exception("got " + count + ", but expected " + i + " or " + j);
        }
    }


    private void verifySameOrder() throws Exception {
        List<Integer> l1=r1.getMsgs();
        List<Integer> l2=r2.getMsgs();
        int[] arr1=new int[l1.size()];
        int[] arr2=new int[l2.size()];

        int index=0;
        for(int el: l1) {
            arr1[index++]=el;
        }
        index=0;
        for(int el: l2) { 
            arr2[index++]=el;
        }

        int count1, count2;
        for(int i=0; i < arr1.length; i++) {
            count1=arr1[i]; count2=arr2[i];
            if(count1 != count2)
                throw new Exception("lists are different at index " + i + ": count1=" + count1 + ", count2=" + count2);
        }
    }


    private static class MyReceiver extends ReceiverAdapter {
        Address local_addr;
        List<Integer> msgs=new LinkedList<Integer>();

        private MyReceiver(Address local_addr) {
            this.local_addr=local_addr;
        }

        public List<Integer> getMsgs() {
            return msgs;
        }

        public void receive(Message msg) {
            msgs.add((Integer)msg.getObject());
        }
    }


    public static void main(String[] args) {
        String[] name={SequencerOrderTest.class.getName()};
        junit.textui.TestRunner.main(name);
    }


}
