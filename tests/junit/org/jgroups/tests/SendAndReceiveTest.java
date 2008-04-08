// $Id: SendAndReceiveTest.java,v 1.10 2008/04/08 07:18:56 belaban Exp $

package org.jgroups.tests;


import org.jgroups.JChannel;
import org.jgroups.Message;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;


/**
 * Tests sending and receiving of messages within the same VM. Sends N messages
 * and expects reception of N messages within a given time. Fails otherwise.
 * @author Bela Ban
 */
public class SendAndReceiveTest {
    JChannel channel;
    static final int NUM_MSGS=1000;
    static final long TIMEOUT=30000;

    String props1="UDP(loopback=true;mcast_addr=228.8.8.8;mcast_port=27000;ip_ttl=1;" +
            "mcast_send_buf_size=64000;mcast_recv_buf_size=64000):" +
            //"PIGGYBACK(max_wait_time=100;max_size=32000):" +
            "PING(timeout=2000;num_initial_members=3):" +
            "MERGE2(min_interval=5000;max_interval=10000):" +
            "FD_SOCK:" +
            "VERIFY_SUSPECT(timeout=1500):" +
            "pbcast.NAKACK(gc_lag=50;retransmit_timeout=300,600,1200,2400,4800):" +
            "UNICAST(timeout=600,1200,2400,4800):" +
            "pbcast.STABLE(desired_avg_gossip=20000):" +
            "FRAG(frag_size=8096):" +
            "pbcast.GMS(join_timeout=5000;" +
            "shun=false;print_local_addr=true)";

        String props2="UDP(loopback=false;mcast_addr=228.8.8.8;mcast_port=27000;ip_ttl=1;" +
                "mcast_send_buf_size=64000;mcast_recv_buf_size=64000):" +
                //"PIGGYBACK(max_wait_time=100;max_size=32000):" +
                "PING(timeout=2000;num_initial_members=3):" +
                "MERGE2(min_interval=5000;max_interval=10000):" +
                "FD_SOCK:" +
                "VERIFY_SUSPECT(timeout=1500):" +
                "pbcast.NAKACK(gc_lag=50;retransmit_timeout=300,600,1200,2400,4800):" +
                "UNICAST(timeout=600,1200,2400,4800):" +
                "pbcast.STABLE(desired_avg_gossip=20000):" +
                "FRAG(frag_size=8096):" +
                "pbcast.GMS(join_timeout=5000;" +
                "shun=false;print_local_addr=true)";

    String props3="SHARED_LOOPBACK:" +
            "PING(timeout=2000;num_initial_members=3):" +
            "MERGE2(min_interval=5000;max_interval=10000):" +
            "FD_SOCK:" +
            "VERIFY_SUSPECT(timeout=1500):" +
            "pbcast.NAKACK(gc_lag=50;retransmit_timeout=300,600,1200,2400,4800):" +
            "UNICAST(timeout=600,1200,2400,4800):" +
             "pbcast.STABLE(desired_avg_gossip=20000):" +
            "FRAG(frag_size=8096):" +
            "pbcast.GMS(join_timeout=5000;" +
            "shun=false;print_local_addr=true)";


    public SendAndReceiveTest(String n) {

    }

    public void setUp(String props) {
        try {
            channel=new JChannel(props);
            channel.connect("test1");
        }
        catch(Throwable t) {
            t.printStackTrace(System.err);
            assert false : "channel could not be created";
        }
    }


    @AfterMethod
    public void tearDown() {
        if(channel != null) {
            channel.close();
            channel=null;
        }
    }


    /**
     * Sends NUM messages and expects NUM messages to be received. If
     * NUM messages have not been received after 20 seconds, the test failed.
     */
    @Test
    public void testSendAndReceiveWithDefaultUDP_Loopback() {
        setUp(props1);
        sendMessages(NUM_MSGS);
        int received_msgs=receiveMessages(NUM_MSGS, TIMEOUT);
        assert received_msgs >= NUM_MSGS;
    }

    @Test
    public void testSendAndReceiveWithDefaultUDP_NoLoopback() {
        setUp(props2);
        sendMessages(NUM_MSGS);
        int received_msgs=receiveMessages(NUM_MSGS, TIMEOUT);
        assert received_msgs >= NUM_MSGS;
    }

    @Test
    public void testSendAndReceiveWithLoopback() {
        setUp(props3);
        sendMessages(NUM_MSGS);
        int received_msgs=receiveMessages(NUM_MSGS, TIMEOUT);
        assert received_msgs >= NUM_MSGS;
    }

    private void sendMessages(int num) {
        Message msg;
        for(int i=0; i < num; i++) {
            try {
                msg=new Message();
                channel.send(msg);
                System.out.print(i + " ");
            }
            catch(Throwable t) {
                assert false : "could not send message #" + i;
            }
        }
    }


    /**
     * Receive at least <tt>num</tt> messages. Total time should not exceed <tt>timeout</tt>
     * @param num
     * @param timeout Must be > 0
     * @return
     */
    private int receiveMessages(int num, long timeout) {
        int received=0;
        Object msg;

        if(timeout <= 0)
            timeout=5000;

        long start=System.currentTimeMillis(), current, wait_time;
        while(true) {
            current=System.currentTimeMillis();
            wait_time=timeout - (current - start);
            if(wait_time <= 0)
                break;
            try {
                msg=channel.receive(wait_time);
                if(msg instanceof Message) {
                    received++;
                    System.out.print("+" + received + ' ');
                }
                if(received >= num)
                    break;
            }
            catch(Throwable t) {
                assert false : "failed receiving message";
            }
        }
        return received;
    }


    public static void main(String[] args) {
        String[] testCaseName={SendAndReceiveTest.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    } //public static void main(String[] args)

}
