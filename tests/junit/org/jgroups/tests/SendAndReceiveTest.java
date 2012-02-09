
package org.jgroups.tests;


import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.Global;
import org.jgroups.ReceiverAdapter;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;


/**
 * Tests sending and receiving of messages within the same VM. Sends N messages
 * and expects reception of N messages within a given time. Fails otherwise.
 * @author Bela Ban
 */
@Test(groups=Global.STACK_INDEPENDENT,sequential=true)
public class SendAndReceiveTest {
    JChannel channel;
    static final int NUM_MSGS=1000;
    static final long TIMEOUT=30000;

    String props1="UDP(bind_addr=127.0.0.1;loopback=true;mcast_port=27000;ip_ttl=1;" +
      "mcast_send_buf_size=64000;mcast_recv_buf_size=640000):" +
      "PING(timeout=2000;num_initial_members=3):" +
      "MERGE2(min_interval=5000;max_interval=10000):" +
      "FD_SOCK:" +
      "VERIFY_SUSPECT(timeout=1500):" +
      "pbcast.NAKACK2:" +
      "UNICAST:" +
      "pbcast.STABLE(desired_avg_gossip=20000):" +
      "FRAG2(frag_size=8096):" +
      "pbcast.GMS(join_timeout=5000;print_local_addr=true)";

    String props2="UDP(bind_addr=127.0.0.1;loopback=false;mcast_port=27000;ip_ttl=1;" +
      "mcast_send_buf_size=64000;mcast_recv_buf_size=640000):" +
      "PING(timeout=2000;num_initial_members=3):" +
      "MERGE2(min_interval=5000;max_interval=10000):" +
      "FD_SOCK:" +
      "VERIFY_SUSPECT(timeout=1500):" +
      "pbcast.NAKACK2:" +
      "UNICAST:" +
      "pbcast.STABLE(desired_avg_gossip=20000):" +
      "FRAG2(frag_size=8096):" +
      "pbcast.GMS(join_timeout=5000;print_local_addr=true)";

    String props3="SHARED_LOOPBACK:" +
      "PING(timeout=2000;num_initial_members=3):" +
      "MERGE2(min_interval=5000;max_interval=10000):" +
      "FD_SOCK:" +
      "VERIFY_SUSPECT(timeout=1500):" +
      "pbcast.NAKACK2:" +
      "UNICAST:" +
      "pbcast.STABLE(desired_avg_gossip=20000):" +
      "FRAG2(frag_size=8096):" +
      "pbcast.GMS(join_timeout=5000;print_local_addr=true)";




    private void setUp(String props) throws Exception {
        channel=new JChannel(props);
        channel.connect("SendAndReceiveTest");
    }


    @AfterMethod
    void tearDown() {
        Util.close(channel);
    }


    /**
     * Sends NUM messages and expects NUM messages to be received. If
     * NUM messages have not been received after 20 seconds, the test failed.
     */
    public void testSendAndReceiveWithDefaultUDP_Loopback() throws Exception {
        setUp(props1);
        MyReceiver receiver=new MyReceiver();
        channel.setReceiver(receiver);
        sendMessages(NUM_MSGS);
        int received_msgs=receiveMessages(receiver, NUM_MSGS, TIMEOUT);
        assert received_msgs >= NUM_MSGS;
    }

    public void testSendAndReceiveWithDefaultUDP_NoLoopback() throws Exception {
        setUp(props2);
        MyReceiver receiver=new MyReceiver();
        channel.setReceiver(receiver);
        sendMessages(NUM_MSGS);
        int received_msgs=receiveMessages(receiver, NUM_MSGS, TIMEOUT);
        assert received_msgs >= NUM_MSGS;
    }

    public void testSendAndReceiveWithLoopback() throws Exception {
        setUp(props3);
        MyReceiver receiver=new MyReceiver();
        channel.setReceiver(receiver);
        sendMessages(NUM_MSGS);
        int received_msgs=receiveMessages(receiver, NUM_MSGS, TIMEOUT);
        assert received_msgs >= NUM_MSGS;
    }

    private void sendMessages(int num) throws Exception {
        Message msg;
        for(int i=0; i < num; i++) {
            msg=new Message();
            System.out.print(i + " ");
            channel.send(msg);
        }
    }


    /**
     * Receive at least <tt>num</tt> messages. Total time should not exceed <tt>timeout</tt>
     * @param num
     * @param timeout Must be > 0
     * @return
     */
    private static int receiveMessages(MyReceiver receiver, int num, long timeout) {
        if(timeout <= 0)
            timeout=5000;

        long target=System.currentTimeMillis() + timeout;
        while(receiver.getReceived() < num && System.currentTimeMillis() < target) {
            Util.sleep(500);
        }
        return receiver.getReceived();
    }


    protected static class MyReceiver extends ReceiverAdapter {
        int received=0;

        public int getReceived() {
            return received;
        }

        public void receive(Message msg) {
            System.out.print("+" + received + ' ');
            received++;
        }
    }


}
