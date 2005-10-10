// $Id: ConnectTest.java,v 1.7 2005/10/10 12:17:55 belaban Exp $

package org.jgroups.tests;


import junit.framework.TestCase;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.MessageListener;
import org.jgroups.View;
import org.jgroups.blocks.PullPushAdapter;
import org.jgroups.util.Promise;
import org.jgroups.util.Util;


/**
 * Runs through multiple channel connect and disconnects, without closing the channel.
 */
public class ConnectTest extends TestCase {
    JChannel channel;
    final int TIMES=10;


    String props="UDP(mcast_addr=228.8.8.8;mcast_port=45566;ip_ttl=32;" +
            "mcast_send_buf_size=150000;mcast_recv_buf_size=80000;" +
            "enable_bundling=true;use_incoming_packet_handler=true;loopback=true):" +
            "PING(timeout=2000;num_initial_members=3):" +
            "MERGE2(min_interval=5000;max_interval=10000):" +
            "FD_SOCK:" +
            "VERIFY_SUSPECT(timeout=1500):" +
            "pbcast.NAKACK(gc_lag=50;retransmit_timeout=600,1200,2400,4800):" +
            "UNICAST(timeout=600,1200,2400):" +
            "pbcast.STABLE(desired_avg_gossip=20000):" +
            "FRAG(frag_size=4096;down_thread=false;up_thread=false):" +
            "pbcast.GMS(join_timeout=5000;join_retry_timeout=2000;" +
            "shun=true;print_local_addr=false)";



    public ConnectTest(String name) {
        super(name);
    }

    public void tearDown() throws Exception {
        super.tearDown();
        if(channel != null) {
            channel.close();
            channel = null;
        }
    }

    void doIt(int times) {
        for(int i=0; i < times; i++) {
            System.out.println("\nAttempt #" + (i + 1));
            System.out.print("Connecting to channel: ");
            try {
                channel.connect("ConnectTest");
                System.out.println("-- connected: " + channel.getView() + " --");
            }
            catch(Exception e) {
                System.out.println("-- connection failed --");
                System.err.println(e);
            }
            System.out.print("Disconnecting from channel: ");
            channel.disconnect();
            System.out.println("-- disconnected --");
        }
    }


    public void testConnectAndDisconnect() throws Exception {
        System.out.print("Creating channel: ");
        channel=new JChannel(props);
        System.out.println("-- created --");
        doIt(TIMES);
        System.out.print("Closing channel: ");
        channel.close();
        System.out.println("-- closed --");
        System.out.println("Remaining threads are:");
        Util.printThreads();
    }

    public void testDisconnectConnectOne() throws Exception {
        channel=new JChannel(props);
        channel.connect("testgroup1");
        channel.disconnect();
        channel.connect("testgroup2");
        View view=channel.getView();
        assertEquals(1, view.size());
        assertTrue(view.containsMember(channel.getLocalAddress()));
        channel.close();
        System.out.println("Remaining threads are:");
        Util.printThreads();
    }


    /**
     * Tests connect-disconnect-connect sequence for a group with two members
     **/
    public void testDisconnectConnectTwo() throws Exception {
        View     view;
        JChannel coordinator=new JChannel(props);
        coordinator.connect("testgroup");
        view=coordinator.getView();
        System.out.println("-- view for coordinator: " + view);

        channel=new JChannel(props);
        channel.connect("testgroup1");
        view=channel.getView();
        System.out.println("-- view for channel: " + view);

        channel.disconnect();

        channel.connect("testgroup");
        view=channel.getView();
        System.out.println("-- view for channel: " + view);

        assertEquals(2, view.size());
        assertTrue(view.containsMember(channel.getLocalAddress()));
        assertTrue(view.containsMember(coordinator.getLocalAddress()));
        coordinator.close();
        channel.close();
        System.out.println("Remaining threads are:");
        Util.printThreads();
    }


    /**
     * Tests connect-disconnect-connect-send sequence for a group with two
     * members. Test case introduced before fixing pbcast.NAKACK
     * bug, which used to leave pbcast.NAKACK in a broken state after
     * DISCONNECT. Because of this problem, the channel couldn't be used to
     * multicast messages.
     **/
    public void testDisconnectConnectSendTwo() throws Exception {
        final Promise msgPromise=new Promise();
        JChannel coordinator=new JChannel(props);
        coordinator.connect("testgroup");
        PullPushAdapter ppa=
                new PullPushAdapter(coordinator,
                                    new PromisedMessageListener(msgPromise));
        ppa.start();

        channel=new JChannel(props);
        channel.connect("testgroup1");
        channel.disconnect();
        channel.connect("testgroup");
        channel.send(new Message(null, null, "payload"));
        Message msg=(Message)msgPromise.getResult(20000);
        assertTrue(msg != null);
        assertEquals("payload", msg.getObject());
        ppa.stop();
        coordinator.close();
        channel.close();
        System.out.println("Remaining threads are:");
        Util.printThreads();
    }







    private class PromisedMessageListener implements MessageListener {

        private Promise promise;

        public PromisedMessageListener(Promise promise) {
            this.promise=promise;
        }

        public byte[] getState() {
            return null;
        }

        public void receive(Message msg) {
            promise.setResult(msg);
        }

        public void setState(byte[] state) {
        }
    }


    public static void main(String[] args) {
        String[] testCaseName={ConnectTest.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }


}
