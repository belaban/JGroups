// $Id: DisconnectTest.java,v 1.8 2005/02/19 12:34:39 ovidiuf Exp $

package org.jgroups.tests;


import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.MessageListener;
import org.jgroups.View;
import org.jgroups.tests.stack.Utilities;
import org.jgroups.blocks.PullPushAdapter;
import org.jgroups.util.Promise;


/**
 * Ensures that a disconnected channel reconnects correctly, for different
 * stack configurations.
 *
 * @author Ovidiu Feodorov <ovidiu@feodorov.com>
 * @author Bela Ban belaban@yahoo.com
 * @version $Revision: 1.8 $
 **/
public class DisconnectTest extends TestCase {

    private JChannel channel;
    private int routerPort;

    public DisconnectTest(String name) {
        super(name);
    }

    public void setUp() throws Exception {

        super.setUp();
        routerPort = Utilities.startGossipRouter();
    }

    public void tearDown() throws Exception {

        super.tearDown();
        if(channel != null) {
            channel.close();
            channel=null;
        }
        Utilities.stopGossipRouter();
    }


    private String getTUNNELProps(int routerPort, int gossipPort) {
        return
                "TUNNEL(router_host=localhost;router_port=" + routerPort + "):" +
                "PING(gossip_host=localhost;gossip_port=" + gossipPort + "):" +
                "pbcast.FD:" +
                "VERIFY_SUSPECT(timeout=1500;down_thread=false;up_thread=false):" +
                "pbcast.NAKACK(gc_lag=100;retransmit_timeout=3000;" +
                "down_thread=true;up_thread=true):" +
                "pbcast.STABLE(desired_avg_gossip=20000;down_thread=false;" +
                "up_thread=false):" +
                "pbcast.GMS(join_timeout=5000;join_retry_timeout=2000;shun=false;" +
                "print_local_addr=false;down_thread=true;up_thread=true)";
    }

//    /**
//     * Tests if the channel has a null local address after disconnect (using
//     * TUNNEL).
//     *
//     * TO_DO: uncomment or delete after clarifying the semantics of
//     * getLocalAddress() on a disconnected channel.
//     *
//     **/
////     public void testNullLocalAddress_TUNNEL() throws Exception {
//
//// 	String props = getTUNNELProps(startRouter(),startGossipServer());
//
////   	channel = new JChannel(props);
//// 	channel.connect("testgroup");
//// 	assertTrue(channel.getLocalAddress()!=null);
//// 	channel.disconnect();
//// 	assertNull(channel.getLocalAddress());
////     }


    /**
     * Tests connect-disconnect-connect sequence for a group with one member
     * (using default configuration).
     **/
    public void testDisconnectConnectOne_Default() throws Exception {

        channel=new JChannel();
        channel.connect("testgroup1");
        channel.disconnect();
        channel.connect("testgroup2");
        View view=channel.getView();
        assertEquals(1, view.size());
        assertTrue(view.containsMember(channel.getLocalAddress()));
    }


    /**
     * Tests connect-disconnect-connect sequence for a group with two members
     * (using default configuration).
     **/
    public void testDisconnectConnectTwo_Default() throws Exception {

        JChannel coordinator=new JChannel();
        coordinator.connect("testgroup");

        channel=new JChannel();
        channel.connect("testgroup1");
        channel.disconnect();
        channel.connect("testgroup");
        View view=channel.getView();
        assertEquals(2, view.size());
        assertTrue(view.containsMember(channel.getLocalAddress()));
        assertTrue(view.containsMember(coordinator.getLocalAddress()));

        coordinator.close();
    }


    /**
     * Tests connect-disconnect-connect-send sequence for a group with two
     * members, using the default stack configuration. Assumes that default
     * configuration includes pbcast.NAKACK. Test case introduced before fixing
     * pbcast.NAKACK bug, which used to leave pbcast.NAKACK in a broken state
     * after DISCONNECT. Because of this problem, the channel couldn't be used
     * to multicast messages.
     **/
    public void testDisconnectConnectSendTwo_Default() throws Exception {

        final Promise msgPromise=new Promise();
        JChannel coordinator=new JChannel();
        coordinator.connect("testgroup");
        PullPushAdapter ppa= new PullPushAdapter(coordinator,
                                                 new PromisedMessageListener(msgPromise));
        ppa.start();

        channel=new JChannel();
        channel.connect("testgroup1");
        channel.disconnect();
        channel.connect("testgroup");

        channel.send(new Message(null, null, "payload"));

        Message msg=(Message)msgPromise.getResult(20000);
        assertTrue(msg != null);
        assertEquals("payload", msg.getObject());

        ppa.stop();
        coordinator.close();
    }


    /**
      * Tests connect-disconnect-connect sequence for a group with one member
      * (using TUNNEL).
      **/
     public void testDisconnectConnectOne_TUNNEL() throws Exception {

         String props=getTUNNELProps(routerPort, routerPort);

         channel=new JChannel(props);
         channel.connect("testgroup1");
         channel.disconnect();
         channel.connect("testgroup2");
         View view=channel.getView();
         assertEquals(1, view.size());
         assertTrue(view.containsMember(channel.getLocalAddress()));
     }


     /**
      * Tests connect-disconnect-connect sequence for a group with two members
      * (using TUNNEL).
      **/
     public void testDisconnectConnectTwo_TUNNEL() throws Exception {

         String props=getTUNNELProps(routerPort, routerPort);

         JChannel coordinator=new JChannel(props);
         coordinator.connect("testgroup");

         channel=new JChannel(props);
         channel.connect("testgroup1");
         channel.disconnect();
         channel.connect("testgroup");

         Thread.sleep(1000);
         
         View view=channel.getView();
         assertEquals(2, view.size());
         assertTrue(view.containsMember(channel.getLocalAddress()));
         assertTrue(view.containsMember(coordinator.getLocalAddress()));

         coordinator.close();
     }


     /**
      * Tests connect-disconnect-connect-send sequence for a group with two
      * members, using TUNNEL. Test case introduced before fixing pbcast.NAKACK
      * bug, which used to leave pbcast.NAKACK in a broken state after
      * DISCONNECT. Because of this problem, the channel couldn't be used to
      * multicast messages.
      **/
     public void testDisconnectConnectSendTwo_TUNNEL() throws Exception {

         String props=getTUNNELProps(routerPort, routerPort);

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
     }


    public static Test suite() {
        TestSuite s=new TestSuite(DisconnectTest.class);
        return s;
    }

    public static void main(String[] args) {
        String[] testCaseName={DisconnectTest.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
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

}
