// $Id: DisconnectTest.java,v 1.11.2.1 2009/04/22 20:22:29 rachmatowicz Exp $

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
 * @version $Revision: 1.11.2.1 $
 **/
public class DisconnectTest extends TestCase {

    private JChannel channel;
    private int routerPort;

    public DisconnectTest(String name) {
        super(name);
    }

    public void setUp() throws Exception {
        super.setUp();
    }

    public void tearDown() throws Exception {
        super.tearDown();
        if(channel != null) {
            channel.close();
            channel=null;
        }
    }


    private String getTUNNELProps(String routerHost, String gossipHost, int routerPort, int gossipPort) {
        return
                "TUNNEL(router_host=" + routerHost + ";router_port=" + routerPort + "):" +
                "PING(gossip_host=" + gossipHost + ";gossip_port=" + gossipPort + "):" +
                "FD:" +
                "VERIFY_SUSPECT(timeout=1500;down_thread=false;up_thread=false):" +
                "pbcast.NAKACK(gc_lag=100;retransmit_timeout=3000;" +
                "down_thread=true;up_thread=true):" +
                "pbcast.STABLE(desired_avg_gossip=20000;down_thread=false;" +
                "up_thread=false):" +
                "pbcast.GMS(join_timeout=50000;join_retry_timeout=2000;shun=false;" +
                "print_local_addr=true;down_thread=true;up_thread=true)";
    }

    /**
     * Tests if the channel has a null local address after disconnect (using
     * TUNNEL).
     *
     * TO_DO: uncomment or delete after clarifying the semantics of
     * getLocalAddress() on a disconnected channel.
     *
     **/
    public void testNullLocalAddress_TUNNEL() throws Exception {
        try {
        	String bind_addr = System.getProperty("jgroups.bind_addr", "127.0.0.1") ;
            routerPort=Utilities.startGossipRouter(bind_addr);
            String props = getTUNNELProps(bind_addr, bind_addr, routerPort, routerPort);
            channel = new JChannel(props);
            channel.connect("testgroup");
            assertTrue(channel.getLocalAddress() != null);
            channel.disconnect();
            assertNull(channel.getLocalAddress());
        }
        finally {
            Utilities.stopGossipRouter();
        }
    }


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
        try {
        	String bind_addr = System.getProperty("jgroups.bind_addr", "127.0.0.1") ;
            routerPort=Utilities.startGossipRouter(bind_addr);
            String props = getTUNNELProps(bind_addr, bind_addr, routerPort, routerPort);
            channel=new JChannel(props);
            channel.connect("testgroup1");
            channel.disconnect();
            channel.connect("testgroup2");
            View view=channel.getView();
            assertEquals(1, view.size());
            assertTrue(view.containsMember(channel.getLocalAddress()));
        }
        finally {
            Utilities.stopGossipRouter();
        }
     }


     /**
      * Tests connect-disconnect-connect sequence for a group with two members
      * (using TUNNEL).
      **/
     public void testDisconnectConnectTwo_TUNNEL() throws Exception {
         try {
         	 String bind_addr = System.getProperty("jgroups.bind_addr", "127.0.0.1") ;
             routerPort=Utilities.startGossipRouter(bind_addr);
             String props = getTUNNELProps(bind_addr, bind_addr, routerPort, routerPort);
             // String props="tunnel.xml";
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
         finally {
             Utilities.stopGossipRouter();
         }
     }


    /**
     * Tests connect-disconnect-connect-send sequence for a group with two
      * members, using TUNNEL. Test case introduced before fixing pbcast.NAKACK
      * bug, which used to leave pbcast.NAKACK in a broken state after
      * DISCONNECT. Because of this problem, the channel couldn't be used to
      * multicast messages.
      **/
     public void testDisconnectConnectSendTwo_TUNNEL() throws Exception {
        try {
        	String bind_addr = System.getProperty("jgroups.bind_addr", "127.0.0.1") ;
            routerPort=Utilities.startGossipRouter(bind_addr);
            String props = getTUNNELProps(bind_addr, bind_addr, routerPort, routerPort);

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
        finally {
            Utilities.stopGossipRouter();
        }
    }


    public static Test suite() {
        return new TestSuite(DisconnectTest.class);
    }

    public static void main(String[] args) {
        String[] testCaseName={DisconnectTest.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }

    private static class PromisedMessageListener implements MessageListener {

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
