
package org.jgroups.tests;


import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.MessageListener;
import org.jgroups.View;
import org.jgroups.blocks.PullPushAdapter;
import org.jgroups.tests.stack.Utilities;
import org.jgroups.util.Promise;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;


/**
 * Ensures that a disconnected channel reconnects correctly, for different
 * stack configurations.
 *
 * @author Ovidiu Feodorov <ovidiu@feodorov.com>
 * @author Bela Ban belaban@yahoo.com
 * @version $Id: DisconnectTest.java,v 1.17 2008/04/14 07:30:35 belaban Exp $
 **/
public class DisconnectTest {
    private JChannel channel;
    private int routerPort;



    @AfterMethod
    public void tearDown() throws Exception {
        if(channel != null) {
            channel.close();
            channel=null;
        }
    }


    private static String getTUNNELProps(int routerPort, int gossipPort) {
        return "TUNNEL(router_host=127.0.0.1;router_port=" + routerPort + "):" +
                "PING(gossip_host=127.0.0.1;gossip_port=" + gossipPort + "):" +
                "FD:" +
                "VERIFY_SUSPECT(timeout=1500):" +
                "pbcast.NAKACK(gc_lag=100;retransmit_timeout=3000):" +
                "pbcast.STABLE(desired_avg_gossip=20000):" +
                "pbcast.GMS(join_timeout=50000;shun=false;" +
                "print_local_addr=true)";
    }

    /**
     * Tests if the channel has a null local address after disconnect (using
     * TUNNEL).
     *
     * TO_DO: uncomment or delete after clarifying the semantics of
     * getLocalAddress() on a disconnected channel.
     *
     **/
    @Test
    public void testNullLocalAddress_TUNNEL() throws Exception {
        try {
            routerPort=Utilities.startGossipRouter();
            String props = getTUNNELProps(routerPort, routerPort);
            channel = new JChannel(props);
            channel.connect("testgroup");
            assert channel.getLocalAddress() != null;
            channel.disconnect();
            assert channel.getLocalAddress() == null;
        }
        finally {
            Utilities.stopGossipRouter();
        }
    }


    /**
     * Tests connect-disconnect-connect sequence for a group with one member
     * (using default configuration).
     **/
    @Test
    public void testDisconnectConnectOne_Default() throws Exception {
        channel=new JChannel();
        channel.connect("testgroup1");
        channel.disconnect();
        channel.connect("testgroup2");
        View view=channel.getView();
        Assert.assertEquals(1, view.size());
        assert view.containsMember(channel.getLocalAddress());
    }


    /**
     * Tests connect-disconnect-connect sequence for a group with two members
     * (using default configuration).
     **/
    @Test
    public void testDisconnectConnectTwo_Default() throws Exception {
        JChannel coordinator=new JChannel();
        coordinator.connect("testgroup");
        channel=new JChannel();
        channel.connect("testgroup1");
        channel.disconnect();
        channel.connect("testgroup");
        View view=channel.getView();
        Assert.assertEquals(2, view.size());
        assert view.containsMember(channel.getLocalAddress());
        assert view.containsMember(coordinator.getLocalAddress());

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
    @Test
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
        assert msg != null;
        Assert.assertEquals("payload", msg.getObject());

        ppa.stop();
        coordinator.close();
    }


    /**
      * Tests connect-disconnect-connect sequence for a group with one member
      * (using TUNNEL).
      **/
     @Test
     public void testDisconnectConnectOne_TUNNEL() throws Exception {
        try {
            routerPort = Utilities.startGossipRouter();
            String props=getTUNNELProps(routerPort, routerPort);
            channel=new JChannel(props);
            channel.connect("testgroup1");
            channel.disconnect();
            channel.connect("testgroup2");
            View view=channel.getView();
            Assert.assertEquals(1, view.size());
            assert view.containsMember(channel.getLocalAddress());
        }
        finally {
            Utilities.stopGossipRouter();
        }
     }


     /**
      * Tests connect-disconnect-connect sequence for a group with two members
      * (using TUNNEL).
      **/
     @Test
     public void testDisconnectConnectTwo_TUNNEL() throws Exception {
         try {
             routerPort = Utilities.startGossipRouter();
             String props=getTUNNELProps(routerPort, routerPort);
             // String props="tunnel.xml";
             JChannel coordinator=new JChannel(props);
             coordinator.connect("testgroup");
             channel=new JChannel(props);
             channel.connect("testgroup1");
             channel.disconnect();
             channel.connect("testgroup");

             Thread.sleep(1000);

             View view=channel.getView();
             Assert.assertEquals(2, view.size());
             assert view.containsMember(channel.getLocalAddress());
             assert view.containsMember(coordinator.getLocalAddress());

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
     @Test
     public void testDisconnectConnectSendTwo_TUNNEL() throws Exception {
        try {
            routerPort = Utilities.startGossipRouter();
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
            assert msg != null;
            Assert.assertEquals("payload", msg.getObject());

            ppa.stop();
            coordinator.close();
        }
        finally {
            Utilities.stopGossipRouter();
        }
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
