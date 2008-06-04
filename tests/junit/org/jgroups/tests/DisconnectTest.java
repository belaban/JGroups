
package org.jgroups.tests;


import org.jgroups.*;
import org.jgroups.tests.stack.Utilities;
import org.jgroups.util.Promise;
import org.jgroups.util.Util;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Ensures that a disconnected channel reconnects correctly, for different
 * stack configurations.
 *
 * @author Ovidiu Feodorov <ovidiu@feodorov.com>
 * @author Bela Ban belaban@yahoo.com
 * @version $Id: DisconnectTest.java,v 1.22 2008/06/04 06:49:01 belaban Exp $
 **/
@Test(groups=Global.STACK_INDEPENDENT,sequential=true)
public class DisconnectTest {
    private JChannel channel, coordinator;
    private String props;
    private final static String GROUP="DisconnectTest";

    @BeforeClass
    void startRouter() throws Exception {
        int routerPort=Utilities.startGossipRouter();
        props=getTUNNELProps(routerPort, routerPort);
    }

    @AfterClass
    static void stopRouter() throws Exception {
        Utilities.stopGossipRouter();
    }

    @AfterMethod
    void tearDown() throws Exception {
        Util.close(channel, coordinator);
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
     * Tests if the channel has a null local address after disconnect (using TUNNEL).
     **/
    public void testNullLocalAddress_TUNNEL() throws Exception {
        channel = new JChannel(props);
        channel.connect(GROUP);
        assert channel.getLocalAddress() != null;
        channel.disconnect();
        assert channel.getLocalAddress() == null;
    }


    /**
     * Tests connect-disconnect-connect sequence for a group with one member
     * (using default configuration).
     **/
    public void testDisconnectConnectOne_Default() throws Exception {
        channel=new JChannel(props);
        channel.connect("DisconnectTest.testgroup-1");
        channel.disconnect();
        channel.connect("DisconnectTest.testgroup-2");
        View view=channel.getView();
        assert view.size() == 1;
        assert view.containsMember(channel.getLocalAddress());
    }


    /**
     * Tests connect-disconnect-connect sequence for a group with two members
     * (using default configuration).
     **/
    public void testDisconnectConnectTwo_Default() throws Exception {
        coordinator=new JChannel(props);
        channel=new JChannel(props);
        coordinator.connect(GROUP);
        channel.connect("DisconnectTest.testgroup-1");
        channel.disconnect();
        channel.connect(GROUP);
        View view=channel.getView();
        assert view.size() == 2;
        assert view.containsMember(channel.getLocalAddress());
        assert view.containsMember(coordinator.getLocalAddress());
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
        coordinator=new JChannel(props);
        coordinator.connect(GROUP);
        coordinator.setReceiver(new PromisedMessageListener(msgPromise));

        channel=new JChannel(props);
        channel.connect("DisconnectTest.testgroup-1");
        channel.disconnect();
        channel.connect(GROUP);

        channel.send(new Message(null, null, "payload"));

        Message msg=(Message)msgPromise.getResult(20000);
        assert msg != null;
        assert "payload".equals(msg.getObject());
    }


    /**
      * Tests connect-disconnect-connect sequence for a group with one member
      * (using TUNNEL).
      **/
     public void testDisconnectConnectOne_TUNNEL() throws Exception {
        channel=new JChannel(props);
        channel.connect("DisconnectTest.testgroup-1");
        channel.disconnect();
        channel.connect("DisconnectTest.testgroup-2");
        View view=channel.getView();
        assert view.size() == 1;
        assert view.containsMember(channel.getLocalAddress());
    }


     /**
      * Tests connect-disconnect-connect sequence for a group with two members
      * (using TUNNEL).
      **/
     public void testDisconnectConnectTwo_TUNNEL() throws Exception {
         coordinator=new JChannel(props);
         coordinator.connect(GROUP);
         channel=new JChannel(props);
         channel.connect("DisconnectTest.testgroup-1");
         channel.disconnect();
         channel.connect(GROUP);

         Thread.sleep(1000);

         View view=channel.getView();
         assert view.size() == 2;
         assert view.containsMember(channel.getLocalAddress());
         assert view.containsMember(coordinator.getLocalAddress());
     }


    /**
     * Tests connect-disconnect-connect-send sequence for a group with two
      * members, using TUNNEL. Test case introduced before fixing pbcast.NAKACK
      * bug, which used to leave pbcast.NAKACK in a broken state after
      * DISCONNECT. Because of this problem, the channel couldn't be used to
      * multicast messages.
      **/
     public void testDisconnectConnectSendTwo_TUNNEL() throws Exception {
        final Promise<Message> msgPromise=new Promise<Message>();
        coordinator=new JChannel(props);
        coordinator.connect(GROUP);
        coordinator.setReceiver(new PromisedMessageListener(msgPromise));

        channel=new JChannel(props);
        channel.connect("DisconnectTest.testgroup-1");
        channel.disconnect();
        channel.connect(GROUP);

        channel.send(new Message(null, null, "payload"));

        Message msg=msgPromise.getResult(20000);
        assert msg != null;
        assert "payload".equals(msg.getObject());
    }



    private static class PromisedMessageListener extends ReceiverAdapter {
        private final Promise promise;

        public PromisedMessageListener(Promise promise) {
            this.promise=promise;
        }

        public void receive(Message msg) {
            promise.setResult(msg);
        }
    }

}
