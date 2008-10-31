
package org.jgroups.tests;


import org.jgroups.*;
import org.jgroups.protocols.MERGE2;
import org.jgroups.protocols.TUNNEL;
import org.jgroups.protocols.PING;
import org.jgroups.stack.GossipRouter;
import org.jgroups.stack.ProtocolStack;
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
 * @version $Id: TUNNEL_Test.java,v 1.1 2008/10/31 09:20:11 belaban Exp $
 **/
@Test(groups=Global.STACK_INDEPENDENT,sequential=true)
public class TUNNEL_Test extends ChannelTestBase{
    private JChannel channel, coordinator;
    private final static String GROUP="TUNNEL_Test";
    private GossipRouter gossipRouter;
    private static final String props="tunnel.xml";

    @BeforeClass
    void startRouter() throws Exception {
        gossipRouter=new GossipRouter();
        gossipRouter.start();
    }
    
    @AfterClass
    void stopRouter() throws Exception {
        gossipRouter.stop();
    }

    @AfterMethod
    void tearDown() throws Exception {
        Util.close(channel, coordinator);
    }




    /**
     * Tests if the channel has a null local address after disconnect (using TUNNEL).
     **/
    public void testNullLocalAddress_TUNNEL() throws Exception {
        channel = new JChannel(props);
        setProps(channel);
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
        setProps(channel);
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
        setProps(coordinator);

        channel=new JChannel(props);
        setProps(channel);
        
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
        setProps(coordinator);
        coordinator.connect(GROUP);
        coordinator.setReceiver(new PromisedMessageListener(msgPromise));

        channel=new JChannel(props);
        setProps(channel);
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
        setProps(channel);
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
         setProps(coordinator);
         coordinator.connect(GROUP);
         channel=new JChannel(props);
         setProps(channel);
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
        setProps(coordinator);
        coordinator.connect(GROUP);
        coordinator.setReceiver(new PromisedMessageListener(msgPromise));

        channel=new JChannel(props);
        setProps(channel);
        channel.connect("DisconnectTest.testgroup-1");
        channel.disconnect();
        channel.connect(GROUP);

        channel.send(new Message(null, null, "payload"));

        Message msg=msgPromise.getResult(20000);
        assert msg != null;
        assert "payload".equals(msg.getObject());
    }

    private static void setProps(JChannel channel) {
        ProtocolStack stack=channel.getProtocolStack();
        MERGE2 merge=(MERGE2)stack.findProtocol(MERGE2.class);
        if(merge != null) {
            merge.setMinInterval(1000);
            merge.setMaxInterval(3000);
        }

        TUNNEL tunnel=(TUNNEL)stack.getTransport();
        if(tunnel != null) {
            tunnel.setReconnectInterval(2000);
        }

        PING ping=(PING)stack.findProtocol(PING.class);
        if(ping != null) {
            ping.setGossipRefresh(1000);
        }
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
