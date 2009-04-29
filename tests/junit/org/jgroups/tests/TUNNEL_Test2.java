package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.jgroups.stack.GossipRouter;
import org.jgroups.util.Promise;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Ensures that a disconnected channel reconnects correctly, for different stack
 * configurations.
 * 
 * 
 * @version $Id: TUNNEL_Test2.java,v 1.12 2009/04/29 20:19:24 vlada Exp $
 **/

@Test(groups = {Global.STACK_INDEPENDENT, "known-failures"}, sequential = true)
public class TUNNEL_Test2 extends ChannelTestBase {
    private JChannel channel, coordinator;
    private GossipRouter gossipRouter1, gossipRouter2;
    private static final String props = "tunnel2.xml";

    @BeforeMethod
    void startRouter() throws Exception {
        gossipRouter1 = new GossipRouter(12002);
        gossipRouter1.start();

        gossipRouter2 = new GossipRouter(12003);
        gossipRouter2.start();
    }

    @AfterMethod
    void tearDown() throws Exception {
        Util.close(channel, coordinator);
        gossipRouter1.stop();
        gossipRouter2.stop();
    }

    public void testSimpleConnect() throws Exception {
        channel = new JChannel(props);
        channel.connect("testSimpleConnect");
        assert channel.getLocalAddress() != null;
        assert channel.getView().size() == 1;
        channel.disconnect();
        assert channel.getLocalAddress() == null;
        assert channel.getView() == null;
    }

    /**
     * Tests connect with two members
     * 
     **/
    public void testConnectTwoChannels() throws Exception {
        coordinator = new JChannel(props);
        channel = new JChannel(props);
        coordinator.connect("testConnectTwoChannels");
        channel.connect("testConnectTwoChannels");
        View view = channel.getView();
        assert view.size() == 2;
        assert view.containsMember(channel.getLocalAddress());
        assert view.containsMember(coordinator.getLocalAddress());

        channel.disconnect();

        view = coordinator.getView();
        assert view.size() == 1;
        assert view.containsMember(coordinator.getLocalAddress());
    }

    /**
     * Tests connect with two members but when both GR fail and restart
     * 
     **/
    public void testConnectTwoChannelsBothGRDownReconnect() throws Exception {
        coordinator = new JChannel(props);
        channel = new JChannel(props);
        coordinator.connect("testConnectTwoChannelsBothGRDownReconnect");
        channel.connect("testConnectTwoChannelsBothGRDownReconnect");

        gossipRouter1.stop();
        gossipRouter2.stop();
        // give time to reconnect
        Util.sleep(3000);

        gossipRouter1.start();
        gossipRouter2.start();

        // give time to reconnect
        Util.sleep(3000);
        View view = coordinator.getView();
        assert view.size() == 2;
        assert view.containsMember(coordinator.getLocalAddress());
        assert view.containsMember(channel.getLocalAddress());

        view = channel.getView();
        assert view.size() == 2;
        assert view.containsMember(coordinator.getLocalAddress());
        assert view.containsMember(channel.getLocalAddress());
    }

    public void testConnectThreeChannelsWithGRDown() throws Exception {
        JChannel third = null;
        coordinator = new JChannel(props);
        channel = new JChannel(props);
        coordinator.connect("testConnectThreeChannelsWithGRDown");
        channel.connect("testConnectThreeChannelsWithGRDown");

        third = new JChannel(props);
        third.connect("testConnectThreeChannelsWithGRDown");

        View view = channel.getView();
        assert channel.getView().size() == 3;
        assert third.getView().size() == 3;
        assert view.containsMember(channel.getLocalAddress());
        assert view.containsMember(coordinator.getLocalAddress());

        // kill router and recheck views
        gossipRouter2.stop();
        Util.sleep(1000);

        view = channel.getView();
        assert channel.getView().size() == 3;
        assert third.getView().size() == 3;
        assert third.getView().containsMember(channel.getLocalAddress());
        assert third.getView().containsMember(coordinator.getLocalAddress());

    }

    /**
     * 
      **/
    public void testConnectSendMessage() throws Exception {
        final Promise<Message> msgPromise = new Promise<Message>();
        coordinator = new JChannel(props);
        coordinator.connect("testConnectSendMessage");
        coordinator.setReceiver(new PromisedMessageListener(msgPromise));

        channel = new JChannel(props);
        channel.connect("testConnectSendMessage");

        channel.send(new Message(null, null, "payload"));

        Message msg = msgPromise.getResult(20000);
        assert msg != null;
        assert "payload".equals(msg.getObject());
    }

    /**
      * 
       **/
    public void testConnectSendMessageSecondGRDown() throws Exception {
        final Promise<Message> msgPromise = new Promise<Message>();
        coordinator = new JChannel(props);
        coordinator.connect("testConnectSendMessageSecondGRDown");
        coordinator.setReceiver(new PromisedMessageListener(msgPromise));

        channel = new JChannel(props);
        channel.connect("testConnectSendMessageSecondGRDown");

        gossipRouter2.stop();

        channel.send(new Message(null, null, "payload"));

        View view = coordinator.getView();
        assert view.size() == 2;
        assert view.containsMember(coordinator.getLocalAddress());
        assert view.containsMember(channel.getLocalAddress());

        view = channel.getView();
        assert view.size() == 2;
        assert view.containsMember(coordinator.getLocalAddress());
        assert view.containsMember(channel.getLocalAddress());

        Message msg = msgPromise.getResult(20000);
        assert msg != null;
        assert "payload".equals(msg.getObject());

    }

    /**
     * 
      **/
    public void testConnectSendMessageBothGRDown() throws Exception {
        final Promise<Message> msgPromise = new Promise<Message>();
        coordinator = new JChannel(props);
        coordinator.connect("testConnectSendMessageBothGRDown");
        coordinator.setReceiver(new PromisedMessageListener(msgPromise));

        channel = new JChannel(props);
        channel.connect("testConnectSendMessageBothGRDown");

        gossipRouter1.stop();
        gossipRouter2.stop();
        
        // give time to reconnect
        Util.sleep(3000);

        gossipRouter1.start();
        gossipRouter2.start();
        
        // give time to reconnect
        Util.sleep(3000);

        

        channel.send(new Message(null, null, "payload"));

        View view = coordinator.getView();
        assert view.size() == 2;
        assert view.containsMember(coordinator.getLocalAddress());
        assert view.containsMember(channel.getLocalAddress());

        view = channel.getView();
        assert view.size() == 2;
        assert view.containsMember(coordinator.getLocalAddress());
        assert view.containsMember(channel.getLocalAddress());

        Message msg = msgPromise.getResult(20000);
        assert msg != null;
        assert "payload".equals(msg.getObject());
    }

    /**
     * 
      **/
    public void testConnectSendMessageBothGRDownOnlyOneUp() throws Exception {

        final Promise<Message> msgPromise = new Promise<Message>();
        coordinator = new JChannel(props);
        coordinator.connect("testConnectSendMessageBothGRDownOnlyOneUp");
        coordinator.setReceiver(new PromisedMessageListener(msgPromise));

        channel = new JChannel(props);
        channel.connect("testConnectSendMessageBothGRDownOnlyOneUp");

        gossipRouter1.stop();
        gossipRouter2.stop();

        gossipRouter1.start();

        // give time to reconnect
        Util.sleep(6000);

        channel.send(new Message(null, null, "payload"));

        View view = coordinator.getView();
        assert view.size() == 2;
        assert view.containsMember(coordinator.getLocalAddress());
        assert view.containsMember(channel.getLocalAddress());

        view = channel.getView();
        assert view.size() == 2;
        assert view.containsMember(coordinator.getLocalAddress());
        assert view.containsMember(channel.getLocalAddress());

        Message msg = msgPromise.getResult(20000);
        assert msg != null;
        assert "payload".equals(msg.getObject());
    }

    public void testConnectSendMessageFirstGRDown() throws Exception {

        final Promise<Message> msgPromise = new Promise<Message>();
        coordinator = new JChannel(props);
        coordinator.connect("testConnectSendMessageFirstGRDown");
        coordinator.setReceiver(new PromisedMessageListener(msgPromise));

        channel = new JChannel(props);
        channel.connect("testConnectSendMessageFirstGRDown");

        gossipRouter1.stop();

        channel.send(new Message(null, null, "payload"));
        View view = coordinator.getView();
        assert view.size() == 2;
        assert view.containsMember(coordinator.getLocalAddress());
        assert view.containsMember(channel.getLocalAddress());

        view = channel.getView();
        assert view.size() == 2;
        assert view.containsMember(coordinator.getLocalAddress());
        assert view.containsMember(channel.getLocalAddress());

        Message msg = msgPromise.getResult(20000);
        assert msg != null;
        assert "payload".equals(msg.getObject());

    }

    private static class PromisedMessageListener extends ReceiverAdapter {
        private final Promise<Message> promise;

        public PromisedMessageListener(Promise<Message> promise) {
            this.promise = promise;
        }

        public void receive(Message msg) {
            promise.setResult(msg);
        }
    }
}
