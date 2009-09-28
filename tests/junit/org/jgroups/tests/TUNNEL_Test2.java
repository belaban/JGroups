package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.jgroups.protocols.TUNNEL;
import org.jgroups.stack.GossipRouter;
import org.jgroups.stack.ProtocolStack;
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
 * @version $Id: TUNNEL_Test2.java,v 1.20 2009/09/28 17:03:06 vlada Exp $
 **/

@Test(groups = {Global.STACK_INDEPENDENT, "known-failures",Global.GOSSIP_ROUTER}, sequential = true)
public class TUNNEL_Test2 extends ChannelTestBase {
    private JChannel channel, coordinator;
    private GossipRouter gr1, gr2;
    private static final String props ="tunnel.xml";
    private static final String bindAddress = "127.0.0.1";

    @BeforeMethod
    void startRouter() throws Exception {
        gr1 = new GossipRouter(12003);
        gr1.start();

        gr2 = new GossipRouter(12004);
        gr2.start();
    }

    @AfterMethod
    void tearDown() throws Exception {
        Util.close(channel, coordinator);
        gr1.stop();
        gr2.stop();
    }
    
    private void modifyChannel(JChannel... channels) throws Exception {
        for (JChannel c : channels) {
            ProtocolStack stack = c.getProtocolStack();
            TUNNEL t = (TUNNEL) stack.getBottomProtocol();
            String s = bindAddress + ":" + gr1.getPort() + ",";
            s+=bindAddress+":" + gr2.getPort();
            t.setGossipRouterHosts(s);
            t.init();
        }        
    }

    public void testSimpleConnect() throws Exception {
        channel = new JChannel(props);
        modifyChannel(channel);
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
        modifyChannel(channel,coordinator);
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
        modifyChannel(channel,coordinator);
        coordinator.connect("testConnectTwoChannelsBothGRDownReconnect");
        channel.connect("testConnectTwoChannelsBothGRDownReconnect");

        gr1.stop();
        gr2.stop();
        // give time to reconnect
        Util.sleep(3000);

        gr1.start();
        gr2.start();

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
        modifyChannel(channel,coordinator);
        coordinator.connect("testConnectThreeChannelsWithGRDown");
        channel.connect("testConnectThreeChannelsWithGRDown");

        third = new JChannel(props);
        modifyChannel(third);
        third.connect("testConnectThreeChannelsWithGRDown");

        View view = channel.getView();
        assert channel.getView().size() == 3;
        assert third.getView().size() == 3;
        assert view.containsMember(channel.getLocalAddress());
        assert view.containsMember(coordinator.getLocalAddress());

        // kill router and recheck views
        gr2.stop();
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
        modifyChannel(coordinator);
        coordinator.connect("testConnectSendMessage");
        coordinator.setReceiver(new PromisedMessageListener(msgPromise));

        channel = new JChannel(props);
        modifyChannel(channel);
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
        modifyChannel(coordinator);
        coordinator.connect("testConnectSendMessageSecondGRDown");
        coordinator.setReceiver(new PromisedMessageListener(msgPromise));

        channel = new JChannel(props);
        modifyChannel(channel);
        channel.connect("testConnectSendMessageSecondGRDown");

        gr2.stop();

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
        modifyChannel(coordinator);
        coordinator.connect("testConnectSendMessageBothGRDown");
        coordinator.setReceiver(new PromisedMessageListener(msgPromise));

        channel = new JChannel(props);
        modifyChannel(channel);
        channel.connect("testConnectSendMessageBothGRDown");

        gr1.stop();
        gr2.stop();
        
        // give time to reconnect
        Util.sleep(3000);

        gr1.start();
        gr2.start();
        
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
        modifyChannel(coordinator);
        coordinator.connect("testConnectSendMessageBothGRDownOnlyOneUp");
        coordinator.setReceiver(new PromisedMessageListener(msgPromise));

        channel = new JChannel(props);
        modifyChannel(channel);
        channel.connect("testConnectSendMessageBothGRDownOnlyOneUp");
        

        gr1.stop();
        gr2.stop();

        gr1.start();

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
        modifyChannel(coordinator);
        coordinator.connect("testConnectSendMessageFirstGRDown");
        coordinator.setReceiver(new PromisedMessageListener(msgPromise));

        channel = new JChannel(props);
        modifyChannel(channel);
        channel.connect("testConnectSendMessageFirstGRDown");

        gr1.stop();

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
