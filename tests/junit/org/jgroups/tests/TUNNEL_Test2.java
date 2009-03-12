package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.jgroups.stack.GossipRouter;
import org.jgroups.util.Promise;
import org.jgroups.util.Util;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Ensures that a disconnected channel reconnects correctly, for different stack
 * configurations.
 * 
 * 
 * @version $Id: TUNNEL_Test2.java,v 1.8 2009/03/12 17:38:41 vlada Exp $
 **/

//TODO investigate while it fails on Hudson
@Test(enabled=false,groups = Global.STACK_INDEPENDENT, sequential = true)
public class TUNNEL_Test2 extends ChannelTestBase {
	private JChannel channel, coordinator;
	private final static String GROUP = "TUNNEL_Test2";
	private GossipRouter gossipRouter1, gossipRouter2;
	private static final String props = "tunnel2.xml";

	@BeforeClass
	void startRouter() throws Exception {
		gossipRouter1 = new GossipRouter(12002);
		gossipRouter1.start();

		gossipRouter2 = new GossipRouter(12003);
		gossipRouter2.start();
	}

	@AfterClass
	void stopRouter() throws Exception {
		gossipRouter1.stop();
		gossipRouter2.stop();

	}

	@AfterMethod
	void tearDown() throws Exception {
		Util.close(channel, coordinator);
	}

	public void testSimpleConnect() throws Exception {
		channel = new JChannel(props);
		channel.connect(GROUP);
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
		coordinator.connect(GROUP);
		channel.connect(GROUP);
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
		coordinator.connect(GROUP);
		channel.connect(GROUP);
		
		gossipRouter1.stop();
		gossipRouter2.stop();
		
		gossipRouter1.start();
		gossipRouter2.start();
		
		
		//give time to reconnect
		Util.sleep(6000);
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
		try {
			coordinator = new JChannel(props);
			channel = new JChannel(props);
			coordinator.connect(GROUP);
			channel.connect(GROUP);

			third = new JChannel(props);
			third.connect(GROUP);

			View view = channel.getView();
			assert channel.getView().size() == 3;
			assert third.getView().size() == 3;
			assert view.containsMember(channel.getLocalAddress());
			assert view.containsMember(coordinator.getLocalAddress());
			
			//kill router and recheck views
			gossipRouter2.stop();
			Util.sleep(1000);

			view = channel.getView();
			assert channel.getView().size() == 3;
			assert third.getView().size() == 3;
			assert third.getView().containsMember(channel.getLocalAddress());
			assert third.getView().containsMember(coordinator.getLocalAddress());
		} finally {
			if (!gossipRouter2.isStarted())
				gossipRouter2.start();
			Util.close(third);
		}
	}

	/**
     * 
      **/
	public void testConnectSendMessage() throws Exception {
		final Promise<Message> msgPromise = new Promise<Message>();
		coordinator = new JChannel(props);
		coordinator.connect(GROUP);
		coordinator.setReceiver(new PromisedMessageListener(msgPromise));

		channel = new JChannel(props);
		channel.connect(GROUP);

		channel.send(new Message(null, null, "payload"));

		Message msg = msgPromise.getResult(20000);
		assert msg != null;
		assert "payload".equals(msg.getObject());
	}

	/**
      * 
       **/
	public void testConnectSendMessageSecondGRDown() throws Exception {

		try {
			final Promise<Message> msgPromise = new Promise<Message>();
			coordinator = new JChannel(props);
			coordinator.connect(GROUP);
			coordinator.setReceiver(new PromisedMessageListener(msgPromise));

			channel = new JChannel(props);
			channel.connect(GROUP);

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
		} finally {
			if (!gossipRouter2.isStarted())
				gossipRouter2.start();
		}
	}
	
	
	/**
     * 
      **/
	public void testConnectSendMessageBothGRDown() throws Exception {

		try {
			final Promise<Message> msgPromise = new Promise<Message>();
			coordinator = new JChannel(props);
			coordinator.connect(GROUP);
			coordinator.setReceiver(new PromisedMessageListener(msgPromise));

			channel = new JChannel(props);
			channel.connect(GROUP);

			gossipRouter1.stop();
			gossipRouter2.stop();
			
			gossipRouter1.start();
			gossipRouter2.start();
			
			//give time to reconnect
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
		} finally {
			if (!gossipRouter2.isStarted())
				gossipRouter2.start();
			
			if (!gossipRouter1.isStarted())
				gossipRouter1.start();
		}
	}
	
	/**
     * 
      **/
	public void testConnectSendMessageBothGRDownOnlyOneUp() throws Exception {

		try {
			final Promise<Message> msgPromise = new Promise<Message>();
			coordinator = new JChannel(props);
			coordinator.connect(GROUP);
			coordinator.setReceiver(new PromisedMessageListener(msgPromise));

			channel = new JChannel(props);
			channel.connect(GROUP);

			gossipRouter1.stop();
			gossipRouter2.stop();
			
			gossipRouter1.start();
			
			//give time to reconnect
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
		} finally {
			if (!gossipRouter2.isStarted())
				gossipRouter2.start();
			
			if (!gossipRouter1.isStarted())
				gossipRouter1.start();
		}
	}

	public void testConnectSendMessageFirstGRDown() throws Exception {
		try {
			final Promise<Message> msgPromise = new Promise<Message>();
			coordinator = new JChannel(props);
			coordinator.connect(GROUP);
			coordinator.setReceiver(new PromisedMessageListener(msgPromise));

			channel = new JChannel(props);
			channel.connect(GROUP);

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
		} finally {
			if (!gossipRouter1.isStarted())
				gossipRouter1.start();
		}
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
