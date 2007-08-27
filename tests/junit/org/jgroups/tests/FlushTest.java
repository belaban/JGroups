package org.jgroups.tests;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.jgroups.Address;
import org.jgroups.BlockEvent;
import org.jgroups.Channel;
import org.jgroups.ChannelException;
import org.jgroups.Event;
import org.jgroups.ExtendedReceiverAdapter;
import org.jgroups.GetStateEvent;
import org.jgroups.JChannel;
import org.jgroups.JChannelFactory;
import org.jgroups.Message;
import org.jgroups.SetStateEvent;
import org.jgroups.UnblockEvent;
import org.jgroups.View;
import org.jgroups.mux.MuxChannel;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;

/**
 * Tests the FLUSH protocol, requires flush-udp.xml in ./conf to be present and
 * configured to use FLUSH
 * 
 * @author Bela Ban
 * @version $Id: FlushTest.java,v 1.53 2007/08/27 08:09:17 belaban Exp $
 */
public class FlushTest extends ChannelTestBase {
	private JChannel c1, c2;

	public FlushTest(){
		super();
	}

	public FlushTest(String name){
		super(name);
	}

	public void setUp() throws Exception {
		super.setUp();
		CHANNEL_CONFIG = System.getProperty("channel.conf.flush", "flush-udp.xml");
	}

	public void tearDown() throws Exception {
		if(c2 != null){
			c2.close();
			assertFalse(c2.isOpen());
			assertFalse(c2.isConnected());
			c2 = null;
		}

		if(c1 != null){
			c1.close();
			assertFalse(c1.isOpen());
			assertFalse(c1.isConnected());
			c1 = null;
		}

		Util.sleep(500);
		super.tearDown();
	}

	public boolean useBlocking() {
		return true;
	}

	public void testSingleChannel() throws Exception {
		Semaphore s = new Semaphore(1);
		FlushTestReceiver receivers[] = new FlushTestReceiver[] { new FlushTestReceiver("c1",
																						s,
																						0,
																						false) };
		receivers[0].start();
		s.release(1);

		// Make sure everyone is in sync
		blockUntilViewsReceived(receivers, 60000);

		// Sleep to ensure the threads get all the semaphore tickets
		Util.sleep(1000);

		// Reacquire the semaphore tickets; when we have them all
		// we know the threads are done
		s.tryAcquire(1, 60, TimeUnit.SECONDS);
		receivers[0].cleanup();
		Util.sleep(1000);

		checkEventSequence(receivers[0], false);

	}

	/**
	 * Tests issue #1 in http://jira.jboss.com/jira/browse/JGRP-335
	 */
	public void testJoinFollowedByUnicast() throws ChannelException {
		c1 = createChannel();
		c1.setReceiver(new SimpleReplier(c1, true));
		c1.connect("test");

		Address target = c1.getLocalAddress();
		Message unicast_msg = new Message(target);

		c2 = createChannel();
		c2.setReceiver(new SimpleReplier(c2, false));
		c2.connect("test");

		// now send unicast, this might block as described in the case
		c2.send(unicast_msg);
		// if we don't get here this means we'd time out
	}

	/**
	 * Tests issue #2 in http://jira.jboss.com/jira/browse/JGRP-335
	 */
	public void testStateTransferFollowedByUnicast() throws ChannelException {
		c1 = createChannel();
		c1.setReceiver(new SimpleReplier(c1, true));
		c1.connect("test");

		Address target = c1.getLocalAddress();
		Message unicast_msg = new Message(target);

		c2 = createChannel();
		c2.setReceiver(new SimpleReplier(c2, false));
		c2.connect("test");

		// Util.sleep(100);
		log.info("\n** Getting the state **");
		c2.getState(null, 10000);
		// now send unicast, this might block as described in the case
		c2.send(unicast_msg);
	}

	/**
	 * Tests emition of block/unblock/get|set state events in both mux and bare
	 * channel mode. In mux mode this test creates getFactoryCount() real
	 * channels and creates only one mux application on top of each channel. In
	 * bare channel mode 4 real channels are created.
	 * 
	 */
	public void testBlockingNoStateTransfer() {
		String[] names = null;
		if(isMuxChannelUsed()){
			int muxFactoryCount = 2;
			names = createMuxApplicationNames(1, muxFactoryCount);
			_testChannels(names, muxFactoryCount, false, muxFactoryCount);
		}else{
			names = createApplicationNames(4);
			_testChannels(names, false, 4);
		}
	}

	/**
	 * Tests emition of block/unblock/get|set state events in mux mode. In this
	 * test all mux applications share the same "real" channel. This test runs
	 * only when mux.on=true. This test does not take into account
	 * -Dmux.factorycount parameter.
	 * 
	 */
	public void testBlockingSharedMuxFactory() {
		String[] names = null;
		int muxFactoryCount = 1;
		if(isMuxChannelUsed()){
			names = createMuxApplicationNames(4, muxFactoryCount);
			_testChannels(names, muxFactoryCount, false, new ChannelAssertable(1));
		}
	}

	/**
	 * Tests emition of block/unblock/get|set state events in mux mode. In this
	 * test there will be exactly two real channels created where each real
	 * channel has two mux applications on top of it. This test runs only when
	 * mux.on=true. This test does not take into account -Dmux.factorycount
	 * parameter.
	 * 
	 */
	public void testBlockingUnsharedMuxFactoryMultipleService() {
		String[] names = null;
		int muxFactoryCount = 2;
		if(isMuxChannelUsed()){
			names = createMuxApplicationNames(2, muxFactoryCount);
			_testChannels(names, muxFactoryCount, false, new ChannelAssertable(2));
		}
	}

	/**
	 * Tests emition of block/unblock/set|get state events for both mux and bare
	 * channel depending on mux.on parameter. In mux mode there will be only one
	 * mux channel for each "real" channel created and the number of real
	 * channels created is getMuxFactoryCount().
	 * 
	 */
	public void testBlockingWithStateTransfer() {
		String[] names = null;
		if(isMuxChannelUsed()){
			int muxFactoryCount = 2;
			names = createMuxApplicationNames(1, muxFactoryCount);
			_testChannels(names, muxFactoryCount, true, muxFactoryCount);
		}else{
			names = createApplicationNames(4);
			_testChannels(names, true, 4);
		}
	}

	/**
	 * Tests emition of block/unblock/set|get state events in mux mode setup
	 * where each "real" channel has two mux service on top of it. The number of
	 * real channels created is getMuxFactoryCount(). This test runs only when
	 * mux.on=true.
	 * 
	 */
	public void testBlockingWithStateTransferAndMultipleServiceMuxChannel() {
		String[] names = null;
		if(isMuxChannelUsed()){
			names = createMuxApplicationNames(2, 2);
			_testChannels(names, 2, true, 2);
		}
	}

	private void _testChannels(	String names[],
								int muxFactoryCount,
								boolean useTransfer,
								Assertable a) {
		int count = names.length;

		ArrayList<FlushTestReceiver> channels = new ArrayList<FlushTestReceiver>(count);
		try{
			// Create a semaphore and take all its permits
			Semaphore semaphore = new Semaphore(count);
			semaphore.acquire(count);

			// Create channels and their threads that will block on the
			// semaphore
			for(int i = 0;i < count;i++){
				FlushTestReceiver channel = null;
				if(isMuxChannelUsed()){
					channel = new FlushTestReceiver(names[i],
													muxFactory[i % muxFactoryCount],
													semaphore,
													useTransfer);
				}else{
					channel = new FlushTestReceiver(names[i], semaphore, 0, useTransfer);
				}
				channels.add(channel);

				// Release one ticket at a time to allow the thread to start
				// working
				channel.start();
				if(!useTransfer){
					semaphore.release(1);
				}
				Util.sleep(1000);
			}

			if(isMuxChannelUsed()){
				blockUntilViewsReceived(channels, muxFactoryCount, 10000);
			}else{
				blockUntilViewsReceived(channels, 10000);
			}

			// if state transfer is used release all at once
			// clear all channels of view events
			if(useTransfer){
				for(FlushTestReceiver app:channels){
					app.clear();
				}
				semaphore.release(count);
			}

			// Sleep to ensure the threads get all the semaphore tickets
			Util.sleep(1000);

			// Reacquire the semaphore tickets; when we have them all
			// we know the threads are done
			semaphore.tryAcquire(count, 60, TimeUnit.SECONDS);

			// do general asserts about channels
			a.verify(channels);

			// kill random member
			FlushTestReceiver randomRecv = channels.remove(RANDOM.nextInt(count));
			log.info("Closing random member " + randomRecv.getName()
						+ " at "
						+ randomRecv.getLocalAddress());
			ChannelCloseAssertable closeAssert = new ChannelCloseAssertable(randomRecv);
			randomRecv.cleanup();

			// let the view propagate and verify related asserts
			Util.sleep(5000);
			closeAssert.verify(channels);

			// verify block/unblock/view/get|set state sequence

			for(FlushTestReceiver receiver:channels){
				if(useTransfer){
					checkEventStateTransferSequence(receiver);
				}else{
					checkEventSequence(receiver, isMuxChannelUsed());
				}
			}
		}catch(Exception ex){
			log.warn("Exception encountered during test", ex);
			fail("Exception encountered during test execution: " + ex);
		}finally{
			for(FlushTestReceiver app:channels){
				app.cleanup();
				Util.sleep(500);
			}
		}
	}

	public void _testChannels(String names[], boolean useTransfer, int viewSize) {
		_testChannels(names, getMuxFactoryCount(), useTransfer, new ChannelAssertable(viewSize));
	}

	public void _testChannels(String names[], int muxFactoryCount, boolean useTransfer, int viewSize) {
		_testChannels(names, muxFactoryCount, useTransfer, new ChannelAssertable(viewSize));
	}

	private class ChannelCloseAssertable implements Assertable {
		ChannelApplication app;

		View viewBeforeClose;

		Address appAddress;

		String muxId;

		public ChannelCloseAssertable(ChannelApplication app){
			this.app = app;
			this.viewBeforeClose = app.getChannel().getView();
			appAddress = app.getChannel().getLocalAddress();
			if(app.isUsingMuxChannel()){
				MuxChannel mch = (MuxChannel) app.getChannel();
				muxId = mch.getId();
			}
		}

		public void verify(Object verifiable) {
			Collection channels = (Collection) verifiable;
			Channel ch = app.getChannel();
			assertFalse("Channel open", ch.isOpen());
			assertFalse("Chnanel connected", ch.isConnected());

			// if this channel had more than one member then verify that
			// the other member does not have departed member in its view
			if(viewBeforeClose.getMembers().size() > 1){
				for(Iterator iter = channels.iterator();iter.hasNext();){
					FlushTestReceiver receiver = (FlushTestReceiver) iter.next();
					Channel channel = receiver.getChannel();
					boolean pairServiceFound = (receiver.isUsingMuxChannel() && muxId.equals(((MuxChannel) channel).getId()));
					if(pairServiceFound || !receiver.isUsingMuxChannel()){
						assertTrue("Removed from view, address " + appAddress
									+ " view is "
									+ channel.getView(), !channel	.getView()
																	.getMembers()
																	.contains(appAddress));
					}
				}
			}
		}
	}

	private class ChannelAssertable implements Assertable {
		int expectedViewSize = 0;

		public ChannelAssertable(int expectedViewSize){
			this.expectedViewSize = expectedViewSize;
		}

		public void verify(Object verifiable) {
			Collection channels = (Collection) verifiable;
			for(Iterator iter = channels.iterator();iter.hasNext();){
				FlushTestReceiver receiver = (FlushTestReceiver) iter.next();
				Channel ch = receiver.getChannel();
				assertEquals("Correct view", ch.getView().getMembers().size(), expectedViewSize);
				assertTrue("Channel open", ch.isOpen());
				assertTrue("Chnanel connected", ch.isConnected());
				assertNotNull("Valid address ", ch.getLocalAddress());
				assertTrue("Address included in view ", ch	.getView()
															.getMembers()
															.contains(ch.getLocalAddress()));
				assertNotNull("Valid cluster name ", ch.getClusterName());
			}

			// verify views for pair services created on top of different "real"
			// channels
			if(expectedViewSize > 1 && isMuxChannelUsed()){
				for(Iterator iter = channels.iterator();iter.hasNext();){
					FlushTestReceiver receiver = (FlushTestReceiver) iter.next();
					MuxChannel ch = (MuxChannel) receiver.getChannel();
					int servicePairs = 1;
					for(Iterator it = channels.iterator();it.hasNext();){
						FlushTestReceiver receiver2 = (FlushTestReceiver) it.next();
						MuxChannel ch2 = (MuxChannel) receiver2.getChannel();
						if(ch.getId().equals(ch2.getId()) && !ch.getLocalAddress()
																.equals(ch2.getLocalAddress())){
							assertEquals(	"Correct view for service pair",
											ch.getView(),
											ch2.getView());
							assertTrue("Presence in view", ch	.getView()
																.getMembers()
																.contains(ch.getLocalAddress()));
							assertTrue("Presence in view", ch	.getView()
																.getMembers()
																.contains(ch2.getLocalAddress()));
							assertTrue("Presence in view", ch2	.getView()
																.getMembers()
																.contains(ch2.getLocalAddress()));
							assertTrue("Presence in view", ch2	.getView()
																.getMembers()
																.contains(ch.getLocalAddress()));
							servicePairs++;
						}
					}
					assertEquals("Correct service count", expectedViewSize, servicePairs);
				}
			}
		}
	}

	private void checkEventSequence(FlushTestReceiver receiver, boolean isMuxUsed) {
		List events = receiver.getEvents();
		String eventString = "[" + receiver.getName()
								+ "|"
								+ receiver.getLocalAddress()
								+ ",events:"
								+ events;
		log.info(eventString);
		assertNotNull(events);
		int size = events.size();
		for(int i = 0;i < size;i++){
			Object event = events.get(i);
			if(event instanceof BlockEvent){
				if(i + 1 < size){
					Object ev = events.get(i + 1);
					if(isMuxUsed){
						assertTrue(	"After Block should be View or Unblock" + eventString,
									ev instanceof View || ev instanceof UnblockEvent);
					}else{
						assertTrue(	"After Block should be View " + eventString,
									events.get(i + 1) instanceof View);
					}
				}
				if(i != 0){
					assertTrue(	"Before Block should be Unblock " + eventString,
								events.get(i - 1) instanceof UnblockEvent);
				}
			}
			if(event instanceof View){
				if(i + 1 < size){
					assertTrue(	"After View should be Unblock " + eventString,
								events.get(i + 1) instanceof UnblockEvent);
				}
				assertTrue(	"Before View should be Block " + eventString,
							events.get(i - 1) instanceof BlockEvent);
			}
			if(event instanceof UnblockEvent){
				if(i + 1 < size){
					assertTrue(	"After UnBlock should be Block " + eventString,
								events.get(i + 1) instanceof BlockEvent);
				}

				Object ev = events.get(i - 1);
				if(isMuxUsed){
					assertTrue(	"Before UnBlock should be View or Block" + eventString,
								ev instanceof View || ev instanceof BlockEvent);
				}else{
					assertTrue(	"Before UnBlock should be View " + eventString,
								events.get(i - 1) instanceof View);
				}
			}
		}
		receiver.clear();
	}

	private void checkEventStateTransferSequence(FlushTestReceiver receiver) {
		List events = receiver.getEvents();
		String eventString = "[" + receiver.getName() + ",events:" + events;
		log.info(eventString);
		assertNotNull(events);
		int size = events.size();
		for(int i = 0;i < size;i++){
			Object event = events.get(i);
			if(event instanceof BlockEvent){
				if(i + 1 < size){
					Object o = events.get(i + 1);
					assertTrue(	"After Block should be state|unblock|view" + eventString,
								o instanceof SetStateEvent || o instanceof GetStateEvent
										|| o instanceof UnblockEvent
										|| o instanceof View);
				}else if(i != 0){
					Object o = events.get(i + 1);
					assertTrue(	"Before Block should be state or Unblock " + eventString,
								o instanceof SetStateEvent || o instanceof GetStateEvent
										|| o instanceof UnblockEvent);
				}
			}
			if(event instanceof SetStateEvent || event instanceof GetStateEvent){
				if(i + 1 < size){
					assertTrue(	"After state should be Unblock " + eventString,
								events.get(i + 1) instanceof UnblockEvent);
				}
				assertTrue(	"Before state should be Block " + eventString,
							events.get(i - 1) instanceof BlockEvent);
			}

			if(event instanceof UnblockEvent){
				if(i + 1 < size){
					assertTrue(	"After UnBlock should be Block " + eventString,
								events.get(i + 1) instanceof BlockEvent);
				}else{
					Object o = events.get(size - 2);
					assertTrue(	"Before UnBlock should be block|state|view  " + eventString,
								o instanceof SetStateEvent || o instanceof GetStateEvent
										|| o instanceof BlockEvent
										|| o instanceof View);
				}
			}

		}
		receiver.clear();
	}

	protected JChannel createChannel() throws ChannelException {
		JChannel ret = new JChannel(CHANNEL_CONFIG);
		ret.setOpt(Channel.BLOCK, Boolean.TRUE);
		Protocol flush = ret.getProtocolStack().findProtocol("FLUSH");
		if(flush != null){
			Properties p = new Properties();
			p.setProperty("timeout", "0");
			flush.setProperties(p);

			// send timeout up and down the stack, so other protocols can use
			// the same value too
			Map<String, Object> map = new HashMap<String, Object>();
			map.put("flush_timeout", new Long(0));
			flush.getUpProtocol().up(new Event(Event.CONFIG, map));
			flush.getDownProtocol().down(new Event(Event.CONFIG, map));
		}
		return ret;
	}

	private interface Assertable {
		public void verify(Object verifiable);
	}

	private class FlushTestReceiver extends PushChannelApplicationWithSemaphore {
		List<Object> events;

		boolean shouldFetchState;

		int msgCount = 0;

		protected FlushTestReceiver(String name,
									Semaphore semaphore,
									int msgCount,
									boolean shouldFetchState) throws Exception{
			super(name, semaphore);
			this.shouldFetchState = shouldFetchState;
			this.msgCount = msgCount;
			events = Collections.synchronizedList(new LinkedList<Object>());
			channel.connect("test");
		}

		protected FlushTestReceiver(String name,
									JChannelFactory factory,
									Semaphore semaphore,
									boolean shouldFetchState) throws Exception{
			super(name, factory, semaphore);
			this.shouldFetchState = shouldFetchState;
			events = Collections.synchronizedList(new LinkedList<Object>());
			channel.connect("test");
		}

		public void clear() {
			events.clear();
		}

		public List<Object> getEvents() {
			return new LinkedList<Object>(events);
		}

		public void block() {
			events.add(new BlockEvent());
		}

		public void unblock() {
			events.add(new UnblockEvent());
		}

		public void viewAccepted(View new_view) {
			events.add(new_view);
		}

		public byte[] getState() {
			events.add(new GetStateEvent(null, null));
			return new byte[] { 'b', 'e', 'l', 'a' };
		}

		public void setState(byte[] state) {
			events.add(new SetStateEvent(null, null));
		}

		public void getState(OutputStream ostream) {
			events.add(new GetStateEvent(null, null));
			byte[] payload = new byte[] { 'b', 'e', 'l', 'a' };
			try{
				ostream.write(payload);
			}catch(IOException e){
				e.printStackTrace();
			}finally{
				Util.close(ostream);
			}
		}

		public void setState(InputStream istream) {
			events.add(new SetStateEvent(null, null));
			byte[] payload = new byte[4];
			try{
				istream.read(payload);
			}catch(IOException e){
				e.printStackTrace();
			}finally{
				Util.close(istream);
			}
		}

		protected void useChannel() throws Exception {
			if(shouldFetchState){
				channel.getState(null, 25000);
			}
			if(msgCount > 0){
				for(int i = 0;i < msgCount;i++){
					channel.send(new Message());
					Util.sleep(100);
				}
			}
		}
	}

	private class SimpleReplier extends ExtendedReceiverAdapter {
		Channel channel;

		boolean handle_requests = false;

		public SimpleReplier(Channel channel,boolean handle_requests){
			this.channel = channel;
			this.handle_requests = handle_requests;
		}

		public void receive(Message msg) {
			Message reply = new Message(msg.getSrc());
			try{
				log.info("-- MySimpleReplier[" + channel.getLocalAddress()
							+ "]: received message from "
							+ msg.getSrc());
				if(handle_requests){
					log.info(", sending reply");
					channel.send(reply);
				}else
					System.out.println("\n");
			}catch(Exception e){
				e.printStackTrace();
			}
		}

		public void viewAccepted(View new_view) {
			log.info("-- MySimpleReplier[" + channel.getLocalAddress()
						+ "]: viewAccepted("
						+ new_view
						+ ")");
		}

		public void block() {
			log.info("-- MySimpleReplier[" + channel.getLocalAddress() + "]: block()");
		}

		public void unblock() {
			log.info("-- MySimpleReplier[" + channel.getLocalAddress() + "]: unblock()");
		}
	}

	public static Test suite() {
		return new TestSuite(FlushTest.class);
	}

	public static void main(String[] args) {
		junit.textui.TestRunner.run(FlushTest.suite());
	}
}
