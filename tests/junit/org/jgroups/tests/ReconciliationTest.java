package org.jgroups.tests;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.ChannelException;
import org.jgroups.Event;
import org.jgroups.ExtendedReceiverAdapter;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.protocols.DISCARD;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;

/**
 * Tests the FLUSH protocol, requires flush-udp.xml in ./conf to be present and
 * configured to use FLUSH
 * 
 * @author Bela Ban
 * @version $Id: ReconciliationTest.java,v 1.1 2007/07/05 19:02:35 vlada Exp $
 */
public class ReconciliationTest extends ChannelTestBase {
	private JChannel c1, c2, c3, c4;

	private MyReceiver a, b, c;

	public ReconciliationTest(){
		super();
	}

	public ReconciliationTest(String name){
		super(name);
	}

	public void setUp() throws Exception {
		super.setUp();
		CHANNEL_CONFIG = System.getProperty("channel.conf.flush", "flush-udp.xml");
	}

	public void tearDown() throws Exception {
		if(c4 != null){
			c4.close();
			assertFalse(c4.isOpen());
			assertFalse(c4.isConnected());
			c4 = null;
		}
		if(c3 != null){
			c3.close();
			assertFalse(c3.isOpen());
			assertFalse(c3.isConnected());
			c3 = null;
		}

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

	/**
	 * Test scenario:
	 * <ul>
	 * <li>3 members: A,B,C
	 * <li>All members have DISCARD which does <em>not</em> discard any
	 * messages !
	 * <li>B (in DISCARD) ignores all messages from C
	 * <li>C multicasts 5 messages to the cluster, A and C receive them
	 * <li>New member D joins
	 * <li>Before installing view {A,B,C,D}, FLUSH updates B with all of C's 5
	 * messages
	 * </ul>
	 */
	public void testReconciliationFlushTriggeredByNewMemberJoin() throws Exception {
		createMembers();

		insertDISCARD(c2, c3.getLocalAddress());

		printDigests("\nDigests before C sends any messages:");

		// now C sends 5 messages:
		System.out.println("\nC sending 5 messages; B will ignore them, but A and C will receive them");
		for(int i = 1;i <= 5;i++){
			c3.send(null, null, new Integer(i));
		}
		Util.sleep(1000); // until al messages have been received, this is
							// asynchronous so we need to wait a bit

		printDigests("\nDigests after C sent 5 messages:");

		// check C (must have received its own messages)
		Map<Address, List<Integer>> map = c.getMsgs();
		assertEquals("we should have only 1 sender, namely C at this time", 1, map.size());
		List<Integer> list = map.get(c3.getLocalAddress());
		System.out.println("C: messages received from C: " + list);
		assertEquals("msgs for C: " + list, 5, list.size());

		// check A (should have received C's messages)
		map = a.getMsgs();
		assertEquals("we should have only 1 sender, namely C at this time", 1, map.size());
		list = map.get(c3.getLocalAddress());
		System.out.println("A: messages received from C: " + list);
		assertEquals("msgs for C: " + list, 5, list.size());

		// check B (should have received none of C's messages)
		map = b.getMsgs();
		assertEquals("we should have no sender at this time", 0, map.size());
		list = map.get(c3.getLocalAddress());
		System.out.println("B: messages received from C: " + list);
		assertNull(list);

		removeDISCARD(c2);

		System.out.println("\nJoining D, this will trigger FLUSH and a subsequent view change to {A,B,C,D}");
		c4 = createChannel();
		c4.connect("x");

		// wait until view {A,B} has been installed
		int cnt = 1000;
		View v;
		while((v = c1.getView()) != null && cnt > 0){
			cnt--;
			if(v.size() == 4)
				break;
			Util.sleep(500);
		}
		System.out.println("v=" + v);
		assert v != null;
		assertEquals(4, v.size());

		printDigests("\nDigests after D joined (FLUSH protocol should have updated B with C's messages)");

		// check B (should have received all 5 of C's messages, through B as
		// part of the flush phase)
		map = b.getMsgs();
		assertEquals("we should have 1 sender at this time", 1, map.size());
		list = map.get(c3.getLocalAddress());
		System.out.println("B: messages received from C: " + list);
		assertEquals(5, list.size());
	}

	/**
	 * Test scenario:
	 * <ul>
	 * <li>3 members: A,B,C
	 * <li>All members have DISCARD which does <em>not</em> discard any
	 * messages !
	 * <li>B (in DISCARD) ignores all messages from C
	 * <li>C multicasts 5 messages to the cluster, A and C receive them
	 * <li>A then runs a manual flush by calling Channel.start/stopFlush()
	 * <li>Before installing view {A,B}, FLUSH makes A sends its 5 messages
	 * received from C to B
	 * </ul>
	 */
	public void testReconciliationFlushTriggeredByManualFlush() throws Exception {
		createMembers();

		insertDISCARD(c2, c3.getLocalAddress());

		printDigests("\nDigests before C sends any messages:");

		// now C sends 5 messages:
		System.out.println("\nC sending 5 messages; B will ignore them, but A and C will receive them");
		for(int i = 1;i <= 5;i++){
			c3.send(null, null, new Integer(i));
		}
		Util.sleep(1000); // until al messages have been received, this is
							// asynchronous so we need to wait a bit

		printDigests("\nDigests after C sent 5 messages:");

		// check C (must have received its own messages)
		Map<Address, List<Integer>> map = c.getMsgs();
		assertEquals("we should have only 1 sender, namely C at this time", 1, map.size());
		List<Integer> list = map.get(c3.getLocalAddress());
		System.out.println("C: messages received from C: " + list);
		assertEquals("msgs for C: " + list, 5, list.size());

		// check A (should have received C's messages)
		map = a.getMsgs();
		assertEquals("we should have only 1 sender, namely C at this time", 1, map.size());
		list = map.get(c3.getLocalAddress());
		System.out.println("A: messages received from C: " + list);
		assertEquals("msgs for C: " + list, 5, list.size());

		// check B (should have received none of C's messages)
		map = b.getMsgs();
		assertEquals("we should have no sender at this time", 0, map.size());
		list = map.get(c3.getLocalAddress());
		System.out.println("B: messages received from C: " + list);
		assertNull(list);

		removeDISCARD(c2);

		System.out.println("\nTriggering a manual FLUSH; this will update B with C's 5 messages:");
		boolean rc = c1.startFlush(0, false);
		System.out.println("rc=" + rc);
		c1.stopFlush();

		System.out.println("\nDigests afterC left (FLUSH protocol should have updated B with C's messages)");
		System.out.println("A: " + c1.downcall(Event.GET_DIGEST_EVT));
		System.out.println("B: " + c2.downcall(Event.GET_DIGEST_EVT));

		// check B (should have received all 5 of C's messages, through B as
		// part of the flush phase)
		map = b.getMsgs();
		assertEquals("we should have 1 sender at this time", 1, map.size());
		list = map.get(c3.getLocalAddress());
		System.out.println("B: messages received from C: " + list);
		assertEquals(5, list.size());
	}

	/**
	 * Test scenario:
	 * <ul>
	 * <li>3 members: A,B,C
	 * <li>All members have DISCARD which does <em>not</em> discard any
	 * messages !
	 * <li>B (in DISCARD) ignores all messages from C
	 * <li>C multicasts 5 messages to the cluster, A and C receive them
	 * <li>C then 'crashes' (Channel.shutdown())
	 * <li>Before installing view {A,B}, FLUSH makes A sends its 5 messages
	 * received from C to B
	 * </ul>
	 */
	public void testReconciliationFlushTriggeredByMemberCrashing() throws Exception {
		createMembers();

		insertDISCARD(c2, c3.getLocalAddress());

		printDigests("\nDigests before C sends any messages:");

		// now C sends 5 messages:
		System.out.println("\nC sending 5 messages; B will ignore them, but A and C will receive them");
		for(int i = 1;i <= 5;i++){
			c3.send(null, null, new Integer(i));
		}
		Util.sleep(1000); // until al messages have been received, this is
							// asynchronous so we need to wait a bit

		printDigests("\nDigests after C sent 5 messages:");

		// check C (must have received its own messages)
		Map<Address, List<Integer>> map = c.getMsgs();
		assertEquals("we should have only 1 sender, namely C at this time", 1, map.size());
		List<Integer> list = map.get(c3.getLocalAddress());
		System.out.println("C: messages received from C: " + list);
		assertEquals("msgs for C: " + list, 5, list.size());

		// check A (should have received C's messages)
		map = a.getMsgs();
		assertEquals("we should have only 1 sender, namely C at this time", 1, map.size());
		list = map.get(c3.getLocalAddress());
		System.out.println("A: messages received from C: " + list);
		assertEquals("msgs for C: " + list, 5, list.size());

		// check B (should have received none of C's messages)
		map = b.getMsgs();
		assertEquals("we should have no sender at this time", 0, map.size());
		list = map.get(c3.getLocalAddress());
		System.out.println("B: messages received from C: " + list);
		assertNull(list);

		removeDISCARD(c2);

		// Now kill C
		Address cAddress = c3.getLocalAddress();
		System.out.println("\nKilling C, this will trigger FLUSH and a subsequent view change to {A,B}");
		c3.shutdown();

		// wait until view {A,B} has been installed
		int cnt = 1000;
		View v;
		while((v = c1.getView()) != null && cnt > 0){
			cnt--;
			if(v.size() == 2)
				break;
			Util.sleep(500);
		}
		System.out.println("v=" + v);
		assert v != null;
		assertEquals(2, v.size());

		System.out.println("\nDigests after C left (FLUSH protocol should have updated B with C's messages)");
		System.out.println("A: " + c1.downcall(Event.GET_DIGEST_EVT));
		System.out.println("B: " + c2.downcall(Event.GET_DIGEST_EVT));

		// check B (should have received all 5 of C's messages, through B as
		// part of the flush phase)
		map = b.getMsgs();
		assertEquals("we should have 1 sender at this time", 1, map.size());
		list = map.get(cAddress);
		System.out.println("B: messages received from C: " + list);
		assertEquals(5, list.size());
	}

	private void createMembers() throws ChannelException {
		c1 = createChannel();
		c2 = createChannel();
		c3 = createChannel();
		a = new MyReceiver(c1, "A");
		b = new MyReceiver(c2, "B");
		c = new MyReceiver(c3, "C");
		c1.setReceiver(a);
		c2.setReceiver(b);
		c3.setReceiver(c);
		c1.connect("x");
		c2.connect("x");
		c3.connect("x");

		View v = c3.getView();
		assertEquals("view: " + v, 3, v.size());
	}

	private void printDigests(String message) {
		System.out.println(message);
		System.out.println("A: " + c1.downcall(Event.GET_DIGEST_EVT));
		System.out.println("B: " + c2.downcall(Event.GET_DIGEST_EVT));
		System.out.println("C: " + c3.downcall(Event.GET_DIGEST_EVT));
	}

	private static void insertDISCARD(JChannel ch, Address exclude) throws Exception {
		Properties prop = new Properties();
		prop.setProperty("excludeitself", "true"); // don't discard messages to
													// self
		DISCARD discard = new DISCARD();
		discard.setProperties(prop);
		discard.addIgnoreMember(exclude); // ignore messages from this member
		ch.getProtocolStack().insertProtocol(discard, ProtocolStack.BELOW, "NAKACK");
	}

	private static void removeDISCARD(JChannel... channels) throws Exception {
		for(JChannel ch:channels){
			ch.getProtocolStack().removeProtocol("DISCARD");
		}
	}

	private static class MyReceiver extends ExtendedReceiverAdapter {
		Map<Address, List<Integer>> msgs = new HashMap<Address, List<Integer>>(10);

		Channel channel;

		String name;

		public MyReceiver(Channel ch,String name){
			this.channel = ch;
			this.name = name;
		}

		public Map<Address, List<Integer>> getMsgs() {
			return msgs;
		}

		public void reset() {
			msgs.clear();
		}

		public void receive(Message msg) {
			List<Integer> list = msgs.get(msg.getSrc());
			if(list == null){
				list = new ArrayList<Integer>();
				msgs.put(msg.getSrc(), list);
			}
			list.add((Integer) msg.getObject());
			System.out.println("[" + name
								+ " / "
								+ channel.getLocalAddress()
								+ "]: received message from "
								+ msg.getSrc()
								+ ": "
								+ msg.getObject());
		}

		public void viewAccepted(View new_view) {
			System.out.println("[" + name + " / " + channel.getLocalAddress() + "]: " + new_view);
		}
	}

	public void testVirtualSynchrony() throws Exception {
		c1 = createChannel(CHANNEL_CONFIG);
		Cache cache_1 = new Cache(c1, "cache-1");
		c1.connect("bla");

		c2 = createChannel(CHANNEL_CONFIG);
		Cache cache_2 = new Cache(c2, "cache-2");
		c2.connect("bla");
		assertEquals("view: " + c1.getView(), 2, c2.getView().size());

		// start adding messages
		flush(c1, 5000); // flush all pending message out of the system so
							// everyone receives them

		for(int i = 1;i <= 20;i++){
			if(i % 2 == 0){
				cache_1.put("key-" + i, Boolean.TRUE); // even numbers
			}else{
				cache_2.put("key-" + i, Boolean.TRUE); // odd numbers
			}
		}

		flush(c1, 5000);
		System.out.println("cache_1 (" + cache_1.size()
							+ " elements): "
							+ cache_1
							+ "\ncache_2 ("
							+ cache_2.size()
							+ " elements): "
							+ cache_2);
		assertEquals(cache_1.size(), cache_2.size());
		assertEquals(20, cache_1.size());
	}

	private static void flush(Channel channel, long timeout) {
		if(channel.flushSupported()){
			boolean success = channel.startFlush(timeout, true);
			System.out.println("startFlush(): " + success);
			assertTrue(success);
		}else
			Util.sleep(timeout);
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
			Map<Object, Object> map = new HashMap<Object, Object>();
			map.put("flush_timeout", new Long(0));
			flush.getUpProtocol().up(new Event(Event.CONFIG, map));
			flush.getDownProtocol().down(new Event(Event.CONFIG, map));
		}
		return ret;
	}

	private static class Cache extends ExtendedReceiverAdapter {
		protected final Map<Object, Object> data;

		Channel ch;

		String name;

		public Cache(Channel ch,String name){
			this.data = new HashMap<Object, Object>();
			this.ch = ch;
			this.name = name;
			this.ch.setReceiver(this);
		}

		protected Object get(Object key) {
			synchronized(data){
				return data.get(key);
			}
		}

		protected void put(Object key, Object val) throws Exception {
			Object[] buf = new Object[2];
			buf[0] = key;
			buf[1] = val;
			Message msg = new Message(null, null, buf);
			ch.send(msg);
		}

		protected int size() {
			synchronized(data){
				return data.size();
			}
		}

		public void receive(Message msg) {
			Object[] modification = (Object[]) msg.getObject();
			Object key = modification[0];
			Object val = modification[1];
			synchronized(data){
				// System.out.println("****** [" + name + "] received PUT(" +
				// key + ", " + val + ") " + " from " + msg.getSrc() + "
				// *******");
				data.put(key, val);
			}
		}

		public byte[] getState() {
			byte[] state = null;
			synchronized(data){
				try{
					state = Util.objectToByteBuffer(data);
				}catch(Exception e){
					e.printStackTrace();
					return null;
				}
			}
			return state;
		}

		public byte[] getState(String state_id) {
			return getState();
		}

		public void setState(byte[] state) {
			Map<Object, Object> m;
			try{
				m = (Map<Object, Object>) Util.objectFromByteBuffer(state);
				synchronized(data){
					data.clear();
					data.putAll(m);
				}
			}catch(Exception e){
				e.printStackTrace();
			}
		}

		public void setState(String state_id, byte[] state) {
			setState(state);
		}

		public void getState(OutputStream ostream) {
			ObjectOutputStream oos = null;
			try{
				oos = new ObjectOutputStream(ostream);
				synchronized(data){
					oos.writeObject(data);
				}
				oos.flush();
			}catch(IOException e){
			}finally{
				try{
					if(oos != null)
						oos.close();
				}catch(IOException e){
					System.err.println(e);
				}
			}
		}

		public void getState(String state_id, OutputStream ostream) {
			getState(ostream);
		}

		public void setState(InputStream istream) {
			ObjectInputStream ois = null;
			try{
				ois = new ObjectInputStream(istream);
				Map<Object, Object> m = (Map<Object, Object>) ois.readObject();
				synchronized(data){
					data.clear();
					data.putAll(m);
				}

			}catch(Exception e){
			}finally{
				try{
					if(ois != null)
						ois.close();
				}catch(IOException e){
					System.err.println(e);
				}
			}
		}

		public void setState(String state_id, InputStream istream) {
			setState(istream);
		}

		public void clear() {
			synchronized(data){
				data.clear();
			}
		}

		public void viewAccepted(View new_view) {
			log("view is " + new_view);
		}

		public String toString() {
			synchronized(data){
				return data.toString();
			}
		}

		private void log(String msg) {
			System.out.println("-- [" + name + "] " + msg);
		}

	}

	public static Test suite() {
		return new TestSuite(ReconciliationTest.class);
	}

	public static void main(String[] args) {
		junit.textui.TestRunner.run(ReconciliationTest.suite());
	}
}
