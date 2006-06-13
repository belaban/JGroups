package org.jgroups.tests;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.jgroups.Address;
import org.jgroups.ChannelClosedException;
import org.jgroups.ChannelNotConnectedException;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.TimeoutException;
import org.jgroups.View;
import org.jgroups.ViewId;
import org.jgroups.util.Util;

/**
 * Virtual Synchrony guarantees that views in a group are observed 
 * in the same order by all group members and that views are totally ordered 
 * with respect to all regular messages in a group. 
 * 
 * Therefore, in virtually synchronous group communication model, all members 
 * that observe the same two consecutive views, receive the same set of regular 
 * multicast messages between those two views.
 * 
 * VirtualSynchronyTest verifies virtual synchrony as follows. Each surviving member P  
 * from a view Vm that receives a new view Vn sends a message M to group coordinator Q 
 * containing number of messages received in view Vm. Each member P upon receiving 
 * a new view sends a random number of messages to everyone in the group.
 * 
 * Group coordinator Q upon receiving each message M from a member P verifies 
 * if virtual synchrony is satisifed.
 * 
 * 
 * @author Vladimir Blagojevic
 * @version $Id$ 
 *
 */
public class VirtualSynchronyTest extends TestCase {

	private final static String CHANNEL_PROPS="fc-fast-minimalthreads.xml";
	private final static int INITIAL_NUMBER_OF_MEMBERS=5;
	private int runningTime = 1000*60*5; //5 minutes
	
	
	public VirtualSynchronyTest(String arg0) {
		super(arg0);
	}
	
	public void testVSynch() throws Exception
	{
		long start = System.currentTimeMillis();
		boolean running=true;
		List members=new ArrayList();	
		Random r = new Random();
		
		//first spawn and join
		for(int i =0;i<INITIAL_NUMBER_OF_MEMBERS;i++)
		{
			GroupMemberThread member = new GroupMemberThread("Member");
			member.start();			
			members.add(member);					
			Util.sleep(5000);
		}
		
		
		for (; running;) {						
			
			//and then flip a coin
			if(r.nextBoolean())
			{
				GroupMemberThread member = new GroupMemberThread("Member");				
				member.start();			
				members.add(member);
				Util.sleep(5000);
			}
			else if(members.size()>1)
			{
				GroupMemberThread unluckyBastard = (GroupMemberThread) members.get(r.nextInt(members.size()));
				members.remove(unluckyBastard);				
				unluckyBastard.setRunning(false);
				Util.sleep(5000);
			}	
			else
			{
				Util.sleep(1000);
			}
			running = System.currentTimeMillis()-start>runningTime?false:true;
			System.out.println("Running time " + ((System.currentTimeMillis()-start)/1000) + " secs");
		}
		System.out.println("Done, Virtual Synchrony satisfied in all tests ");
	}

	protected void setUp() throws Exception {
		super.setUp();
	}

	protected void tearDown() throws Exception {
		super.tearDown();
	}
	
	public static Test suite() {
	      return new TestSuite(VirtualSynchronyTest.class);
	}
	 
	public static void main(String[] args) {
		 String[] testCaseName={VirtualSynchronyTest.class.getName()};
	     junit.textui.TestRunner.main(testCaseName);
	}
	
	private static class GroupMemberThread extends Thread {
		JChannel ch = null;
		int numberOfMessagesInView = 0;
		View currentView;
		View prevView;
		List payloads;
		VSynchPayload payload;
		volatile boolean running = true;
		Random r;
		int messagesSentPerView = 0;

		public GroupMemberThread(String name) {
			super(name);			
			payloads = new ArrayList();
			r = new Random();
			messagesSentPerView = r.nextInt(25);
		}

		public String getAddress() {
			if(ch!=null && ch.isConnected())
			{
				return ch.getLocalAddress().toString();
			}
			else
			{
				return "disconnected " + getName();
			}
		}

		public void setRunning(boolean b) {
			running=false;	
			System.out.println("Disconnect " + getAddress());
			if(ch!=null)ch.disconnect();
		}

		public void run() {
			try {
				ch = new JChannel(CHANNEL_PROPS);
				ch.connect("vsynchtest");
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			while (running) {
				Object msgReceived = null;				
				try {
					msgReceived = ch.receive(500);
					if (msgReceived instanceof View) {
						gotView(msgReceived);
					}

					if (msgReceived instanceof Message) {
						gotMessage(msgReceived);
					}
				} catch (TimeoutException e) {
				} catch (Exception e) {					
					ch.disconnect();
					running=false;
				}			
			}					
		}

		private void gotMessage(Object msgReceived) {
			Message msg = (Message) msgReceived;
			Object m = msg.getObject();

			if (m instanceof VSynchPayload) {
				VSynchPayload pay = (VSynchPayload) m;
				if (prevView != null && prevView.getVid().equals(pay.viewId)) {
					payloads.add(pay);
					boolean receivedAllPayloads = ((payloads.size() == prevView
							.getMembers().size()) || (payloads.size() == currentView
							.getMembers().size()));
					if (receivedAllPayloads) {												
						VSynchPayload first=(VSynchPayload) payloads.get(0);
						for (Iterator i = payloads.listIterator(1); i.hasNext();) {
							VSynchPayload p = (VSynchPayload) i.next();
							assertEquals("Member " + p + " and " + first
									+ " failed VS", first.msgViewCount,
									p.msgViewCount);
						}
						System.out.println("VS ok, all " + payloads.size()
								+ " members in " + prevView.getVid()
								+ " view have received " + first.msgViewCount
								+ " messages");
					}
				}
			} else if (m instanceof String) {
				numberOfMessagesInView++;
			}
		}

		private void gotView(Object msg) throws ChannelNotConnectedException, ChannelClosedException {
			View tmpView = (View) msg;			
			if (currentView != null) {
				payload = new VSynchPayload(currentView.getVid(),
						numberOfMessagesInView,ch.getLocalAddress());
				ch.send((Address)tmpView.getMembers().get(0), null, payload);
			}
			numberOfMessagesInView = 0;
			payloads.clear();
			prevView = currentView;
			currentView = tmpView;
			// send our allotment of messages
			for (int i = 0; i < messagesSentPerView; i++) {
				ch.send(null, null, "vsynchtest");
			}
		}
	}

	private static class VSynchPayload implements Serializable {
		public ViewId viewId;

		public int msgViewCount;

		public Address member;

		public VSynchPayload(ViewId viewId, int numbreOfMessagesInView,Address a) {
			super();
			this.viewId = viewId;
			this.msgViewCount = numbreOfMessagesInView;
			this.member=a;
		}

		public String toString() {
			return "[member=" +member + ",viewId=" + viewId.getId() + ",msgCount=" + msgViewCount
					+ "]";
		}

	}
}
