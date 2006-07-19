package org.jgroups.tests;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.jgroups.Channel;
import org.jgroups.ChannelClosedException;
import org.jgroups.ChannelNotConnectedException;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.StreamingGetStateEvent;
import org.jgroups.StreamingMessageListener;
import org.jgroups.StreamingSetStateEvent;
import org.jgroups.TimeoutException;
import org.jgroups.View;
import org.jgroups.util.Util;

/**
 *
 * 
 * @author Vladimir Blagojevic
 * @version $Id$ 
 *
 */
public class StreamingStateTransferTest extends TestCase {

	private final static String CHANNEL_PROPS="streaming-state-transfer.xml";
	private final static int INITIAL_NUMBER_OF_MEMBERS=5;
	private int runningTime = 1000*60*1; //1 minute
	private Random r = new Random();
	
	private final static int MEGABYTE = 1048576;
	
	
	public StreamingStateTransferTest(String arg0) {
		super(arg0);
	}
	
	public void testTransfer() throws Exception
	{
		long start = System.currentTimeMillis();
		boolean running=true;
		List members=new ArrayList();			
		
		//first spawn and join
		for(int i =0;i<INITIAL_NUMBER_OF_MEMBERS;i++)
		{
			GroupMember member = new GroupMember("Member");			
			members.add(member);
			Thread t = new Thread(member);
			t.start();
			Util.sleep(getRandomDelayInSeconds(15,16)*1000);
		}
		
		for (; running;) {						
			
			//and then flip a coin
			if(r.nextBoolean())
			{
				Util.sleep(getRandomDelayInSeconds(15,16)*1000);
				GroupMember member = new GroupMember("Member");					
				members.add(member);				
				Thread t = new Thread(member);
				t.start();
			}
			else if(members.size()>1)
			{
				Util.sleep(getRandomDelayInSeconds(3,8)*1000);
				GroupMember unluckyBastard = (GroupMember) members.get(r.nextInt(members.size()));
				members.remove(unluckyBastard);				
				unluckyBastard.setRunning(false);				
			}				
			running = System.currentTimeMillis()-start>runningTime?false:true;
			System.out.println("Running time " + ((System.currentTimeMillis()-start)/1000) + " secs");
		}
		System.out.println("Done");
	}
	
	protected int getRandomDelayInSeconds(int from,int to)
	{
		return from + r.nextInt(to-from);
	}

	protected void setUp() throws Exception {
		super.setUp();
	}

	protected void tearDown() throws Exception {
		super.tearDown();
	}
	
	public static Test suite() {
	      return new TestSuite(StreamingStateTransferTest.class);
	}
	 
	public static void main(String[] args) {
		 String[] testCaseName={StreamingStateTransferTest.class.getName()};
	     junit.textui.TestRunner.main(testCaseName);
	}
	
	private static class GroupMember implements Runnable,StreamingMessageListener{
		JChannel ch = null;		
		View currentView;		
		volatile boolean running = true;		
		private int stateSize;	
		private int bufferSize = 8*1024;

		public GroupMember(String name) {				
			setStateSize(1024*MEGABYTE); //1GB
		}

		public String getAddress() {
			if(ch!=null && ch.isConnected())
			{
				return ch.getLocalAddress().toString();
			}
			return null;
		}

		public void setRunning(boolean b) {
			running=false;	
			System.out.println("Disconnect " + getAddress());
			if(ch!=null)ch.close();
		}		

		public void setStateSize(int stateSize) {
			this.stateSize = stateSize;
		}

		public void run() {
			try {
				ch = new JChannel(CHANNEL_PROPS);
				ch.setOpt(Channel.AUTO_RECONNECT, Boolean.TRUE);
	            ch.setOpt(Channel.AUTO_GETSTATE, Boolean.TRUE);
				ch.connect("transfer");				
				ch.getState(null,5000);
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			while (running) {
				Object msgReceived = null;
				try {
					msgReceived = ch.receive(0);
					if (!running) {
						// I am not a group member anymore so
						// I will discard any transient message I
						// receive
					} else {
						if (msgReceived instanceof View) {
							gotView(msgReceived);
						} else if (msgReceived instanceof StreamingGetStateEvent) {
							StreamingGetStateEvent evt = (StreamingGetStateEvent) msgReceived;
							this.getState(evt.getArg());
						} else if (msgReceived instanceof StreamingSetStateEvent) {
							StreamingSetStateEvent evt = (StreamingSetStateEvent) msgReceived;
							this.setState(evt.getArg());
						}
					}

				} catch (TimeoutException e) {
				} catch (Exception e) {
					ch.close();
					running = false;
				}			
			}					
		}

		private void gotView(Object msg) throws ChannelNotConnectedException, ChannelClosedException {
			
		}

		public void getState(OutputStream ostream) {			
			InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream("org/jgroups/JChannel.class");
			System.out.println(getAddress() +" is sending state of " + (stateSize/MEGABYTE) + " MB");
			int markSize = 1024*100; //100K should be enough
			byte buffer [] = new byte[bufferSize];
			int bytesRead=-1;	
			int size = stateSize;
			try {															
				while(size>0)
				{
					stream.mark(markSize);
					bytesRead=stream.read(buffer);
					ostream.write(buffer);
					stream.reset();									
					size = size-bytesRead;					
				}					
			} catch (IOException e) {				
				e.printStackTrace();
			}	
			finally
			{				
				try {
					ostream.flush();
					ostream.close();
				} catch (IOException e) {					
					e.printStackTrace();
				}				
			}
		}
		
		public byte[] getState()
		{
			return null;
		}

		public void setState(InputStream istream) {
			int totalRead=0;
			byte buffer [] = new byte[bufferSize];
			int bytesRead=-1;
			long start = System.currentTimeMillis();
			try {
				while((bytesRead=istream.read(buffer))>=0)
				{					
					totalRead+=bytesRead;
				}								
			} catch (IOException e) {				
				e.printStackTrace();
			}
			finally
			{
				try {
					istream.close();
				} catch (IOException e) {					
					e.printStackTrace();
				}
			}
			long readingTime = System.currentTimeMillis()-start;
			System.out.println(getAddress() + " read state of " + (totalRead / MEGABYTE)
					+ " MB in " + readingTime + " msec");						
		}

		public void receive(Message msg) {
			// TODO Auto-generated method stub
			
		}

		public void setState(byte[] state) {
			// TODO Auto-generated method stub
			
		}
	}
}
