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

import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.ExtendedReceiver;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.StreamingGetStateEvent;
import org.jgroups.StreamingSetStateEvent;
import org.jgroups.TimeoutException;
import org.jgroups.View;
import org.jgroups.ViewId;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.util.Util;

/**
 * Tests streaming state transfer for both pull and push mode of channel 
 * operations. Size of the transfer is configurable. Test runner should 
 * specify "pull" and "size" parameter as JVM parameters when running this 
 * test. If not specified default values are to use push mode and transfer 
 * size of 100 MB. 
 * 
 *  <p>
 *  
 *  To specify pull mode and size transfer of 500 MB test runner should pass 
 *  JVM parameters:
 *  
 *  <p>
 *  -Dpull=true -Dsize=500
 *
 * 
 * @author Vladimir Blagojevic
 * @version $Id$ 
 *
 */
public class StreamingStateTransferTest extends TestCase{

	private final static String CHANNEL_PROPS="streaming-state-transfer.xml";
	private final static int INITIAL_NUMBER_OF_MEMBERS=5;
	private int runningTime = 1000*50; // 50 secs 
	private Random r = new Random();
	private boolean usePullMode = false;
    private boolean useDisp = false;
	private int size = 100; //100MB
	
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
			GroupMember member = new GroupMember(usePullMode,useDisp,size);			
			members.add(member);
			Thread t = new Thread(member);
			t.start();
			Util.sleep(getRandomDelayInSeconds(10,12)*1000);
		}
		
		for (; running;) {						
			
			//and then flip a coin
			if(r.nextBoolean())
			{
				Util.sleep(getRandomDelayInSeconds(10,12)*1000);
				GroupMember member = new GroupMember(usePullMode,useDisp,size);					
				members.add(member);				
				Thread t = new Thread(member);
				t.start();
			}
			else if(members.size()>1)
			{
				Util.sleep(getRandomDelayInSeconds(3,8)*1000);
				GroupMember unluckyBastard = (GroupMember) members.get(r.nextInt(members.size()));
				if(!unluckyBastard.isCoordinator())
				{
					members.remove(unluckyBastard);				
					unluckyBastard.setRunning(false);					
				}
				else
				{
					System.out.println("Not killing coordinator ");
				}
			}				
			running =System.currentTimeMillis() - start <= runningTime;
			System.out.println("Running time " + ((System.currentTimeMillis()-start)/1000) + " secs");
		}
		System.out.println("Done");
	}
	
	protected int getRandomDelayInSeconds(int from,int to)
	{
		return from + r.nextInt(to-from);
	}

	protected void setUp() throws Exception {
		
		//NOTE use -Ddisp=true|false -Dpull=true|false -Dsize=int (size of transfer)
       
        String prop = System.getProperty("disp");
        if(prop!=null)
        {
            useDisp = prop.equalsIgnoreCase("true");
            System.out.println("Using parameter disp=" + useDisp);
        }
		prop = System.getProperty("pull");
		if(prop!=null)
		{
			usePullMode = prop.equalsIgnoreCase("true");
			System.out.println("Using parameter usePullMode=" + usePullMode);
		}
		
		prop = System.getProperty("size");
		if(prop!=null)
		{
			size = Integer.parseInt(System.getProperty("size"));
			System.out.println("Using parameter size=" + size);
		}
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
	
	private static class GroupMember implements Runnable,ExtendedReceiver{
		JChannel ch = null;		
		View currentView;		
		volatile boolean running = true;		
		private int stateSize;	
		private int bufferSize = 8*1024;
		private boolean usePullMode;	
		private Random ran = new Random();
        private boolean useDispacher;

		public GroupMember(boolean pullMode,boolean dispMode,int size) {				
			setStateSize(size*MEGABYTE);
			setUsePullMode(pullMode);
            setUseDispatcher(dispMode);
		}
		
		public void setUsePullMode(boolean usePullMode) {
			this.usePullMode = usePullMode;
		}
        
        public void setUseDispatcher(boolean useDispacher) {
            this.useDispacher = useDispacher;
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
		
		protected boolean isCoordinator() {
			if (ch == null)
				return false;
			Object local_addr = ch.getLocalAddress();
			if (local_addr == null)
				return false;
			View view = ch.getView();
			if (view == null)
				return false;
			ViewId vid = view.getVid();
			if (vid == null)
				return false;
			Object coord = vid.getCoordAddress();
			if (coord == null)
				return false;
			return local_addr.equals(coord);
		}

		public void setStateSize(int stateSize) {
			this.stateSize = stateSize;
		}

		public void run() {
			try {
				ch = new JChannel(CHANNEL_PROPS);
				ch.setOpt(Channel.AUTO_RECONNECT, Boolean.TRUE);
	            ch.setOpt(Channel.AUTO_GETSTATE, Boolean.TRUE);
                if(useDispacher)
                {
                   RpcDispatcher disp = new RpcDispatcher(ch,this,this,this);                  
                } else if (!usePullMode)
	            {                   
	            	ch.setReceiver(this);
	            }                
				ch.connect("transfer");	
				if(ran.nextBoolean())
				{
					ch.getState(null,5000);
				}
				else
				{
					String randomStateId = Long.toString(Math.abs(ran.nextLong()), 36);
					ch.getState(null,randomStateId,5000);
				}
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
						} else if (msgReceived instanceof StreamingGetStateEvent) {
							StreamingGetStateEvent evt = (StreamingGetStateEvent) msgReceived;
							if(evt.getStateId()!=null)
							{
								this.getState(evt.getStateId(),evt.getArg());	
							}
							else
							{
								this.getState(evt.getArg());
							}
						} else if (msgReceived instanceof StreamingSetStateEvent) {
							StreamingSetStateEvent evt = (StreamingSetStateEvent) msgReceived;
							if(evt.getStateId()!=null)
							{
								this.setState(evt.getStateId(),evt.getArg());
							}
							else
							{
								this.setState(evt.getArg());
							}
						}
					}

				} catch (TimeoutException e) {
				} catch (Exception e) {
					ch.close();
					running = false;
				}			
			}					
		}
	
		public void getState(OutputStream ostream) {			
			InputStream stream = Thread.currentThread().getContextClassLoader()
					.getResourceAsStream("org/jgroups/JChannel.class");
			System.out.println(Thread.currentThread() + " at " + getAddress()
					+ " is sending state of " + (stateSize / MEGABYTE) + " MB");
			
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
			System.out.println(Thread.currentThread() + " at " + getAddress()
					+ " read state of " + (totalRead / MEGABYTE) + " MB in "
					+ readingTime + " msec");						
		}

		public void receive(Message msg) {		
		}

		public void setState(byte[] state) {			
		}

		public void viewAccepted(View new_view) {			
		}

		public void suspect(Address suspected_mbr) {		
		}

		public void block() {			
		}

		public byte[] getState() {			
			return null;
		}
		
		public byte[] getState(String state_id) {		
			return null;
		}

		public void setState(String state_id, byte[] state) {						
		}

		public void getState(String state_id, OutputStream ostream) {
			System.out.println("Writing partial streaming state transfer for " + state_id);
			getState(ostream);
		}

		public void setState(String state_id, InputStream istream) {
			System.out.println("Reading partial streaming state transfer for " + state_id);
			setState(istream);			
		}		
	}
}
