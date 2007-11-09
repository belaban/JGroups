package org.jgroups.tests;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.jgroups.Message;
import org.jgroups.util.Util;

/**
 * Tests correct state transfer while other members continue sending messages to
 * the group
 * 
 * @author Bela Ban
 * @version $Id: StateTransferTest.java,v 1.18 2007/11/09 01:59:40 vlada Exp $
 */
public class StateTransferTest extends ChannelTestBase {
	private static final int MSG_SEND_COUNT = 10000;
	private static final int APP_COUNT = 2;

	public StateTransferTest(String name){
		super(name);
	}
	
	public void setUp() throws Exception {
        super.setUp();        
        CHANNEL_CONFIG = System.getProperty("channel.conf.flush", "flush-udp.xml");
    }

    public boolean useBlocking() {
        return true;
    }

	public void testStateTransferWhileSending() throws Exception {
		StateTransferApplication[] apps = new StateTransferApplication[APP_COUNT];

		// Create a semaphore and take all its permits
		Semaphore semaphore = new Semaphore(APP_COUNT);
		semaphore.acquire(APP_COUNT);

		int from = 0, to = MSG_SEND_COUNT;
		String[] names = createApplicationNames(APP_COUNT);
		if(isMuxChannelUsed()){
		    names = createMuxApplicationNames(2, 2);
		}
		    
		for(int i = 0;i < apps.length;i++){
			apps[i] = new StateTransferApplication(semaphore, names[i], from, to);
			from += MSG_SEND_COUNT;
			to += MSG_SEND_COUNT;
		}

		for(int i = 0;i < apps.length;i++){
			StateTransferApplication app = apps[i];
			app.start();						
			semaphore.release();
			Util.sleep(2500);
		}

		// Reacquire the semaphore tickets; when we have them all
		// we know the threads are done
		semaphore.tryAcquire(APP_COUNT, 40, TimeUnit.SECONDS);
		
		
		Util.sleep(1000);
		//have we received all and the correct messages?
		for(int i = 0;i < apps.length;i++){
			StateTransferApplication w = apps[i];
			Map m = w.getMap();
			log.info("map has " + m.size() + " elements");
			assertEquals(MSG_SEND_COUNT * APP_COUNT, m.size());
		}

		Set keys = apps[0].getMap().keySet();
		for(int i = 0;i < apps.length;i++){
			StateTransferApplication app = apps[i];
			Map m = app.getMap();
			Set s = m.keySet();
			assertEquals(keys, s);
		}
		
		for(StateTransferApplication app:apps){
			app.cleanup();
		}
	}

	protected int getMuxFactoryCount() {
		//one MuxChannel per real Channel
		return APP_COUNT;
	}

	protected class StateTransferApplication extends PushChannelApplicationWithSemaphore {
		private final ReentrantLock mapLock = new ReentrantLock();
		private Map map = new HashMap(MSG_SEND_COUNT * APP_COUNT);
		private int from, to;

		public StateTransferApplication(Semaphore semaphore,String name,int from,int to) throws Exception{
			super(name, semaphore);
			this.from = from;
			this.to = to;
		}

		public Map getMap() {
			Map result = null;
			mapLock.lock();
			result = Collections.unmodifiableMap(map);
			mapLock.unlock();
			return result;
		}
		
		@Override
		public void receive(Message msg) {
			Object[] data = (Object[]) msg.getObject();
			mapLock.lock();
			map.put(data[0], data[1]);
			int num_received = map.size();
			mapLock.unlock();
			
			if(num_received % 1000 == 0)
				log.info("received " + num_received);
			
			//are we done?
			if(num_received >= MSG_SEND_COUNT * APP_COUNT)
				semaphore.release();
		}

		@Override
		public byte[] getState() {
			byte[] result = null;
			mapLock.lock();
			try{
				result = Util.objectToByteBuffer(map);
			}catch(Exception e){
				e.printStackTrace();
			}
			finally{
				mapLock.unlock();
			}
			return result;
		}		

		@Override
		public void setState(byte[] state) {
			mapLock.lock();
			try{
				map = (Map) Util.objectFromByteBuffer(state);
			}catch(Exception e){
				e.printStackTrace();
			}
			finally{
				mapLock.unlock();
			}
			log.info("received state, map has " + map.size() + " elements");
			
		}
		@Override
		public void getState(OutputStream ostream) {
			ObjectOutputStream out;
			mapLock.lock();
			try{
				out = new ObjectOutputStream(ostream);
				out.writeObject(map);				
				out.close();
			}catch(IOException e){				
				e.printStackTrace();
			}
			finally{
				mapLock.unlock();
			}
			
		}

		@Override
		public void setState(InputStream istream) {
			ObjectInputStream in;
			mapLock.lock();
			try{
				in = new ObjectInputStream(istream);
				map = (Map) in.readObject();
				log.info("received state, map has " + map.size() + " elements");
				in.close();
			}catch(IOException e){
				e.printStackTrace();
			}catch(ClassNotFoundException e){
				e.printStackTrace();
			}
			finally{
				mapLock.unlock();
			}
		}

		@Override
		protected void useChannel() throws Exception {
			channel.connect("StateTransferTest-Group");
			channel.getState(null, 10000);
			Object[] data = new Object[2];
			for(int i = from;i < to;i++){
				data[0] = new Integer(i);
				data[1] = "Value #" + i;
				try{
					channel.send(null, null, data);
					if(i % 1000 == 0)
						log.info("sent " + i);
				}catch(Exception e){
					e.printStackTrace();
					break;
				}
			}
		}

		public void run() {
			boolean acquired = false;
			try{
				acquired = semaphore.tryAcquire(60000L, TimeUnit.MILLISECONDS);
				if(!acquired){
					throw new Exception(name + " cannot acquire semaphore");
				}
				useChannel();
			}catch(Exception e){
				log.error(name + ": " + e.getLocalizedMessage(), e);
				// Save it for the test to check
				exception = e;
			}
		}
	}
	
	public static Test suite() {
		return new TestSuite(StateTransferTest.class);
	}

	public static void main(String[] args) {
		junit.textui.TestRunner.run(suite());
	}
}