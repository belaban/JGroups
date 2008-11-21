package org.jgroups.tests;


import org.testng.annotations.*;
import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;

import java.io.*;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests concurrent state transfer with flush.
 * 
 * @author bela
 * @version $Id: ConcurrentStateTransferTest.java,v 1.15 2008/11/21 15:47:18 vlada Exp $
 */
@Test(groups={Global.FLUSH},sequential=true)
public class ConcurrentStateTransferTest extends ChannelTestBase {

    private final AtomicInteger mod = new AtomicInteger(1);

    @BeforeMethod
    public void setUp() throws Exception {
        mod.set(1);
    }    

    
    @Test
    public void testConcurrentLargeStateTransfer() {
        concurrentStateTranferHelper(true, false);
    }

    @Test
    public void testConcurrentSmallStateTranfer() {
        concurrentStateTranferHelper(false, true);
    }

    /**
     * Tests concurrent state transfer. This test should pass at 100% rate when
     * [1] is solved.
     * 
     * [1]http://jira.jboss.com/jira/browse/JGRP-332
     * 
     * 
     */
    protected void concurrentStateTranferHelper(boolean largeState, boolean useDispatcher) {
        String[] names=new String[] { "A", "B", "C", "D" };

        int count=names.length;
        ConcurrentStateTransfer[] channels=new ConcurrentStateTransfer[count];

        // Create a semaphore and take all its tickets
        Semaphore semaphore=new Semaphore(count);

        try {

            semaphore.acquire(count);
            // Create activation threads that will block on the semaphore
            for(int i=0;i < count;i++) {
                if(largeState) {
                    if(i == 0)
                        channels[i]=new ConcurrentLargeStateTransfer(names[i],
                                                                     semaphore,
                                                                     useDispatcher);
                    else
                        channels[i]=new ConcurrentLargeStateTransfer((JChannel)channels[0].getChannel(),
                                                                     names[i],
                                                                     semaphore,
                                                                     useDispatcher);

                }
                else {
                    if(i == 0)
                        channels[i]=new ConcurrentStateTransfer(names[i], semaphore, useDispatcher);
                    else
                        channels[i]=new ConcurrentStateTransfer((JChannel)channels[0].getChannel(),
                                                                names[i],
                                                                semaphore,
                                                                useDispatcher);
                }

                // Start threads and let them join the channel
                channels[i].start();
                //Util.sleep(2000);
            }

            // Make sure everyone is in sync

            blockUntilViewsReceived(channels, 60000);

            Util.sleep(2000);
            // Unleash hell !
            semaphore.release(count);

            // Sleep to ensure the threads get all the semaphore tickets
            Util.sleep(2000);

            // Reacquire the semaphore tickets; when we have them all
            // we know the threads are done
            boolean acquired=semaphore.tryAcquire(count, 30, TimeUnit.SECONDS);
            if(!acquired) {
                log.warn("Most likely a bug, analyse the stack below:");
                log.warn(Util.dumpThreads());
            }

            // Sleep to ensure async message arrive
            Util.sleep(2000);

            // do test verification            
            for(ConcurrentStateTransfer channel:channels) {
                log.info(channel.getName() + "=" + channel.getList());
            }
            for(ConcurrentStateTransfer channel:channels) {
                log.info(channel.getName() + "=" + channel.getModifications());
            }

            for(ConcurrentStateTransfer channel:channels) {
                Assert.assertEquals(channel.getList().size(),
                                    count,
                                    channel.getName() + " should have " + count + " elements");
            }
        }
        catch(Exception ex) {
            log.warn("Exception encountered during test", ex);
            assert false:ex.getLocalizedMessage();
        }
        finally {
            for(ConcurrentStateTransfer channel:channels) {
                channel.cleanup();
                Util.sleep(2000); 
            }
            for(ConcurrentStateTransfer channel:channels) {
                checkEventStateTransferSequence(channel);
            }
        }
    }
    
    protected int getMod() {       
        return mod.incrementAndGet();
    } 

    protected class ConcurrentStateTransfer extends PushChannelApplicationWithSemaphore{
        private final List<Address> l = new LinkedList<Address>();

        Channel ch; 

        private final Map<Integer,Object> mods = new TreeMap<Integer,Object>();       
      

        public ConcurrentStateTransfer(String name,Semaphore semaphore,boolean useDispatcher) throws Exception{
            super(name, semaphore, useDispatcher);
            channel.connect("ConcurrentStateTransfer");
        }
        
        public ConcurrentStateTransfer(JChannel ch, String name,Semaphore semaphore,boolean useDispatcher) throws Exception{
            super(ch,name, semaphore, useDispatcher);
            channel.connect("ConcurrentStateTransfer");
        }

        public void useChannel() throws Exception {
            boolean success = channel.getState(null, 30000);
            log.info("channel.getState at " + getName()
                     + getLocalAddress()
                     + " returned "
                     + success);
            channel.send(null, null, channel.getLocalAddress());
        }       
        
        List<Address> getList() {
            return l;
        }

        Map<Integer,Object> getModifications() {
            return mods;
        }

        public void receive(Message msg) {
            if(msg.getBuffer() == null)
                return;
            Address obj = (Address)msg.getObject();
            log.info("-- [#" + getName() + " (" + channel.getLocalAddress() + ")]: received " + obj);
            synchronized(this){
                l.add(obj);
                Integer key = new Integer(getMod());
                mods.put(key, obj);
            }
        }

        public void viewAccepted(View new_view) {
            super.viewAccepted(new_view);
            synchronized(this){
                Integer key = new Integer(getMod());
                mods.put(key, new_view.getVid());
            }
        }

        public void setState(byte[] state) {
            super.setState(state);
            try{
                List<Address> tmp = (List) Util.objectFromByteBuffer(state);
                synchronized(this){
                    l.clear();
                    l.addAll(tmp);
                    log.info("-- [#" + getName()
                             + " ("
                             + channel.getLocalAddress()
                             + ")]: state is "
                             + l);
                    Integer key = new Integer(getMod());
                    mods.put(key, tmp);
                }
            }catch(Exception e){
                e.printStackTrace();
            }
        }

        public byte[] getState() {
            super.getState();
            List<Address> tmp = null;
            synchronized(this){
                tmp = new LinkedList<Address>(l);
                try{
                    return Util.objectToByteBuffer(tmp);
                }catch(Exception e){
                    e.printStackTrace();
                    return null;
                }
            }
        }

        public void getState(OutputStream ostream) {
            super.getState(ostream);
            ObjectOutputStream oos = null;
            try{
                oos = new ObjectOutputStream(ostream);
                List<Address> tmp = null;
                synchronized(this){
                    tmp = new LinkedList<Address>(l);
                }
                oos.writeObject(tmp);
                oos.flush();
            }catch(IOException e){
                e.printStackTrace();
            }finally{
                Util.close(oos);
            }
        }

        public void setState(InputStream istream) {
            super.setState(istream);
            ObjectInputStream ois = null;
            try{
                ois = new ObjectInputStream(istream);
                List<Address> tmp = (List) ois.readObject();
                synchronized(this){
                    l.clear();
                    l.addAll(tmp);
                    log.info("-- [#" + getName()
                             + " ("
                             + channel.getLocalAddress()
                             + ")]: state is "
                             + l);
                    Integer key = new Integer(getMod());
                    mods.put(key, tmp);
                }
            }catch(Exception e){
                e.printStackTrace();
            }finally{
                Util.close(ois);
            }
        }
    }

    protected class ConcurrentLargeStateTransfer extends ConcurrentStateTransfer {
    	//depends on retry_timeout parameter in FLUSH
        private static final long TRANSFER_TIME = 1000;
        public ConcurrentLargeStateTransfer(String name,Semaphore semaphore,boolean useDispatcher) throws Exception{
            super(name, semaphore, useDispatcher);
        }
        
        public ConcurrentLargeStateTransfer(JChannel channel,String name,Semaphore semaphore,boolean useDispatcher) throws Exception{
            super(channel,name, semaphore, useDispatcher);
        }
        
        public void setState(byte[] state) {
            Util.sleep(TRANSFER_TIME);
            super.setState(state);
        }

        public byte[] getState() {
            Util.sleep(TRANSFER_TIME);
            return super.getState();
        }

        public void getState(OutputStream ostream) {
            Util.sleep(TRANSFER_TIME);
            super.getState(ostream);
        }

        public void setState(InputStream istream) {
            Util.sleep(TRANSFER_TIME);
            super.setState(istream);
        }
    }
    
}
