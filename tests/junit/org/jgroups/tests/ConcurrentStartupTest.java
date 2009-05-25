package org.jgroups.tests;


import org.testng.annotations.*;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.MergeView;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.Global;
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
 * Tests concurrent startup with state transfer.
 * 
 * @author bela
 * @version $Id: ConcurrentStartupTest.java,v 1.51 2009/05/25 07:06:17 vlada Exp $
 */
@Test(groups={Global.FLUSH},sequential=true)
public class ConcurrentStartupTest extends ChannelTestBase {
    private AtomicInteger mod = new AtomicInteger(1);


    @BeforeMethod
    protected void setUp() throws Exception {
        mod.set(1);
    }

    public void testConcurrentStartupLargeState() {
        concurrentStartupHelper(true, false);
    }

    public void testConcurrentStartupSmallState() {
        concurrentStartupHelper(false, true);
    }

    /**
     * Tests concurrent startup and message sending directly after joining See
     * doc/design/ConcurrentStartupTest.txt for details. This will only work
     * 100% correctly once we have FLUSH support (JGroups 2.4)
     *
     * NOTE: This test is not guaranteed to pass at 100% rate until combined
     * join and state transfer using one FLUSH phase is introduced (Jgroups
     * 2.5)[1].
     *
     * [1] http://jira.jboss.com/jira/browse/JGRP-236
     *
     *
     */
    protected void concurrentStartupHelper(boolean largeState, boolean useDispatcher) {
        String[] names=null;

        names=new String[] { "A", "B", "C", "D" };

        int count=names.length;

        ConcurrentStartupChannel[] channels=new ConcurrentStartupChannel[count];
        try {
            // Create a semaphore and take all its permits
            Semaphore semaphore=new Semaphore(count);
            semaphore.acquire(count);

            // Create activation threads that will block on the semaphore
            for(int i=0;i < count;i++) {
                if(largeState) {
                    if(i == 0)
                        channels[i]=new ConcurrentStartupChannelWithLargeState(semaphore,
                                                                               names[i],
                                                                               useDispatcher);
                    else
                        channels[i]=new ConcurrentStartupChannelWithLargeState((JChannel)channels[0].getChannel(),
                                                                               semaphore,
                                                                               names[i],
                                                                               useDispatcher);
                }
                else {
                    if(i == 0)
                        channels[i]=new ConcurrentStartupChannel(names[i], semaphore, useDispatcher);
                    else
                        channels[i]=new ConcurrentStartupChannel((JChannel)channels[0].getChannel(),
                                                                 names[i],
                                                                 semaphore,
                                                                 useDispatcher);
                }

                // Release one ticket at a time to allow the thread to start
                // working
                channels[i].start();
                semaphore.release(1);
                //sleep a second and half thus avoiding merges
                Util.sleep(1500);
            }

            // Make sure everyone is in sync

            blockUntilViewsReceived(channels, 60000);

            // Sleep to ensure the threads get all the semaphore tickets
            Util.sleep(2000);

            // Reacquire the semaphore tickets; when we have them all
            // we know the threads are done
            boolean acquired=semaphore.tryAcquire(count, 20, TimeUnit.SECONDS);
            if(!acquired) {
                log.warn("Most likely a bug, analyse the stack below:");
                log.warn(Util.dumpThreads());
            }

            // Sleep to ensure async message arrive
            Util.sleep(3000);

            // do test verification
            for(ConcurrentStartupChannel channel:channels) {
                log.info(channel.getName() + "=" + channel.getList());
            }
            for(ConcurrentStartupChannel channel:channels) {
                log.info(channel.getName() + "=" + channel.getModifications());
            }
            
            for (ConcurrentStartupChannel channel : channels) {
                if (!channel.hasReceivedMergeView()) {
                    Assert.assertEquals(channel.getList().size(), count, channel.getName()
                                    + " should have " + count + " elements");
                }
            }
        }
        catch(Exception ex) {
            log.warn("Exception encountered during test", ex);
            assert false:ex.getLocalizedMessage();
        }
        finally {
            for(ConcurrentStartupChannel channel:channels) {
                channel.cleanup();
                Util.sleep(2000); // remove before 2.6 GA
            }

            for (ConcurrentStartupChannel channel : channels) {
                if (!channel.hasReceivedMergeView()) {
                    checkEventStateTransferSequence(channel);
                }
            }
        }
    }

    protected int getMod() {
        return mod.incrementAndGet();
    }

    protected class ConcurrentStartupChannelWithLargeState extends ConcurrentStartupChannel {

    	//depends on retry_timeout parameter in FLUSH
    	private static final long TRANSFER_TIME = 1000;
        public ConcurrentStartupChannelWithLargeState(Semaphore semaphore,
                                                      String name,
                                                      boolean useDispatcher) throws Exception{
            super(name, semaphore, useDispatcher);
        }

        public ConcurrentStartupChannelWithLargeState(JChannel ch, Semaphore semaphore,
                                                      String name,
                                                      boolean useDispatcher) throws Exception{
            super(ch,name, semaphore, useDispatcher);
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

    protected class ConcurrentStartupChannel extends PushChannelApplicationWithSemaphore {
        private boolean gotMergeView = false;
        private final List<Address> l = new LinkedList<Address>();

        private final Map<Integer,Object> mods = new TreeMap<Integer,Object>();

        public ConcurrentStartupChannel(String name,Semaphore semaphore,boolean useDispatcher) throws Exception{
            super(name, semaphore, useDispatcher);
        }

        public ConcurrentStartupChannel(JChannel ch,String name,Semaphore semaphore,boolean useDispatcher) throws Exception{
            super(ch,name, semaphore, useDispatcher);
        }

        public void useChannel() throws Exception {
            channel.connect("test", null, null, 25000);
            channel.send(null, null, channel.getAddress());
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
            log.info("-- [#" + getName() + " (" + channel.getAddress() + ")]: received " + obj);
            synchronized(this){
                l.add(obj);
                Integer key = new Integer(getMod());
                mods.put(key, obj);
            }
        }

        public void viewAccepted(View new_view) {
            super.viewAccepted(new_view);
            gotMergeView = (!gotMergeView && new_view instanceof MergeView)?true:false;                           
            synchronized(this){
                Integer key = new Integer(getMod());
                mods.put(key, new_view.getVid());
            }
        }
        
        public boolean hasReceivedMergeView(){
            return gotMergeView;            
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
                             + channel.getAddress()
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
                             + channel.getAddress()
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


}