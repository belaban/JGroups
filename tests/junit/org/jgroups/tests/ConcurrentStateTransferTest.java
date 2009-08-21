package org.jgroups.tests;


import org.jgroups.*;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.*;
import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Tests concurrent state transfer with flush.
 * 
 * @author bela
 * @version $Id: ConcurrentStateTransferTest.java,v 1.17 2009/08/21 05:53:21 belaban Exp $
 */
@Test(groups={Global.FLUSH},sequential=true)
public class ConcurrentStateTransferTest extends ChannelTestBase {


    /**
     * Tests concurrent state transfer. This test should pass at 100% rate when [1] is solved.
     * [1]http://jira.jboss.com/jira/browse/JGRP-332
     */
    @Test
    public void testConcurrentStateTransfer() {
        final String[] names=new String[] { "A", "B", "C", "D" };
        final int count=names.length;
        final ConcurrentStateTransfer[] channels=new ConcurrentStateTransfer[count];
        final Semaphore semaphore=new Semaphore(count);

        try {
            semaphore.acquire(count);
            // Create activation threads that will block on the semaphore
            for(int i=0;i < count;i++) {
                if(i == 0)
                    channels[i]=new ConcurrentStateTransfer(names[i], semaphore, false);
                else
                    channels[i]=new ConcurrentStateTransfer((JChannel)channels[0].getChannel(),
                                                            names[i],
                                                            semaphore, false);

                // Start threads and let them join the channel
                channels[i].start();
                if(i == 0)
                    Util.sleep(3000);
            }

            // Make sure everyone is in sync
            Channel[] tmp=new Channel[channels.length];
            for(int i=0; i < channels.length; i++)
                tmp[i]=channels[i].getChannel();
            Util.blockUntilViewsReceived(60000, 1000, tmp);

            semaphore.release(count); // Unleash hell !

            // Reacquire the semaphore tickets; when we have them all
            // we know the threads are done
            boolean acquired=semaphore.tryAcquire(count, 30, TimeUnit.SECONDS);
            if(!acquired) {
                log.warn("Most likely a bug, analyse the stack below:");
                log.warn(Util.dumpThreads());
            }

            System.out.println("Waiting for all channels to have received the " + count + " messages:");
            long end_time=System.currentTimeMillis() + 10000L;
            while(System.currentTimeMillis() < end_time) {
                boolean terminate=true;
                for(ConcurrentStateTransfer ch: channels) {
                    if(ch.getList().size() != count) {
                        terminate=false;
                        break;
                    }
                }
                if(terminate)
                    break;
                else
                    Util.sleep(500);
            }

            System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++");
            for(ConcurrentStateTransfer channel:channels)
                log.info(channel.getName() + ": state=" + channel.getList());
            System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++");

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
            for(ConcurrentStateTransfer ch: channels)
                ch.getChannel().setReceiver(null);
            for(ConcurrentStateTransfer channel:channels)
                channel.cleanup();
            for(ConcurrentStateTransfer channel:channels)
                checkEventStateTransferSequence(channel);
        }
    }
    

    protected class ConcurrentStateTransfer extends PushChannelApplicationWithSemaphore{
        private final Set<Address> list=new HashSet<Address>();
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
            channel.getState(null, 30000);
            channel.send(null, null, channel.getAddress());
        }       
        
        Set<Address> getList() {
            return list;
        }

        Map<Integer,Object> getModifications() {
            return mods;
        }

        public void receive(Message msg) {
            if(msg.getBuffer() == null)
                return;
            Address obj = (Address)msg.getObject();
            log.info(channel.getAddress() + ": received " + obj);
            synchronized(list){
                list.add(obj);
            }
        }


        @SuppressWarnings("unchecked")
        public void setState(byte[] state) {
            super.setState(state);
            try{
                Set<Address> tmp = (Set<Address>) Util.objectFromByteBuffer(state);
                synchronized(list){
                    list.addAll(tmp);
                    log.info(channel.getAddress() + ": state is " + list);
                }
            }catch(Exception e){
                e.printStackTrace();
            }
        }

        public byte[] getState() {
            super.getState();
            synchronized(list) {
                Set<Address> tmp = new HashSet<Address>(list);
                try {
                    return Util.objectToByteBuffer(tmp);
                } catch(Exception e){
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
                synchronized(list){
                    Set<Address> tmp = new HashSet<Address>(list);
                    oos.writeObject(tmp);
                }
                oos.flush();
            }catch(IOException e){
                e.printStackTrace();
            }finally{
                Util.close(oos);
            }
        }

        @SuppressWarnings("unchecked")
        public void setState(InputStream istream) {
            super.setState(istream);
            ObjectInputStream ois = null;
            try{
                ois = new ObjectInputStream(istream);
                Set<Address> tmp = (HashSet<Address>) ois.readObject();
                synchronized(list){
                    list.addAll(tmp);
                    log.info(channel.getAddress() + ": state is " + list);
                }
            }catch(Exception e){
                e.printStackTrace();
            }finally{
                Util.close(ois);
            }
        }
    }


    
}
