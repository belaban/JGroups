package org.jgroups.tests;


import org.jgroups.*;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.io.*;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests concurrent startup and message sending directly after joining. See doc/design/ConcurrentStartupTest.txt
 * for details. This will only work 100% correctly with FLUSH support.<br/>
 * [1] http://jira.jboss.com/jira/browse/JGRP-236
 * @author bela
 */

@Test(groups={Global.FLUSH},sequential=true)
public class ConcurrentStartupTest extends ChannelTestBase {
    private AtomicInteger mod = new AtomicInteger(0);


    public void testConcurrentStartupWithState() {
        final String[] names=new String[] { "A", "B", "C", "D" };
        final int count=names.length;

        final ConcurrentStartupChannel[] channels=new ConcurrentStartupChannel[count];
        try {
            // Create a semaphore and take all its permits
            Semaphore semaphore=new Semaphore(count);
            semaphore.acquire(count);

            // Create activation threads that will block on the semaphore
            for(int i=0;i < count;i++) {
                if(i == 0)
                    channels[i]=new ConcurrentStartupChannel(names[i], semaphore);
                else
                    channels[i]=new ConcurrentStartupChannel((JChannel)channels[0].getChannel(), names[i], semaphore);
                channels[i].start();
                semaphore.release(1);
                if(i == 0)
                    Util.sleep(1500); // sleep after the first node to educe the chances of a merge
            }

            // Make sure everyone is in sync
            Channel[] tmp=new Channel[channels.length];
            for(int i=0; i < channels.length; i++)
                tmp[i]=channels[i].getChannel();

            Util.blockUntilViewsReceived(30000, 500, tmp);
            System.out.println(">>>> all nodes have the same view <<<<");

            // Re-acquire the semaphore tickets; when we have them all we know the threads are done
            boolean acquired=semaphore.tryAcquire(count, 20, TimeUnit.SECONDS);
            if(!acquired) {
                log.warn("Most likely a bug, analyse the stack below:");
                log.warn(Util.dumpThreads());
            }

            // Sleep to ensure async messages arrive
            System.out.println("Waiting for all channels to have received the " + count + " messages:");
            long end_time=System.currentTimeMillis() + 10000L;
            while(System.currentTimeMillis() < end_time) {
                boolean terminate=true;
                for(ConcurrentStartupChannel ch: channels) {
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
            for(ConcurrentStartupChannel channel:channels)
                System.out.println(channel.getName() + ": state=" + channel.getList());
            System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++");

            //Vladimir Oct 1st 2009: if merged occurred state is corrupted. Do not verify state in that case.
            for(ConcurrentStartupChannel ch: channels) {
                Set<Address> list=ch.getList();
                if(!ch.merged)
                    assert list.size() == count : ": list is " + list + ", should have " + count + " elements";
            }
            System.out.println(">>>> done, all messages received by all channels <<<<");
            for (ConcurrentStartupChannel channel : channels)
                if(!channel.merged)
                    checkEventStateTransferSequence(channel);
        }
        catch(Exception ex) {
        }
        finally {
            for(ConcurrentStartupChannel channel: channels)
                channel.getChannel().setReceiver(null); // silence the receivers so they don't logs views
            for(ConcurrentStartupChannel channel: channels)
                channel.cleanup();
        }
    }

    protected int getMod() {
        return mod.incrementAndGet();
    }

    

    protected class ConcurrentStartupChannel extends PushChannelApplicationWithSemaphore {
        private final Set<Address> state=new HashSet<Address>();
        boolean merged = false;

        public ConcurrentStartupChannel(String name,Semaphore semaphore) throws Exception{
            super(name, semaphore);
        }

        public ConcurrentStartupChannel(JChannel ch,String name,Semaphore semaphore) throws Exception{
            super(ch,name, semaphore);
        }

        public void useChannel() throws Exception {
            channel.connect("test", null, null, 25000); // join and state transfer
            channel.send(null, null, channel.getAddress());
        }

        Set<Address> getList() {
            synchronized(state) {
                return state;
            }
        }

        public void receive(Message msg) {
            if(msg.getBuffer() == null)
                return;
            Address obj = (Address)msg.getObject();
            log.info(channel.getAddress() + ": received " + obj);
            synchronized(state) {
                state.add(obj);
            }
        }

        public void viewAccepted(View new_view) {
            super.viewAccepted(new_view);     
            if(new_view instanceof MergeView) {
                merged = true;
            }
        }

        @SuppressWarnings("unchecked")
        public void setState(byte[] state) {
            super.setState(state);
            try{
                List<Address> tmp = (List) Util.objectFromByteBuffer(state);
                synchronized(this.state) {
                    this.state.addAll(tmp);
                    log.info(channel.getAddress() + ": state is " + this.state);
                }
            }catch(Exception e){
                e.printStackTrace();
            }
        }

        public byte[] getState() {
            super.getState();
            List<Address> tmp = null;
            synchronized(state) {
                tmp = new LinkedList<Address>(state);
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
                synchronized(state) {
                    tmp = new LinkedList<Address>(state);
                }
                oos.writeObject(tmp);
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
                List<Address> tmp = (List) ois.readObject();
                synchronized(state){
                    // state.clear();
                    state.addAll(tmp);
                    log.info(channel.getAddress() + ": state is " + state);
                }
            }catch(Exception e){
                e.printStackTrace();
            }finally{
                Util.close(ois);
            }
        }
    }
}