package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.View;
import org.jgroups.protocols.DISCARD;
import org.jgroups.protocols.FD;
import org.jgroups.protocols.MERGE2;
import org.jgroups.protocols.MPING;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Tests merging on all stacks
 * 
 * @author vlada
 * @version $Id: MergeTest.java,v 1.35 2009/04/14 19:18:39 vlada Exp $
 */
@Test(groups=Global.FLUSH,sequential=true)
public class MergeTest extends ChannelTestBase {
   
    @Test
    public void testMerging2Members() {
        String[] names = {"A", "B"};
        mergeHelper(names);
    }
    
    @Test
    public void testMerging4Members() {
        String[] names = {"A", "B", "C", "D"};
        mergeHelper(names);
    }

    /**
     *
     * 
     */
    protected void mergeHelper(String[] names) {
        int count=names.length;

        //List<MergeApplication> channels = new ArrayList<MergeApplication>();
        MergeApplication[] channels=new MergeApplication[count];
        try {
            // Create a semaphore and take all its permits
            Semaphore semaphore=new Semaphore(count);
            semaphore.acquire(count);

            // Create activation threads that will block on the semaphore
            for(int i=0;i < count;i++) {
                if(i == 0)
                    channels[i]=new MergeApplication(names[i], semaphore, false);
                else
                    channels[i]=new MergeApplication((JChannel)channels[0].getChannel(),
                                                     names[i],
                                                     semaphore,
                                                     false);

                // Release one ticket at a time to allow the thread to start
                // working
                channels[i].start();
                semaphore.release(1);
                //sleep at least a second and max second and a half
                Util.sleepRandom(1500);
            }

            // Make sure everyone is in sync

            blockUntilViewsReceived(channels, 60000);

            // Sleep to ensure the threads get all the semaphore tickets
            Util.sleep(2000);

            int split=count / 2;

            for(int i=0;i < split;i++) {
                DISCARD discard=(DISCARD)((JChannel)channels[i].getChannel()).getProtocolStack()
                                                                             .findProtocol("DISCARD");
                for(int j=split;j < count;j++) {
                    discard.addIgnoreMember(channels[j].getLocalAddress());
                }
            }

            for(int i=count - 1;i >= split;i--) {
                DISCARD discard=(DISCARD)((JChannel)channels[i].getChannel()).getProtocolStack()
                                                                             .findProtocol("DISCARD");
                for(int j=0;j < split;j++) {
                    discard.addIgnoreMember(channels[j].getLocalAddress());
                }
            }

            log.info("Waiting for split to be detected...");
            View view;
            long stop=System.currentTimeMillis() + 35 * 1000;
            do {
                view=channels[0].channel.getView();               
                if(view != null && view.size() == split)
                    break;
                else
                    Util.sleep(1000);
            }while(System.currentTimeMillis() < stop);

            // Util.sleep(35*1000);

            log.info("Waiting for merging to kick in....");

            for(int i=0;i < count;i++) {
                channels[i].getChannel().getProtocolStack().removeProtocol("DISCARD");
            }

            //Either merge properly or time out...
            //we check that each channel again has correct view
            blockUntilViewsReceived(channels, 60000);

            // Re-acquire the semaphore tickets; when we have them all
            // we know the threads are done
            boolean acquired=semaphore.tryAcquire(count, 20, TimeUnit.SECONDS);
            if(!acquired) {
                log.warn("Most likely a bug, analyse the stack below:");
                log.warn(Util.dumpThreads());
            }
            Util.sleep(1000);
        }
        catch(Exception ex) {
            log.warn("Exception encountered during test", ex);
            assert false:ex.getLocalizedMessage();
        }
        finally {
            List<MergeApplication> channelsReversed=Arrays.asList(channels);
            Collections.reverse(channelsReversed);
            for(MergeApplication channel:channelsReversed) {
                channel.cleanup();
                Util.sleep(2000);
            }
            if(useBlocking()) {
                for(MergeApplication channel:channels) {
                    checkEventStateTransferSequence(channel);
                }
            }
        }
    }
    
    protected class MergeApplication extends PushChannelApplicationWithSemaphore {      

        public MergeApplication(String name,Semaphore semaphore,boolean useDispatcher) throws Exception{
            super(name, semaphore, useDispatcher);
            replaceDiscoveryProtocol((JChannel)channel);
            addDiscardProtocol((JChannel)channel); 
            modiftFDAndMergeSettings((JChannel)channel);
        }
        
        public MergeApplication(JChannel ch,String name,Semaphore semaphore,boolean useDispatcher) throws Exception{
            super(ch,name, semaphore, useDispatcher);
            
            /*
             * no need to modify anything since we are making a copy of an
             * already modified channel
             * replaceDiscoveryProtocol((JChannel)channel);
             * addDiscardProtocol((JChannel)channel);
             * modiftFDAndMergeSettings((JChannel)channel);
             */
        }

        public void useChannel() throws Exception {
            channel.connect("MergeApplication");           
        }         
    }
    
    
    private static void addDiscardProtocol(JChannel ch) throws Exception {
        ProtocolStack stack=ch.getProtocolStack();
        Protocol transport=stack.getTransport();
        DISCARD discard=new DISCARD();
        discard.setProtocolStack(ch.getProtocolStack());
        discard.start();
        stack.insertProtocol(discard, ProtocolStack.ABOVE, transport.getName());
    }
    
    private void replaceDiscoveryProtocol(JChannel ch) throws Exception {
        ProtocolStack stack=ch.getProtocolStack();
        Protocol discovery=stack.removeProtocol("TCPPING");
        if(discovery != null){
            Protocol transport = stack.getTransport();
            MPING mping=new MPING();
            InetAddress bindAddress=Util.getBindAddress(new Properties());
            mping.setBindAddr(bindAddress);
            mping.setMulticastAddress("230.3.3.3");
            mping.setMcastPort(7777);            
            stack.insertProtocol(mping, ProtocolStack.ABOVE, transport.getName());
            mping.setProtocolStack(ch.getProtocolStack());
            mping.init();
            mping.start();            
            log.info("Replaced TCPPING with MPING. See http://wiki.jboss.org/wiki/Wiki.jsp?page=JGroupsMERGE2");            
        }        
    }

    private static void modiftFDAndMergeSettings(JChannel ch) {
        ProtocolStack stack=ch.getProtocolStack();

        FD fd=(FD)stack.findProtocol("FD");
        if(fd != null) {
            fd.setMaxTries(3);
            fd.setTimeout(1000);
        }
        MERGE2 merge=(MERGE2)stack.findProtocol("MERGE2");
        if(merge != null) {
            merge.setMinInterval(5000);
            merge.setMaxInterval(10000);
        }
    }
}
