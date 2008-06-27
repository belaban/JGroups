package org.jgroups.tests;


import org.jgroups.*;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.protocols.DISCARD;
import org.jgroups.protocols.FD;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.Semaphore;

/**
 * Tests shunning of a channel
 * 
 * @author vlada
 * @version $Id: ShunTest.java,v 1.19 2008/06/27 23:38:51 vlada Exp $
 */
@Test(groups="vlad",sequential=false)
public class ShunTest extends ChannelTestBase {
    JChannel c1, c2;
    RpcDispatcher disp1, disp2;

    @AfterMethod
    protected void tearDown() throws Exception {
        if(disp2 != null)
            disp2.stop();
        if(disp1 != null)
            disp1.stop();
        Util.close(c2, c1);
    }

    protected boolean useBlocking() {
        return true;
    }

    public void testShunning() {
        connectAndShun(2,false);
    }

    @Test(enabled=false)
    public static long getCurrentTime() {
        return System.currentTimeMillis();
    }

    /**
     * Tests the case where (members A and B) member B is shunned, excluded by A , then closes and reopens the channel.
     * After B has rejoined, it invokes an RPC and it should get valid return values from both A and B.
     * @throws Exception
     */
    public void testTwoMembersShun() throws Exception {
        View view;
        c1=createChannel(true, 2);
        c1.setOpt(Channel.AUTO_GETSTATE, false);
        c1.addChannelListener(new BelasChannelListener("C1"));
        c2=createChannel(c1);
        c2.setOpt(Channel.AUTO_GETSTATE, false);
        c2.addChannelListener(new BelasChannelListener("C2"));
        disp1=new RpcDispatcher(c1, null, new BelasReceiver("C1"), this);
        disp2=new RpcDispatcher(c2, null, new BelasReceiver("C2"), this);
        c1.connect("testTwoMembersShun");
        c2.connect("testTwoMembersShun");
        Assert.assertEquals(2, c1.getView().size());
        
        RspList rsps=disp2.callRemoteMethods(null, "getCurrentTime", null, (Class[])null, GroupRequest.GET_ALL, 10000);
        log.info(">> rsps:\n" + rsps);
        Assert.assertEquals(2, rsps.size());

        ProtocolStack stack=c1.getProtocolStack();
        stack.removeProtocol("VERIFY_SUSPECT");
        Protocol transport=stack.getTransport();
        log.info(">> suspecting C2:");
        transport.up(new Event(Event.SUSPECT, c2.getLocalAddress()));

        log.info(">> shunning C2:");

        c2.up(new Event(Event.EXIT));

        Util.sleep(1000); // give the closer thread time to close the channel
        System.out.println("waiting for C2 to come back");
        int count=1;
        while(true) {
            view=c2.getView();
            // System.out.println("<< C2.view: " + view);
            if((view != null && view.size() >= 2) || count >= 10)
                break;
            count++;
            Util.sleep(1000);
        }
        view=c2.getView();
        log.info(">>> view is " + view + " <<<< (should have 2 members)");
        Assert.assertEquals(2, view.size());

        Util.sleep(1000);
        System.out.println("invoking RPC on shunned member");
        rsps=disp2.callRemoteMethods(null, "getCurrentTime", null, (Class[])null, GroupRequest.GET_ALL, 10000);
        System.out.println(">> rsps:\n" + rsps);
        Assert.assertEquals(2, rsps.size());
        for(Map.Entry<Address, Rsp> entry: rsps.entrySet()) {
            Rsp rsp=entry.getValue();
            assertFalse(rsp.wasSuspected());
            assertTrue(rsp.wasReceived());
        }

        c1.setReceiver(null);
        c2.setReceiver(null);
        c1.clearChannelListeners();
        c2.clearChannelListeners();
    }
    
    protected void connectAndShun(int shunChannelIndex, boolean useDispatcher) {
        String[] names=null;

        names=new String[] { "A", "B", "C", "D" };

        int count=names.length;

        ShunChannel[] channels=new ShunChannel[count];
        try {
            // Create a semaphore and take all its permits
            Semaphore semaphore=new Semaphore(count);
            semaphore.acquire(count);

            // Create activation threads that will block on the semaphore
            for(int i=0;i < count;i++) {
                if(i == 0)
                    channels[i]=new ShunChannel(names[i], semaphore, useDispatcher);

                else
                    channels[i]=new ShunChannel((JChannel)channels[0].getChannel(),
                                                names[i],
                                                semaphore,
                                                useDispatcher);

                JChannel c=(JChannel)channels[i].getChannel();
                c.addChannelListener(new MyChannelListener(channels));
                // Release one ticket at a time to allow the thread to start
                // working
                channels[i].start();
                semaphore.release(1);
                //sleep at least a second and max second and a half
                Util.sleep(2000);
            }

            // block until we all have a valid view         

            blockUntilViewsReceived(channels, 60000);

            ShunChannel shun=channels[shunChannelIndex];
            log.info("Start shun attempt");
            addDiscardProtocol((JChannel)shun.getChannel());

            //allow shunning to kick in
            Util.sleep(20000);

            //and then block until we all have a valid view or fail with timeout
            blockUntilViewsReceived(channels, 60000);

        }
        catch(Exception ex) {
            log.warn("Exception encountered during test", ex);
            assert false:ex.getLocalizedMessage();
        }
        finally {
            for(ShunChannel channel:channels) {
                channel.cleanup();
                Util.sleep(2000);
            }

            /* we sometimes have double BLOCK events for tcp stack
             * TODO investigate why
            for(ShunChannel channel:channels){
                if(useBlocking() && channel.getChannel().flushSupported()){
                    checkEventStateTransferSequence(channel);
                }
            }*/
        }
    }
    
    private static void modifyStack(JChannel ch) {
        ProtocolStack stack=ch.getProtocolStack();

        try {
            ch.getProtocolStack().removeProtocol("VERIFY_SUSPECT");
        } catch (Exception e) {           
            e.printStackTrace();
        }
        FD fd=(FD)stack.findProtocol("FD");
        if(fd != null) {    
            fd.setMaxTries(3);
            fd.setTimeout(1000);
        }       
    }
    
    private static void addDiscardProtocol(JChannel ch) throws Exception {
        ProtocolStack stack=ch.getProtocolStack();
        Protocol transport=stack.getTransport();
        DISCARD discard=new DISCARD();
        discard.setUpDiscardRate(1.0);
        discard.setProtocolStack(ch.getProtocolStack());
        discard.start();
        stack.insertProtocol(discard, ProtocolStack.ABOVE, transport.getName());
    }    
    
    private static class MyChannelListener extends ChannelListenerAdapter{
        ShunChannel[] channels;
        Channel channel;

        public MyChannelListener(ShunChannel[] channels) {
            super();
            this.channels = channels;
        }
        
        public void channelConnected(Channel channel) {
            this.channel = channel;
        }

        public void channelReconnected(Address addr) {    
            System.out.println("Channel reconnected , new address is " + addr);
        }

        public void channelShunned() {
            System.out.println("Shunned channel is " + channel.getLocalAddress());
            System.out.println("Removing discard ");
            for (ShunChannel ch : channels) {
                JChannel c = (JChannel)ch.getChannel();
                try {
                    c.getProtocolStack().removeProtocol("DISCARD");
                } catch (Exception e) {                    
                    e.printStackTrace();
                }               
            }            
        }       
    }

    private  class BelasChannelListener extends ChannelListenerAdapter {
        final String name;

        public BelasChannelListener(String name) {
            this.name=name;
        }

        public void channelClosed(Channel channel) {
            log.info("[" + name + "] channelClosed()");
        }

        public void channelConnected(Channel channel) {
            log.info("[" + name + "] channelConnected()");
        }

        public void channelDisconnected(Channel channel) {
            log.info("[" + name + "] channelDisconnected()");
        }

        public void channelReconnected(Address addr) {
            log.info("[" + name + "] channelReconnected(" + addr + ")");
        }

        public void channelShunned() {
            log.info("[" + name + "] channelShunned()");
        }
    }

    private  class BelasReceiver extends ReceiverAdapter {
        final String name;

        public BelasReceiver(String name) {
            this.name=name;
        }

        public void viewAccepted(View new_view) {
            log.info("[" + name + "] new_view = " + new_view);
        }
    }
    
    protected class ShunChannel extends PushChannelApplicationWithSemaphore {
        
        public ShunChannel(String name,Semaphore semaphore,boolean useDispatcher) throws Exception{
            super(name, semaphore, useDispatcher);
            modifyStack((JChannel)channel);
        }
        
        public ShunChannel(JChannel ch,String name,Semaphore semaphore,boolean useDispatcher) throws Exception{
            super(ch,name, semaphore, useDispatcher);
            modifyStack((JChannel)channel);
        }        

        public void useChannel() throws Exception {           
            channel.connect("ShunChannel");
            channel.getState(null,5000);
            channel.send(null, null, channel.getLocalAddress());            
        }     

       
        public void setState(byte[] state) {
            super.setState(state);            
        }

        public byte[] getState() {
            super.getState(); 
            return new byte[]{'j','g','r','o','u','p','s'};
        }

        public void getState(OutputStream ostream) {
            super.getState(ostream);            
        }

        public void setState(InputStream istream) {
            super.setState(istream);
        }
    }


}
