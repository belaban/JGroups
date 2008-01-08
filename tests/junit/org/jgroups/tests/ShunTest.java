package org.jgroups.tests;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;
import java.util.concurrent.Semaphore;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.ChannelListenerAdapter;
import org.jgroups.JChannel;
import org.jgroups.mux.MuxChannel;
import org.jgroups.protocols.DISCARD;
import org.jgroups.protocols.FD;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;

/**
 * Tests shunning of a channel
 * 
 * @author vlada
 * @version $Id: ShunTest.java,v 1.1.2.2 2008/01/08 07:40:31 vlada Exp $
 */
public class ShunTest extends ChannelTestBase {
   

    public void setUp() throws Exception {
        super.setUp();        
        CHANNEL_CONFIG = System.getProperty("channel.conf.flush", "flush-udp.xml");
    }

    public boolean useBlocking() {
        return true;
    }

    public void testShunning() {
        connectAndShun(2,false);
    }
   
    
    protected void connectAndShun(int shunChannelIndex, boolean useDispatcher) {
        String[] names = null;

        // mux applications on top of same channel have to have unique name
        if(isMuxChannelUsed()){
            names = createMuxApplicationNames(1);
        }else{
            names = new String[] { "A", "B", "C", "D" };
        }

        int count = names.length;

        ShunChannel[] channels = new ShunChannel[count];
        try{
            // Create a semaphore and take all its permits
            Semaphore semaphore = new Semaphore(count);
            semaphore.acquire(count);

            // Create activation threads that will block on the semaphore
            for(int i = 0;i < count;i++){               
               channels[i] = new ShunChannel(names[i],
                                             semaphore,
                                             useDispatcher);  
                              

               JChannel c = (JChannel) channels[i].getChannel();
               if(c instanceof MuxChannel){
                   c = ((MuxChannel)c).getChannel();
               }
               c.addChannelListener(new MyChannelListener(channels));
               // Release one ticket at a time to allow the thread to start
               // working
               channels[i].start();                 
               semaphore.release(1);
               //sleep at least a second and max second and a half
               Util.sleep(2000);                                                                                                          
            }           

            // block until we all have a valid view         
            if(isMuxChannelUsed()){
                blockUntilViewsReceived(channels, getMuxFactoryCount(), 60000);
            }else{
                blockUntilViewsReceived(channels, 60000);
            }
            
            ShunChannel shun = channels[shunChannelIndex];
            log.info("Start shun attempt");
            addDiscardProtocol((JChannel)shun.getChannel());               
            
            //allow shunning to kick in
            Util.sleep(20000);
            
            //and then block until we all have a valid view  
            // or fail with timeout
            if(isMuxChannelUsed()){
                blockUntilViewsReceived(channels, getMuxFactoryCount(), 60000);
            }else{
                blockUntilViewsReceived(channels, 60000);
            }
            
        }catch(Exception ex){
            log.warn("Exception encountered during test", ex);
            fail(ex.getLocalizedMessage());
        }finally{
            for(ShunChannel channel:channels){
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
    
    private void modifyStack(JChannel ch) {
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
    
    private void addDiscardProtocol(JChannel ch) throws Exception {
        ProtocolStack stack=ch.getProtocolStack();
        Protocol transport=stack.getTransport();
        DISCARD discard=new DISCARD();
        Properties props = new Properties();
        props.setProperty("up", "1.0");
        discard.setProperties(props);
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
    
    protected class ShunChannel extends PushChannelApplicationWithSemaphore {
        
        public ShunChannel(String name,Semaphore semaphore,boolean useDispatcher) throws Exception{
            super(name, semaphore, useDispatcher);
            modifyStack((JChannel)channel);
        }

        public void useChannel() throws Exception {           
            channel.connect("test");
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

    public static Test suite() {
        return new TestSuite(ShunTest.class);
    }

    public static void main(String[] args) {
        String[] testCaseName = { ShunTest.class.getName() };
        junit.textui.TestRunner.main(testCaseName);
    }
}
