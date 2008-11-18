package org.jgroups.tests;

import junit.framework.Test;
import junit.framework.TestSuite;
import org.jgroups.*;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Tests the FLUSH protocol, requires flush-udp.xml in ./conf to be present and
 * configured to use FLUSH
 * 
 * @author Bela Ban
 * @version $Id: FlushTest.java,v 1.58.2.5 2008/11/18 15:56:33 vlada Exp $
 */
public class FlushTest extends ChannelTestBase {
    private JChannel c1, c2,c3;

    public FlushTest(){
        super();
    }

    public FlushTest(String name){
        super(name);
    }

    public void setUp() throws Exception {
        super.setUp();
        CHANNEL_CONFIG = System.getProperty("channel.conf.flush", "flush-udp.xml");
    }

    public void tearDown() throws Exception {
    	if(c3 != null){
            c3.close();
            assertFalse(c3.isOpen());
            assertFalse(c3.isConnected());
            c3 = null;
        }
    	
    	if(c2 != null){
            c2.close();
            assertFalse(c2.isOpen());
            assertFalse(c2.isConnected());
            c2 = null;
        }

        if(c1 != null){
            c1.close();
            assertFalse(c1.isOpen());
            assertFalse(c1.isConnected());
            c1 = null;
        }

        Util.sleep(500);
        super.tearDown();
    }

    public boolean useBlocking() {
        return true;
    }

    public void testSingleChannel() throws Exception {
        Semaphore s = new Semaphore(1);
        FlushTestReceiver receivers[] = new FlushTestReceiver[] { new FlushTestReceiver("c1",
                                                                                        s,
                                                                                        0,
                                                                                        FlushTestReceiver.CONNECT_ONLY) };
        receivers[0].start();
        s.release(1);

        // Make sure everyone is in sync
        blockUntilViewsReceived(receivers, 60000);

        // Sleep to ensure the threads get all the semaphore tickets
        Util.sleep(1000);

        // Reacquire the semaphore tickets; when we have them all
        // we know the threads are done
        s.tryAcquire(1, 60, TimeUnit.SECONDS);
        receivers[0].cleanup();
        Util.sleep(1000);

        checkEventStateTransferSequence(receivers[0]);

    }

    /**
     * Tests issue #1 in http://jira.jboss.com/jira/browse/JGRP-335
     */
    public void testJoinFollowedByUnicast() throws ChannelException {
        c1 = createChannel();
        c1.setReceiver(new SimpleReplier(c1, true));
        c1.connect("test");

        Address target = c1.getLocalAddress();
        Message unicast_msg = new Message(target);

        c2 = createChannel();
        c2.setReceiver(new SimpleReplier(c2, false));
        c2.connect("test");

        // now send unicast, this might block as described in the case
        c2.send(unicast_msg);
        // if we don't get here this means we'd time out
    }
    
    public void testFlushWithCrashedFlushCoordinator() throws ChannelException {
        c1 = createChannel();
        c1.connect("test");

        c2 = createChannel();
        c2.connect("test");
        
        c3 = createChannel();
        c3.connect("test");
        
        //start flush
        c2.startFlush(false);
        
        //and then kill the flush coordinator
        ((JChannel)c2).shutdown();
        
        Util.sleep(8000);
        
        //cluster should not hang and two remaining members should have a correct view
        assertTrue("corret view size", c1.getView().size()==2);
        assertTrue("corret view size", c3.getView().size()==2);
    }

    /**
     * Tests issue #2 in http://jira.jboss.com/jira/browse/JGRP-335
     */
    public void testStateTransferFollowedByUnicast() throws ChannelException {
        c1 = createChannel();
        c1.setReceiver(new SimpleReplier(c1, true));
        c1.connect("test");

        Address target = c1.getLocalAddress();
        Message unicast_msg = new Message(target);

        c2 = createChannel();
        c2.setReceiver(new SimpleReplier(c2, false));
        c2.connect("test");

        log.info("\n** Getting the state **");
        c2.getState(null, 10000);
        // now send unicast, this might block as described in the case
        c2.send(unicast_msg);
    }
    
    /**
     * Tests http://jira.jboss.com/jira/browse/JGRP-661
     * @throws Exception 
     */
    public void testPartialFlush() throws Exception {
        c1 = createChannel();
        c1.setReceiver(new SimpleReplier(c1, true));
        c1.connect("test");
    
        c2 = createChannel();
        c2.setReceiver(new SimpleReplier(c2, false));
        c2.connect("test");
       
        List<Address> members = new ArrayList<Address>();
        members.add(c2.getLocalAddress());       
        boolean flushedOk = c2.startFlush(members, false);
        
        assertTrue("Partial flush worked", flushedOk);
        
        c2.stopFlush(members);
               

    }

    /**
     * Tests emition of block/unblock/get|set state events in both mux and bare
     * channel mode. In mux mode this test creates getFactoryCount() real
     * channels and creates only one mux application on top of each channel. In
     * bare channel mode 4 real channels are created.
     * 
     */
    public void testBlockingNoStateTransfer() {
        String[] names = createApplicationNames(4);
        _testChannels(names, FlushTestReceiver.CONNECT_ONLY, 4);
    }



    /**
     * Tests emition of block/unblock/set|get state events for both mux and bare
     * channel depending on mux.on parameter. In mux mode there will be only one
     * mux channel for each "real" channel created and the number of real
     * channels created is getMuxFactoryCount().
     * 
     */
    public void testBlockingWithStateTransfer() {
        String[] names = createApplicationNames(4);
        _testChannels(names, FlushTestReceiver.CONNECT_AND_SEPARATE_GET_STATE, 4);
    }
    
    /**
     * Tests emition of block/unblock/set|get state events for both mux and bare
     * channel depending on mux.on parameter. In mux mode there will be only one
     * mux channel for each "real" channel created and the number of real
     * channels created is getMuxFactoryCount().
     * 
     */
    public void testBlockingWithConnectAndStateTransfer() {
        String[] names = createApplicationNames(4);
        _testChannels(names, FlushTestReceiver.CONNECT_AND_GET_STATE, 4);
    }



    private void _testChannels(String names[],
                               int connectType,
                               Assertable a) {
        int count = names.length;

        ArrayList<FlushTestReceiver> channels = new ArrayList<FlushTestReceiver>(count);
        try{
            // Create a semaphore and take all its permits
            Semaphore semaphore = new Semaphore(count);
            semaphore.acquire(count);

            // Create channels and their threads that will block on the
            // semaphore
            for(int i = 0;i < count;i++){
                FlushTestReceiver channel = new FlushTestReceiver(names[i], semaphore, 0, connectType);                
                channels.add(channel);

                // Release one ticket at a time to allow the thread to start
                // working
                channel.start();
                semaphore.release(1);			
                Util.sleep(1000);
            }

            blockUntilViewsReceived(channels, 10000);

            // if state transfer is used release all at once
            // clear all channels of view events
            if(connectType == FlushTestReceiver.CONNECT_AND_SEPARATE_GET_STATE){
                for(FlushTestReceiver app:channels){
                    app.clear();
                }
                semaphore.release(count);
            }

            // Sleep to ensure the threads get all the semaphore tickets
            Util.sleep(1000);

            // Reacquire the semaphore tickets; when we have them all
            // we know the threads are done
            semaphore.tryAcquire(count, 60, TimeUnit.SECONDS);

            // do general asserts about channels
            a.verify(channels);

            // kill random member
            FlushTestReceiver randomRecv = channels.remove(RANDOM.nextInt(count));
            log.info("Closing random member " + randomRecv.getName()
                     + " at "
                     + randomRecv.getLocalAddress());
            ChannelCloseAssertable closeAssert = new ChannelCloseAssertable(randomRecv);
            randomRecv.cleanup();

            // let the view propagate and verify related asserts
            Util.sleep(5000);
            closeAssert.verify(channels);                     
        }catch(Exception ex){
            log.warn("Exception encountered during test", ex);
            fail("Exception encountered during test execution: " + ex);
        }finally{
            
            //close all channels and ....
            for(FlushTestReceiver app:channels){
                app.cleanup();
                Util.sleep(2000);
            }
            
            // verify block/unblock/view/get|set state sequences for all members
            for(FlushTestReceiver receiver:channels) {
                checkEventStateTransferSequence(receiver);
            }
        }
    }

    private  void _testChannels(String names[], int connectMethod, int viewSize) {
        _testChannels(names, connectMethod, new ChannelAssertable(viewSize));
    }

    private class ChannelCloseAssertable implements Assertable {
        ChannelApplication app;

        View viewBeforeClose;

        Address appAddress;


        public ChannelCloseAssertable(ChannelApplication app){
            this.app = app;
            this.viewBeforeClose = app.getChannel().getView();
            appAddress = app.getChannel().getLocalAddress();
        }

        public void verify(Object verifiable) {
            Channel ch = app.getChannel();
            assertFalse("Channel open", ch.isOpen());
            assertFalse("Chnanel connected", ch.isConnected());
        }
    }

    private class ChannelAssertable implements Assertable {
        int expectedViewSize = 0;

        public ChannelAssertable(int expectedViewSize){
            this.expectedViewSize = expectedViewSize;
        }

        public void verify(Object verifiable) {
            Collection channels = (Collection) verifiable;
            for(Iterator iter = channels.iterator();iter.hasNext();){
                FlushTestReceiver receiver = (FlushTestReceiver) iter.next();
                Channel ch = receiver.getChannel();
                assertEquals("Correct view", ch.getView().getMembers().size(), expectedViewSize);
                assertTrue("Channel open", ch.isOpen());
                assertTrue("Chnanel connected", ch.isConnected());
                assertNotNull("Valid address ", ch.getLocalAddress());
                assertTrue("Address included in view ", ch.getView()
                                                          .getMembers()
                                                          .contains(ch.getLocalAddress()));
                assertNotNull("Valid cluster name ", ch.getClusterName());
            }
        }
    }
    
    protected JChannel createChannel() throws ChannelException {
        JChannel ret = new JChannel(CHANNEL_CONFIG);
        ret.setOpt(Channel.BLOCK, Boolean.TRUE);
        Protocol flush = ret.getProtocolStack().findProtocol("FLUSH");
        if(flush != null){
            Properties p = new Properties();
            p.setProperty("timeout", "0");
            flush.setProperties(p);

            // send timeout up and down the stack, so other protocols can use
            // the same value too
            Map<String, Object> map = new HashMap<String, Object>();
            map.put("flush_timeout", new Long(0));
            flush.getUpProtocol().up(new Event(Event.CONFIG, map));
            flush.getDownProtocol().down(new Event(Event.CONFIG, map));
        }
        return ret;
    }

    private interface Assertable {
        public void verify(Object verifiable);
    }

    private class FlushTestReceiver extends PushChannelApplicationWithSemaphore {
        List<Object> events;
        
        private int connectMethod;
        
        public static final int CONNECT_ONLY = 1;
        
        public static final int CONNECT_AND_SEPARATE_GET_STATE = 2;
        
        public static final int CONNECT_AND_GET_STATE = 3;      

        int msgCount = 0;

        protected FlushTestReceiver(String name,
                                    Semaphore semaphore,
                                    int msgCount,
                                    int connectMethod) throws Exception{
            super(name, semaphore);
            this.connectMethod = connectMethod;
            this.msgCount = msgCount;
            events = Collections.synchronizedList(new LinkedList<Object>());
            if(connectMethod == CONNECT_ONLY || connectMethod == CONNECT_AND_SEPARATE_GET_STATE)
            	channel.connect("test");
            
            if(connectMethod == CONNECT_AND_GET_STATE){
                channel.connect("test",null,null, 25000);
            }
        }        

        public void clear() {
            events.clear();
        }

        public List<Object> getEvents() {
            return new LinkedList<Object>(events);
        }

        public void block() {
            events.add(new BlockEvent());
        }

        public void unblock() {
            events.add(new UnblockEvent());
        }

        public void viewAccepted(View new_view) {
            events.add(new_view);
        }

        public byte[] getState() {
            events.add(new GetStateEvent(null, null));
            return new byte[] { 'b', 'e', 'l', 'a' };
        }

        public void setState(byte[] state) {
            events.add(new SetStateEvent(null, null));
        }

        public void getState(OutputStream ostream) {
            events.add(new GetStateEvent(null, null));
            byte[] payload = new byte[] { 'b', 'e', 'l', 'a' };
            try{
                ostream.write(payload);
            }catch(IOException e){
                e.printStackTrace();
            }finally{
                Util.close(ostream);
            }
        }

        public void setState(InputStream istream) {
            events.add(new SetStateEvent(null, null));
            byte[] payload = new byte[4];
            try{
                istream.read(payload);
            }catch(IOException e){
                e.printStackTrace();
            }finally{
                Util.close(istream);
            }
        }

        protected void useChannel() throws Exception {            
            if(connectMethod == CONNECT_AND_SEPARATE_GET_STATE){
                channel.getState(null, 25000);
            }
            if(msgCount > 0){
                for(int i = 0;i < msgCount;i++){
                    channel.send(new Message());
                    Util.sleep(100);
                }
            }
        }
    }

    private class SimpleReplier extends ExtendedReceiverAdapter {
        Channel channel;

        boolean handle_requests = false;

        public SimpleReplier(Channel channel,boolean handle_requests){
            this.channel = channel;
            this.handle_requests = handle_requests;
        }

        public void receive(Message msg) {
            Message reply = new Message(msg.getSrc());
            try{
                log.info("-- MySimpleReplier[" + channel.getLocalAddress()
                         + "]: received message from "
                         + msg.getSrc());
                if(handle_requests){
                    log.info(", sending reply");
                    channel.send(reply);
                }else
                    System.out.println("\n");
            }catch(Exception e){
                e.printStackTrace();
            }
        }

        public void viewAccepted(View new_view) {
            log.info("-- MySimpleReplier[" + channel.getLocalAddress()
                     + "]: viewAccepted("
                     + new_view
                     + ")");
        }

        public void block() {
            log.info("-- MySimpleReplier[" + channel.getLocalAddress() + "]: block()");
        }

        public void unblock() {
            log.info("-- MySimpleReplier[" + channel.getLocalAddress() + "]: unblock()");
        }
    }

    public static Test suite() {
        return new TestSuite(FlushTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(FlushTest.suite());
    }
}
