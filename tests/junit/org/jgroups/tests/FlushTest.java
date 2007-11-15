package org.jgroups.tests;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.jgroups.Address;
import org.jgroups.BlockEvent;
import org.jgroups.Channel;
import org.jgroups.ExtendedReceiverAdapter;
import org.jgroups.GetStateEvent;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.SetStateEvent;
import org.jgroups.UnblockEvent;
import org.jgroups.View;
import org.jgroups.util.Util;

/**
 * Tests the FLUSH protocol, requires flush-udp.xml in ./conf to be present and
 * configured to use FLUSH
 * 
 * @author Bela Ban
 * @version $Id: FlushTest.java,v 1.60 2007/11/15 19:39:49 vlada Exp $
 */
public class FlushTest extends ChannelTestBase {
    private JChannel c1, c2;

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

        checkEventSequence(receivers[0], isMuxChannelUsed());

    }

    /**
     * Tests issue #1 in http://jira.jboss.com/jira/browse/JGRP-335
     * @throws Exception 
     */
    public void testJoinFollowedByUnicast() throws Exception {
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

    /**
     * Tests issue #2 in http://jira.jboss.com/jira/browse/JGRP-335
     * @throws Exception 
     */
    public void testStateTransferFollowedByUnicast() throws Exception {
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
     * Tests emition of block/unblock/get|set state events in both mux and bare
     * channel mode. In mux mode this test creates getFactoryCount() real
     * channels and creates only one mux application on top of each channel. In
     * bare channel mode 4 real channels are created.
     * 
     */
    public void testBlockingNoStateTransfer() {
        String[] names = null;
        if(isMuxChannelUsed()){
            int muxFactoryCount = 2;
            names = createMuxApplicationNames(1, muxFactoryCount);
            _testChannels(names, muxFactoryCount, FlushTestReceiver.CONNECT_ONLY);
        }else{
            names = createApplicationNames(4);
            _testChannels(names, FlushTestReceiver.CONNECT_ONLY);
        }
    }

    /**
     * Tests emition of block/unblock/get|set state events in mux mode. In this
     * test all mux applications share the same "real" channel. This test runs
     * only when mux.on=true. This test does not take into account
     * -Dmux.factorycount parameter.
     * 
     */
    public void testBlockingSharedMuxFactory() {
        String[] names = null;
        int muxFactoryCount = 1;
        if(isMuxChannelUsed()){
            names = createMuxApplicationNames(4, muxFactoryCount);
            _testChannels(names, muxFactoryCount, FlushTestReceiver.CONNECT_ONLY);
        }
    }

    /**
     * Tests emition of block/unblock/get|set state events in mux mode. In this
     * test there will be exactly two real channels created where each real
     * channel has two mux applications on top of it. This test runs only when
     * mux.on=true. This test does not take into account -Dmux.factorycount
     * parameter.
     * 
     */
    public void testBlockingUnsharedMuxFactoryMultipleService() {
        String[] names = null;
        int muxFactoryCount = 2;
        if(isMuxChannelUsed()){
            names = createMuxApplicationNames(2, muxFactoryCount);
            _testChannels(names, muxFactoryCount, FlushTestReceiver.CONNECT_ONLY);
        }
    }

    /**
     * Tests emition of block/unblock/set|get state events for both mux and bare
     * channel depending on mux.on parameter. In mux mode there will be only one
     * mux channel for each "real" channel created and the number of real
     * channels created is getMuxFactoryCount().
     * 
     */
    public void testBlockingWithStateTransfer() {
        String[] names = null;
        if(isMuxChannelUsed()){
            int muxFactoryCount = 2;
            names = createMuxApplicationNames(1, muxFactoryCount);
            _testChannels(names, muxFactoryCount, FlushTestReceiver.CONNECT_AND_SEPARATE_GET_STATE);
        }else{
            names = createApplicationNames(4);
            _testChannels(names, FlushTestReceiver.CONNECT_AND_SEPARATE_GET_STATE);
        }
    }
    
    /**
     * Tests emition of block/unblock/set|get state events for both mux and bare
     * channel depending on mux.on parameter. In mux mode there will be only one
     * mux channel for each "real" channel created and the number of real
     * channels created is getMuxFactoryCount().
     * 
     */
    public void testBlockingWithConnectAndStateTransfer() {
        String[] names = null;
        if(isMuxChannelUsed()){
            int muxFactoryCount = 2;
            names = createMuxApplicationNames(1, muxFactoryCount);
            _testChannels(names, muxFactoryCount, FlushTestReceiver.CONNECT_AND_GET_STATE);
        }else{
            names = createApplicationNames(4);
            _testChannels(names, FlushTestReceiver.CONNECT_AND_GET_STATE);
        }
    }

    /**
     * Tests emition of block/unblock/set|get state events in mux mode setup
     * where each "real" channel has two mux service on top of it. The number of
     * real channels created is getMuxFactoryCount(). This test runs only when
     * mux.on=true.
     * 
     */
    public void testBlockingWithStateTransferAndMultipleServiceMuxChannel() {
        String[] names = null;
        if(isMuxChannelUsed()){
            names = createMuxApplicationNames(2, 2);
            _testChannels(names, 2, FlushTestReceiver.CONNECT_AND_SEPARATE_GET_STATE);
        }
    }

    private void _testChannels(String names[], int connectType){
        _testChannels(names, getMuxFactoryCount(),connectType);
        
    }
    private void _testChannels(String names[],int muxFactoryCount, int connectType) {
        int count = names.length;

        ArrayList<FlushTestReceiver> channels = new ArrayList<FlushTestReceiver>(count);
        try{
            // Create a semaphore and take all its permits
            Semaphore semaphore = new Semaphore(count);
            semaphore.acquire(count);

            // Create channels and their threads that will block on the
            // semaphore
            for(String channelName:names){
                FlushTestReceiver channel = new FlushTestReceiver(channelName, semaphore, 0, connectType);                
                channels.add(channel);

                // Release one ticket at a time to allow the thread to start
                // working
                channel.start();
                semaphore.release(1);			
                Util.sleep(1000);
            }

            if(isMuxChannelUsed()){
                blockUntilViewsReceived(channels, muxFactoryCount, 10000);
            }else{
                blockUntilViewsReceived(channels, 10000);
            }          
            // Sleep to ensure the threads get all the semaphore tickets
            Util.sleep(1000);

            // Reacquire the semaphore tickets; when we have them all
            // we know the threads are done
            semaphore.tryAcquire(count, 40, TimeUnit.SECONDS);
                      
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
            for (FlushTestReceiver receiver : channels) {
                if (connectType == FlushTestReceiver.CONNECT_AND_SEPARATE_GET_STATE
                        || connectType == FlushTestReceiver.CONNECT_AND_GET_STATE) {
                    checkEventStateTransferSequence(receiver);
                } else {
                    checkEventSequence(receiver, isMuxChannelUsed());
                }
            }
        }
    }  

    private void checkEventSequence(FlushTestReceiver receiver, boolean isMuxUsed) {
        List<Object> events = receiver.getEvents();
        String eventString = "[" + receiver.getName()
                             + "|"
                             + receiver.getLocalAddress()
                             + ",events:"
                             + events;
        log.info(eventString);        
        assertNotNull(events);
        assertTrue(events.size()>1);
        assertTrue("First event is not block but " + events.get(0),events.get(0) instanceof BlockEvent);
        assertTrue("Last event not unblock but " + events.get(events.size()-1),events.get(events.size()-1) instanceof UnblockEvent);
        int size = events.size();
        for(int i = 0;i < size;i++){
            Object event = events.get(i);
            if(event instanceof BlockEvent){
                if(i + 1 < size){
                    Object ev = events.get(i + 1);
                    if(isMuxUsed){
                        assertTrue("After Block should be View or Unblock but it is " + ev.getClass() + ",events= " + eventString,
                                   ev instanceof View || ev instanceof UnblockEvent);
                    }else{
                        assertTrue("After Block should be View but it is " + ev.getClass() + ",events= " + eventString,
                                   ev instanceof View);
                    }
                }
                if(i > 0){
                    Object ev = events.get(i - 1);
                    assertTrue("Before Block should be Unblock but it is " + ev.getClass() + ",events= " + eventString,
                               ev instanceof UnblockEvent);
                }
            }
            else if(event instanceof View){
                if(i + 1 < size){
                    Object ev = events.get(i + 1);
                    assertTrue("After View should be Unblock but it is " + ev.getClass() + ",events= " + eventString,
                               ev instanceof UnblockEvent);
                }
                Object ev = events.get(i - 1);
                assertTrue("Before View should be Block but it is " + ev.getClass() + ",events= " + eventString,
                           ev instanceof BlockEvent);
            }
            else if(event instanceof UnblockEvent){
                if(i + 1 < size){
                    Object ev = events.get(i + 1);
                    assertTrue("After UnBlock should be Block but it is " + ev.getClass() + ",events= " + eventString,
                               ev instanceof BlockEvent);
                }

                Object ev = events.get(i - 1);
                if(isMuxUsed){
                    assertTrue("Before UnBlock should be View or Block but it is " + ev.getClass() + ",events= " + eventString,
                               ev instanceof View || ev instanceof BlockEvent);
                }else{
                    assertTrue("Before UnBlock should be View but it is " + ev.getClass() + ",events= " + eventString,
                               ev instanceof View);
                }
            }
        }       
    }

    private void checkEventStateTransferSequence(FlushTestReceiver receiver) {
        
        List<Object> events = receiver.getEvents();
        String eventString = "[" + receiver.getName() + ",events:" + events;
        log.info(eventString);        
        assertNotNull(events);
        assertTrue(events.size()>1);
        assertTrue("First event is not block but " + events.get(0),events.get(0) instanceof BlockEvent);
        assertTrue("Last event not unblock but " + events.get(events.size()-1),events.get(events.size()-1) instanceof UnblockEvent);
        int size = events.size();
        for(int i = 0;i < size;i++){
            Object event = events.get(i);
            if(event instanceof BlockEvent){
                if(i + 1 < size){
                    Object o = events.get(i + 1);
                    assertTrue("After Block should be state|unblock|view, but it is " + o.getClass() + ",events= "+ eventString,
                               o instanceof SetStateEvent || o instanceof GetStateEvent
                                       || o instanceof UnblockEvent
                                       || o instanceof View);
                }
                if(i > 0){
                    Object o = events.get(i - 1);
                    assertTrue("Before Block should be state or Unblock , but it is " + o.getClass() + ",events= " + eventString, 
                               o instanceof UnblockEvent);
                }
            }
            else if(event instanceof SetStateEvent || event instanceof GetStateEvent){
                if(i + 1 < size){
                    Object o = events.get(i + 1);
                    assertTrue("After state should be Unblock , but it is " + o.getClass() + ",events= " + eventString,
                               o instanceof UnblockEvent);
                }
                Object o = events.get(i - 1);
                assertTrue("Before state should be Block or View , but it is " + o.getClass() + ",events= " + eventString,
                           o instanceof BlockEvent || events.get(i - 1) instanceof View);
            }
            else if(event instanceof UnblockEvent){
                if(i + 1 < size){
                    Object o = events.get(i + 1);
                    assertTrue("After UnBlock should be Block , but it is " + o.getClass() + ",events= " + eventString,
                               o instanceof BlockEvent);
                }
                if(i > 0){
                    Object o = events.get(i - 1);
                    assertTrue("Before UnBlock should be block|state|view , but it is " + o.getClass() + ",events= " + eventString,
                               o instanceof SetStateEvent || o instanceof GetStateEvent
                                       || o instanceof BlockEvent
                                       || o instanceof View);
                }
            }

        }        
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
