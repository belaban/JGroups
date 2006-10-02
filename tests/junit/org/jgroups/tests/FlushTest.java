package org.jgroups.tests;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.*;
import org.jgroups.util.Util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;


/**
 * Tests the FLUSH protocol, requires flush-udp.xml in ./conf to be present and configured to use FLUSH
 * @author Bela Ban
 * @version $Id: FlushTest.java,v 1.7 2006/10/02 08:39:19 belaban Exp $
 */
public class FlushTest extends TestCase {
    Channel c1, c2,c3;
    static final String CONFIG="flush-udp.xml";


    public FlushTest(String name) {
        super(name);
    }
    
    public void tearDown() throws Exception {
        super.tearDown();

        if(c2 != null) {
            c2.close();
            Util.sleep(500);
            c2=null;
        }

        if(c1 != null) {
            c1.close();
            Util.sleep(500);
            c1=null;
        }
        
        if(c3 != null) {
            c3.close();
            Util.sleep(500);
            c3=null;
        }
    }


    public void testSingleChannel() throws ChannelException {
        c1=createChannel();
        MyReceiver receiver=new MyReceiver("c1");
        c1.setReceiver(receiver);
        c1.connect("bla");
        checkSingleMemberJoinSequence(receiver);

        c1.close();
        Util.sleep(500);  
        assertEquals(0, receiver.getEvents().size());
    }     
    
    public void testTwoChannelsWithMessages() throws ChannelException {
    	twoChannelsTestHelper(true);
    }
    
    public void testTwoChannelsNoMessages() throws ChannelException {
    	twoChannelsTestHelper(false);
    }

    public void testThreeChannelsWithMessages() throws ChannelException {
    	threeChannelsTestHelper(true);
    }
    
    public void testThreeChannelsNoMessages() throws ChannelException {
    	threeChannelsTestHelper(false);
    }
    
    public void testStateTransferWithMessages() throws ChannelException {
    	stateTransferTestHelper(true);
    }
    
    public void testStateTransferNoMessages() throws ChannelException {
    	stateTransferTestHelper(false);
    }
    
    private void twoChannelsTestHelper(boolean sendMessages) throws ChannelException {
        c1=createChannel();
        MyReceiver receiver=new MyReceiver("c1");
        c1.setReceiver(receiver);
        c1.connect("bla");
        if(sendMessages){
        	c1.send(new Message());
        }
        checkSingleMemberJoinSequence(receiver);

        c2=createChannel();
        MyReceiver receiver2=new MyReceiver("c2");
        c2.setReceiver(receiver2);
        c2.connect("bla");        
        View view=c2.getView();
        assertEquals(2, view.size());
        Util.sleep(1000);
        if(sendMessages){
	        c1.send(new Message());    
	        c2.send(new Message());
        }

        checkExistingMemberAfterJoinSequence(receiver);
        
        checkNewMemberAfterJoinSequence(receiver2);

        c2.close();
        Util.sleep(500); 
        if(sendMessages){
        	c1.send(new Message());
        }
        Util.sleep(500);     
        
        checkExistingMemberAfterLeaveSequence(receiver);
    }
    
    
    private void threeChannelsTestHelper(boolean sendMessages) throws ChannelException {
        c1=createChannel();
        MyReceiver receiver=new MyReceiver("c1");
        c1.setReceiver(receiver);
        c1.connect("bla");
        if(sendMessages){
        	c1.send(new Message());
        }
        
        checkSingleMemberJoinSequence(receiver);

        c2=createChannel();
        MyReceiver receiver2=new MyReceiver("c2");
        c2.setReceiver(receiver2);
        c2.connect("bla");        
        View view=c2.getView();
        assertEquals(2, view.size());
        Util.sleep(1000);
        
        if(sendMessages){
	        c1.send(new Message());       
	        c2.send(new Message());
        }

        checkExistingMemberAfterJoinSequence(receiver);                              
        checkNewMemberAfterJoinSequence(receiver2);
        
        c3=createChannel();
        MyReceiver receiver3=new MyReceiver("c3");
        c3.setReceiver(receiver3);
        c3.connect("bla");        
        view=c3.getView();
        assertEquals(3, view.size());
        Util.sleep(1000);
        if(sendMessages){
	        c1.send(new Message());        
	        c2.send(new Message());       
	        c3.send(new Message());
        }
        
        checkExistingMemberAfterJoinSequence(receiver); 
        checkExistingMemberAfterJoinSequence(receiver2);                              
        checkNewMemberAfterJoinSequence(receiver3);
                
        //close coordinator
        c1.close();
        if(sendMessages){
	        c2.send(new Message());
	        c2.send(new Message()); 
        }
        Util.sleep(1000);     
        
        checkExistingMemberAfterLeaveSequence(receiver2);
        checkExistingMemberAfterLeaveSequence(receiver3);
        
        //close coordinator one more time
        c2.close();        
        Util.sleep(1000);     
        
        checkExistingMemberAfterLeaveSequence(receiver3);
    }



    private void stateTransferTestHelper(boolean sendMessages) throws ChannelException {
        c1=createChannel();
        MyReceiver receiver=new MyReceiver("c1");
        c1.setReceiver(receiver);
        c1.connect("bla");
        Util.sleep(1000);
        if(sendMessages){
	        c1.send(new Message());
	        c1.send(new Message());
        }
        checkSingleMemberJoinSequence(receiver);        
        
        c2=createChannel();
        MyReceiver receiver2=new MyReceiver("c2");
        c2.setReceiver(receiver2);
        c2.connect("bla");
        Util.sleep(1000);
        
        checkExistingMemberAfterJoinSequence(receiver);                                  
        checkNewMemberAfterJoinSequence(receiver2);
        
        c3=createChannel();
        MyReceiver receiver3=new MyReceiver("c3");
        c3.setReceiver(receiver3);
        c3.connect("bla");  
        Util.sleep(1000);
        if(sendMessages){
	        c1.send(new Message());        
	        c2.send(new Message()); 
        }
        
        checkExistingMemberAfterJoinSequence(receiver); 
        checkExistingMemberAfterJoinSequence(receiver2);                              
        checkNewMemberAfterJoinSequence(receiver3);
       
        Util.sleep(500);
        if(sendMessages){
	        c1.send(new Message());        
	        c2.send(new Message());       
	        c3.send(new Message());
        }
        
        System.out.println("=== fetching the state ====");
        c2.getState(null, 10000);
        Util.sleep(2000);
        if(sendMessages){
	        c1.send(new Message());
	        c1.send(new Message());
	        c2.send(new Message());
	        c2.send(new Message());   
        }
        
        //state transfer nodes
        checkBlockStateUnBlockSequence(receiver);     
        checkBlockStateUnBlockSequence(receiver2);
        
        //this node did not participate in state transfer
        checkNonStateTransferMemberSequence(receiver3);
        
        c2.close();
        c2=null;
        Util.sleep(2000);   
        checkExistingMemberAfterLeaveSequence(receiver);
        checkExistingMemberAfterLeaveSequence(receiver3);      
        
        
        c2=createChannel();
        receiver2=new MyReceiver("c2");
        c2.setReceiver(receiver2);
        c2.connect("bla");
        Util.sleep(2000);
        if(sendMessages){
        	c1.send(new Message());
        }
        
        checkNewMemberAfterJoinSequence(receiver2);
        checkExistingMemberAfterJoinSequence(receiver); 
        checkExistingMemberAfterJoinSequence(receiver3);  
        
        
        System.out.println("=== fetching the state ====");
        c3.getState(null, 10000);
        if(sendMessages){
	        c1.send(new Message());       
	        c2.send(new Message());
        }
        Util.sleep(2000);
        
        //state transfer nodes
        checkBlockStateUnBlockSequence(receiver);     
        checkBlockStateUnBlockSequence(receiver3);
        
        //this node did not participate in state transfer
        checkNonStateTransferMemberSequence(receiver2);                              
    }



    private void checkBlockStateUnBlockSequence(MyReceiver receiver) {  
    	List events = receiver.getEvents();
    	String name = receiver.getName();
        assertNotNull(events);
        assertEquals("Should have three events [block,get|setstate,unblock] but " + name + " has "
				+ events, 3, events.size());
        Object obj=events.remove(0);
        assertTrue(name, obj instanceof BlockEvent);
        obj=events.remove(0);
        assertTrue(name, obj instanceof GetStateEvent || obj instanceof SetStateEvent);
        obj=events.remove(0);
        assertTrue(name, obj instanceof UnblockEvent);
        receiver.clear();
    }
    
    private void checkSingleMemberJoinSequence(MyReceiver receiver) {
       List events = receiver.getEvents();  
       String name = receiver.getName();
       assertNotNull(events);      
       System.out.println("[" + name + "]:" + events);
       assertEquals("Should have two events [view,unblock] but " + name + " has "
                + events, 2, events.size());
       Object obj=events.remove(0);       
       assertTrue("should be a View but is " + obj, obj instanceof View);
       obj=events.remove(0);
       assertTrue(obj instanceof UnblockEvent);        
       receiver.clear();
	}
    
    private void checkExistingMemberAfterJoinSequence(MyReceiver receiver) {
		List events = receiver.getEvents();
		assertNotNull(events);
		assertEquals("Should have three events [block,view,unblock] but " + receiver.getName() + " has "
						+ events, 3, events.size());
		
		Object obj = events.remove(0);
		assertTrue(obj instanceof BlockEvent);
		obj = events.remove(0);
		assertTrue("should be a View but is " + obj, obj instanceof View);
		obj = events.remove(0);
		assertTrue(obj instanceof UnblockEvent);
		receiver.clear();
	}
    
    private void checkNonStateTransferMemberSequence(MyReceiver receiver) {
		List events = receiver.getEvents();
		assertNotNull(events);
		assertEquals("Should have two events [block,unblock] but " + receiver.getName() + " has "
						+ events, 2, events.size());
		
		Object obj = events.remove(0);
		assertTrue(obj instanceof BlockEvent);
		obj = events.remove(0);		
		assertTrue(obj instanceof UnblockEvent);
		receiver.clear();
	}
    
    private void checkExistingMemberAfterLeaveSequence(MyReceiver receiver)
    {
    	checkExistingMemberAfterJoinSequence(receiver);
    }
    
    private void checkNewMemberAfterJoinSequence(MyReceiver receiver)
    {
    	List events = receiver.getEvents();  
    	String name = receiver.getName();
        assertNotNull(events);      
        System.out.println("[" + name + "]:" + events);
        assertEquals("Should have two events [view,unblock] but " + name + " has "
				+ events, 2, events.size());
        Object obj=events.remove(0);       
        assertTrue("should be a View but is " + obj, obj instanceof View);
        obj=events.remove(0);
        assertTrue(obj instanceof UnblockEvent);        
        receiver.clear();
    }

    private Channel createChannel() throws ChannelException {
        Channel ret=new JChannel(CONFIG);
        ret.setOpt(Channel.BLOCK, Boolean.TRUE);
        return ret;
    }


    public static Test suite() {
        return new TestSuite(FlushTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(FlushTest.suite());
    }

    private static class MyReceiver extends ExtendedReceiverAdapter {
        List events;
        String name;
        boolean verbose = true;

        public MyReceiver(String name) {
            this.name=name;
            events=Collections.synchronizedList(new LinkedList());
        }
        
        public String getName()
        {
        	return name;
        }

        public void clear() {
            events.clear();
        }

        public List getEvents() {return new LinkedList(events);}

        public void block() {
        	if(verbose)
        		System.out.println("[" + name + "]: BLOCK");
            events.add(new BlockEvent());
        }

        public void unblock() {
        	if(verbose)
        		System.out.println("[" + name + "]: UNBLOCK");
            events.add(new UnblockEvent());
        }

        public void viewAccepted(View new_view) {
        	if(verbose)
        		System.out.println("[" + name + "]: " + new_view);
            events.add(new_view);
        }

        public byte[] getState() {
        	if(verbose)
        		System.out.println("[" + name + "]: GetStateEvent");
            events.add(new GetStateEvent(null, null));
            return new byte[]{'b', 'e', 'l', 'a'};
        }

        public void setState(byte[] state) {
        	if(verbose)
        		System.out.println("[" + name + "]: SetStateEvent");
            events.add(new SetStateEvent(null, null));
        }
        
        public void getState(OutputStream ostream) {
        	if(verbose)
        		System.out.println("[" + name + "]: GetStateEvent streamed");
        	events.add(new GetStateEvent(null, null));
        	try {
				ostream.close();
			} catch (IOException e) {				
				e.printStackTrace();
			}

    	}    

    	public void setState(InputStream istream) {
    		if(verbose)
    			System.out.println("[" + name + "]: SetStateEvent streamed");
			events.add(new SetStateEvent(null, null));
			try {
				istream.close();
			} catch (IOException e) {				
				e.printStackTrace();
			}
		}
    }
}
