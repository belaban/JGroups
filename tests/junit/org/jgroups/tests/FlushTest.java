package org.jgroups.tests;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.*;
import org.jgroups.util.Util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.LinkedList;
import java.util.List;


/**
 * Tests the FLUSH protocol, requires flush-udp.xml in ./conf to be present and configured to use FLUSH
 * @author Bela Ban
 * @version $Id: FlushTest.java,v 1.5 2006/09/28 17:13:26 vlada Exp $
 */
public class FlushTest extends TestCase {
    Channel c1, c2;
    static final String CONFIG="flush-udp.xml";


    public FlushTest(String name) {
        super(name);
    }
    
    public void tearDown() throws Exception {
        super.tearDown();

        if(c2 != null) {
            c2.close();
            Util.sleep(2000);
            c2=null;
        }

        if(c1 != null) {
            c1.close();
            Util.sleep(2000);
            c1=null;
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


    public void testTwoChannels() throws ChannelException {
        c1=createChannel();
        MyReceiver receiver=new MyReceiver("c1");
        c1.setReceiver(receiver);
        c1.connect("bla");
        checkSingleMemberJoinSequence(receiver);

        c2=createChannel();
        MyReceiver receiver2=new MyReceiver("c2");
        c2.setReceiver(receiver2);
        c2.connect("bla");
        View view=c2.getView();
        assertEquals(2, view.size());
        Util.sleep(500);

        checkExistingMemberAfterJoinSequence(receiver);
        
        checkNewMemberAfterJoinSequence(receiver2);

        c2.close();
        Util.sleep(500);     
        
        checkExistingMemberAfterLeaveSequence(receiver);
    }



    public void testWithStateTransfer() throws ChannelException {
        c1=createChannel();
        MyReceiver receiver=new MyReceiver("c1");
        c1.setReceiver(receiver);
        c1.connect("bla");

        c2=createChannel();
        MyReceiver receiver2=new MyReceiver("c2");
        c2.setReceiver(receiver2);
        c2.connect("bla");
        Util.sleep(2000);

        receiver.clear(); receiver2.clear();
        System.out.println("=== fetching the state ====");
        c2.getState(null, 10000);
        Util.sleep(3000);

        List events=receiver.getEvents();
        checkBlockStateUnBlockSequence(events, "c1");

        events=receiver2.getEvents();
        checkBlockStateUnBlockSequence(events, "c2");
    }



    private void checkBlockStateUnBlockSequence(List events, String name) {        
        assertNotNull(events);
        assertEquals("Should have three events [block,get|setstate,unblock] but " + name + " has "
				+ events, 3, events.size());
        Object obj=events.remove(0);
        assertTrue(name, obj instanceof BlockEvent);
        obj=events.remove(0);
        assertTrue(name, obj instanceof GetStateEvent || obj instanceof SetStateEvent);
        obj=events.remove(0);
        assertTrue(name, obj instanceof UnblockEvent);
    }
    
    private void checkSingleMemberJoinSequence(MyReceiver receiver) {
		List events = receiver.getEvents();
		assertNotNull(events);
		assertEquals("Should have one event [view] but " +receiver.getName()+" has " + events, 1,events.size());
		Object obj = events.remove(0);
		assertTrue("should be view instance but it is " + obj,obj instanceof View);
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
    
    private void checkExistingMemberAfterLeaveSequence(MyReceiver receiver)
    {
    	checkExistingMemberAfterJoinSequence(receiver);
    }
    
    private void checkNewMemberAfterJoinSequence(MyReceiver receiver)
    {
    	List events = receiver.getEvents();    
        assertNotNull(events);               
        assertEquals("Should have two events [view,unblock] but " + receiver.getName() + " has "
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
        List events=new LinkedList();
        String name;

        public MyReceiver(String name) {
            this.name=name;
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
            System.out.println("[" + name + "]: BLOCK");
            events.add(new BlockEvent());
        }

        public void unblock() {
            System.out.println("[" + name + "]: UNBLOCK");
            events.add(new UnblockEvent());
        }

        public void viewAccepted(View new_view) {
            System.out.println("[" + name + "]: " + new_view);
            events.add(new_view);
        }

        public byte[] getState() {
            System.out.println("[" + name + "]: GetStateEvent");
            events.add(new GetStateEvent(null, null));
            return new byte[]{'b', 'e', 'l', 'a'};
        }

        public void setState(byte[] state) {
            System.out.println("[" + name + "]: SetStateEvent");
            events.add(new SetStateEvent(null, null));
        }
        
        public void getState(OutputStream ostream) {
        	System.out.println("[" + name + "]: GetStateEvent streamed");
        	events.add(new GetStateEvent(null, null));
        	try {
				ostream.close();
			} catch (IOException e) {				
				e.printStackTrace();
			}

    	}    

    	public void setState(InputStream istream) {
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
