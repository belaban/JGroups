// $Id: ChannelTest.java,v 1.4 2006/11/22 20:27:53 vlada Exp $

package org.jgroups.tests;


import junit.framework.Test;
import junit.framework.TestSuite;

import org.jgroups.Channel;
import org.jgroups.ChannelClosedException;
import org.jgroups.ChannelException;
import org.jgroups.ChannelNotConnectedException;
import org.jgroups.TimeoutException;
import org.jgroups.View;


/**
 * Tests various methods in JChannel
 * @author Bela Ban
 * @version $Id: ChannelTest.java,v 1.4 2006/11/22 20:27:53 vlada Exp $
 */
public class ChannelTest extends ChannelTestBase {
    Channel ch;

    private static final String GROUP="DiscardTestGroup";    


    public void setUp() throws Exception {
        super.setUp();
        ch=createChannel();
        ch.connect(GROUP);
    }

    public void tearDown() throws Exception {
        ch.close();
        super.tearDown();        
    }
    
    public void testBasicOperations() throws Exception
    {
       String groupName = GROUP;
       Channel c1 = createChannel("A");
       c1.connect(groupName);
       sleepThread(1000);
       assertTrue(c1.isOpen());
       assertTrue(c1.isConnected());
       
       assertNotNull(c1.getLocalAddress());
       assertNotNull(c1.getView());
       assertTrue(c1.getView().getMembers().contains(c1.getLocalAddress()));
       
       try{
         c1.connect(groupName);
       }
       catch (Exception e)
       {
         fail("Should have NOT generated exception");
       }
       
       c1.disconnect();
       
       assertFalse(c1.isConnected());
       assertTrue(c1.isOpen());
       assertNull(c1.getLocalAddress());
       assertNull(c1.getView());
       
       assertNull(c1.getClusterName());
       
       try{
          c1.connect(groupName);
       }
       catch (Exception e)
       {
         fail("Should have NOT generated exception");
       }
       
       c1.close();
       
       try{
          c1.connect(groupName);
          fail("Should generated exception, and it has NOT");
       }
       catch (Exception e)
       {
         assertTrue(e instanceof ChannelClosedException);
       }
       
       assertFalse(c1.isConnected());
       assertFalse(c1.isOpen());
       assertNull(c1.getLocalAddress());
       assertNull(c1.getView());
       
       assertNull(c1.getClusterName());                       
       
       c1 = createChannel("A");
       c1.connect(groupName);
       Channel c2 = createChannel("A");
       c2.connect(groupName);
       
       sleepThread(1000);
       
       assertTrue(c1.isOpen());
       assertTrue(c1.isConnected());
       
       assertNotNull(c1.getLocalAddress());
       assertNotNull(c1.getView());
       assertTrue(c1.getView().getMembers().contains(c1.getLocalAddress()));
       assertTrue(c1.getView().getMembers().contains(c2.getLocalAddress()));
       
       assertTrue(c2.isOpen());
       assertTrue(c2.isConnected());
       
       assertNotNull(c2.getLocalAddress());
       assertNotNull(c2.getView());
       assertTrue(c2.getView().getMembers().contains(c2.getLocalAddress()));
       assertTrue(c2.getView().getMembers().contains(c1.getLocalAddress()));
       
       c2.close();
       sleepThread(1000);
       
       assertFalse(c2.isOpen());
       assertFalse(c2.isConnected());
       
       assertNull(c2.getLocalAddress());
       assertNull(c2.getView());
       
       assertTrue(c1.isOpen());
       assertTrue(c1.isConnected());
       
       assertNotNull(c1.getLocalAddress());
       assertNotNull(c1.getView());
       assertTrue(c1.getView().getMembers().contains(c1.getLocalAddress()));
       assertFalse(c1.getView().getMembers().contains(c2.getLocalAddress()));
       
       c1.close();       
    }

    public void testFirstView() throws Exception {
        Object obj=ch.receive(5000);
        if(!(obj instanceof View)) {
            fail("first object returned needs to be a View (was " + obj + ")");
        }
        else {
            System.out.println("view is " + obj);
        }
    }

    public void testReceiveTimeout() throws ChannelException, TimeoutException {
        ch.receive(1000); // this one works, because we're expecting a View

        // .. but this one doesn't (no msg available) - needs to throw a TimeoutException
        try {
            ch.receive(2000);
        }
        catch(ChannelNotConnectedException e) {
            fail("channel should be connected");
        }
        catch(ChannelClosedException e) {
            fail("channel should not be closed");
        }
        catch(TimeoutException e) {
            System.out.println("caught a TimeoutException - this is the expected behavior");
        }
    }

    public void testNullMessage() throws ChannelClosedException, ChannelNotConnectedException {
        try {
            ch.send(null);
            fail("null message should throw an exception - we should not get here");
        }
        catch(NullPointerException e) {
            System.out.println("caught NullPointerException - this is expected");
        }
    }


    public static Test suite() {
        TestSuite s=new TestSuite(ChannelTest.class);
        return s;
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }




}


