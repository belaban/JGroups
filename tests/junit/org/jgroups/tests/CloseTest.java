// $Id: CloseTest.java,v 1.16 2008/04/08 08:29:32 belaban Exp $

package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.ChannelClosedException;
import org.jgroups.View;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Vector;


/**
 * Demos the creation of a channel and subsequent connection and closing. Demo application should exit (no
 * more threads running)
 */
public class CloseTest extends ChannelTestBase {
    Channel channel, channel1, channel2, c1, c2, c3;  

    @BeforeMethod
    public void setUp() throws Exception {
        ;
    }


    @AfterMethod
    public void tearDown() throws Exception {
        closeChannel(channel);
        closeChannel(channel1);
        closeChannel(channel2);
        closeChannel(c1);
        closeChannel(c2);
        closeChannel(c3);

        ;
    }
    
    protected boolean useBlocking()
    {
       return false;
    }
 

    private void closeChannel(Channel c) {
        if(c != null && (c.isOpen() || c.isConnected())) {
            c.close();
        }
    }


    @Test
    public void testDoubleClose() throws Exception {
        System.out.println("-- creating channel1 --");
        channel1=createChannel();
        System.out.println("-- connecting channel1 --");
        channel1.connect("bla");
        
        assertTrue("channel open", channel1.isOpen());
        assertTrue("channel connected", channel1.isConnected()); 
        
        System.out.println("-- closing channel1 --");
        channel1.close();
        System.out.println("-- closing channel1 (again) --");
        channel1.close();
        assertFalse("channel not connected", channel1.isConnected());
        System.out.println("-- done, threads are ");
        Util.printThreads();
    }

    @Test
    public void testCreationAndClose() throws Exception {
        System.out.println("-- creating channel1 --");
        Channel c = null;
        c = createChannel();
        c.connect("CloseTest1");
        assertTrue("channel open", c.isOpen());
        assertTrue("channel connected", c.isConnected());        
        c.close();        
        assertFalse("channel not connected", c.isConnected());
        c.close();
    }
    
    @Test
    public void testViewChangeReceptionOnChannelCloseByParticipant() throws Exception {
        Address  a1, a2;
        Vector members;
        c1 = createChannel("A");               
        System.out.println("-- connecting c1");
        c1.connect("X");
        Util.sleep(500); // time to receive its own view
        dumpMessages("c1", c1);
        a1=c1.getLocalAddress();       
        c2 = createChannel("A");   
        System.out.println("-- connecting c2");
        c2.connect("X");
        Util.sleep(500); // time to receive its own view
        a2=c2.getLocalAddress();
        dumpMessages("c2", c2);

        System.out.println("-- closing c2");
        c2.close();
        Object obj=c1.receive(100);
        assertTrue(obj instanceof View);
        View v=(View)obj;
        members=v.getMembers();
        System.out.println("-- first view of c1: " + v);
        Assert.assertEquals(2, members.size());
        assertTrue(members.contains(a1));
        assertTrue(members.contains(a2));

        obj=c1.receive(100);
        assertTrue(obj instanceof View);
        v=(View)obj;
        members=v.getMembers();
        System.out.println("-- second view of c1: " + v);
        Assert.assertEquals(1, members.size());
        assertTrue(members.contains(a1));
        assertFalse(members.contains(a2));
    }

    @Test
    public void testViewChangeReceptionOnChannelCloseByCoordinator() throws Exception {
        Address  a1, a2;
        Vector members;
        Object obj;
        View v;
        c1=createChannel("A");
        c1.connect("X");
        Util.sleep(500); // time to receive its own view
        dumpMessages("c1", c1);
        a1=c1.getLocalAddress();
        c2=createChannel("A");       
        c2.connect("X");
        Util.sleep(500); // time to receive its own view
        a2=c2.getLocalAddress();
        v=(View)c2.receive(1);
        members=v.getMembers();
        Assert.assertEquals(2, members.size());
        assertTrue(members.contains(a2));

        c1.close();
        Util.sleep(500);

        System.out.println("queue of c2 is " + c2.dumpQueue());
        assertTrue("found 0 messages in channel", c2.getNumMessages() > 0);
        obj=c2.receive(0);
        assertTrue(obj instanceof View);
        v=(View)obj;
        members=v.getMembers();
        Assert.assertEquals(1, members.size());
        assertFalse(members.contains(a1));
        assertTrue(members.contains(a2));

        Assert.assertEquals(0, c2.getNumMessages());
    }

    private void dumpMessages(String msg, Channel ch) throws Exception {
        while(ch.getNumMessages() > 0) {
            Object obj=ch.receive(0);
            if(obj instanceof View)
                System.out.println(msg + ": " + obj);
        }
    }

    @Test
    public void testConnectDisconnectConnectCloseSequence() throws Exception {
        System.out.println("-- creating channel --");
        channel=createChannel();
        System.out.println("-- connecting channel to CloseTest1--");
        channel.connect("CloseTest1");
        System.out.println("view is " + channel.getView());
        System.out.println("-- disconnecting channel --");
        channel.disconnect();
        System.out.println("-- connecting channel to OtherGroup --");
        channel.connect("OtherGroup");
        System.out.println("view is " + channel.getView());
        System.out.println("-- closing channel --");
        channel.close();
        System.out.println("-- done, threads are ");
        Util.printThreads();
    }


    @Test
    public void testConnectCloseSequenceWith2Members() throws Exception {
        System.out.println("-- creating channel --");
        channel=createChannel("A");
        System.out.println("-- connecting channel --");
        channel.connect("X");
        System.out.println("view is " + channel.getView());

        System.out.println("-- creating channel1 --");
        channel1=createChannel("A");
        System.out.println("-- connecting channel1 --");
        channel1.connect("X");
        System.out.println("view is " + channel1.getView());

        System.out.println("-- closing channel1 --");
        channel1.close();

        Util.sleep(2000);
        System.out.println("-- closing channel --");
        channel.close();
    }


    @Test
    public void testCreationAndClose2() throws Exception {
        System.out.println("-- creating channel2 --");
        channel2=createChannel();
        System.out.println("-- connecting channel2 --");
        channel2.connect("CloseTest2");
        System.out.println("-- closing channel --");
        channel2.close();
        Util.sleep(2000);
        Util.printThreads();
    }


    @Test
    public void testChannelClosedException() throws Exception {
        System.out.println("-- creating channel --");
        channel=createChannel();
        System.out.println("-- connecting channel --");
        channel.connect("CloseTestLoop");
        System.out.println("-- closing channel --");
        channel.close();
        Util.sleep(2000);

        try {
            channel.connect("newGroup");
            assert false;
        }
        catch(ChannelClosedException ex) {
            assertTrue(true);
        }
    }

    @Test
    public void testCreationAndCloseLoop() throws Exception {
        System.out.println("-- creating channel --");
        channel=createChannel();

        for(int i=1; i <= 10; i++) {
            System.out.println("-- connecting channel (attempt #" + i + " ) --");
            channel.connect("CloseTestLoop2");
            System.out.println("-- closing channel --");
            channel.close();

            System.out.println("-- reopening channel --");
            channel.open();
        }
        channel.close();
    }   


    @Test
    public void testMultipleConnectsAndDisconnects() throws Exception {
        c1=createChannel("A");
        assertTrue(c1.isOpen());
        assertFalse(c1.isConnected());
        c1.connect("bla");
        System.out.println("view after c1.connect(): " + c1.getView());
        assertTrue(c1.isOpen());
        assertTrue(c1.isConnected());
        assertServiceAndClusterView(c1, 1);

        c2=createChannel("A");
        assertTrue(c2.isOpen());
        assertFalse(c2.isConnected());

        c2.connect("bla");
        System.out.println("view after c2.connect(): " + c2.getView());
        assertTrue(c2.isOpen());
        assertTrue(c2.isConnected());
        assertServiceAndClusterView(c2, 2);
        Util.sleep(500);
        assertServiceAndClusterView(c1, 2);

        c2.disconnect();
        System.out.println("view after c2.disconnect(): " + c2.getView());
        assertTrue(c2.isOpen());
        assertFalse(c2.isConnected());
        Util.sleep(500);
        assertServiceAndClusterView(c1, 1);

        c2.connect("bla");
        System.out.println("view after c2.connect(): " + c2.getView());
        assertTrue(c2.isOpen());
        assertTrue(c2.isConnected());
        assertServiceAndClusterView(c2, 2);
        Util.sleep(300);
        assertServiceAndClusterView(c1, 2);

        // Now see what happens if we reconnect the first channel
        c3=createChannel("A");
        assertTrue(c3.isOpen());
        assertFalse(c3.isConnected());
        assertServiceAndClusterView(c1, 2);
        assertServiceAndClusterView(c2, 2);

        c1.disconnect();
        Util.sleep(1000);
        assertTrue(c1.isOpen());
        assertFalse(c1.isConnected());
        assertServiceAndClusterView(c2, 1);
        assertTrue(c3.isOpen());
        assertFalse(c3.isConnected());

        c1.connect("bla");
        System.out.println("view after c1.connect(): " + c1.getView());
        assertTrue(c1.isOpen());
        assertTrue(c1.isConnected());
        assertServiceAndClusterView(c1, 2);
        Util.sleep(500);
        assertServiceAndClusterView(c2, 2);
        assertTrue(c3.isOpen());
        assertFalse(c3.isConnected());
    }


    private void assertServiceAndClusterView(Channel ch, int num) {
        View view=ch.getView();
        String msg="view=" + view;
        assertNotNull(view);
        Assert.assertEquals(view.size(), num, msg);
    }

 
}
