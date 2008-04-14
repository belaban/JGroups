package org.jgroups.tests;


import org.testng.annotations.*;
import org.jgroups.*;
import org.jgroups.mux.MuxChannel;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.LinkedList;
import java.util.List;

/**
 * Test the multiplexer functionality provided by JChannelFactory, especially the service views and cluster views
 * @author Bela Ban
 * @version $Id: MultiplexerViewTest.java,v 1.19 2008/04/14 08:18:40 belaban Exp $
 */
public class MultiplexerViewTest extends ChannelTestBase {
    private Channel c1, c2, c3, c4;    
    static final BlockEvent BLOCK_EVENT=new BlockEvent();
    static final UnblockEvent UNBLOCK_EVENT=new UnblockEvent();
    JChannelFactory factory, factory2;



    @BeforeMethod
    public void setUp() throws Exception {
        factory=new JChannelFactory();
        factory.setMultiplexerConfig(mux_conf);

        factory2=new JChannelFactory();
        factory2.setMultiplexerConfig(mux_conf);
    }

    @AfterMethod
    public void tearDown() throws Exception {
        Util.sleep(1000);
        if(c2 != null)
            c2.close();
        if(c1 != null)
            c1.close();
        factory.destroy();
        factory2.destroy();
    }

    @Test
    public void testBasicLifeCycle() throws Exception {
        c1=factory.createMultiplexerChannel(mux_conf_stack, "service-1");
        System.out.println("c1: " + c1);
        assertTrue(c1.isOpen());
        assertFalse(c1.isConnected());
        View v=c1.getView();
        assert v == null;

        v=((MuxChannel)c1).getClusterView();
        assert v == null;
        Address local_addr=c1.getLocalAddress();
        assert local_addr == null;

        c1.connect("bla");
        assertTrue(c1.isConnected());
        local_addr=c1.getLocalAddress();
        assertNotNull(local_addr);
        v=c1.getView();
        assertNotNull(v);
        Assert.assertEquals(1, v.size());
        v=((MuxChannel)c1).getClusterView();
        assertNotNull(v);
        Assert.assertEquals(1, v.size());

        c1.disconnect();
        assertFalse(c1.isConnected());
        assertTrue(c1.isOpen());
        local_addr=c1.getLocalAddress();
        assert local_addr == null;
        c1.close();
        assertFalse(c1.isOpen());
    }



    @Test
    public void testBlockPush() throws Exception {
        c1=factory.createMultiplexerChannel(mux_conf_stack, "service-1");
        c1.setOpt(Channel.BLOCK, Boolean.TRUE);
        MyReceiver receiver=new MyReceiver();
        c1.setReceiver(receiver);
        c1.connect("bla");

        c2=factory2.createMultiplexerChannel(mux_conf_stack, "service-1");
        c2.setOpt(Channel.BLOCK, Boolean.TRUE);
        MyReceiver receiver2=new MyReceiver();
        c2.setReceiver(receiver2);
        c2.connect("bla");

        //let async block events propagate
        Util.sleep(1000);
        
        List events=receiver.getEvents();
        int num_events=events.size();
        System.out.println("-- receiver: " + events);
        Assert.assertEquals(num_events, 8, "we should have a BLOCK, UNBLOCK, BLOCK, UNBLOCK,BLOCK, UNBLOCK, BLOCK, UNBLOCK sequence");
        Object evt=events.remove(0);
        assertTrue("evt=" + evt, evt instanceof BlockEvent);
        evt=events.remove(0);
        assertTrue("evt=" + evt, evt instanceof UnblockEvent);
        evt=events.remove(0);
        assertTrue("evt=" + evt, evt instanceof BlockEvent);
        evt=events.remove(0);
        assertTrue("evt=" + evt, evt instanceof UnblockEvent);

        events=receiver2.getEvents();
        num_events=events.size();
        System.out.println("-- receiver2: " + events);
        evt=events.remove(0);
        assertTrue(evt instanceof BlockEvent);
        evt=events.remove(0);
        assertTrue(evt instanceof UnblockEvent);
    }


    @Test
    public void testBlockPush2() throws Exception {
        c1=factory.createMultiplexerChannel(mux_conf_stack, "service-1");
        c1.setOpt(Channel.BLOCK, Boolean.TRUE);
        MyReceiver receiver=new MyReceiver();
        c1.setReceiver(receiver);
        c1.connect("bla");
        // Util.sleep(500);

        c2=factory.createMultiplexerChannel(mux_conf_stack, "service-2");
        c2.setOpt(Channel.BLOCK, Boolean.TRUE);
        MyReceiver receiver2=new MyReceiver();
        c2.setReceiver(receiver2);
        c2.connect("bla");
        // Util.sleep(500);

        c3=factory.createMultiplexerChannel(mux_conf_stack, "service-3");
        c3.setOpt(Channel.BLOCK, Boolean.TRUE);
        MyReceiver receiver3=new MyReceiver();
        c3.setReceiver(receiver3);
        c3.connect("bla");
        // Util.sleep(500);

        c4=factory2.createMultiplexerChannel(mux_conf_stack, "service-3");
        c4.setOpt(Channel.BLOCK, Boolean.TRUE);
        MyReceiver receiver4=new MyReceiver();
        c4.setReceiver(receiver4);
        c4.connect("bla");
        
        //let asynch block/unblock events propagate
        Util.sleep(1000);
        List events=receiver.getEvents();
        checkBlockAndUnBlock(events, "receiver", new Object[]
      {BLOCK_EVENT, UNBLOCK_EVENT, BLOCK_EVENT, UNBLOCK_EVENT, BLOCK_EVENT, UNBLOCK_EVENT, BLOCK_EVENT, UNBLOCK_EVENT,
            BLOCK_EVENT, UNBLOCK_EVENT, BLOCK_EVENT, UNBLOCK_EVENT});

        events=receiver2.getEvents();
        checkBlockAndUnBlock(events, "receiver2", new Object[]{BLOCK_EVENT, UNBLOCK_EVENT, BLOCK_EVENT, UNBLOCK_EVENT, BLOCK_EVENT, UNBLOCK_EVENT, BLOCK_EVENT, UNBLOCK_EVENT,});

        events=receiver3.getEvents();
        checkBlockAndUnBlock(events, "receiver3", new Object[]{BLOCK_EVENT, UNBLOCK_EVENT,BLOCK_EVENT, UNBLOCK_EVENT,BLOCK_EVENT, UNBLOCK_EVENT});

        events=receiver4.getEvents();
        System.out.println("-- [receiver4] events: " + events);
        // now the new joiner should *not* have a block event !
        // assertFalse("new joiner should not have a BlockEvent", events.contains(new BlockEvent()));
        checkBlockAndUnBlock(events, "receiver4", new Object[]{BLOCK_EVENT, UNBLOCK_EVENT,BLOCK_EVENT, UNBLOCK_EVENT});

        receiver.clear();
        receiver2.clear();
        receiver3.clear();
        receiver4.clear();

        System.out.println("-- Closing c4");

        c4.close();
        //close under FLUSH is not *synchronous* as connect/getState 
        //so we have to sleep here
        Util.sleep(5000);

        events=receiver.getEvents();
        checkBlockAndUnBlock(events, "receiver", new Object[]{BLOCK_EVENT, UNBLOCK_EVENT,BLOCK_EVENT, UNBLOCK_EVENT});

        events=receiver2.getEvents();
        checkBlockAndUnBlock(events, "receiver2", new Object[]{BLOCK_EVENT, UNBLOCK_EVENT,BLOCK_EVENT, UNBLOCK_EVENT});

        events=receiver3.getEvents();
        checkBlockAndUnBlock(events, "receiver3", new Object[]{BLOCK_EVENT, UNBLOCK_EVENT,BLOCK_EVENT, UNBLOCK_EVENT});

        events=receiver4.getEvents();
        System.out.println("-- [receiver4] events: " + events);
        // now the new joiner should *not* have a block event !
        assertFalse("new joiner should not have a BlockEvent", events.contains(new BlockEvent()));


    }


    private void checkBlockAndUnBlock(List events, String service_name, Object[] seq) {
        int num_events=events.size();
        System.out.println("-- [" + service_name + "] events: " + events);
        Assert.assertEquals(num_events, seq.length, "[" + service_name + "] we should have the following sequence: " + Util.array2String(seq));

        Object expected_type;
        Object actual_type;
        for(int i=0; i < seq.length; i++) {
            expected_type=seq[i];
            actual_type=events.remove(0);
            assertEquals("element should be " + expected_type.getClass().getName(), actual_type.getClass(), expected_type.getClass());
        }

    }


    @Test
    public void testTwoServicesOneChannel() throws Exception {
        c1=factory.createMultiplexerChannel(mux_conf_stack, "service-1");
        c2=factory.createMultiplexerChannel(mux_conf_stack, "service-2");
        c1.connect("bla");
        c2.connect("blo");

        View v=((MuxChannel)c1).getClusterView(), v2=((MuxChannel)c2).getClusterView();
        assertNotNull(v);
        assertNotNull(v2);
        assertEquals(v, v2);
        Assert.assertEquals(1, v.size());
        Assert.assertEquals(1, v2.size());

        v=c1.getView();
        v2=c2.getView();
        assertNotNull(v);
        assertNotNull(v2);
        assertEquals(v, v2);
        Assert.assertEquals(1, v.size());
        Assert.assertEquals(1, v2.size());
    }



    @Test
    public void testTwoServicesTwoChannels() throws Exception {
        View v, v2;
        c1=factory.createMultiplexerChannel(mux_conf_stack, "service-1");
        c2=factory.createMultiplexerChannel(mux_conf_stack, "service-2");
        c1.connect("bla");
        c2.connect("blo");

        c3=factory2.createMultiplexerChannel(mux_conf_stack, "service-1");
        c4=factory2.createMultiplexerChannel(mux_conf_stack, "service-2");
        c3.connect("foo");

        Util.sleep(500);
        v=((MuxChannel)c1).getClusterView();
        v2=((MuxChannel)c3).getClusterView();
        assertNotNull(v);
        assertNotNull(v2);
        Assert.assertEquals(2, v2.size());
        assertEquals(v, v2);

        v=c1.getView();
        v2=c3.getView();
        assertNotNull(v);
        assertNotNull(v2);
        Assert.assertEquals(2, v2.size());
        assertEquals(v, v2);

        v=c2.getView();
        Assert.assertEquals(1, v.size());

        c4.connect("bar");

        Util.sleep(500);
        v=c2.getView();
        v2=c4.getView();
        Assert.assertEquals(2, v.size());
        assertEquals(v, v2);

        c3.disconnect();

        Util.sleep(500);
        v=c1.getView();
        Assert.assertEquals(1, v.size());
        v=c2.getView();
        Assert.assertEquals(2, v.size());
        v=c4.getView();
        Assert.assertEquals(2, v.size());

        c3.close();
        c2.close();
        Util.sleep(500);
        v=c4.getView();
        Assert.assertEquals(1, v.size());
    }


    @Test
    public void testReconnect() throws Exception {
        View v;
        c1=factory.createMultiplexerChannel(mux_conf_stack, "service-1");
        c1.connect("bla");

        c3=factory2.createMultiplexerChannel(mux_conf_stack, "service-1");
        c4=factory2.createMultiplexerChannel(mux_conf_stack, "service-2");
        c3.connect("foo");
        c4.connect("bar");

        Util.sleep(500);
        v=c1.getView();
        Assert.assertEquals(2, v.size());

        c3.disconnect();
        boolean connected=c3.isConnected();
        assertFalse("c3 must be disconnected because we called disconnect()", connected);

        Util.sleep(500);
        v=c1.getView();
        Assert.assertEquals(1, v.size());

        c3.connect("foobar");
        Util.sleep(2000);
        v=c1.getView();
        Assert.assertEquals(v.size(), 2, "v is " + v);

        c4.close();
        Util.sleep(500);
        v=c1.getView();
        Assert.assertEquals(2, v.size());
    }




    private static class MyReceiver extends ExtendedReceiverAdapter {
        List events=new LinkedList();

        public List getEvents() {
            return events;
        }

        public void clear() {events.clear();}

        public void block() {
            events.add(new BlockEvent());
        }

        public void unblock() {
            events.add(new UnblockEvent());
        }

        public void viewAccepted(View new_view) {
        }
    }
}
