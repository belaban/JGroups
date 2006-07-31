package org.jgroups.tests;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.JChannelFactory;
import org.jgroups.View;
import org.jgroups.util.Util;
import org.jgroups.mux.MuxChannel;

/**
 * Test the multiplexer functionality provided by JChannelFactory, especially the service views and cluster views
 * @author Bela Ban
 * @version $Id: MultiplexerViewTest.java,v 1.3 2006/07/31 09:06:07 belaban Exp $
 */
public class MultiplexerViewTest extends TestCase {
    private Channel c1, c2, c3, c4;
    static final String CFG="stacks.xml";
    static final String STACK_NAME="fc-fast-minimalthreads";
    JChannelFactory factory, factory2;

    public MultiplexerViewTest(String name) {
        super(name);
    }


    public void setUp() throws Exception {
        super.setUp();
        factory=new JChannelFactory();
        factory.setMultiplexerConfig(MultiplexerViewTest.CFG);

        factory2=new JChannelFactory();
        factory2.setMultiplexerConfig(MultiplexerViewTest.CFG);
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        if(c2 != null)
            c2.close();
        if(c1 != null)
            c1.close();
        factory.destroy();
        factory2.destroy();
    }

    public void testBasicLifeCycle() throws Exception {
        c1=factory.createMultiplexerChannel(STACK_NAME, "service-1");
        System.out.println("c1: " + c1);
        assertTrue(c1.isOpen());
        assertFalse(c1.isConnected());
        View v=c1.getView();
        assertNull(v);

        ((MuxChannel)c1).getClusterView();
        assertNull(v);
        Address local_addr=c1.getLocalAddress();
        assertNull(local_addr);

        c1.connect("bla");
        assertTrue(c1.isConnected());
        local_addr=c1.getLocalAddress();
        assertNotNull(local_addr);
        v=c1.getView();
        assertNotNull(v);
        assertEquals(1, v.size());
        v=((MuxChannel)c1).getClusterView();
        assertNotNull(v);
        assertEquals(1, v.size());

        c1.disconnect();
        assertFalse(c1.isConnected());
        assertTrue(c1.isOpen());
        local_addr=c1.getLocalAddress();
        assertNull(local_addr);
        c1.close();
        assertFalse(c1.isOpen());
    }


    public void testTwoServicesOneChannel() throws Exception {
        c1=factory.createMultiplexerChannel(STACK_NAME, "service-1");
        c2=factory.createMultiplexerChannel(STACK_NAME, "service-2");
        c1.connect("bla");
        c2.connect("blo");

        View v=((MuxChannel)c1).getClusterView(), v2=((MuxChannel)c2).getClusterView();
        assertNotNull(v);
        assertNotNull(v2);
        assertEquals(v, v2);
        assertEquals(1, v.size());
        assertEquals(1, v2.size());

        v=c1.getView();
        v2=c2.getView();
        assertNotNull(v);
        assertNotNull(v2);
        assertEquals(v, v2);
        assertEquals(1, v.size());
        assertEquals(1, v2.size());
    }



    public void testTwoServicesTwoChannels() throws Exception {
        View v, v2;
        c1=factory.createMultiplexerChannel(STACK_NAME, "service-1");
        c2=factory.createMultiplexerChannel(STACK_NAME, "service-2");
        c1.connect("bla");
        c2.connect("blo");

        c3=factory2.createMultiplexerChannel(STACK_NAME, "service-1");
        c4=factory2.createMultiplexerChannel(STACK_NAME, "service-2");
        c3.connect("foo");

        Util.sleep(500);
        v=((MuxChannel)c1).getClusterView();
        v2=((MuxChannel)c3).getClusterView();
        assertNotNull(v);
        assertNotNull(v2);
        assertEquals(2, v2.size());
        assertEquals(v, v2);

        v=c1.getView();
        v2=c3.getView();
        assertNotNull(v);
        assertNotNull(v2);
        assertEquals(2, v2.size());
        assertEquals(v, v2);

        v=c2.getView();
        assertEquals(1, v.size()); // c4 has not joined yet

        c4.connect("bar");

        Util.sleep(500);
        v=c2.getView();
        v2=c4.getView();
        assertEquals(2, v.size());
        assertEquals(v, v2);

        c3.disconnect();

        Util.sleep(500);
        v=c1.getView();
        assertEquals(1, v.size());
        v=c2.getView();
        assertEquals(2, v.size());
        v=c4.getView();
        assertEquals(2, v.size());

        c3.close();
        c2.close();
        Util.sleep(500);
        v=c4.getView();
        assertEquals(1, v.size());
    }


    public void testReconnect() throws Exception {
        View v;
        c1=factory.createMultiplexerChannel(STACK_NAME, "service-1");
        c1.connect("bla");

        c3=factory2.createMultiplexerChannel(STACK_NAME, "service-1");
        c4=factory2.createMultiplexerChannel(STACK_NAME, "service-2");
        c3.connect("foo");
        c4.connect("bar");

        Util.sleep(500);
        v=c1.getView();
        assertEquals(2, v.size());
        c3.disconnect();

        Util.sleep(500);
        v=c1.getView();
        assertEquals(1, v.size());

        c3.connect("foobar");
        Util.sleep(500);
        v=c1.getView();
        assertEquals(2, v.size());

        c4.close();
        Util.sleep(500);
        v=c1.getView();
        assertEquals(2, v.size());
    }



    public static Test suite() {
        return new TestSuite(MultiplexerViewTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(MultiplexerViewTest.suite());
    }
}
