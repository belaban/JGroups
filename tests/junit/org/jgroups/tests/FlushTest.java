package org.jgroups.tests;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.*;
import org.jgroups.util.Util;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;


/**
 * Tests the FLUSH protocol, requires flush-udp.xml in ./conf to be present and configured to use FLUSH
 * @author Bela Ban
 * @version $Id: FlushTest.java,v 1.2 2006/09/28 10:29:13 belaban Exp $
 */
public class FlushTest extends TestCase {
    Channel c1, c2;
    static final String CONFIG="flush-udp.xml";


    public FlushTest(String name) {
        super(name);
    }


    public void setUp() throws Exception {
        super.setUp();

        if(c2 != null) {
            c2.close();
            c2=null;
        }

        if(c1 != null) {
            c1.close();
            c1=null;
        }
    }


    public void testSingleChannel() throws ChannelException {
        c1=createChannel();
        MyReceiver receiver=new MyReceiver("c1");
        c1.setReceiver(receiver);
        c1.connect("bla");
        List events=receiver.getEvents();
        System.out.println("events: " + events);
        assertEquals(0, events.size());
        receiver.clear();

        c1.close();
        events=receiver.getEvents();
        System.out.println("events: " + events);
        assertFalse(events.contains(new BlockEvent()));
    }


    public void testTwoChannels() throws ChannelException {
        c1=createChannel();
        MyReceiver receiver=new MyReceiver("c1");
        c1.setReceiver(receiver);
        c1.connect("bla");
        List events=receiver.getEvents();
        System.out.println("events c1: " + events);
        assertEquals(1, events.size());
        Object obj=events.remove(0);
        assertTrue(obj instanceof View);
        receiver.clear();

        c2=createChannel();
        MyReceiver receiver2=new MyReceiver("c2");
        c2.setReceiver(receiver2);
        c2.connect("bla");
        View view=c2.getView();
        assertEquals(2, view.size());
        Util.sleep(100);

        events=receiver.getEvents();
        System.out.println("events c1: " + events);
        assertEquals(3, events.size());
        obj=events.remove(0);
        assertTrue(obj instanceof BlockEvent);
        obj=events.remove(0);
        assertTrue("should be a View but is " + obj, obj instanceof View);
        obj=events.remove(0);
        assertTrue(obj instanceof UnblockEvent);
        receiver.clear();

        events=receiver2.getEvents();
        System.out.println("events c2: " + events);
        assertFalse(events.contains(new BlockEvent()));
        receiver2.clear();

        c2.close();
        Util.sleep(200);
        events=receiver.getEvents();
        System.out.println("events c1: " + events);
        assertEquals(3, events.size());
        obj=events.remove(0);
        assertTrue(obj instanceof BlockEvent);
        obj=events.remove(0);
        assertTrue(obj instanceof View);
        obj=events.remove(0);
        assertTrue(obj instanceof UnblockEvent);
    }



    public void testWithStateTransfer() {
        throw new UnsupportedOperationException("implement this !");
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
    }
}
