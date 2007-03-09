package org.jgroups.tests;

import junit.framework.TestCase;
import org.jgroups.*;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Util;

import java.util.LinkedList;
import java.util.List;

/**
 * Tests sending of unicasts to members not in the group (http://jira.jboss.com/jira/browse/JGRP-357)
 * @author Bela Ban
 * @version $Id: UnicastEnableToTest.java,v 1.1 2007/03/09 20:27:05 belaban Exp $
 */
public class UnicastEnableToTest extends TestCase {
    JChannel channel=null, channel2=null;


    public UnicastEnableToTest(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        super.setUp();
        channel=new JChannel("udp.xml");
        channel.connect("demo-group");
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        if(channel2 != null) {
            channel2.close();
            channel2=null;
        }
        if(channel != null) {
            channel.close();
            channel=null;
        }
    }


    public void testUnicastMessageToUnknownMember() throws Exception {
        IpAddress addr=new IpAddress("127.0.0.1", 8976);
        System.out.println("sending message to non-existing destination " + addr);
        try {
            channel.send(new Message(addr, null, "Hello world"));
            fail("we should not get here; sending of message to " + addr + " should have failed");
        }
        catch(IllegalArgumentException ex) {
            System.out.println("received exception as expected");
        }
    }


    public void testUnicastMessageToExistingMember() throws Exception {
        channel2=new JChannel("udp.xml");
        channel2.connect("demo-group");
        assertEquals(2, channel2.getView().size());
        MyReceiver receiver=new MyReceiver();
        channel2.setReceiver(receiver);
        Address dest=channel2.getLocalAddress();
        channel.send(new Message(dest, null, "hello"));
        Util.sleep(500);
        List list=receiver.getMsgs();
        System.out.println("channel2 received the following msgs: " + list);
        assertEquals(1, list.size());
        receiver.reset();
    }


    public void testUnicastMessageToLeftMember() throws Exception {
        channel2=new JChannel("udp.xml");
        channel2.connect("demo-group");
        assertEquals(2, channel2.getView().size());
        Address dest=channel2.getLocalAddress();
        channel2.close();
        Util.sleep(100);
        try {
            channel.send(new Message(dest, null, "hello"));
            fail("we should not come here as message to previous member " + dest + " should throw exception");
        }
        catch(IllegalArgumentException ex) {
            System.out.println("got an exception, as expected");
        }
    }


    public void testUnicastMessageToLeftMemberWithEnableUnicastToEvent() throws Exception {
        channel2=new JChannel("udp.xml");
        channel2.connect("demo-group");
        assertEquals(2, channel2.getView().size());
        Address dest=channel2.getLocalAddress();
        channel2.close();
        Util.sleep(100);
        channel.down(new Event(Event.ENABLE_UNICASTS_TO, dest));
        channel.send(new Message(dest, null, "hello"));
    }


    private static class MyReceiver extends ExtendedReceiverAdapter {
        List<Message> msgs=new LinkedList<Message>();

        public void receive(Message msg) {
            msgs.add(msg);
        }

        List getMsgs() {
            return msgs;
        }

        void reset() {msgs.clear();}
    }


    public static void main(String[] args) {
        String[] testCaseName={UnicastEnableToTest.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }
}
