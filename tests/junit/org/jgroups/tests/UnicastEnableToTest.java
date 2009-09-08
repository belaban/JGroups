package org.jgroups.tests;

import junit.framework.TestCase;
import org.jgroups.*;
import org.jgroups.protocols.UNICAST;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Util;
import org.jgroups.util.AgeOutCache;

import java.util.LinkedList;
import java.util.List;

/**
 * Tests sending of unicasts to members not in the group (http://jira.jboss.com/jira/browse/JGRP-357)
 * @author Bela Ban
 * @version $Id: UnicastEnableToTest.java,v 1.1.4.1 2009/09/08 12:26:07 belaban Exp $
 */
public class UnicastEnableToTest extends TestCase {
    JChannel channel=null, channel2=null;
    AgeOutCache cache;


    public UnicastEnableToTest(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        super.setUp();
        channel=new JChannel("udp.xml");
        channel.connect("demo-group");
        UNICAST ucast=(UNICAST)channel.getProtocolStack().findProtocol(UNICAST.class);
        cache=ucast != null? ucast.getAgeOutCache() : null;
        if(cache != null)
            cache.setTimeout(1000);
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        Util.close(channel2,channel);
    }


    public void testUnicastMessageToUnknownMember() throws Exception {
        IpAddress addr=new IpAddress("127.0.0.1", 8976);
        System.out.println("sending message to non-existing destination " + addr);
        channel.send(new Message(addr, null, "Hello world"));
        if(cache != null) {
            System.out.println("age out cache:\n" + cache);
            assert cache.size() == 1;
        }
        Util.sleep(1500);
        if(cache != null) {
            assert cache.size() == 0;
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
        channel.send(new Message(dest, null, "hello"));
        if(cache != null) {
            System.out.println("age out cache:\n" + cache);
            assert cache.size() == 1;
        }
        Util.sleep(1500);
        if(cache != null)
            assert cache.size() == 0 : "cache size is " + cache.size();
    }


    public void testUnicastMessageToLeftMemberWithEnableUnicastToEvent() throws Exception {
        channel2=new JChannel("udp.xml");
        channel2.connect("demo-group");
        assertEquals(2, channel2.getView().size());
        Address dest=channel2.getLocalAddress();
        channel2.close();
        Util.sleep(100);
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
