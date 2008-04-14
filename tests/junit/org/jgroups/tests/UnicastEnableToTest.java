package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.LinkedList;
import java.util.List;

/**
 * Tests sending of unicasts to members not in the group (http://jira.jboss.com/jira/browse/JGRP-357)
 * @author Bela Ban
 * @version $Id: UnicastEnableToTest.java,v 1.5 2008/04/14 08:42:55 belaban Exp $
 */
public class UnicastEnableToTest extends ChannelTestBase {
    JChannel channel=null, channel2=null;


    @BeforeMethod
    protected void setUp() throws Exception {
        channel=createChannel();
        channel.connect("demo-group");
    }

    @AfterMethod
    protected void tearDown() throws Exception {
        if(channel2 != null) {
            channel2.close();
            channel2=null;
        }
        if(channel != null) {
            channel.close();
            channel=null;
        }
    }


    @Test
    public void testUnicastMessageToUnknownMember() throws Exception {
        IpAddress addr=new IpAddress("127.0.0.1", 8976);
        System.out.println("sending message to non-existing destination " + addr);
        try {
            channel.send(new Message(addr, null, "Hello world"));
            assert false : "we should not get here; sending of message to " + addr + " should have failed";
        }
        catch(IllegalArgumentException ex) {
            System.out.println("received exception as expected");
        }
    }


    @Test
    public void testUnicastMessageToExistingMember() throws Exception {
        channel2=createChannel();
        channel2.connect("demo-group");
        Assert.assertEquals(2, channel2.getView().size());
        MyReceiver receiver=new MyReceiver();
        channel2.setReceiver(receiver);
        Address dest=channel2.getLocalAddress();
        channel.send(new Message(dest, null, "hello"));
        Util.sleep(500);
        List list=receiver.getMsgs();
        System.out.println("channel2 received the following msgs: " + list);
        Assert.assertEquals(1, list.size());
        receiver.reset();
    }


    @Test
    public void testUnicastMessageToLeftMember() throws Exception {
        channel2=createChannel();
        channel2.connect("demo-group");
        Assert.assertEquals(2, channel2.getView().size());
        Address dest=channel2.getLocalAddress();
        channel2.close();
        Util.sleep(100);
        try {
            channel.send(new Message(dest, null, "hello"));
            assert false : "we should not come here as message to previous member " + dest + " should throw exception";
        }
        catch(IllegalArgumentException ex) {
            System.out.println("got an exception, as expected");
        }
    }


    @Test
    public void testUnicastMessageToLeftMemberWithEnableUnicastToEvent() throws Exception {
        channel2=createChannel();
        channel2.connect("demo-group");
        Assert.assertEquals(2, channel2.getView().size());
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


}
