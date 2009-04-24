package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.UNICAST;
import org.jgroups.util.UUID;
import org.jgroups.util.Util;
import org.jgroups.util.AgeOutCache;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Tests sending of unicasts to members not in the group (http://jira.jboss.com/jira/browse/JGRP-357)
 * @author Bela Ban
 * @version $Id: UnicastEnableToTest.java,v 1.11 2009/04/24 14:01:17 belaban Exp $
 */
@Test(groups=Global.STACK_DEPENDENT,sequential=true)
public class UnicastEnableToTest extends ChannelTestBase {
    private JChannel c1=null, c2=null;
    private static final String GROUP="UnicastEnableToTest";
    AgeOutCache cache;

    @BeforeMethod
    protected void setUp() throws Exception {
        c1=createChannel(true);
        c1.connect(GROUP);
        UNICAST ucast=(UNICAST)c1.getProtocolStack().findProtocol(UNICAST.class);
        cache=ucast != null? ucast.getAgeOutCache() : null;
        if(cache != null)
            cache.setTimeout(1000);
    }

    @AfterMethod
    protected void tearDown() throws Exception {
        Util.close(c2, c1);
    }


    public void testUnicastMessageToUnknownMember() throws Exception {
        Address addr=UUID.randomUUID();
        System.out.println("sending message to non-existing destination " + addr);
        c1.send(new Message(addr, null, "Hello world"));
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
        c2=createChannel(c1);
        c2.connect(GROUP);
        assert 2 == c2.getView().size() : " view=" + c2.getView();
        MyReceiver receiver=new MyReceiver();
        c2.setReceiver(receiver);
        Address dest=c2.getAddress();
        c1.send(new Message(dest, null, "hello"));
        if(cache != null) {
            System.out.println("age out cache:\n" + cache);
            assert cache.size() == 0;
        }
        Util.sleep(500);
        List<Message> list=receiver.getMsgs();
        System.out.println("channel2 received the following msgs: " + list);
        assert 1 == list.size();
        receiver.reset();
    }


    public void testUnicastMessageToLeftMember() throws Exception {
        c2=createChannel(c1);
        c2.connect(GROUP);
        assert 2 == c2.getView().size() : "view=" + c2.getView();
        Address dest=c2.getAddress();
        c2.close();
        Util.sleep(100);
        c1.send(new Message(dest, null, "hello"));
        if(cache != null) {
            System.out.println("age out cache:\n" + cache);
            assert cache.size() == 1;
        }
        Util.sleep(1500);
        if(cache != null)
            assert cache.size() == 1;
    }




    private static class MyReceiver extends ExtendedReceiverAdapter {
        List<Message> msgs=Collections.synchronizedList(new LinkedList<Message>());

        public void receive(Message msg) {
            msgs.add(msg);
        }

        List<Message> getMsgs() {
            return msgs;
        }

        void reset() {
            msgs.clear();
        }
    }
}
