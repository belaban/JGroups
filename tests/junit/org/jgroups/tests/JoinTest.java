package org.jgroups.tests;

import java.util.LinkedList;
import java.util.List;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.jgroups.Channel;
import org.jgroups.ChannelException;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.jgroups.util.Util;


/**
 * @author Bela Ban
 * @version $Id: JoinTest.java,v 1.3 2006/11/22 19:33:07 vlada Exp $
 */
public class JoinTest extends ChannelTestBase {
    Channel c1, c2;
    static String STACK="udp.xml";


    public JoinTest(String name) {
        super(name);
    }


    public void setUp() throws Exception {
        super.setUp();
        STACK=System.getProperty("stack", STACK);
        c1=createChannel("A");
        c2=createChannel("A");
    }


    public void tearDown() throws Exception {        
        if(c1 != null)
            c1.close();
        if(c2 != null)
            c2.close();
        super.tearDown();
    }

    public void testSingleJoin() throws ChannelException {
        c1.connect("X");
        View v=c1.getView();
        assertNotNull(v);
        assertEquals(1, v.size());
    }


    /**
     * Tests that immediately after a connect(), a getView() returns the correct view
     * @throws ChannelException
     */
    public void testJoinsOnTwoChannels() throws ChannelException {
        c1.connect("X");
        c2.connect("X");
        
        //no blocking is used, let the view propagate
        sleepThread(2000);
        
        View v1=c1.getView(), v2=c2.getView();
        System.out.println("v1=" + v1 + ", v2=" + v2);
        assertNotNull(v1);
        assertNotNull(v2);
        assertEquals(2, v1.size());
        assertEquals(2, v2.size());
        assertEquals(v1, v2);
    }


    public void testJoinsOnTwoChannelsAndSend() throws ChannelException {
        MyReceiver r1=new MyReceiver("c1");
        MyReceiver r2=new MyReceiver("c2");
        c1.setReceiver(r1);
        c2.setReceiver(r2);
        Message m1=new Message(null, null, "message-1"), m2=new Message(null, null, "message-2");
        c1.connect("X");
        c2.connect("X");
        c1.send(m1);
        c2.send(m2);

        Util.sleep(200);
        List c1_list=r1.getMsgs(), c2_list=r2.getMsgs();
        System.out.println("c1: " + c1_list.size() + " msgs, c2: " + c2_list.size() + " msgs");
        assertNotNull(c1_list);
        assertNotNull(c2_list);
        assertEquals(2, c1_list.size());
        assertEquals(2, c2_list.size());
        assertTrue(c1_list.contains("message-1"));
        assertTrue(c2_list.contains("message-1"));
        assertTrue(c1_list.contains("message-2"));
        assertTrue(c2_list.contains("message-2"));
    }


    public static Test suite() {
        return new TestSuite(JoinTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(JoinTest.suite());
    }


    private static class MyReceiver extends ReceiverAdapter {
        String name;
        List msgs=new LinkedList();

        public MyReceiver(String name) {
            this.name=name;
        }

        public List getMsgs() {
            return msgs;
        }

        public void clear() {msgs.clear();}

        public void receive(Message msg) {
            String s=(String)msg.getObject();
            msgs.add(s);
            System.out.println("[" + name + "] received " + s + " from " + msg.getSrc());
        }

        public void viewAccepted(View new_view) {
            System.out.println("[" + name + "] view: " + new_view);
        }

    }
}
