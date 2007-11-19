package org.jgroups.tests;

import junit.framework.Test;
import junit.framework.TestSuite;
import org.jgroups.*;
import org.jgroups.protocols.DELAY_JOIN_REQ;
import org.jgroups.protocols.Discovery;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;


/**
 * @author Bela Ban
 * @version $Id: JoinTest.java,v 1.13 2007/11/19 16:08:27 belaban Exp $
 */
public class JoinTest extends ChannelTestBase {
    JChannel c1, c2;

    public JoinTest(String name) {
        super(name);
    }


    public void setUp() throws Exception {
        super.setUp();               
        c1=createChannel("A");
        c2=createChannel("A");
    }


    public void tearDown() throws Exception {        
        if(c2 != null)
            c2.close();
        if(c1 != null)
            c1.close();
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
        Util.sleep(2000);
        
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
        View view=c1.getView();
        assertEquals("c1's view: " + view, 1, view.size());
        c2.connect("X");
        view=c2.getView();
        assertEquals("c2's view: " + view, 2, view.size());
        Util.sleep(200);
        view=c1.getView();
        assertEquals("c1's view: " + view, 2, view.size());

        c1.send(m1);
        c2.send(m2);

        Util.sleep(1500);
        List c1_list=r1.getMsgs(), c2_list=r2.getMsgs();
        System.out.println("c1: " + c1_list.size() + " msgs, c2: " + c2_list.size() + " msgs");
        assertNotNull(c1_list);
        assertNotNull(c2_list);
        assertEquals("cl_list: " + c1_list, 2, c1_list.size());
        assertEquals("c2_list: " + c2_list, 2, c2_list.size());
        assertTrue(c1_list.contains("message-1"));
        assertTrue(c2_list.contains("message-1"));
        assertTrue(c1_list.contains("message-2"));
        assertTrue(c2_list.contains("message-2"));
    }


    /**
     * Tests the case where we send a JOIN-REQ, but get back a JOIN-RSP after GMS.join_timeout, so we've already
     * started another discovery. Tests whether the discovery process is cancelled correctly.
     * http://jira.jboss.com/jira/browse/JGRP-621
     */
    public void testDelayedJoinResponse() throws Exception {
        final long JOIN_TIMEOUT=2000, DELAY_JOIN_REQ=4000;
        final long DISCOVERY_TIMEOUT=5000;
        final long TOLERANCE=1000;

        _testDelayedJoinResponse(DISCOVERY_TIMEOUT, JOIN_TIMEOUT, DELAY_JOIN_REQ, TOLERANCE);
    }

    public void testDelayedJoinResponse2() throws Exception {
        final long JOIN_TIMEOUT=2000, DELAY_JOIN_REQ=4000;
        final long DISCOVERY_TIMEOUT=5000;
        final long TOLERANCE=1000;

        _testDelayedJoinResponse(DISCOVERY_TIMEOUT, JOIN_TIMEOUT, DELAY_JOIN_REQ, TOLERANCE);
    }

    public void testDelayedJoinResponse3() throws Exception {
        final long JOIN_TIMEOUT=5000, DELAY_JOIN_REQ=4000;
        final long DISCOVERY_TIMEOUT=5000;
        final long TOLERANCE=1000;

        _testDelayedJoinResponse(DISCOVERY_TIMEOUT, JOIN_TIMEOUT, DELAY_JOIN_REQ, TOLERANCE);
    }


    public void testDelayedJoinResponse4() throws Exception {
        final long JOIN_TIMEOUT=1000, DELAY_JOIN_REQ=4000;
        final long DISCOVERY_TIMEOUT=2000;
        final long TOLERANCE=1000;

        _testDelayedJoinResponse(DISCOVERY_TIMEOUT, JOIN_TIMEOUT, DELAY_JOIN_REQ, TOLERANCE);
    }



    public void _testDelayedJoinResponse(long discovery_timeout, long join_timeout,
                                         long delay_join_req, long tolerance) throws Exception {
        c1.connect("x");

        ProtocolStack stack=c2.getProtocolStack();
        GMS gms=(GMS)stack.findProtocol("GMS");
        if(gms != null) {
            gms.setJoinTimeout(join_timeout);
        }

        Discovery discovery=(Discovery)stack.findProtocol("PING");
        if(discovery == null)
            discovery=(Discovery)stack.findProtocol("TCPPING");
        if(discovery == null)
            discovery=(Discovery)stack.findProtocol("MPING");
        if(discovery == null)
            discovery=(Discovery)stack.findProtocol("TCPGOSSIP");
        if(discovery != null) {
            discovery.setNumInitialMembers(10);
            discovery.setTimeout(discovery_timeout);
        }

        stack=c1.getProtocolStack();
        DELAY_JOIN_REQ delay=new DELAY_JOIN_REQ();
        delay.setDelay(delay_join_req);
        stack.insertProtocol(delay, ProtocolStack.BELOW, "GMS");

        System.out.println(new Date() + ": joining c2");
        long start=System.currentTimeMillis(), stop;
        c2.connect("x");
        stop=System.currentTimeMillis();
        long join_time=stop-start;
        long tolerated_join_time=discovery_timeout + delay_join_req + tolerance; // 1 sec more is okay (garbage collection etc)
        System.out.println(new Date() + ": joining of c2 took " + join_time + " ms (should have taken not more than "+tolerated_join_time +" ms)");
        assertTrue("join time (" + join_time + ") was > tolerated join time (" + tolerated_join_time + ")", join_time <= tolerated_join_time);
    }


    public static Test suite() {
        return new TestSuite(JoinTest.class);
    }



    private static class MyReceiver extends ReceiverAdapter {
        private final String name;
        private final List<String> msgs;

        public MyReceiver(String name) {
            this.name=name;
            msgs = Collections.synchronizedList(new ArrayList<String>());
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
