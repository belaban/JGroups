package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.DELAY_JOIN_REQ;
import org.jgroups.protocols.Discovery;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;


/**
 * @author Bela Ban
 */
@Test(groups=Global.STACK_DEPENDENT,sequential=true)
public class JoinTest extends ChannelTestBase {
    JChannel c1, c2;

    @BeforeMethod
    void setUp() throws Exception {
        c1=createChannel(true, 2);
        c2=createChannel(c1);
    }


    @AfterMethod
    void tearDown() throws Exception {
        Util.close(c2,c1);
    }

    @Test
    public void testSingleJoin() throws Exception {
        c1.connect("JoinTest");
        View v=c1.getView();
        assert v != null;
        assert v.size() == 1;
    }


    /**
     * Tests that immediately after a connect(), a getView() returns the correct view
     */
    @Test
    public void testJoinsOnTwoChannels() throws Exception {
        c1.connect("JoinTest");
        c2.connect("JoinTest");
        
        Util.sleep(2000); //no blocking is used, let the view propagate
        
        View v1=c1.getView(), v2=c2.getView();
        System.out.println("v1=" + v1 + ", v2=" + v2);
        assert v1 != null;
        assert v2 != null;
        assert v1.size() == 2;
        assert v2.size() == 2;
        assert v1.equals(v2);
    }


    @Test
    public void testJoinsOnTwoChannelsAndSend() throws Exception {
        c1.connect("JoinTest");
        c2.connect("JoinTest");
        MyReceiver r1=new MyReceiver("c1");
        MyReceiver r2=new MyReceiver("c2");
        c1.setReceiver(r1);
        c2.setReceiver(r2);
        Message m1=new Message(null, null, "message-1"), m2=new Message(null, null, "message-2");
        c1.connect("JoinTest-2");
        View view=c1.getView();
        assert view.size() == 2 : "c1's view: " + view;
        c2.connect("JoinTest-2");
        view=c2.getView();
        assert view.size() == 2 : "c2's view: " + view;
        Util.sleep(200);
        view=c1.getView();
        assert view.size() == 2 : "c1's view: " + view;

        c1.send(m1);
        c2.send(m2);

        Util.sleep(1500);
        List<String>c1_list=r1.getMsgs(), c2_list=r2.getMsgs();
        System.out.println("c1: " + c1_list.size() + " msgs, c2: " + c2_list.size() + " msgs");
        assert c1_list.size() == 2 : "cl_list: " + c1_list;
        assert c2_list.size() == 2 : "c2_list: " + c2_list;
        assert c1_list.contains("message-1");
        assert c2_list.contains("message-1");
        assert c1_list.contains("message-2");
        assert c2_list.contains("message-2");
    }


    /**
     * Tests the case where we send a JOIN-REQ, but get back a JOIN-RSP after GMS.join_timeout, so we've already
     * started another discovery. Tests whether the discovery process is cancelled correctly.
     * http://jira.jboss.com/jira/browse/JGRP-621
     */
    @Test
    public void testDelayedJoinResponse() throws Exception {
        final long JOIN_TIMEOUT=2000, DELAY_JOIN_REQ=4000;
        final long DISCOVERY_TIMEOUT=5000;
        final long TOLERANCE=1000;

        _testDelayedJoinResponse(DISCOVERY_TIMEOUT, JOIN_TIMEOUT, DELAY_JOIN_REQ, TOLERANCE);
    }

    @Test
    public void testDelayedJoinResponse2() throws Exception {
        final long JOIN_TIMEOUT=2000, DELAY_JOIN_REQ=4000;
        final long DISCOVERY_TIMEOUT=5000;
        final long TOLERANCE=1000;

        _testDelayedJoinResponse(DISCOVERY_TIMEOUT, JOIN_TIMEOUT, DELAY_JOIN_REQ, TOLERANCE);
    }

    @Test
    public void testDelayedJoinResponse3() throws Exception {
        final long JOIN_TIMEOUT=5000, DELAY_JOIN_REQ=4000;
        final long DISCOVERY_TIMEOUT=5000;
        final long TOLERANCE=1000;

        _testDelayedJoinResponse(DISCOVERY_TIMEOUT, JOIN_TIMEOUT, DELAY_JOIN_REQ, TOLERANCE);
    }


    @Test
    public void testDelayedJoinResponse4() throws Exception {
        final long JOIN_TIMEOUT=1000, DELAY_JOIN_REQ=4000;
        final long DISCOVERY_TIMEOUT=2000;
        final long TOLERANCE=1000;

        _testDelayedJoinResponse(DISCOVERY_TIMEOUT, JOIN_TIMEOUT, DELAY_JOIN_REQ, TOLERANCE);
    }



    void _testDelayedJoinResponse(long discovery_timeout, long join_timeout,
                                  long delay_join_req, long tolerance) throws Exception {
        c1.connect("JoinTest");
        c2.connect("JoinTest");

        ProtocolStack stack=c2.getProtocolStack();
        GMS gms=(GMS)stack.findProtocol("GMS");
        if(gms != null) {
            gms.setJoinTimeout(join_timeout);
        }

        Discovery discovery=(Discovery)stack.findProtocol(Discovery.class);        
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
        c2.connect("JoinTest-2");
        stop=System.currentTimeMillis();
        long join_time=stop-start;
        long tolerated_join_time=discovery_timeout + delay_join_req + tolerance; // 1 sec more is okay (garbage collection etc)
        System.out.println(new Date() + ": joining of c2 took " + join_time + " ms (should have taken not more than "+tolerated_join_time +" ms)");
        assert join_time <= tolerated_join_time : "join time (" + join_time + ") was > tolerated join time (" + tolerated_join_time + ")";
    }



    private static class MyReceiver extends ReceiverAdapter {
        private final String name;
        private final List<String> msgs;

        public MyReceiver(String name) {
            this.name=name;
            msgs = Collections.synchronizedList(new ArrayList<String>());
        }

        public List<String> getMsgs() {
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
