package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.stack.Protocol;
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
@Test(groups=Global.STACK_DEPENDENT,singleThreaded=true)
public class JoinTest extends ChannelTestBase {
    protected JChannel a, b;

    @BeforeMethod
    void setUp() throws Exception {
        a=createChannel(true, 2, "A");
        b=createChannel(a, "B");
    }


    @AfterMethod
    void tearDown() throws Exception {
        Util.close(b,a);
    }

    public void testSingleJoin() throws Exception {
        a.connect("JoinTest");
        View v=a.getView();
        assert v != null;
        assert v.size() == 1;
    }


    /**
     * Tests that immediately after a connect(), a getView() returns the correct view
     */
    public void testJoinsOnTwoChannels() throws Exception {
        a.connect("JoinTest");
        b.connect("JoinTest");
        
        Util.sleep(2000); //no blocking is used, let the view propagate
        
        View v1=a.getView(), v2=b.getView();
        System.out.println("v1=" + v1 + ", v2=" + v2);
        assert v1 != null;
        assert v2 != null;
        assert v1.size() == 2;
        assert v2.size() == 2;
        assert v1.equals(v2);
    }


    public void testJoinsOnTwoChannelsAndSend() throws Exception {
        a.connect("JoinTest");
        b.connect("JoinTest");
        MyReceiver r1=new MyReceiver("c1");
        MyReceiver r2=new MyReceiver("c2");
        a.setReceiver(r1);
        b.setReceiver(r2);
        Message m1=new Message(null, "message-1"), m2=new Message(null, "message-2");
        a.connect("JoinTest-2");
        View view=a.getView();
        assert view.size() == 2 : "c1's view: " + view;
        b.connect("JoinTest-2");
        view=b.getView();
        assert view.size() == 2 : "c2's view: " + view;
        Util.sleep(200);
        view=a.getView();
        assert view.size() == 2 : "c1's view: " + view;

        a.send(m1);
        b.send(m2);

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
    public void testDelayedJoinResponse() throws Exception {
        final long JOIN_TIMEOUT=2000, DELAY_JOIN_REQ=4000;
        final long TOLERANCE=1000;

        _testDelayedJoinResponse(JOIN_TIMEOUT, DELAY_JOIN_REQ, TOLERANCE);
    }

    public void testDelayedJoinResponse2() throws Exception {
        final long JOIN_TIMEOUT=2000, DELAY_JOIN_REQ=4000;
        final long TOLERANCE=1000;

        _testDelayedJoinResponse(JOIN_TIMEOUT, DELAY_JOIN_REQ, TOLERANCE);
    }

    @Test
    public void testDelayedJoinResponse3() throws Exception {
        final long JOIN_TIMEOUT=5000, DELAY_JOIN_REQ=4000;
        final long TOLERANCE=1000;

        _testDelayedJoinResponse(JOIN_TIMEOUT, DELAY_JOIN_REQ, TOLERANCE);
    }


    public void testDelayedJoinResponse4() throws Exception {
        final long JOIN_TIMEOUT=1000, DELAY_JOIN_REQ=4000;
        final long TOLERANCE=1000;

        _testDelayedJoinResponse(JOIN_TIMEOUT, DELAY_JOIN_REQ, TOLERANCE);
    }



    void _testDelayedJoinResponse(long join_timeout, long delay_join_req, long tolerance) throws Exception {
        a.connect("JoinTest");
        b.connect("JoinTest");

        ProtocolStack stack=b.getProtocolStack();
        GMS gms=stack.findProtocol(GMS.class);
        if(gms != null)
            gms.setJoinTimeout(join_timeout);


        stack=a.getProtocolStack();
        DELAY_JOIN_REQ delay=new DELAY_JOIN_REQ().delay(delay_join_req);
        stack.insertProtocol(delay, ProtocolStack.Position.BELOW, GMS.class);

        System.out.println(new Date() + ": joining c2");
        long start=System.currentTimeMillis(), stop;
        b.connect("JoinTest-2");
        stop=System.currentTimeMillis();
        long join_time=stop-start;
        long tolerated_join_time=join_timeout + delay_join_req + tolerance; // 1 sec more is okay (garbage collection etc)
        System.out.println(new Date() + ": joining of c2 took " + join_time + " ms (should have taken not more than "+tolerated_join_time +" ms)");
        assert join_time <= tolerated_join_time : "join time (" + join_time + ") was > tolerated join time (" + tolerated_join_time + ")";
    }



    private static class MyReceiver extends ReceiverAdapter {
        private final String name;
        private final List<String> msgs;

        public MyReceiver(String name) {
            this.name=name;
            msgs = Collections.synchronizedList(new ArrayList<>());
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


    protected static class DELAY_JOIN_REQ extends Protocol {
        private long        delay=4000;
        private final short gms_id=ClassConfigurator.getProtocolId(GMS.class);

        public long           delay()           {return delay;}
        public DELAY_JOIN_REQ delay(long delay) {this.delay=delay; return this;}

        public Object up(final Message msg) {
            final GMS.GmsHeader hdr=msg.getHeader(gms_id);
            if(hdr != null) {
                switch(hdr.getType()) {
                    case GMS.GmsHeader.JOIN_REQ:
                    case GMS.GmsHeader.JOIN_REQ_WITH_STATE_TRANSFER:
                        System.out.println(new Date() + ": delaying JOIN-REQ by " + delay + " ms");
                        Thread thread=new Thread() {
                            public void run() {
                                Util.sleep(delay);
                                System.out.println(new Date() + ": sending up delayed JOIN-REQ by " + hdr.getMember());
                                up_prot.up(msg);
                            }
                        };
                        thread.start();
                        return null;
                }
            }
            return up_prot.up(msg);
        }
    }

}
