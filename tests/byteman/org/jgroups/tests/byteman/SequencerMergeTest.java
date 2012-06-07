package org.jgroups.tests.byteman;


import org.jboss.byteman.contrib.bmunit.BMNGRunner;
import org.jgroups.*;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Digest;
import org.jgroups.util.MutableDigest;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


/**
 * Tests merging with SEQUENCER
 * @author Bela Ban
 * @since 3.1
 */
@Test(groups=Global.BYTEMAN,sequential=true)
public class SequencerMergeTest extends BMNGRunner {
    JChannel a, b, c, d;
    static final String GROUP="SequencerMergeTest";


    @BeforeMethod
    void setUp() throws Exception {
        
    }

    @AfterMethod
    void tearDown() throws Exception {
        Util.close(d, c, b, a);
    }





    /**
     * Tests a merge between {A} and {B,C,D}, plus a concurrent multicast.
     * https://issues.jboss.org/browse/JGRP-1468
     */
    // @BMScript(dir="scripts/SequencerFailoverTest", value="testResendingVersusNewMessages")
    public void testMergeAndSendOrdering() throws Exception {
        // Create subgroup {A}:
        a=create("A", true);
        a.connect(GROUP);

        // Create subgroup {B,C,D}
        b=create("B", false);
        b.connect(GROUP);

        c=create("C", false);
        d=create("D", false);

        Util.sleep(1000);
        c.connect(GROUP);
        d.connect(GROUP);

        Util.waitUntilAllChannelsHaveSameSize(10000, 1000, b,c,d);
        removeDiscard(a,b,c,d);
        System.out.println("Channels:\n" + printChannels(a,b,c,d));
        MyReceiver ra=new MyReceiver("A");
        MyReceiver rb=new MyReceiver("B");
        MyReceiver rc=new MyReceiver("C");
        MyReceiver rd=new MyReceiver("D");
        a.setReceiver(ra);
        b.setReceiver(rb);
        c.setReceiver(rc);
        d.setReceiver(rd);

        View new_view=Util.createView(a.getAddress(), 5, a.getAddress(),b.getAddress(),c.getAddress(),d.getAddress());
        Digest digest=getDigest(a,b,c,d);

        System.out.println("Installing " + new_view + " in B,C and D");
        injectViewAndDigest(new_view,digest,b,c,d);
        System.out.println("Channels:\n" + printChannels(a,b,c,d));
        assert Util.isCoordinator(a);
        assert !Util.isCoordinator(b);
        assert !Util.isCoordinator(c);
        assert !Util.isCoordinator(d);



        System.out.println("D sends a multicast message M");
        Message msg=new Message(null, "M");
        d.send(msg);

        Util.sleep(500);

        // Finally installing the new view at A; this simulates a delayed view installation
        System.out.println("Installing " + new_view + " in A");
        injectViewAndDigest(new_view, getDigest(), a);

        Util.sleep(60000);


        System.out.println("\nReceivers:");
        List<String> list_a=ra.getList();
        List<String> list_b=rb.getList();
        List<String> list_c=rc.getList();
        List<String> list_d=rd.getList();
        System.out.println("A: " + list_a + "\nB: " + list_b + "\nC: " + list_c + "\nD: " + list_d);

        System.out.println("Checking ordering:");
        final List<String> expected=Arrays.asList("V5", "M");
        for(List<String> list: Arrays.asList(list_a, list_b, list_c, list_d)) {
            assert list.equals(expected) : "expected=" + expected + ", actual list=" + list;
        }
    }


    protected JChannel create(String name, boolean insert_discard) throws Exception {
        JChannel ch=Util.createChannel(new UDP(),
                                       new DISCARD().setValue("discard_all", insert_discard),
                                       new PING().setValue("timeout",100),
                                       new NAKACK2().setValue("use_mcast_xmit",false)
                                         .setValue("log_discard_msgs",false).setValue("log_not_found_msgs",false),
                                       new UNICAST2(),
                                       new STABLE().setValue("max_bytes",50000),
                                       new SEQUENCER(), // below GMS, to establish total order between views and messages
                                       new GMS().setValue("print_local_addr",false)
                                         .setValue("leave_timeout",100)
                                         .setValue("log_view_warnings",false)
                                         .setValue("view_ack_collection_timeout",50)
                                         .setValue("log_collect_msgs",false));
        ch.setName(name);
        return ch;
    }

    protected static void injectViewAndDigest(View view, Digest digest, JChannel ... channels) {
        for(JChannel ch: channels) {
            GMS gms=(GMS)ch.getProtocolStack().findProtocol(GMS.class);
            gms.installView(view);

            Protocol nak=ch.getProtocolStack().findProtocol(NAKACK.class, NAKACK2.class);
            if(nak != null)
                nak.down(new Event(Event.SET_DIGEST, digest));
        }
    }


    protected static Digest getDigest(JChannel ... channels) {
        MutableDigest digest=new MutableDigest(channels.length);
        for(JChannel ch: channels) {
            Protocol nak=ch.getProtocolStack().findProtocol(NAKACK.class, NAKACK2.class);
            Digest tmp=(Digest)nak.down(new Event(Event.GET_DIGEST, ch.getAddress()));
            if(tmp != null)
                digest.add(tmp);
        }
        return digest;
    }


    protected static void makeCoordinator(JChannel ch) {
        GMS gms=(GMS)ch.getProtocolStack().findProtocol(GMS.class);
        gms.becomeCoordinator();
    }

    protected static String printChannels(JChannel ... channels) {
        StringBuilder sb=new StringBuilder();
        for(JChannel ch: channels) {
            sb.append(ch.getName() + ": view is " + ch.getView() + "\n");
        }
        return sb.toString();
    }

    protected static void removeDiscard(JChannel ... channels) {
        for(JChannel ch: channels)
            ch.getProtocolStack().removeProtocol(DISCARD.class);
    }

    

    protected static class MyReceiver extends ReceiverAdapter {
        protected final String name;
        protected final List<String> list=new ArrayList<String>();

        public MyReceiver(String name) {
            this.name=name;
        }

        public List<String> getList() {
            return list;
        }

        public void receive(Message msg) {
            System.out.println("-- [" + name + "] received " + msg.getObject());
            list.add(msg.getObject().toString());
        }

        public void viewAccepted(View view) {
            String tmp="V" + view.getViewId().getId();
            System.out.println("-- [" + name + "] received " + view.getVid());
            list.add(tmp);
        }
    }

}
