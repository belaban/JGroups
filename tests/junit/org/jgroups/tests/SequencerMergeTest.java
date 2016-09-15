package org.jgroups.tests;


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

import java.util.*;


/**
 * Tests merging with SEQUENCER
 * @author Bela Ban
 * @since 3.1
 */
@Test(groups=Global.STACK_INDEPENDENT,singleThreaded=true)
public class SequencerMergeTest {
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

        Util.waitUntilAllChannelsHaveSameView(10000, 1000, b, c, d);
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

        final View new_view=View.create(a.getAddress(), 5, a.getAddress(),b.getAddress(),c.getAddress(),d.getAddress());
        final Digest digest=getDigest(new_view, a,b,c,d);

        System.out.println("Installing " + new_view + " in B,C and D");
        injectViewAndDigest(new_view,digest,b,c,d);
        System.out.println("Channels:\n" + printChannels(a,b,c,d));
        assert Util.isCoordinator(a);
        assert !Util.isCoordinator(b);
        assert !Util.isCoordinator(c);
        assert !Util.isCoordinator(d);

        Thread thread=new Thread() {
            public void run() {
                Util.sleep(1000);

                // Finally installing the new view at A; this simulates a delayed view installation
                System.out.println("Installing " + new_view + " in A");
                injectViewAndDigest(new_view, getDigest(new_view), a);
            }
        };
        thread.start();

        System.out.println("D sends a multicast message M");
        Message msg=new Message(null, "M");
        d.send(msg);

        System.out.println("\nReceivers:");
        List<String> list_a=ra.getList();
        List<String> list_b=rb.getList();
        List<String> list_c=rc.getList();
        List<String> list_d=rd.getList();
        final List<String> expected=Arrays.asList("V5", "M");

        for(int i=0; i < 20; i++) {
            boolean all_ok=true;
            for(List<String> list: new ArrayList<>(Arrays.asList(list_a, list_b, list_c, list_d))) {
                if(!list.equals(expected)) {
                    all_ok=false;
                    break;
                }
            }
            if(all_ok)
                break;
            Util.sleep(500);
            list_a=ra.getList();
            list_b=rb.getList();
            list_c=rc.getList();
            list_d=rd.getList();
        }

        System.out.println("A: " + list_a + "\nB: " + list_b + "\nC: " + list_c + "\nD: " + list_d);

        for(List<String> list: Arrays.asList(list_a, list_b, list_c, list_d))
            assert list.equals(expected) : "expected=" + expected + ", actual list=" + list;
        System.out.println("OK: order of all 3 lists is correct");
    }

    /**
     * Tests a merge between {D,A} and {B,C,D}.
     * https://issues.jboss.org/browse/JGRP-1484
     */
    public void testMergeWithParticipant() throws Exception {
        a=create("A", false);
        b=create("B", false);
        c=create("C", false);
        d=create("D", false);

        a.connect(GROUP);
        b.connect(GROUP);
        c.connect(GROUP);
        d.connect(GROUP);

        Util.waitUntilAllChannelsHaveSameView(10000, 1000, a, b, c, d);
        Util.sleep(1000);
        System.out.println("Channels:\n" + printChannels(a,b,c,d));
        MyReceiver ra=new MyReceiver("A");
        MyReceiver rb=new MyReceiver("B");
        MyReceiver rc=new MyReceiver("C");
        MyReceiver rd=new MyReceiver("D");
        a.setReceiver(ra);
        b.setReceiver(rb);
        c.setReceiver(rc);
        d.setReceiver(rd);

        // To have the best chance of hitting JGRP-1484, construct A's view so that his coordinator isn't coordinator after merging.
        JChannel a_coord;
        if (c.getAddress().compareTo(d.getAddress()) < 0)
            a_coord = d;
        else
            a_coord = c;

        // Inject either {C,A} or {D,A} at A
        final View a_view=View.create(a_coord.getAddress(), 10, a_coord.getAddress(),a.getAddress());
        final Digest a_digest=getDigest(a_view, a_coord,a);
        System.out.println("\nInstalling " + a_view + " in A");
        injectViewAndDigest(a_view, a_digest, a);
        assert !Util.isCoordinator(a);

        // Inject {B,C,D} at B,C,D
        final View bcd_view=View.create(b.getAddress(), 20, b.getAddress(),c.getAddress(),d.getAddress());
        final Digest bcd_digest=getDigest(bcd_view, b,c,d);
        System.out.println("\nInstalling " + bcd_view + " in B,C and D");
        injectViewAndDigest(bcd_view,bcd_digest,b,c,d);
        assert Util.isCoordinator(b);
        assert !Util.isCoordinator(c);
        assert !Util.isCoordinator(d);

        // start merging
        final Map<Address,View> views=new HashMap<>();
        views.put(a.getAddress(), a.getView());
        views.put(b.getAddress(), b.getView());
        views.put(c.getAddress(), c.getView());
        views.put(d.getAddress(), d.getView());
        final Event merge_evt=new Event(Event.MERGE, views);
        System.out.println("\n==== Injecting a merge event (leader=" + b.getAddress() + ") ====");
        injectMergeEvent(merge_evt, b);

        // Wait for merge to complete.
        Util.waitUntilAllChannelsHaveSameView(20000, 1000, a, b, c, d);
        final View merged_view = a.getView();
        System.out.println("\nMerged view is " + merged_view);
    }


    protected JChannel create(String name, boolean insert_discard) throws Exception {
        JChannel ch=new JChannel(new SHARED_LOOPBACK(),
                                 new DISCARD().setValue("discard_all", insert_discard),
                                 new PING(),
                                 new NAKACK2().setValue("use_mcast_xmit",false)
                                   .setValue("log_discard_msgs",false).setValue("log_not_found_msgs",false),
                                 new UNICAST3(),
                                 new STABLE().setValue("max_bytes",50000),
                                 new SEQUENCER(), // below GMS, to establish total order between views and messages
                                 new GMS().setValue("print_local_addr",false).setValue("join_timeout", 100).setValue("leave_timeout",100)
                                   .setValue("log_view_warnings",false).setValue("view_ack_collection_timeout",50)
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

    private static void injectMergeEvent(Event evt, JChannel ... channels) {
        for(JChannel ch: channels) {
            GMS gms=(GMS)ch.getProtocolStack().findProtocol(GMS.class);
            gms.up(evt);
        }
    }

    protected static Digest getDigest(final View view, JChannel ... channels) {
        MutableDigest digest=new MutableDigest(view.getMembersRaw());
        for(JChannel ch: channels) {
            Protocol nak=ch.getProtocolStack().findProtocol(NAKACK.class, NAKACK2.class);
            Digest tmp=(Digest)nak.down(new Event(Event.GET_DIGEST, ch.getAddress()));
            if(tmp != null)
                digest.set(tmp);
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
        protected final List<String> list=new ArrayList<>();

        public MyReceiver(String name) {
            this.name=name;
        }

        public List<String> getList() {
            synchronized(list) {
                return new ArrayList(list);
            }
        }

        public void receive(Message msg) {
            synchronized(list) {
                list.add(msg.getObject().toString());
            }
        }

        public void viewAccepted(View view) {
            String tmp="V" + view.getViewId().getId();
            synchronized(list) {
                list.add(tmp);
            }
        }
    }

}
