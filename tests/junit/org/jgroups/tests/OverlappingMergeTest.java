package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.Discovery;
import org.jgroups.protocols.MERGE2;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Digest;
import org.jgroups.util.Tuple;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.*;

/**
 * Tests overlapping merges, e.g. A: {A,B}, B: {A,B} and C: {A,B,C}. Tests unicast as well as multicast seqno tables.<br/>
 * Related JIRA: https://jira.jboss.org/jira/browse/JGRP-940
 * @author Bela Ban
 */
@Test(groups=Global.STACK_DEPENDENT,sequential=true)
public class OverlappingMergeTest extends ChannelTestBase {
    protected JChannel a, b, c;
    protected MyReceiver ra, rb, rc;
    protected boolean multicast_transport;

    @BeforeMethod
    protected void start() throws Exception {
        a=createChannel(true, 3);
        a.setName("A");
        ra=new MyReceiver("A", a);
        a.setReceiver(ra);

        b=createChannel(a);
        b.setName("B");
        rb=new MyReceiver("B", b);
        b.setReceiver(rb);

        c=createChannel(a);
        c.setName("C");
        rc=new MyReceiver("C", c);
        c.setReceiver(rc);
        modifyConfigs(a, b, c);

        a.connect("OverlappingMergeTest");
        b.connect("OverlappingMergeTest");
        c.connect("OverlappingMergeTest");
        for(JChannel ch: Arrays.asList(a,b,c)) {
            View view=ch.getView();
            assert view.size() == 3 : "view is " + view + " for channel " + ch.getAddress();
        }

        multicast_transport=isMulticastTransport(a);
    }

    @AfterMethod
    protected void stop() throws Exception {
        Util.close(c,b,a);
        ra.clear(); rb.clear(); rc.clear();
    }

    @SuppressWarnings("unchecked")
    public void testRegularMessageSending() throws Exception {
        sendMessages(5, a, b, c);
        checkReceivedMessages(make(ra, 15), make(rb,15), make(rc,15));
    }

    /**
     * Verifies that unicasts are received correctly by all participants after an overlapping merge. The following steps
     * are executed:
     * <ol>
     * <li/>Group is {A,B,C}, A is the coordinator
     * <li/>MERGE2 is removed from all members
     * <li/>VERIFY_SUSPECT is removed from all members
     * <li/>Everyone sends 5 unicast messages to everyone else
     * <li/>Everyone sends 5 multicasts
     * <li/>A SUSPECT(A) event is injected into B's stack (GMS). This causes a new view {B,C} to be multicast by B
     * <li/>B and C install {B,C}
     * <li/>B and C trash the connection table for A in UNICAST
     * <li/>A ignores the view, it still has view {A,B,C} and all connection tables intact in UNICAST
     * <li/>We now inject a MERGE(A,B) event into A. This should ause A and B as coords to create a new MergeView {A,B,C}
     * <li/>The merge already fails because the unicast between A and B fails due to the reason given below !
     *      Once this is fixed, the next step below should work, too !
     * <li/>A sends a unicast to B and C. This should fail until JGRP-940 has been fixed !
     * <li/>Reason: B and C trashed A's conntables in UNICAST, but A didn't trash its conn tables for B and C, so
     * we have non-matching seqnos !
     * </ol>
     */
    public void testOverlappingMergeWithBC() throws Exception {
        sendMessages(5, a, b, c);
        checkReceivedMessages(make(ra, 15), make(rb,15), make(rc,15));

        // Inject view {B,C} into B and C:
        View new_view=Util.createView(b.getAddress(), 10, b.getAddress(), c.getAddress());
        System.out.println("\n ==== Injecting view " + new_view + " into B and C ====");
        injectView(new_view, b, c);
        makeCoordinator(b);
        assert Util.isCoordinator(a);
        assert Util.isCoordinator(b);
        assert !Util.isCoordinator(c);

        System.out.println("A's view: " + a.getView());
        System.out.println("B's view: " + b.getView());
        System.out.println("C's view: " + c.getView());
        assert a.getView().size() == 3 : "A's view is " + a.getView();
        assert b.getView().size() == 2 : "B's view is " + b.getView();
        assert c.getView().size() == 2 : "C's view is " + c.getView();

        System.out.println("\n==== Sending messages while the cluster is partitioned ====");
        sendMessages(5, a, b, c);
        if(multicast_transport) {
            // B and C drop A's multicasts, but A will receive B's and C's multicasts
            checkReceivedMessages(make(ra, 15), make(rb,10), make(rc,10));
        }
        else {
            // B and C drop A's multicasts, and won't send their multicasts to A (A only receives its owm multicasts)
            checkReceivedMessages(make(ra, 5), make(rb,10), make(rc,10));
        }
        
        System.out.println("\n ==== Digests are:\n" + dumpDigests(a,b,c));

        // start merging
        Map<Address,View> views=new HashMap<Address,View>();
        views.put(a.getAddress(), a.getView());
        views.put(b.getAddress(), b.getView());
        views.put(c.getAddress(), c.getView());
        Event merge_evt=new Event(Event.MERGE, views);
        JChannel merge_leader=determineMergeLeader(a, b);
        System.out.println("\n==== Injecting a merge event (leader=" + merge_leader.getAddress() + ") ====");
        injectMergeEvent(merge_evt, merge_leader);

        System.out.println("\n==== checking views after merge ====:");
        for(int i=0; i < 10; i++) {
            if(a.getView().size() == 3 && b.getView().size() == 3 && c.getView().size() == 3) {
                System.out.println("views are correct: all views have a size of 3");
                break;
            }
            System.out.print(".");
            runStableProtocol(a); runStableProtocol(b); runStableProtocol(c);
            Util.sleep(1000);
        }

        System.out.println("\n ==== Digests after the merge:\n" + dumpDigests(a,b,c));

        View va=a.getView(), vb=b.getView(), vc=c.getView();
        System.out.println("\nA's view: " + va);
        System.out.println("B's view: " + vb);
        System.out.println("C's view: " + vc);
        assert va.size() == 3 : "A's view is " + va;
        assert vb.size() == 3 : "B's view is " + vb;
        assert vc.size() == 3 : "C's view is " + vc;

        System.out.println("\n==== Sending messages after merge ====");
        sendMessages(5, a, b, c);
        checkReceivedMessages(make(ra, 15), make(rb,15), make(rc,15));
    }


    /**
     * Verifies that unicasts are received correctly by all participants after an overlapping merge. The following steps
     * are executed:
     * <ol>
     * <li/>Group is {A,B,C}
     * <li/>Install view {A,C} in A and {A,B,C} in B and C
     * <li/>Try to initiate a merge. This should FAIL until https://jira.jboss.org/jira/browse/JGRP-937 has
     *      been implemented: B and C's MERGE2 protocols will never send out merge requests as they see A as coord 
     * </ol>
     */
    @Test
    public void testOverlappingMergeWithABC() throws Exception {
        sendMessages(5, a, b, c);
        checkReceivedMessages(make(ra, 15), make(rb,15), make(rc,15));

        // Inject view {A,C} into A:
        View new_view=Util.createView(a.getAddress(), 4, a.getAddress(), c.getAddress());
        System.out.println("\n ==== Injecting view " + new_view + " into A ====");
        injectView(new_view, a);
        assertTrue(Util.isCoordinator(a));
        assertFalse(Util.isCoordinator(b));
        assertFalse(Util.isCoordinator(c));

        System.out.println("A's view: " + a.getView());
        System.out.println("B's view: " + b.getView());
        System.out.println("C's view: " + c.getView());
        assertEquals("A's view is " + a.getView(), 2, a.getView().size());
        assertEquals("B's view is " + b.getView(), 3, b.getView().size());
        assertEquals("C's view is " + c.getView(), 3, c.getView().size());


        // start merging
        Map<Address,View> views=new HashMap<Address,View>();
        views.put(a.getAddress(), a.getView());
        views.put(b.getAddress(), b.getView());
        views.put(c.getAddress(), c.getView());
        Event merge_evt=new Event(Event.MERGE, views);

        for(JChannel ch: new JChannel[]{a,b,c}) {
            GMS gms=(GMS)ch.getProtocolStack().findProtocol(GMS.class);
            gms.setLevel("trace");
        }

        System.out.println("\n==== Injecting a merge event into A, B and C ====");
        injectMergeEvent(merge_evt, a,b,c);

        System.out.println("\n==== checking views after merge ====:");
        for(int i=0; i < 10; i++) {
            if(a.getView().size() == 3 && b.getView().size() == 3 && c.getView().size() == 3) {
                System.out.println("views are correct: all views have a size of 3");
                break;
            }
            System.out.print(".");
            for(JChannel ch: new JChannel[]{a,b,c})
                runStableProtocol(ch);
            injectMergeEvent(merge_evt, a,b,c);
            Util.sleep(1000);
        }

        System.out.println("\n ==== Digests after the merge:\n" + dumpDigests(a,b,c));

        View va=a.getView(), vb=b.getView(), vc=c.getView();
        System.out.println("\nA's view: " + va);
        System.out.println("B's view: " + vb);
        System.out.println("C's view: " + vc);
        assertEquals("A's view is " + va, 3, va.size());
        assertEquals("B's view is " + vb, 3, vb.size());
        assertEquals("C's view is " + vc, 3, vc.size());

        System.out.println("\n==== Sending messages after merge ====");
        sendMessages(5, a, b, c);
        checkReceivedMessages(make(ra, 15), make(rb,15), make(rc,15));

        for(JChannel ch: new JChannel[]{a,b,c}) {
            GMS gms=(GMS)ch.getProtocolStack().findProtocol(GMS.class);
            gms.setLevel("warn");
        }
    }


    /**
     * Tests a merge where all members have views whose ViewIds have the same creator, but different IDs, e.g.:
     * A: A|5 {A}
     * B: A|6 {A,B}
     * C: A|7 {A,B,C}
     */
    public void testSameCreatorDifferentIDs() throws Exception {
        MERGE2 merge=(MERGE2)a.getProtocolStack().findProtocol(MERGE2.class);
        if(merge == null) {
            merge=new MERGE2();
            a.getProtocolStack().insertProtocol(merge,ProtocolStack.ABOVE,Discovery.class);
            merge.init();
            merge.down(new Event(Event.SET_LOCAL_ADDRESS, a.getAddress()));
        }
        View view=Util.createView(a.getAddress(), 5, a.getAddress());
        injectView(view, a);

        view=Util.createView(a.getAddress(), 6, a.getAddress(), b.getAddress());
        injectView(view, b);

        view=Util.createView(a.getAddress(), 7, a.getAddress(), b.getAddress(), c.getAddress());
        injectView(view, c);

        System.out.println("\nA's view: " + a.getView());
        System.out.println("B's view: " + b.getView());
        System.out.println("C's view: " + c.getView());


        Map<Address,View> views=new HashMap<Address,View>();
        views.put(a.getAddress(), a.getView());
        views.put(b.getAddress(), b.getView());
        views.put(c.getAddress(), c.getView());
        // Event merge_evt=new Event(Event.MERGE, views);

        for(JChannel ch: new JChannel[]{a,b,c})
            ch.getProtocolStack().findProtocol(GMS.class).setLevel("trace");


        // System.out.println("\n==== Injecting a merge event into " + a.getAddress() + " ====");
        // injectMergeEvent(merge_evt, a);





        merge.sendMergeSolicitation();


        System.out.println("\n==== checking views after merge ====:");
        for(int i=0; i < 10; i++) {
            if(a.getView().size() == 3 && b.getView().size() == 3 && c.getView().size() == 3) {
                System.out.println("views are correct: all views have a size of 3");
                break;
            }
            System.out.print(".");
            runStableProtocol(a); runStableProtocol(b); runStableProtocol(c);
            Util.sleep(1000);
        }

        for(JChannel ch: new JChannel[]{a,b,c})
            ch.getProtocolStack().findProtocol(GMS.class).setLevel("warn");

        View va=a.getView(), vb=b.getView(), vc=c.getView();
        System.out.println("\nA's view: " + va);
        System.out.println("B's view: " + vb);
        System.out.println("C's view: " + vc);
        assertEquals("A's view is " + va, 3, va.size());
        assertEquals("B's view is " + vb, 3, vb.size());
        assertEquals("C's view is " + vc, 3, vc.size());
    }



    private static void makeCoordinator(JChannel ch) {
        GMS gms=(GMS)ch.getProtocolStack().findProtocol(GMS.class);
        gms.becomeCoordinator();
    }


    private static String dumpDigests(JChannel ... channels) {
        StringBuilder sb=new StringBuilder();
        for(JChannel ch: channels) {
            sb.append(ch.getAddress()).append(": ");
            NAKACK nakack=(NAKACK)ch.getProtocolStack().findProtocol(NAKACK.class);
            Digest digest=nakack.getDigest();
            sb.append(digest).append("\n");
        }
        return sb.toString();
    }

    private static JChannel determineMergeLeader(JChannel ... coords) {
        Membership tmp=new Membership();
        for(JChannel ch: coords) {
            tmp.add(ch.getAddress());
        }
        tmp.sort();
        Address  merge_leader=tmp.elementAt(0);
        for(JChannel ch: coords) {
            if(ch.getAddress().equals(merge_leader))
                return ch;
        }
        return null;
    }

    private static void injectView(View view, JChannel ... channels) {
        for(JChannel ch: channels) {
            GMS gms=(GMS)ch.getProtocolStack().findProtocol(GMS.class);
            gms.installView(view);
        }
        for(JChannel ch: channels) {
            MyReceiver receiver=(MyReceiver)ch.getReceiver();
            System.out.println("[" + receiver.name + "] view=" + ch.getView());
        }
    }


    private static void injectMergeEvent(Event evt, JChannel ... channels) {
        for(JChannel ch: channels) {
            GMS gms=(GMS)ch.getProtocolStack().findProtocol(GMS.class);
            gms.up(evt);
        }
    }


    private void sendMessages(int num_msgs, JChannel... channels) throws Exception {
        ra.clear(); rb.clear(); rc.clear();
        for(JChannel ch: channels) {
            for(int i=1; i <= num_msgs; i++)
                ch.send(null, String.valueOf(i));
        }
    }

    private static void runStableProtocol(JChannel ch) {
        STABLE stable=(STABLE)ch.getProtocolStack().findProtocol(STABLE.class);
        if(stable != null)
            stable.runMessageGarbageCollection();
    }

    protected boolean isMulticastTransport(JChannel ch) {
        return ch.getProtocolStack().getTransport().supportsMulticasting();
    }


    @SuppressWarnings("unchecked")
    protected void checkReceivedMessages(Tuple<MyReceiver,Integer> ... expected_messages) {
        for(int i=0; i < 15; i++) {
            boolean all_received=true;
            for(Tuple<MyReceiver,Integer> tuple: expected_messages) {
                MyReceiver receiver=tuple.getVal1();
                List<Message> mcasts=receiver.getMulticasts();
                int mcasts_received=mcasts.size();
                int expected_mcasts=tuple.getVal2();
                if(mcasts_received != expected_mcasts) {
                    all_received=false;
                    break;
                }
                runStableProtocol(receiver.ch);
            }
            if(all_received)
                break;
            Util.sleep(1000);
        }

        for(Tuple<MyReceiver,Integer> tuple: expected_messages) {
            MyReceiver receiver=tuple.getVal1();
            List<Message> mcasts=receiver.getMulticasts();
            int mcasts_received=mcasts.size();
            System.out.println("receiver " + receiver + ": mcasts=" + mcasts_received);
        }

        for(Tuple<MyReceiver,Integer> tuple: expected_messages) {
            MyReceiver receiver=tuple.getVal1();
            List<Message> mcasts=receiver.getMulticasts();
            int mcasts_received=mcasts.size();
            int expected_mcasts=tuple.getVal2();
            assert mcasts_received == expected_mcasts : "(" + receiver.name + ") num_mcasts=" + print(mcasts) +
              " expected: " + expected_mcasts + ")";
        }
    }

    @SuppressWarnings("unchecked")
    protected Tuple<MyReceiver,Integer> make(MyReceiver r, int expected_msgs) {
        return new Tuple<MyReceiver,Integer>(r, expected_msgs);
    }


    private static String print(List<Message> msgs) {
        StringBuilder sb=new StringBuilder();
        for(Message msg: msgs) {
            sb.append(msg.getSrc()).append(": ").append(msg.getObject()).append(" ");
        }
        return sb.toString();
    }


    private static void modifyConfigs(JChannel ... channels) throws Exception {
        for(JChannel ch: channels) {
            ProtocolStack stack=ch.getProtocolStack();
            stack.removeProtocols("MERGE2","MERGE3","FD_SOCK","FD","FD_ALL","FC","MFC","UFC","VERIFY_SUSPECT", "STATE_TRANSFER");
            NAKACK nak=(NAKACK)stack.findProtocol(NAKACK.class);
            if(nak != null)
                nak.setLogDiscardMessages(false);
        }
    }



    protected static class MyReceiver extends ReceiverAdapter {
        final String name;
        View view=null;
        final JChannel ch;
        final List<Message> mcasts=new ArrayList<Message>(20);

        public MyReceiver(String name, JChannel ch) {
            this.name=name;
            this.ch=ch;
        }

        public void receive(Message msg) {
            Address dest=msg.getDest();
            if(dest == null)
                mcasts.add(msg);
        }

        public void viewAccepted(View new_view) {
            view=new_view;
        }

        public List<Message> getMulticasts() { return mcasts; }
        public void clear() {mcasts.clear();}
        public Address getAddress() {return ch != null? ch.getAddress() : null;}

        public String toString() {
            return name;
        }
    }



}