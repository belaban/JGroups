package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.MutableDigest;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.*;


/**
 * Tests a merge between asymmetric partitions where an isolated coord is still seen by others.
 * Example : {A,B,C,D} and {A}.
 *
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class MergeTest4 {
    protected JChannel a,b,c,d,e,f,g,h,i,j,s,t,u,v;

    protected static final Method stopInfoSender, startInfoSender;
    protected static final short  merge_id=ClassConfigurator.getProtocolId(MERGE3.class);

    static {
        try {
            stopInfoSender=MERGE3.class.getDeclaredMethod("stopInfoSender");
            startInfoSender=MERGE3.class.getDeclaredMethod("startInfoSender");
            stopInfoSender.setAccessible(true);
            startInfoSender.setAccessible(true);
        }
        catch(NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeMethod
    void setUp() throws Exception {
        a=createChannel("A", true);
        b=createChannel("B", true);
        c=createChannel("C", true);
        d=createChannel("D", true);
        Util.waitUntilAllChannelsHaveSameView(10000, 500, a, b, c, d);
        enableInfoSender(false, a, b, c, d); // stops INFO sending in MERGE3
    }

    @AfterMethod void tearDown() throws Exception {Util.close(v,u,t,s,j,i,h,g,f,e,d,c,b,a);}

    /**
     * Tests a simple split: {A,B} and {C,D} need to merge back into one subgroup. Checks how many MergeViews are installed
     */
    public void testSplitInTheMiddle() throws Exception {
        View v1=View.create(a.getAddress(), 10, a.getAddress(), b.getAddress());
        View v2=View.create(c.getAddress(), 10, c.getAddress(), d.getAddress());

        injectView(v1, a,b);
        injectView(v2, c,d);
        Util.waitUntilAllChannelsHaveSameView(10000, 500, a, b);
        Util.waitUntilAllChannelsHaveSameView(10000, 500, c, d);
        for(JChannel ch: Arrays.asList(a,b,c,d))
            System.out.println(ch.getName() + ": " + ch.getView());
        Address merge_leader_one=new Membership().add(a.getAddress(), b.getAddress()).sort().elementAt(0);
        Address merge_leader_two=new Membership().add(c.getAddress(), d.getAddress()).sort().elementAt(0);

        for(int x=0; x < 20; x++) {
            if(a.getView().size() == 4 && b.getView().size() == 4 && c.getView().size() == 4 && d.getView().size() == 4)
                break;

            for(JChannel ch: Arrays.asList(a,b,c,d)) {
                MERGE3 merge=ch.getProtocolStack().findProtocol(MERGE3.class);
                merge.sendInfo(); // multicasts an INFO msg to everybody else
            }

            for(JChannel ch: Arrays.asList(findChannel(merge_leader_one), findChannel(merge_leader_two))) {
                MERGE3 merge=ch.getProtocolStack().findProtocol(MERGE3.class);
                merge.checkInconsistencies();
            }
            Util.sleep(1000);
        }

        for(JChannel ch: Arrays.asList(a,b,c,d))
            System.out.println(ch.getName() + ": " + ch.getView());
        for(JChannel ch: Arrays.asList(a,b,c,d))
            assert ch.getView().size() == 4 : "view of " + ch.getName() + ": " + ch.getView();
    }


    /**
     * Tests a simple split: {A,B} and {C,D} need to merge back into one subgroup. Checks how many MergeViews are installed
     */
    public void testSplitInTheMiddle2() throws Exception {
        View v1=View.create(a.getAddress(), 10, a.getAddress(), b.getAddress());
        View v2=View.create(c.getAddress(), 10, c.getAddress(), d.getAddress());
        injectView(v1, a,b);
        injectView(v2, c,d);
        enableInfoSender(false, a,b,c,d);
        Util.waitUntilAllChannelsHaveSameView(10000, 500, a, b);
        Util.waitUntilAllChannelsHaveSameView(10000, 500, c, d);
        enableInfoSender(false, a,b,c,d);
        for(JChannel ch: Arrays.asList(a,b,c,d))
            System.out.println(ch.getName() + ": " + ch.getView());
        System.out.println("\nEnabling INFO sending in merge protocols to merge subclusters");
        enableInfoSender(true, a, b, c, d);

        Util.waitUntilAllChannelsHaveSameView(30000, 1000, a, b, c, d);
        System.out.println("\nResulting views:");
        for(JChannel ch: Arrays.asList(a,b,c,d)) {
            GMS gms=ch.getProtocolStack().findProtocol(GMS.class);
            View mv=gms.view();
            System.out.println(mv);
        }
        for(JChannel ch: Arrays.asList(a,b,c,d)) {
            GMS gms=ch.getProtocolStack().findProtocol(GMS.class);
            View mv=gms.view();
            assert mv instanceof MergeView;
            assert mv.size() == 4;
            assert ((MergeView)mv).getSubgroups().size() == 2;
        }
        for(JChannel ch: Arrays.asList(a,b,c,d)) {
            View view=ch.getView();
            assert view.size() == 4 : "view should have 4 members: " + view;
        }
    }


    public void testMergeWithAsymetricViewsCoordIsolated() throws Exception {
        // Isolate the coord
        Address coord = a.getView().getCreator();
        System.out.println("Isolating coord: " + coord);
        List<Address> members = new ArrayList<>();
        members.add(coord);
        View coord_view=new View(coord, 4, members);
        System.out.println("coord_view: " + coord_view);
        JChannel coord_channel = findChannel(coord);
        System.out.println("coord_channel: " + coord_channel.getAddress());

        MutableDigest digest=new MutableDigest(coord_view.getMembersRaw());
        NAKACK2 nakack=coord_channel.getProtocolStack().findProtocol(NAKACK2.class);
        digest.merge(nakack.getDigest(coord));

        GMS gms=coord_channel.getProtocolStack().findProtocol(GMS.class);
        gms.installView(coord_view, digest);
        System.out.println("gms.getView() " + gms.getView());

        System.out.println("Views are:");
        for(JChannel ch: Arrays.asList(a,b,c,d))
            System.out.println(ch.getAddress() + ": " + ch.getView());

        JChannel merge_leader=findChannel(coord);
        MyReceiver receiver=new MyReceiver();
        merge_leader.setReceiver(receiver);

        System.out.println("merge_leader: " + merge_leader.getAddressAsString());

        System.out.println("Injecting MERGE event into merge leader " + merge_leader.getAddress());
        Map<Address,View> merge_views=new HashMap<>(4);
        merge_views.put(a.getAddress(), a.getView());
        merge_views.put(b.getAddress(), b.getView());
        merge_views.put(c.getAddress(), c.getView());
        merge_views.put(d.getAddress(), d.getView());

        gms=merge_leader.getProtocolStack().findProtocol(GMS.class);
        gms.up(new Event(Event.MERGE, merge_views));

        Util.waitUntilAllChannelsHaveSameView(10000, 1000, a, b, c, d);

        System.out.println("Views are:");
        for(JChannel ch: Arrays.asList(a,b,c,d)) {
            View view=ch.getView();
            System.out.println(ch.getAddress() + ": " + view);
            assert view.size() == 4;
        }
        MergeView merge_view=receiver.getView();
        System.out.println("merge_view = " + merge_view);
        assert merge_view.size() == 4;
        assert merge_view.getSubgroups().size() == 2;

        for(View view: merge_view.getSubgroups())
            assert contains(view, a.getAddress()) || contains(view, b.getAddress(), c.getAddress(), d.getAddress());
    }

    /**
     * Tests a merge between ViewIds of the same coord, e.g. A|6, A|7, A|8, A|9
     */
    public void testViewsBySameCoord() throws Exception {
        View v1=View.create(a.getAddress(), 6, a.getAddress(),b.getAddress(),c.getAddress(),d.getAddress()); // {A,B,C,D}
        View v2=View.create(a.getAddress(), 7, a.getAddress(),b.getAddress(),c.getAddress());                // {A,B,C}
        View v3=View.create(a.getAddress(), 8, a.getAddress(),b.getAddress());                               // {A,B}
        View v4=View.create(a.getAddress(), 9, a.getAddress());                                              // {A}

        Util.close(b,c,d); // not interested in those...

        MERGE3 merge=a.getProtocolStack().findProtocol(MERGE3.class);
        for(View view: Arrays.asList(v1,v2,v4,v3)) {
            MERGE3.MergeHeader hdr=MERGE3.MergeHeader.createInfo(view.getViewId(), null, null);
            Message msg=new Message(null).src(a.getAddress()).putHeader(merge.getId(), hdr);
            merge.up(msg);
        }

        merge.checkInconsistencies(); // no merge will happen

        Util.waitUntilAllChannelsHaveSameView(10000, 500, a);
        System.out.println("A's view: " + a.getView());
        assert a.getView().size() == 1;
        assert a.getView().containsMember(a.getAddress());
    }


    /**
     * Tests A|6, A|7, A|8 and B|7, B|8, B|9 -> we should have a subviews in MergeView consisting of
     * only 2 elements: A|5 and B|5
     */
    public void testMultipleViewsBySameMembers() throws Exception {
        View a1=View.create(a.getAddress(), 6, a.getAddress(),b.getAddress(),c.getAddress(),d.getAddress()); // {A,B,C,D}
        View a2=View.create(a.getAddress(), 7, a.getAddress(),b.getAddress(),c.getAddress());                // {A,B,C}
        View a3=View.create(a.getAddress(), 8, a.getAddress(),b.getAddress());                               // {A,B}
        View a4=View.create(a.getAddress(), 9, a.getAddress());                                              // {A}

        View b1=View.create(b.getAddress(), 7, b.getAddress(), c.getAddress(), d.getAddress());
        View b2=View.create(b.getAddress(), 8, b.getAddress(), c.getAddress());
        View b3=View.create(b.getAddress(), 9, b.getAddress());

        Util.close(c,d); // not interested in those...

        // A and B cannot communicate:
        discard(true, a,b);

        // inject view A|6={A} into A and B|5={B} into B
        injectView(a4, a);
        injectView(b3, b);

        assert a.getView().equals(a4);
        assert b.getView().equals(b3);

        List<Message> merge_msgs=new ArrayList<>();
        for(View view: Arrays.asList(a3,a4,a2,a1)) {
            MERGE3.MergeHeader hdr=MERGE3.MergeHeader.createInfo(view.getViewId(), null, null);
            Message msg=new Message(null).src(a.getAddress()).putHeader(merge_id, hdr);
            merge_msgs.add(msg);
        }
        for(View view: Arrays.asList(b2,b3,b1)) {
            MERGE3.MergeHeader hdr=MERGE3.MergeHeader.createInfo(view.getViewId(), null, null);
            Message msg=new Message(null).src(b.getAddress()).putHeader(merge_id, hdr);
            merge_msgs.add(msg);
        }

        // A and B can communicate again
        discard(false, a,b);

        injectMergeEvents(merge_msgs, a,b);
        checkInconsistencies(a,b); // merge will happen between A and B
        Util.waitUntilAllChannelsHaveSameView(10000, 500, a, b);
        System.out.println("A's view: " + a.getView() + "\nB's view: " + b.getView());
        assert a.getView().size() == 2;
        assert a.getView().containsMember(a.getAddress());
        assert a.getView().containsMember(b.getAddress());
        assert a.getView().equals(b.getView());
        for(View merge_view: Arrays.asList(getViewFromGMS(a), getViewFromGMS(b))) {
            System.out.println(merge_view);
            assert merge_view instanceof MergeView;
            List<View> subviews=((MergeView)merge_view).getSubgroups();
            assert subviews.size() == 2;
        }
    }

    /**
     * Takes a membership of 10, e.g. {A,B,C,D,E,F,G,H,I,J}, creates multiple partitions (e.g. {A,B,C}, {D}, {E,F,G,H}, {I,J}),
     * and merges them back into one cluster. Asserts that there's only 1 MergeView
     */
    public void testMultipleSplits() throws Exception {
        e=createChannel("E", true);
        f=createChannel("F", true);
        g=createChannel("G", true);
        h=createChannel("H", true);
        i=createChannel("I", true);
        j=createChannel("J", true);
        Util.waitUntilAllChannelsHaveSameView(10000, 500, a, b, c, d, e, f, g, h, i, j);
        enableInfoSender(false, a,b,c,d,e,f,g,h,i,j); // stops INFO sending in MERGE3
        for(JChannel ch: Arrays.asList(a,b,c,d,e,f,g,h,i,j))
            System.out.println(ch.getName() + ": " + ch.getView());
        List<View> partitions=createPartitions(4, a,b,c,d,e,f,g,h,i,j);

        // Install views
        for(View partition: partitions) {
            for(Address mbr: partition.getMembersRaw()) {
                JChannel ch=findChannel(mbr);
                injectView(partition, ch);
            }
        }

        System.out.printf("\n%d partitions:\n", partitions.size());
        for(JChannel ch: Arrays.asList(a,b,c,d,e,f,g,h,i,j))
            System.out.println(ch.getName() + ": " + ch.getView());

        System.out.println("\nEnabling INFO sending in merge protocols to merge subclusters");
        enableInfoSender(true, a,b,c,d,e,f,g,h,i,j);

        Util.waitUntilAllChannelsHaveSameView(30000, 1000, a, b, c, d, e, f, g, h, i, j);
        System.out.println("\nResulting views:");
        for(JChannel ch: Arrays.asList(a,b,c,d,e,f,g,h,i,j)) {
            GMS gms=ch.getProtocolStack().findProtocol(GMS.class);
            View mv=gms.view();
            System.out.println(mv);
        }
        for(JChannel ch: Arrays.asList(a,b,c,d,e,f,g,h,i,j)) {
            GMS gms=ch.getProtocolStack().findProtocol(GMS.class);
            View mv=gms.view();
            assert mv instanceof MergeView;
            assert mv.size() == 10;
            assert ((MergeView)mv).getSubgroups().size() == partitions.size();
        }
        for(JChannel ch: Arrays.asList(a,b,c,d,e,f,g,h,i,j)) {
            View view=ch.getView();
            assert view.size() == 10 : "view should have 10 members: " + view;
        }
    }


    /**
     * A: {A,B}
     * B: {A,B}
     * C: {C}
     * C receives INFO from B only
     */
    public void testMergeWithIncompleteInfos() throws Exception {
        Util.close(d);
        enableInfoSender(false, a,b,c);
        View one=View.create(a.getAddress(), 10, a.getAddress(),b.getAddress());
        View two=View.create(c.getAddress(), 10, c.getAddress());
        injectView(one, a,b);
        injectView(two, c);
        enableInfoSender(false, a,b,c);

        Util.waitUntilAllChannelsHaveSameView(10000, 500, a, b);
        Util.waitUntilAllChannelsHaveSameView(10000, 500, c);
        System.out.print("\nPartitions:\n");
        for(JChannel ch: Arrays.asList(a,b,c))
            System.out.println(ch.getName() + ": " + ch.getView());

        MERGE3.MergeHeader hdr=MERGE3.MergeHeader.createInfo(one.getViewId(), null, null);
        Message msg=new Message(null).src(b.getAddress()).putHeader(merge_id, hdr); // B sends the INFO message to C
        MERGE3 merge=c.getProtocolStack().findProtocol(MERGE3.class);
        merge.up(msg);
        enableInfoSender(true,a,b,c);
        System.out.println("\nEnabling INFO sending in merge protocols to merge subclusters");

        Util.waitUntilAllChannelsHaveSameView(30000, 1000, a, b, c);
        System.out.println("\nResulting views:");
        for(JChannel ch: Arrays.asList(a,b,c)) {
            GMS gms=ch.getProtocolStack().findProtocol(GMS.class);
            View mv=gms.view();
            System.out.println(mv);
        }
        for(JChannel ch: Arrays.asList(a,b,c)) {
            GMS gms=ch.getProtocolStack().findProtocol(GMS.class);
            View mv=gms.view();
            assert mv instanceof MergeView;
            assert mv.size() == 3;
            assert ((MergeView)mv).getSubgroups().size() == 2;
        }
        for(JChannel ch: Arrays.asList(a,b,c)) {
            View view=ch.getView();
            assert view.size() == 3 : "view should have 3 members: " + view;
        }
    }

    /** Tests the scenario described by Karim in https://issues.jboss.org/browse/JGRP-1876 */
    public void testJGRP_1876() throws Exception {
        e=createChannel("E", true);
        f=createChannel("F", true);
        g=createChannel("G", true);
        h=createChannel("H", true);
        Util.waitUntilAllChannelsHaveSameView(10000, 500, a, b, c, d, e, f, g, h);
        enableInfoSender(false, a,b,c,d,e,f,g,h); // stops INFO sending in MERGE3
        for(JChannel ch: Arrays.asList(a,b,c,d,e,f,g,h))
            System.out.println(ch.getName() + ": " + ch.getView());
        View v1=View.create(a.getAddress(), 10, a.getAddress(), b.getAddress(), c.getAddress(), d.getAddress());
        View v2=View.create(a.getAddress(), 9, a.getAddress(), b.getAddress(), c.getAddress(), d.getAddress(), e.getAddress(), f.getAddress());
        View v3=View.create(g.getAddress(), 8, g.getAddress(), h.getAddress());

        injectView(v1, a,b,c,d);
        injectView(v2, e,f);
        injectView(v3, g,h);
        enableInfoSender(false, a,b,c,d,e,f,g,h); // stops INFO sending in MERGE3
        Util.waitUntilAllChannelsHaveSameView(10000, 500, a, b, c, d);
        for(int x=0; x < 10; x++) { // can't use waitUntilAllChannelsHaveSameSize() as the channel list's length != view !
            if(e.getView().size() == v2.size() && f.getView().size() == v2.size())
                break;
            Util.sleep(500);
        }
        assert e.getView().size() == v2.size() : "E's view: " + e.getView();
        assert f.getView().size() == v2.size() : "F's view: " + f.getView();
        Util.waitUntilAllChannelsHaveSameView(10000, 500, g, h);

        System.out.print("\nPartitions:\n");
        for(JChannel ch: Arrays.asList(a,b,c,d,e,f,g,h))
            System.out.println(ch.getName() + ": " + ch.getView());

        enableInfoSender(true,a,b,c,d,e,f,g,h);
        System.out.println("\nEnabling INFO sending in merge protocols to merge subclusters");

        Util.waitUntilAllChannelsHaveSameView(30000, 1000, a, b, c, d, e, f, g, h);
        System.out.println("\nResulting views:");
        for(JChannel ch: Arrays.asList(a,b,c,d,e,f,g,h)) {
            GMS gms=ch.getProtocolStack().findProtocol(GMS.class);
            View mv=gms.view();
            System.out.println(mv);
        }
        for(JChannel ch: Arrays.asList(a,b,c,d,e,f,g,h)) {
            GMS gms=ch.getProtocolStack().findProtocol(GMS.class);
            View mv=gms.view();
            assert mv instanceof MergeView;
            assert mv.size() == 8;
            assert ((MergeView)mv).getSubgroups().size() == 3;
        }
        for(JChannel ch: Arrays.asList(a,b,c,d,e,f,g,h)) {
            View view=ch.getView();
            assert view.size() == 8 : "view should have 8 members: " + view;
        }
    }


    /** Tests the scenario described by Dan in https://issues.jboss.org/browse/JGRP-1876 */
    public void testJGRP_1876_Dan() throws Exception {
        Util.close(d,c,b,a);
        s=createChannel("S", true);
        t=createChannel("T", true);
        u=createChannel("U", true);
        v=createChannel("V", true);
        Util.waitUntilAllChannelsHaveSameView(10000, 500, s, t, u, v);
        enableInfoSender(false, s,t,u,v); // stops INFO sending in MERGE3
        for(JChannel ch: Arrays.asList(s,t,u,v))
            System.out.println(ch.getName() + ": " + ch.getView());
        View v1=View.create(s.getAddress(), 10, s.getAddress());
        View v2=View.create(t.getAddress(), 10, t.getAddress());
        View v3=View.create(u.getAddress(), 11, u.getAddress(), v.getAddress());

        injectView(v1, s);
        injectView(v2, t);
        injectView(v3, u,v);
        enableInfoSender(false, s,t,u,v); // stops INFO sending in MERGE3
        Util.waitUntilAllChannelsHaveSameView(10000, 500, s);
        Util.waitUntilAllChannelsHaveSameView(10000, 500, t);
        Util.waitUntilAllChannelsHaveSameView(10000, 500, u, v);

        System.out.print("\nPartitions:\n");
        for(JChannel ch: Arrays.asList(s,t,u,v))
            System.out.println(ch.getName() + ": " + ch.getView());

        enableInfoSender(true,s,t,u,v);
        System.out.println("\nEnabling INFO sending in merge protocols to merge subclusters");

        Util.waitUntilAllChannelsHaveSameView(30000, 1000, s, t, u, v);
        System.out.println("\nResulting views:");
        for(JChannel ch: Arrays.asList(s,t,u,v)) {
            GMS gms=ch.getProtocolStack().findProtocol(GMS.class);
            View mv=gms.view();
            System.out.println(mv);
        }
        for(JChannel ch: Arrays.asList(s,t,u,v)) {
            GMS gms=ch.getProtocolStack().findProtocol(GMS.class);
            View mv=gms.view();
            assert mv instanceof MergeView;
            assert mv.size() == 4;
            assert ((MergeView)mv).getSubgroups().size() == 3;
        }
        for(JChannel ch: Arrays.asList(s,t,u,v)) {
            View view=ch.getView();
            assert view.size() == 4 : "view should have 4 members: " + view;
        }
    }


    /** Tests the scenario described by Dan in https://issues.jboss.org/browse/JGRP-1876; INFO messages are injected */
    public void testJGRP_1876_Dan2() throws Exception {
        Util.close(d,c,b,a);
        s=createChannel("S", false);
        t=createChannel("T", false);
        u=createChannel("U", false);
        v=createChannel("V", false);
        for(JChannel ch: Arrays.asList(s,t,u,v))
            ch.getProtocolStack().removeProtocol(MERGE3.class);
        s.connect("MergeTest4");
        t.connect("MergeTest4");
        u.connect("MergeTest4");
        v.connect("MergeTest4");
        Util.waitUntilAllChannelsHaveSameView(10000, 500, s, t, u, v);

        for(JChannel ch: Arrays.asList(s,t,u,v))
            System.out.println(ch.getName() + ": " + ch.getView());
        View v1=View.create(s.getAddress(), 10, s.getAddress());
        View v2=View.create(t.getAddress(), 10, t.getAddress());
        View v3=View.create(u.getAddress(), 11, u.getAddress(), v.getAddress());
        injectView(v1, s);
        injectView(v2, t);
        injectView(v3, u, v);
        Util.waitUntilAllChannelsHaveSameView(10000, 500, s);
        Util.waitUntilAllChannelsHaveSameView(10000, 500, t);
        Util.waitUntilAllChannelsHaveSameView(10000, 500, u, v);

        System.out.print("\nPartitions:\n");
        for(JChannel ch: Arrays.asList(s,t,u,v))
            System.out.println(ch.getName() + ": " + ch.getView());

        System.out.println("\nInjecting MERGE events into GMS to merge subclusters");
        Map<Address,View> different_views=new HashMap<>();
        different_views.put(s.getAddress(), v1);
        different_views.put(t.getAddress(), v2);
        different_views.put(v.getAddress(), v3);

        Collection<Address> coords=Util.determineActualMergeCoords(different_views);
        Address merge_leader=new Membership().add(coords).sort().elementAt(0);
        System.out.printf("--> coords=%s, merge_leader=%s\n", coords, merge_leader);
        GMS gms=findChannel(merge_leader).getProtocolStack().findProtocol(GMS.class);
        gms.up(new Event(Event.MERGE, different_views));

        Util.waitUntilAllChannelsHaveSameView(30000, 1000, s, t, u, v);
        System.out.println("\nResulting views:");
        for(JChannel ch: Arrays.asList(s,t,u,v)) {
            gms=ch.getProtocolStack().findProtocol(GMS.class);
            View mv=gms.view();
            System.out.println(mv);
        }
        for(JChannel ch: Arrays.asList(s,t,u,v)) {
            gms=ch.getProtocolStack().findProtocol(GMS.class);
            View mv=gms.view();
            assert mv instanceof MergeView;
            assert mv.size() == 4;
            assert ((MergeView)mv).getSubgroups().size() == 3;
        }
        for(JChannel ch: Arrays.asList(s,t,u,v)) {
            View view=ch.getView();
            assert view.size() == 4 : "view should have 4 members: " + view;
        }
    }



    /** Creates a list of randomly generated partitions, each having a max size of max_partition_size */
    protected static List<View> createPartitions(int max_partition_size, JChannel... channels) {
        long view_id=1;
        for(JChannel ch: channels)
            view_id=Math.max(view_id, ch.getView().getViewId().getId());
        List<View> partitions=new ArrayList<>();
        List<Address> tmp=new ArrayList<>();
        for(JChannel ch: channels)
            tmp.add(ch.getAddress());

        while(!tmp.isEmpty()) {
            int num_to_remove=(int)Util.random(max_partition_size);
            List<Address> part=new ArrayList<>(max_partition_size);
            for(int x=0; x < num_to_remove && !tmp.isEmpty(); x++)
                part.add(tmp.remove(0));
            partitions.add(new View(part.get(0), view_id+1, part));
        }
        return partitions;
    }


    protected static boolean contains(View view, Address ... members) {
        List<Address> mbrs=view.getMembers();
        for(Address member: members)
            if(!mbrs.contains(member))
                return false;
        return true;
    }

    protected static class MyReceiver extends ReceiverAdapter {
        protected MergeView view;

        public MergeView getView() {return view;}

        @Override public void viewAccepted(View view) {
            if(view instanceof MergeView)
                this.view=(MergeView)view;
        }
    }

    protected static JChannel createChannel(String name, boolean connect) throws Exception {
        JChannel retval=new JChannel(new SHARED_LOOPBACK(),
                                     new SHARED_LOOPBACK_PING(),
                                     new MERGE3().setValue("min_interval", 3000).setValue("max_interval", 4000).setValue("check_interval", 7000),
                                     new NAKACK2().setValue("use_mcast_xmit",false)
                                       .setValue("log_discard_msgs",false).setValue("log_not_found_msgs",false),
                                     new UNICAST3(),
                                     new STABLE().setValue("max_bytes",50000),
                                     new GMS().setValue("print_local_addr",false)
                                       .setValue("join_timeout", 100)
                                       .setValue("leave_timeout", 100)
                                       .setValue("merge_timeout",5000)
                                       .setValue("log_view_warnings",false)
                                       .setValue("view_ack_collection_timeout",50)
                                       .setValue("log_collect_msgs",false))
          .name(name);
        if(connect)
            retval.connect("MergeTest4");
        return retval;
    }

    protected JChannel findChannel(Address mbr) {
        for(JChannel ch: Arrays.asList(a,b,c,d,e,f,g,h,i,j,s,t,u,v)) {
            if(ch != null && ch.getAddress() != null && ch.getAddress().equals(mbr))
                return ch;
        }
        return null;
    }


    protected static void injectMergeEvents(List<Message> msgs, JChannel... channels) {
        for(JChannel ch: channels) {
            MERGE3 merge=ch.getProtocolStack().findProtocol(MERGE3.class);
            msgs.forEach(merge::up);
        }
    }

    protected static void discard(boolean flag, JChannel... channels) throws Exception {
        for(JChannel ch: channels) {
            ProtocolStack stack=ch.getProtocolStack();
            DISCARD discard=stack.findProtocol(DISCARD.class);
            if(discard == null)
                stack.insertProtocol(discard=new DISCARD(), ProtocolStack.Position.ABOVE, stack.getTransport().getClass());
            discard.setDiscardAll(flag);
        }
    }

    protected static void injectView(View view, JChannel... channels) {
        for(JChannel ch: channels) {
            GMS gms=ch.getProtocolStack().findProtocol(GMS.class);
            gms.installView(view);
        }
    }

    protected static void checkInconsistencies(JChannel... channels) {
        for(JChannel ch: channels) {
            MERGE3 merge=ch.getProtocolStack().findProtocol(MERGE3.class);
            merge.checkInconsistencies();
        }
    }

    protected static View getViewFromGMS(JChannel ch) {
        GMS gms=ch.getProtocolStack().findProtocol(GMS.class);
        return gms.view();
    }

    protected static List<Address> getMembers(JChannel... channels) {
        List<Address> members=new ArrayList<>(channels.length);
        for(JChannel ch: channels)
            members.add(ch.getAddress());
        return members;
    }

    protected static void enableInfoSender(boolean enable, JChannel... channels) throws Exception {
        for(JChannel ch: channels) {
            MERGE3 merge=ch.getProtocolStack().findProtocol(MERGE3.class);
            Method meth=enable? startInfoSender : stopInfoSender;
            meth.invoke(merge);
        }
    }


    @Test(enabled=false)
    public static void main(String[] args) throws Exception {
        MergeTest4 test=new MergeTest4();
        test.setUp();
        test.testMergeWithAsymetricViewsCoordIsolated();
        test.tearDown();
    }
}
