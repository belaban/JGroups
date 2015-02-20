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

import java.util.*;


/**
 * Tests a merge between asymmetric partitions where an isolated coord is still seen by others.
 * Example : {A,B,C,D} and {A}.  
 * 
 */
@Test(groups=Global.FUNCTIONAL,sequential=true)
public class MergeTest4 {
    protected JChannel a,b,c,d;

    @BeforeMethod
    void setUp() throws Exception {
        a=createChannel("A");
        b=createChannel("B");
        c=createChannel("C");
        d=createChannel("D");
    }

    @AfterMethod void tearDown() throws Exception {Util.close(d,c,b,a);}

    public void testMergeWithAsymetricViewsCoordIsolated() {
        // createPartition(a,b,c,d);

        // Isolate the coord
        Address coord = a.getView().getCreator();
        System.out.println("Isolating coord: " + coord);
        List<Address> members = new ArrayList<>();
        members.add(coord);
        View coord_view=new View(coord, 4, members);
        System.out.println("coord_view: " + coord_view);
        Channel coord_channel = findChannel(coord);
        System.out.println("coord_channel: " + coord_channel.getAddress());
        
        MutableDigest digest=new MutableDigest(coord_view.getMembersRaw());
        NAKACK2 nakack=(NAKACK2)coord_channel.getProtocolStack().findProtocol(NAKACK2.class);
        digest.merge(nakack.getDigest(coord));
        
        GMS gms=(GMS)coord_channel.getProtocolStack().findProtocol(GMS.class);
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
        
        gms=(GMS)merge_leader.getProtocolStack().findProtocol(GMS.class);
        gms.up(new Event(Event.MERGE, merge_views));
        
        Util.waitUntilAllChannelsHaveSameSize(10000, 1000, a, b, c, d);
        
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

        for(View v: merge_view.getSubgroups())
            assert contains(v, a.getAddress()) || contains(v, b.getAddress(), c.getAddress(), d.getAddress());
    }

    /**
     * Tests a merge between ViewIds of the same coord, e.g. A|6, A|7, A|8, A|9
     */
    public void testViewsBySameCoord() {
        View v1=View.create(a.getAddress(), 6, a.getAddress(),b.getAddress(),c.getAddress(),d.getAddress()); // {A,B,C,D}
        View v2=View.create(a.getAddress(), 7, a.getAddress(),b.getAddress(),c.getAddress());                // {A,B,C}
        View v3=View.create(a.getAddress(), 8, a.getAddress(),b.getAddress());                               // {A,B}
        View v4=View.create(a.getAddress(), 9, a.getAddress());                                              // {A}

        Util.close(b,c,d); // not interested in those...

        MERGE3 merge=(MERGE3)a.getProtocolStack().findProtocol(MERGE3.class);
        for(View v: Arrays.asList(v1,v2,v4,v3)) {
            MERGE3.MergeHeader hdr=MERGE3.MergeHeader.createInfo(v.getViewId(), null, null);
            Message msg=new Message(null, a.getAddress(), null).putHeader(merge.getId(), hdr);
            merge.up(new Event(Event.MSG, msg));
        }

        merge.checkInconsistencies(); // no merge will happen

        Util.waitUntilAllChannelsHaveSameSize(10000, 500, a);
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

        List<Event> merge_events=new ArrayList<>();
        short merge_id=ClassConfigurator.getProtocolId(MERGE3.class);

        for(View v: Arrays.asList(a3,a4,a2,a1)) {
            MERGE3.MergeHeader hdr=MERGE3.MergeHeader.createInfo(v.getViewId(), null, null);
            Message msg=new Message(null, a.getAddress(), null).putHeader(merge_id, hdr);
            merge_events.add(new Event(Event.MSG, msg));
        }
        for(View v: Arrays.asList(b2,b3,b1)) {
            MERGE3.MergeHeader hdr=MERGE3.MergeHeader.createInfo(v.getViewId(), null, null);
            Message msg=new Message(null, b.getAddress(), null).putHeader(merge_id, hdr);
            merge_events.add(new Event(Event.MSG, msg));
        }

        // A and B can communicate again
        discard(false, a,b);

        injectMergeEvents(merge_events, a,b);
        checkInconsistencies(a,b); // merge will happen between A and B
        Util.waitUntilAllChannelsHaveSameSize(10000, 500, a,b);
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
    
    protected JChannel createChannel(String name) throws Exception {
        JChannel retval=new JChannel(new SHARED_LOOPBACK(),
                                     new SHARED_LOOPBACK_PING(),
                                     new MERGE3().setValue("min_interval", 300000).setValue("max_interval", 600000),
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
        retval.connect("MergeTest4");
        return retval;
    }

    protected JChannel findChannel(Address mbr) {
        for(JChannel ch: Arrays.asList(a,b,c,d)) {
            if(ch.getAddress().equals(mbr))
                return ch;
        }
        return null;
    }


    protected void injectMergeEvents(List<Event> events, JChannel ... channels) {
        for(JChannel ch: channels) {
            MERGE3 merge=(MERGE3)ch.getProtocolStack().findProtocol(MERGE3.class);
            for(Event evt: events)
                merge.up(evt);
        }
    }

    protected void discard(boolean flag, JChannel ... channels) throws Exception {
        for(JChannel ch: channels) {
            ProtocolStack stack=ch.getProtocolStack();
            DISCARD discard=(DISCARD)stack.findProtocol(DISCARD.class);
            if(discard == null)
                stack.insertProtocol(discard=new DISCARD(), ProtocolStack.ABOVE, stack.getTransport().getClass());
            discard.setDiscardAll(flag);
        }
    }

    protected void injectView(View view, JChannel ch) {
        GMS gms=(GMS)ch.getProtocolStack().findProtocol(GMS.class);
        gms.installView(view);
    }

    protected void checkInconsistencies(JChannel ... channels) {
        for(JChannel ch: channels) {
            MERGE3 merge=(MERGE3)ch.getProtocolStack().findProtocol(MERGE3.class);
            merge.checkInconsistencies();
        }
    }

    protected View getViewFromGMS(JChannel ch) {
        GMS gms=(GMS)ch.getProtocolStack().findProtocol(GMS.class);
        return gms.view();
    }

    protected List<Address> getMembers(JChannel ... channels) {
        List<Address> members=new ArrayList<>(channels.length);
        for(JChannel ch: channels)
            members.add(ch.getAddress());
        return members;
    }


    @Test(enabled=false)
    public static void main(String[] args) throws Exception {
        MergeTest4 test=new MergeTest4();
        test.setUp();
        test.testMergeWithAsymetricViewsCoordIsolated();
        test.tearDown();
    }
}
