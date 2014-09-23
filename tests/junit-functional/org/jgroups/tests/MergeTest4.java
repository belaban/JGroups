package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.SHARED_LOOPBACK;
import org.jgroups.protocols.SHARED_LOOPBACK_PING;
import org.jgroups.protocols.UNICAST3;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
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
        List<Address> members = new ArrayList<Address>();
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
        Map<Address,View> merge_views=new HashMap<Address,View>(4);
        merge_views.put(a.getAddress(), a.getView());
        merge_views.put(b.getAddress(), b.getView());
        merge_views.put(c.getAddress(), c.getView());
        merge_views.put(d.getAddress(), d.getView());
        
        gms=(GMS)merge_leader.getProtocolStack().findProtocol(GMS.class);
        gms.up(new Event(Event.MERGE, merge_views));
        
        Util.waitUntilAllChannelsHaveSameSize(10000000, 1000, a,b,c,d);
        
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

    protected void createPartition(JChannel ... channels) {
        List<Address> members=getMembers(channels);
        Collections.sort(members);
        Address coord=members.get(0);
        View view=new View(coord, 0, members);
        MutableDigest digest=new MutableDigest(view.getMembersRaw());
        for(JChannel ch: channels) {
            NAKACK2 nakack=(NAKACK2)ch.getProtocolStack().findProtocol(NAKACK2.class);
            digest.merge(nakack.getDigest(ch.getAddress()));
        }
        for(JChannel ch: channels) {
            GMS gms=(GMS)ch.getProtocolStack().findProtocol(GMS.class);
            System.out.println("Injecting view " + view + " into " + ch.getAddress());
            gms.installView(view, digest);
        }
    }

    protected List<Address> getMembers(JChannel ... channels) {
        List<Address> members=new ArrayList<Address>(channels.length);
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
