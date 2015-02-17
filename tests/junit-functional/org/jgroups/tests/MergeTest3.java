package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.protocols.DISCARD;
import org.jgroups.protocols.SHARED_LOOPBACK;
import org.jgroups.protocols.SHARED_LOOPBACK_PING;
import org.jgroups.protocols.UNICAST3;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.MergeId;
import org.jgroups.util.MutableDigest;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.*;


/**
 * Tests a merge between partitions {A,B,C} and {D,E,F} merge, but 1 member in each partition is already involved
 * in a different merge (GMS.merge_id != null). For example, if C and E are busy with a different merge, the MergeView
 * should exclude them: {A,B,D,F}. The digests must also exclude C and E.
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class MergeTest3 {
    protected JChannel a,b,c,d,e,f;



    @BeforeMethod
    void setUp() throws Exception {
        a=createChannel("A");
        b=createChannel("B");
        c=createChannel("C");
        d=createChannel("D");
        e=createChannel("E");
        f=createChannel("F");
    }



    @AfterMethod
    void tearDown() throws Exception {
        for(JChannel ch: new JChannel[]{a,b,c,d,e,f}) {
            ProtocolStack stack=ch.getProtocolStack();
            String cluster_name=ch.getClusterName();
            stack.stopStack(cluster_name);
            stack.destroy();
        }
    }


    public void testMergeWithMissingMergeResponse() {
        createPartition(a,b,c);
        createPartition(d,e,f);

        System.out.println("Views are:");
        for(JChannel ch: Arrays.asList(a,b,c,d,e,f))
            System.out.println(ch.getAddress() + ": " + ch.getView());

        JChannel merge_leader=findMergeLeader(a,b,c,d,e,f);
        List<Address> first_partition=getMembers(a,b,c);
        List<Address> second_partition=getMembers(d,e,f);

        Collections.sort(first_partition);
        Address first_coord=first_partition.remove(0); // remove the coord
        Address busy_first=first_partition.get(0);

        Collections.sort(second_partition);
        Address second_coord=second_partition.remove(0);
        Address busy_second=second_partition.get(second_partition.size() -1);

        System.out.println("\nMerge leader: " + merge_leader.getAddress() + "\nBusy members: " + Arrays.asList(busy_first, busy_second));

        MergeId busy_merge_id=MergeId.create(a.getAddress());
        setMergeIdIn(busy_first, busy_merge_id);
        setMergeIdIn(busy_second, busy_merge_id);
        for(JChannel ch: new JChannel[]{a,b,c,d,e,f}) { // excluding faulty member, as it still discards messages
            assert ch.getView().size() == 3;
            GMS gms=(GMS)ch.getProtocolStack().findProtocol(GMS.class);
            gms.setJoinTimeout(3000);
            DISCARD discard=(DISCARD)ch.getProtocolStack().findProtocol(DISCARD.class);
            discard.setDiscardAll(false);
        }

        System.out.println("Injecting MERGE event into merge leader " + merge_leader.getAddress());
        Map<Address,View> merge_views=new HashMap<>(6);
        merge_views.put(first_coord, findChannel(first_coord).getView());
        merge_views.put(second_coord, findChannel(second_coord).getView());

        GMS gms=(GMS)merge_leader.getProtocolStack().findProtocol(GMS.class);
        gms.up(new Event(Event.MERGE, merge_views));

        for(int i=0; i < 20; i++) {
            boolean done=true;
            System.out.println();
            for(JChannel ch: new JChannel[]{a,b,c,d,e,f}) {
                System.out.println("==> " + ch.getAddress() + ": " + ch.getView());
                Address addr=ch.getAddress();
                if(addr.equals(busy_first) || addr.equals(busy_second)) {
                    if(ch.getView().size() != 3)
                        done=false;
                }
                else {
                    if(ch.getView().size()  != 4)
                        done=false;
                }
            }
            
            if(done)
                break;
            Util.sleep(1000);
        }
        for(JChannel ch: new JChannel[]{a,b,c,d,e,f}) {
            if(ch.getAddress().equals(busy_first) || ch.getAddress().equals(busy_second))
                assert ch.getView().size() == 3;
            else
                assert ch.getView().size() == 4 : ch.getAddress() + "'s view: " + ch.getView();
        }



        System.out.println("\n************************ Now merging the entire cluster ****************");
        cancelMerge(busy_first);
        cancelMerge(busy_second);

        System.out.println("Injecting MERGE event into merge leader " + merge_leader.getAddress());
        merge_views=new HashMap<>(6);
        merge_views.put(merge_leader.getAddress(), merge_leader.getView());
        merge_views.put(busy_first, findChannel(busy_first).getView());
        merge_views.put(busy_second, findChannel(busy_second).getView());

        System.out.println("merge event is " + merge_views);

        gms=(GMS)merge_leader.getProtocolStack().findProtocol(GMS.class);
        gms.up(new Event(Event.MERGE, merge_views));

        for(int i=0; i < 20; i++) {
            boolean done=true;
            System.out.println();
            for(JChannel ch: new JChannel[]{a,b,c,d,e,f}) {
                System.out.println("==> " + ch.getAddress() + ": " + ch.getView());
                if(ch.getView().size()  != 6)
                    done=false;
            }

            if(done)
                break;
            Util.sleep(1000);
        }
        for(JChannel ch: new JChannel[]{a,b,c,d,e,f}) {
            if(ch.getAddress().equals(busy_first) || ch.getAddress().equals(busy_second))
                assert ch.getView().size() == 6 : ch.getAddress() + "'s view: " + ch.getView();
        }
    }

    protected JChannel createChannel(String name) throws Exception {
        JChannel retval=new JChannel(new SHARED_LOOPBACK(),
                                     new DISCARD().setValue("discard_all",true),
                                     new SHARED_LOOPBACK_PING(),
                                     new NAKACK2().setValue("use_mcast_xmit",false)
                                       .setValue("log_discard_msgs",false).setValue("log_not_found_msgs",false),
                                     new UNICAST3(),
                                     new STABLE().setValue("max_bytes",50000),
                                     new GMS().setValue("print_local_addr",false)
                                       .setValue("join_timeout", 1)
                                       .setValue("leave_timeout",100)
                                       .setValue("merge_timeout",5000)
                                       .setValue("log_view_warnings",false)
                                       .setValue("view_ack_collection_timeout",50)
                                       .setValue("log_collect_msgs",false))
          .name(name);
        retval.connect("MergeTest3");
        JmxConfigurator.registerChannel(retval, Util.getMBeanServer(), name, retval.getClusterName(), true);
        return retval;
    }

    protected void setMergeIdIn(Address mbr, MergeId busy_merge_id) {
        GMS gms=(GMS)findChannel(mbr).getProtocolStack().findProtocol(GMS.class);
        gms.getMerger().setMergeId(null, busy_merge_id);
    }

    protected void cancelMerge(Address mbr) {
        GMS gms=(GMS)findChannel(mbr).getProtocolStack().findProtocol(GMS.class);
        gms.cancelMerge();
    }

    protected JChannel findChannel(Address mbr) {
        for(JChannel ch: Arrays.asList(a,b,c,d,e,f)) {
            if(ch.getAddress().equals(mbr))
                return ch;
        }
        return null;
    }

    protected void createPartition(JChannel ... channels) {
        long view_id=1; // find the highest view-id +1
        for(JChannel ch: channels)
            view_id=Math.max(ch.getView().getViewId().getId(), view_id);
        view_id++;

        List<Address> members=getMembers(channels);
        Collections.sort(members);
        Address coord=members.get(0);
        View view=new View(coord, view_id, members);
        MutableDigest digest=new MutableDigest(view.getMembersRaw());
        for(JChannel ch: channels) {
            NAKACK2 nakack=(NAKACK2)ch.getProtocolStack().findProtocol(NAKACK2.class);
            digest.merge(nakack.getDigest(ch.getAddress()));
        }
        for(JChannel ch: channels) {
            GMS gms=(GMS)ch.getProtocolStack().findProtocol(GMS.class);
            gms.installView(view, digest);
        }
    }

    protected List<Address> getMembers(JChannel ... channels) {
        List<Address> members=new ArrayList<>(channels.length);
        for(JChannel ch: channels)
            members.add(ch.getAddress());
        return members;
    }

    protected Address determineCoordinator(JChannel ... channels) {
        List<Address> list=new ArrayList<>(channels.length);
        for(JChannel ch: channels)
            list.add(ch.getAddress());
        Collections.sort(list);
        return list.get(0);
    }

    protected JChannel findMergeLeader(JChannel ... channels) {
        Set<Address> tmp=new TreeSet<>();
        for(JChannel ch: channels)
            tmp.add(ch.getAddress());
        Address leader=tmp.iterator().next();
        for(JChannel ch: channels)
            if(ch.getAddress().equals(leader))
                return ch;
        return null;
    }



    @Test(enabled=false)
    public static void main(String[] args) throws Exception {
        MergeTest3 test=new MergeTest3();
        test.setUp();
        test.testMergeWithMissingMergeResponse();
        test.tearDown();
    }


  
}
