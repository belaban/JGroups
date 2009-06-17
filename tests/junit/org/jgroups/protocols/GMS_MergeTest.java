package org.jgroups.protocols;


import org.jgroups.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.tests.ChannelTestBase;
import org.jgroups.util.MergeId;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.util.*;

/**
 * Tests the GMS protocol for merging functionality
 * @author Bela Ban
 * @version $Id: GMS_MergeTest.java,v 1.8 2009/06/17 11:30:42 belaban Exp $
 */
@Test(groups={Global.STACK_INDEPENDENT}, sequential=true)
public class GMS_MergeTest extends ChannelTestBase {
    static final String props="SHARED_LOOPBACK:PING(timeout=50):pbcast.NAKACK(log_discard_msgs=false)" +
            ":UNICAST:pbcast.STABLE:pbcast.GMS:FC:FRAG2";



    /**
     * Simulates the death of a merge leader after having sent a MERG_REQ. Because there is no MergeView or CANCEL_MERGE
     * message, the MergeCanceller has to null merge_id after a timeout
     */
    public static void testMergeRequestTimeout() throws Exception {
        JChannel c1=new JChannel(props);
        try {
            c1.connect("GMS_MergeTest.testMergeRequestTimeout()");
            Message merge_request=new Message();
            GMS.GmsHeader hdr=new GMS.GmsHeader(GMS.GmsHeader.MERGE_REQ);
            MergeId new_merge_id=MergeId.create(c1.getAddress());
            hdr.setMergeId(new_merge_id);
            merge_request.putHeader(GMS.name, hdr);
            GMS gms=(GMS)c1.getProtocolStack().findProtocol(GMS.class);
            MergeId merge_id=gms.getMergeId();
            assert merge_id == null;
            System.out.println("starting merge");
            gms.up(new Event(Event.MSG, merge_request));
            merge_id=gms.getMergeId();
            System.out.println("merge_id = " + merge_id);
            assert new_merge_id.equals(merge_id);

            long timeout=(long)(gms.getMergeTimeout() * 1.5);
            System.out.println("sleeping for " + timeout + " ms, then fetching merge_id: should be null (cancelled by the MergeCanceller)");
            Util.sleep(timeout);
            merge_id=gms.getMergeId();
            System.out.println("merge_id = " + merge_id);
            assert merge_id == null : "MergeCanceller didn't kick in";
        }
        finally {
            Util.close(c1);
        }
    }


    public static void testSimpleMerge() throws Exception {
        JChannel[] channels=null;
        try {
            channels=create("GMS_MergeTest.testSimpleMerge()", "A", "B", "C", "D");
            print(channels);
            View view=channels[channels.length -1].getView();
            assert view.size() == channels.length : "view is " + view;

            System.out.println("\ncreating partitions: ");
            String[][] partitions=generate(new String[]{"A", "B"}, new String[]{"C", "D"});
            createPartitions(channels, partitions);
            print(channels);
            checkViews(channels, "A", "A", "B");
            checkViews(channels, "B", "A", "B");
            checkViews(channels, "C", "C", "D");
            checkViews(channels, "D", "C", "D");


            Address leader=determineLeader(channels, "A", "C");
            System.out.println("\n==== injecting merge event into " + leader + " ====");
            injectMergeEvent(channels, leader, "A", "C");

            for(int i=0; i < 20; i++) {
                System.out.print(".");
                if(allChannelsHaveViewOf(channels, 4))
                    break;
                Util.sleep(500);
            }
            System.out.println("\n");
            print(channels);
            assertAllChannelsHaveViewOf(channels, 4);
        }
        finally {
            close(channels);
        }
    }


    public static void testConcurrentMergeTwoPartitions() throws Exception {
        JChannel[] channels=null;
        try {
            channels=create("GMS_MergeTest.testConcurrentMergeTwoPartitions", "A", "B", "C", "D");
            print(channels);
            View view=channels[channels.length -1].getView();
            assert view.size() == channels.length : "view is " + view;

            System.out.println("\ncreating partitions: ");
            String[][] partitions=generate(new String[]{"A", "B"}, new String[]{"C", "D"});
            createPartitions(channels, partitions);
            print(channels);
            checkViews(channels, "A", "A", "B");
            checkViews(channels, "B", "A", "B");
            checkViews(channels, "C", "C", "D");
            checkViews(channels, "D", "C", "D");

            System.out.println("\n==== injecting merge event into A and C concurrently ====");
            injectMergeEvent(channels, "C", "A", "C");
            injectMergeEvent(channels, "A", "A", "C");
            
            for(int i=0; i < 20; i++) {
                System.out.print(".");
                if(allChannelsHaveViewOf(channels, 4))
                    break;
                Util.sleep(500);
            }
            System.out.println("\n");
            print(channels);
            assertAllChannelsHaveViewOf(channels, 4);
        }
        finally {
            close(channels);
        }
    }


    public static void testConcurrentMergeMultiplePartitions() throws Exception {
        JChannel[] channels=null;
        try {
            channels=create("GMS_MergeTest.testConcurrentMergeMultiplePartitions", "A", "B", "C", "D", "E", "F", "G", "H");
            print(channels);
            View view=channels[channels.length -1].getView();
            assert view.size() == channels.length : "view is " + view;
            assertAllChannelsHaveViewOf(channels, 8);

            System.out.println("\ncreating partitions: ");
            String[][] partitions=generate(new String[]{"A", "B"},
                                           new String[]{"C", "D"},
                                           new String[]{"E", "F"},
                                           new String[]{"G", "H"});
            createPartitions(channels, partitions);
            print(channels);
            checkViews(channels, "A", "A", "B");
            checkViews(channels, "B", "A", "B");
            checkViews(channels, "C", "C", "D");
            checkViews(channels, "D", "C", "D");
            checkViews(channels, "E", "E", "F");
            checkViews(channels, "F", "E", "F");
            checkViews(channels, "G", "G", "H");
            checkViews(channels, "H", "G", "H");


            System.out.println("\n==== injecting merge event into A, C, E and G concurrently ====");
            injectMergeEvent(channels, "G", "A", "C", "E", "G");
            injectMergeEvent(channels, "E", "A", "C", "E", "G");
            injectMergeEvent(channels, "A", "A", "C", "E", "G");
            injectMergeEvent(channels, "C", "A", "C", "E", "G");

            for(int i=0; i < 20; i++) {
                System.out.print(".");
                if(allChannelsHaveViewOf(channels, 8))
                    break;
                Util.sleep(1000);
            }
            System.out.println("\n");
            print(channels);
            assertAllChannelsHaveViewOf(channels, 8);
        }
        finally {
            close(channels);
        }
    }



    private static boolean allChannelsHaveViewOf(JChannel[] channels, int count) {
        for(JChannel ch: channels) {
            if(ch.getView().size() != count)
                return false;
        }
        return true;
    }

    private static void assertAllChannelsHaveViewOf(JChannel[] channels, int count) {
        for(JChannel ch: channels)
            assert ch.getView().size() == count : ch.getName() + " has view " + ch.getView();
    }


    private static void close(JChannel[] channels) {
        if(channels == null) return;
        for(int i=channels.length -1; i <= 0; i--) {
            JChannel ch=channels[i];
            Util.close(ch);
        }
    }


    private static JChannel[] create(String cluster_name, String ... names) throws Exception {
        JChannel[] retval=new JChannel[names.length];
        for(int i=0; i < retval.length; i++) {
            JChannel ch=new JChannel(props);
            ch.setName(names[i]);
            retval[i]=ch;
            ch.connect(cluster_name);
            if(i == 0)
                Util.sleep(3000);
        }
        return retval;
    }

    private static void createPartitions(JChannel[] channels, String[]... partitions) throws Exception {
        checkUniqueness(partitions);
        List<View> views=new ArrayList<View>(partitions.length);
        for(String[] partition: partitions) {
            View view=createView(partition, channels);
            views.add(view);
        }
        applyViews(views, channels);
    }

    private static void injectMergeEvent(JChannel[] channels, String leader, String ... coordinators) {
        Address leader_addr=leader != null? findAddress(leader, channels) : determineLeader(channels);
        injectMergeEvent(channels, leader_addr, coordinators);
    }

    private static void injectMergeEvent(JChannel[] channels, Address leader_addr, String ... coordinators) {
        List<View> views=new ArrayList<View>();
        for(String tmp: coordinators)
            views.add(findView(tmp, channels));

        JChannel coord=findChannel(leader_addr, channels);
        GMS gms=(GMS)coord.getProtocolStack().findProtocol(GMS.class);
        gms.setLevel("trace");
        gms.up(new Event(Event.MERGE, views));
    }

    private static Address determineLeader(JChannel[] channels, String ... coords) {
        Membership membership=new Membership();
        for(String coord: coords)
            membership.add(findAddress(coord, channels));
        membership.sort();
        return membership.elementAt(0);
    }


    private static String[][] generate(String[] ... partitions) {
        String[][] retval=new String[partitions.length][];
        System.arraycopy(partitions, 0, retval, 0, partitions.length);
        return retval;
    }

    private static void checkUniqueness(String[] ... partitions) throws Exception {
        Set<String> set=new HashSet<String>();
        for(String[] partition: partitions) {
            for(String tmp: partition) {
                if(!set.add(tmp))
                    throw new Exception("partitions are overlapping: element " + tmp + " is in multiple partitions");
            }
        }
    }

    private static View createView(String[] partition, JChannel[] channels) throws Exception {
        Vector<Address> members=new Vector<Address>(partition.length);
        for(String tmp: partition) {
            Address addr=findAddress(tmp, channels);
            if(addr == null)
                throw new Exception(tmp + " not associated with a channel");
            members.add(addr);
        }
        return new View(members.firstElement(), 10, members);
    }

    private static void checkViews(JChannel[] channels, String channel_name, String ... members) {
        JChannel ch=findChannel(channel_name, channels);
        View view=ch.getView();
        assert view.size() == members.length : "view is " + view + ", members: " + Arrays.toString(members);
        for(String member: members) {
            Address addr=findAddress(member, channels);
            assert view.getMembers().contains(addr) : "view " + view + " does not contain " + addr;
        }
    }

    private static JChannel findChannel(String tmp, JChannel[] channels) {
        for(JChannel ch: channels) {
            if(ch.getName().equals(tmp))
                return ch;
        }
        return null;
    }

    private static JChannel findChannel(Address addr, JChannel[] channels) {
        for(JChannel ch: channels) {
            if(ch.getAddress().equals(addr))
                return ch;
        }
        return null;
    }

    private static Address findAddress(String tmp, JChannel[] channels) {
        for(JChannel ch: channels) {
            if(ch.getName().equals(tmp))
                return ch.getAddress();
        }
        return null;
    }

    private static View findView(String tmp, JChannel[] channels) {
        for(JChannel ch: channels) {
            if(ch.getName().equals(tmp))
                return ch.getView();
        }
        return null;
    }

    private static void applyViews(List<View> views, JChannel[] channels) {
        for(View view: views) {
            Collection<Address> members=view.getMembers();
            for(JChannel ch: channels) {
                Address addr=ch.getAddress();
                if(members.contains(addr)) {
                    GMS gms=(GMS)ch.getProtocolStack().findProtocol(GMS.class);
                    gms.installView(view);
                }
            }
        }
    }

    private static void print(JChannel[] channels) {
        for(JChannel ch: channels) {
            System.out.println(ch.getName() + ": " + ch.getView());
        }
    }


}