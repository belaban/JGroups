package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.util.*;

/**
 * Tests merging on all stacks
 * 
 * @author vlada
 * @version $Id: MergeTest.java,v 1.40 2009/09/26 05:37:17 belaban Exp $
 */
@Test(groups=Global.FLUSH,sequential=true)
public class MergeTest extends ChannelTestBase {
   
    @Test
    public void testMerging2Members() throws Exception {
        mergeHelper("MergeTest.testMerging2Members", "A", "B");
    }
    
    @Test
    public void testMerging4Members() throws Exception {
        mergeHelper("MergeTest.testMerging4Members", "A", "B", "C", "D");
    }


    protected void mergeHelper(String cluster_name, String ... members) throws Exception {
        JChannel[] channels=null;
        try {
            channels=createChannels(cluster_name, members);
            print(channels);

            System.out.println("\ncreating partitions: ");
            createPartitions(channels, members);
            print(channels);
            for(String member: members) {
                JChannel ch=findChannel(member, channels);
                assert ch.getView().size() == 1 : "view of " + ch.getAddress() + ": " + ch.getView();
            }

            System.out.println("\n==== injecting merge event ====");
            for(String member: members) {
                injectMergeEvent(channels, member, members);
            }
            for(int i=0; i < 20; i++) {
                System.out.print(".");
                if(allChannelsHaveViewOf(channels, members.length))
                    break;
                Util.sleep(500);
            }
            System.out.println("\n");
            print(channels);
            assertAllChannelsHaveViewOf(channels, members.length);
        }
        finally {
            if(channels != null)
                close(channels);
        }
    }

    private JChannel[] createChannels(String cluster_name, String[] members) throws Exception {
        JChannel[] retval=new JChannel[members.length];
        JChannel ch=null;
        for(int i=0; i < retval.length; i++) {
            JChannel tmp;
            if(ch == null) {
                ch=createChannel(true, members.length);
                tmp=ch;
            }
            else {
                tmp=createChannel(ch);
            }
            tmp.setName(members[i]);
            NAKACK nakack=(NAKACK)tmp.getProtocolStack().findProtocol(NAKACK.class);
            if(nakack != null)
                nakack.setLogDiscardMessages(false);
            tmp.connect(cluster_name);
            retval[i]=tmp;
        }

        return retval;
    }

    private static void close(JChannel[] channels) {
        if(channels == null) return;
        for(int i=channels.length -1; i <= 0; i--) {
            JChannel ch=channels[i];
            Util.close(ch);
        }
    }


    private static void createPartitions(JChannel[] channels, String ... partitions) throws Exception {
        checkUniqueness(partitions);
        List<View> views=new ArrayList<View>(partitions.length);
        for(String partition: partitions) {
            View view=createView(partition, channels);
            views.add(view);
        }
        applyViews(views, channels);
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

    private static void injectMergeEvent(JChannel[] channels, String leader, String ... coordinators) {
        Address leader_addr=leader != null? findAddress(leader, channels) : determineLeader(channels);
        injectMergeEvent(channels, leader_addr, coordinators);
    }

    private static void injectMergeEvent(JChannel[] channels, Address leader_addr, String ... coordinators) {
        Map<Address,View> views=new HashMap<Address,View>();
        for(String tmp: coordinators) {
            Address coord=findAddress(tmp, channels);
            views.put(coord, findView(tmp, channels));
        }

        JChannel coord=findChannel(leader_addr, channels);
        GMS gms=(GMS)coord.getProtocolStack().findProtocol(GMS.class);
        gms.setLevel("trace");
        gms.up(new Event(Event.MERGE, views));
    }


    private static View createView(String partition, JChannel[] channels) throws Exception {
        Vector<Address> members=new Vector<Address>();
        Address addr=findAddress(partition, channels);
        if(addr == null)
            throw new Exception(partition + " not associated with a channel");
        members.add(addr);
        return new View(members.firstElement(), 10, members);
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

    private static View findView(String tmp, JChannel[] channels) {
        for(JChannel ch: channels) {
            if(ch.getName().equals(tmp))
                return ch.getView();
        }
        return null;
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


    private static Address determineLeader(JChannel[] channels, String ... coords) {
        Membership membership=new Membership();
        for(String coord: coords)
            membership.add(findAddress(coord, channels));
        membership.sort();
        return membership.elementAt(0);
    }

     private static Address findAddress(String tmp, JChannel[] channels) {
         for(JChannel ch: channels) {
             if(ch.getName().equals(tmp))
                 return ch.getAddress();
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
