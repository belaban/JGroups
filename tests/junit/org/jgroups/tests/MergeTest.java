package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.DISCARD;
import org.jgroups.protocols.MERGE3;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests merging on all stacks
 * 
 * @author vlada
 */
@Test(groups=Global.STACK_DEPENDENT,singleThreaded=true)
public class MergeTest extends ChannelTestBase {
    protected JChannel[] channels=null;

    @AfterMethod protected void destroy() {
        level("warn", channels);
        for(JChannel ch: channels)
            try {
                Util.shutdown(ch);
            }
            catch(Exception e) {
                e.printStackTrace();
            }
    }
   
    public void testMerging2Members() throws Exception {
        mergeHelper("MergeTest.testMerging2Members", "A", "B");
    }
    
    public void testMerging4Members() throws Exception {
        mergeHelper("MergeTest.testMerging4Members", "A", "B", "C", "D");
    }


    protected void mergeHelper(String cluster_name, String ... members) throws Exception {
        channels=createChannels(cluster_name, members);
        Util.waitUntilAllChannelsHaveSameView(10000, 500, channels);
        print(channels);

        System.out.println("\ncreating partitions: ");
        createPartitions(channels);

        print(channels);
        for(JChannel ch: channels)
            assert ch.getView().size() == 1 : "view is " + ch.getView();

        Address merge_leader=determineLeader(channels, members);
        System.out.println("\n==== injecting merge event into merge leader : " + merge_leader + " ====");
        for(JChannel ch: channels)
            ch.getProtocolStack().removeProtocol(DISCARD.class);
        injectMergeEvent(channels, merge_leader, members);
        for(int i=0; i < 40; i++) {
            System.out.print(".");
            if(allChannelsHaveViewOf(channels, members.length))
                break;
            Util.sleep(1000);
        }
        System.out.println("\n");
        print(channels);
        assertAllChannelsHaveViewOf(channels, members.length);

    }

    protected static void level(String level, JChannel ... channels) {
        for(JChannel ch: channels) {
            GMS gms=(GMS)ch.getProtocolStack().findProtocol(GMS.class);
            gms.setLevel(level);
        }
    }

    protected JChannel[] createChannels(String cluster_name, String[] members) throws Exception {
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
            ProtocolStack stack=tmp.getProtocolStack();

            NAKACK2 nakack=(NAKACK2)stack.findProtocol(NAKACK2.class);
            if(nakack != null)
                nakack.setLogDiscardMessages(false);

            stack.removeProtocol(MERGE3.class);

            tmp.connect(cluster_name);
            retval[i]=tmp;
        }

        return retval;
    }

    private static void close(JChannel[] channels) {
        Util.close(channels);
    }


    private static void createPartitions(JChannel[] channels) throws Exception {
        long view_id=1; // find the highest view-id +1
        for(JChannel ch: channels)
            view_id=Math.max(ch.getView().getViewId().getId(), view_id);
        view_id++;

        for(JChannel ch: channels) {
            DISCARD discard=new DISCARD();
            discard.setDiscardAll(true);
            ch.getProtocolStack().insertProtocol(discard, ProtocolStack.Position.ABOVE,TP.class);
        }

        for(JChannel ch: channels) {
            View view=View.create(ch.getAddress(), view_id, ch.getAddress());
            GMS gms=(GMS)ch.getProtocolStack().findProtocol(GMS.class);
            gms.installView(view);
        }
    }


    private static void injectMergeEvent(JChannel[] channels, String leader, String ... coordinators) {
        Address leader_addr=leader != null? findAddress(leader, channels) : determineLeader(channels);
        injectMergeEvent(channels, leader_addr, coordinators);
    }

    private static void injectMergeEvent(JChannel[] channels, Address leader_addr, String ... coordinators) {
        Map<Address,View> views=new HashMap<>();
        for(String tmp: coordinators) {
            Address coord=findAddress(tmp, channels);
            views.put(coord, findView(tmp, channels));
        }

        JChannel coord=findChannel(leader_addr, channels);
        GMS gms=(GMS)coord.getProtocolStack().findProtocol(GMS.class);
        gms.setLevel("trace");
        gms.up(new Event(Event.MERGE, views));
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
        return membership.sort().elementAt(0);
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
