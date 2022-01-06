package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.*;


/**
 * Tests a merge where {A}, {B}, {C} and {D} are found, and then one of the members doesn't answer the merge request.
 * Goal: the merge should *not* get cancelled, but instead all 3 non-faulty member should merge
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class MergeTest2 {
    protected JChannel a,b,c,d;


    @BeforeMethod
    void setUp() throws Exception {
        a=createChannel("A");
        b=createChannel("B");
        c=createChannel("C");
        d=createChannel("D");
    }


    protected static JChannel createChannel(String name) throws Exception {
        JChannel retval=new JChannel(new SHARED_LOOPBACK(),
                                     new DISCARD().discardAll(true),
                                     new SHARED_LOOPBACK_PING(),
                                     new NAKACK2().useMcastXmit(false)
                                       .logDiscardMessages(false).logNotFoundMessages(false),
                                     new UNICAST3(),
                                     new STABLE().setMaxBytes(50000),
                                     new GMS().printLocalAddress(false).setJoinTimeout(500).setLeaveTimeout(100)
                                       .setMergeTimeout(3000).logViewWarnings(false).setViewAckCollectionTimeout(50)
                                       .logCollectMessages(false));
        retval.setName(name);
        retval.getProtocolStack().getTransport().getDiagnosticsHandler().setEnabled(false);
        retval.connect("MergeTest2");
        return retval;
    }


    @AfterMethod
    void tearDown() throws Exception {
        for(JChannel ch: new JChannel[]{a,b,c,d}) {
            ProtocolStack stack=ch.getProtocolStack();
            String cluster_name=ch.getClusterName();
            GMS gms=stack.findProtocol(GMS.class);
            if(gms != null)
                gms.setLevel("warn");
            stack.stopStack(cluster_name);
            stack.destroy();
        }
    }


    public void testMergeWithMissingMergeResponse() {
        JChannel merge_leader=findMergeLeader(a,b,c,d);
        List<Address> non_faulty_members=new ArrayList<>(Arrays.asList(a.getAddress(), b.getAddress(), c.getAddress(), d.getAddress()));
        List<Address> tmp=new ArrayList<>(non_faulty_members);
        tmp.remove(merge_leader.getAddress());
        Address faulty_member=Util.pickRandomElement(tmp);
        non_faulty_members.remove(faulty_member);

        System.out.println("\nMerge leader: " + merge_leader.getAddress() + "\nFaulty member: " + faulty_member +
                             "\nNon-faulty members: " + non_faulty_members);

        for(JChannel ch: new JChannel[]{a,b,c,d}) { // excluding faulty member, as it still discards messages
            assert ch.getView().size() == 1;
            if(ch.getAddress().equals(faulty_member)) // skip the faulty member; it keeps discarding messages
                continue;
            DISCARD discard=ch.getProtocolStack().findProtocol(DISCARD.class);
            discard.discardAll(false);
        }

        Map<Address,View> merge_views=new HashMap<>(4);
        for(JChannel ch: new JChannel[]{a,b,c,d}) {
            merge_views.put(ch.getAddress(), ch.getView()); // here, we include D
        }

        System.out.println("Injecting MERGE event into merge leader " + merge_leader.getAddress());
        GMS gms=merge_leader.getProtocolStack().findProtocol(GMS.class);
        gms.setLevel("trace");
        gms.up(new Event(Event.MERGE, merge_views));


        for(int i=0; i < 20; i++) {
            boolean done=true;
            System.out.println();
            for(JChannel ch: new JChannel[]{a,b,c,d}) {
                if(!ch.getAddress().equals(faulty_member)) {
                    System.out.println("==> " + ch.getAddress() + ": " + ch.getView());
                    if(ch.getView().size() != 3)
                        done=false;
                }
            }
            
            if(done)
                break;
            Util.sleep(3000);
        }

        for(JChannel ch: new JChannel[]{a,b,c,d}) {
            if(ch.getAddress().equals(faulty_member))
                assert ch.getView().size() == 1;
            else
                assert ch.getView().size() == 3 : ch.getAddress() + "'s view: " + ch.getView();
        }
    }

    protected static JChannel findMergeLeader(JChannel... channels) {
        Set<Address> tmp=new TreeSet<>();
        for(JChannel ch: channels)
            tmp.add(ch.getAddress());
        Address leader=tmp.iterator().next();
        for(JChannel ch: channels)
            if(ch.getAddress().equals(leader))
                return ch;
        return null;
    }


  
}
