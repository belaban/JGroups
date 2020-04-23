package org.jgroups.protocols;


import org.jgroups.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Digest;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.util.*;

/**
 * Tests the INJECT_VIEW protocol. Note that the logical names of the members should be unique, otherwise - when
 * running the testsuite in parallel and the names were A,B,C - the lookup of real addresses by logical names when
 * injecting views might be incorrect: e.g. a member A added by a different test might return the 'wrong' address!
 * @author Andrea Tarocchi
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class INJECT_VIEWTest {
    protected static final String A, B, C;

    static {
        A=INJECT_VIEWTest.class.getSimpleName() + "-A";
        B=INJECT_VIEWTest.class.getSimpleName() + "-B";
        C=INJECT_VIEWTest.class.getSimpleName() + "-C";
    }

    protected static Protocol[] getProps() {
        return modify(Util.getTestStack());
    }


    protected static Protocol[] modify(Protocol[] retval) {
        for(Protocol prot: retval) {
            if(prot instanceof GMS)
                ((GMS)prot).setJoinTimeout(1000);
            if(prot instanceof STABLE)
                ((STABLE)prot).setStabilityDelay(200);
            if(prot instanceof NAKACK2) {
                ((NAKACK2)prot).logDiscardMessages(false);
                ((NAKACK2)prot).logNotFoundMessages(false);
            }
        }
        return retval;
    }

    public void testInjectView() throws Exception {
        JChannel[] channels=null;
        try {
            channels=create( "testInjectView", A, B, C);
            print(channels);
            View view=channels[channels.length -1].getView();
            assert view.size() == channels.length : "view is " + view;

            String injectionViewString = String.format("%s=%s/%s;%s=%s/%s;%s=%s",
                                                       A,A,B,
                                                       B,B,C,
                                                       C,C);
            System.out.println("\ninjecting views: "+injectionViewString);
            for (JChannel channel : channels) {
                channel.getProtocolStack().addProtocol( new INJECT_VIEW());
            }
            for (JChannel channel : channels) {
                INJECT_VIEW iv = channel.getProtocolStack().findProtocol(INJECT_VIEW.class);
                iv.injectView(injectionViewString);
            }

            System.out.println("\nInjected views: "+injectionViewString);
            print(channels);
            System.out.println("\nchecking views: ");
            checkViews(channels, A, A, B);
            System.out.println("\nA is OK");
            checkViews(channels, B, B, C);
            System.out.println("\nB is OK");
            checkViews(channels, C, C);
            System.out.println("\nC is OK");

            System.out.println("\ndigests:");
            printDigests(channels);

            Address leader=determineLeader(channels, A, B, C);
            long end_time=System.currentTimeMillis() + 30000;
            do {
                System.out.println("\n==== injecting merge events into " + leader + " ====");
                injectMergeEvent(channels, leader, A, B, C);
                Util.sleep(1000);
                if(allChannelsHaveViewOf(channels, channels.length))
                    break;
            }
            while(end_time > System.currentTimeMillis());

            System.out.println("\n");
            print(channels);
            assertAllChannelsHaveViewOf(channels, channels.length);

            System.out.println("\ndigests:");
            printDigests(channels);
        }
        finally {
            System.out.println("closing channels");
            close(channels);
            System.out.println("done");
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
            assert ch.getView().size() == count : ch.getName() + " has view " + ch.getView() + " (should have " + count + " mbrs)";
    }

    private static void close(JChannel[] channels) {
        if(channels == null) return;
        disableTracing(channels);
        for(int i=channels.length -1; i >= 0; i--) {
            JChannel ch=channels[i];
            Util.close(ch);
        }
    }

    protected static void disableTracing(JChannel ... channels) {
        for(JChannel ch: channels) {
            GMS gms=ch.getProtocolStack().findProtocol(GMS.class);
            gms.setLevel("warn");
        }
    }

    private static JChannel[] create(String cluster_name, String ... names) throws Exception {
        JChannel[] retval=new JChannel[names.length];

        for(int i=0; i < retval.length; i++) {
            JChannel ch;
            Protocol[] props=getProps();
            ch=new JChannel(props);
            // ((MyChannel)ch).setId(i+1);
            ch.setName(names[i]);
            retval[i]=ch;
            ch.connect(cluster_name);
            if(i == 0)
                Util.sleep(3000);
        }
        return retval;
    }

    private static void injectMergeEvent(JChannel[] channels, Address leader_addr, String ... coordinators) {
        Map<Address,View> views=new HashMap<>();
        for(String tmp: coordinators) {
            Address coord=findAddress(tmp, channels);
            views.put(coord, findView(tmp, channels));
        }

        JChannel coord=findChannel(leader_addr, channels);
        GMS gms=coord.getProtocolStack().findProtocol(GMS.class);
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
                    GMS gms=ch.getProtocolStack().findProtocol(GMS.class);
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

    private static void printDigests(JChannel[] channels) {
        for(JChannel ch: channels) {
            NAKACK2 nak=ch.getProtocolStack().findProtocol(NAKACK2.class);
            Digest digest=nak.getDigest();
            System.out.println(ch.getName() + ": " + digest.toString());
        }
    }

}