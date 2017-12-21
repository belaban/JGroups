package org.jgroups.protocols;


import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Membership;
import org.jgroups.View;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.conf.ProtocolStackConfigurator;
import org.jgroups.protocols.pbcast.FLUSH;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Digest;
import org.jgroups.util.Util;
import org.testng.annotations.Test;
import org.w3c.dom.Element;

import java.io.File;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests the INJECT_VIEW protocol
 * @author Andrea Tarocchi
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class INJECT_VIEWTest {

    static final short GMS_ID=ClassConfigurator.getProtocolId(GMS.class);

    protected static Protocol[] getProps() {
        return modify(Util.getTestStack());
    }

    protected static Protocol[] getFlushProps() {
        return modify(Util.getTestStack(new FLUSH()));
    }

    protected static Protocol[] modify(Protocol[] retval) {
        for(Protocol prot: retval) {
            if(prot instanceof GMS)
                ((GMS)prot).setJoinTimeout(1000);
            if(prot instanceof STABLE)
                prot.setValue("stability_delay", 200);
            if(prot instanceof NAKACK2) {
                ((NAKACK2)prot).setLogDiscardMessages(false);
                ((NAKACK2)prot).setLogNotFoundMessages(false);
            }
        }
        return retval;
    }

    public static void testInjectView() throws Exception {
        JChannel[] channels=null;
        try {
            channels=create(false, true, "testInjectView", "A", "B", "C");
            print(channels);
            View view=channels[channels.length -1].getView();
            assert view.size() == channels.length : "view is " + view;

            String injectionViewString = "A=A,B,C;B=B,C;C=C";
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
            checkViews(channels, "A", "A", "B", "C");
            System.out.println("\nA is OK");
            checkViews(channels, "B", "B", "C");
            System.out.println("\nB is OK");
            checkViews(channels, "C", "C");
            System.out.println("\nC is OK");

            System.out.println("\ndigests:");
            printDigests(channels);

            Address leader=determineLeader(channels, "A", "B","C");
            long end_time=System.currentTimeMillis() + 30000;
            do {
                System.out.println("\n==== injecting merge events into " + leader + " ====");
                injectMergeEvent(channels, leader, "A", "B", "C");
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
        } catch (Throwable e) {
            e.printStackTrace();
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
        for(int i=channels.length -1; i >= 0; i--) {
            JChannel ch=channels[i];
            Util.close(ch);
        }
    }

    private static JChannel[] create(boolean use_flush_props, boolean simple_ids, String cluster_name, String ... names) throws Exception {
        JChannel[] retval=new JChannel[names.length];

        for(int i=0; i < retval.length; i++) {
            JChannel ch;
            Protocol[] props=use_flush_props? getFlushProps() : getProps();
            if(simple_ids) {
                ch=new MyChannel(props);
                ((MyChannel)ch).setId(i+1);
            }
            else
                ch=new JChannel(props);
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


    private static class MyChannel extends JChannel {
        protected int id=0;

        private MyChannel() throws Exception {
            super();
        }

        private MyChannel(File properties) throws Exception {
            super(properties);
        }

        private MyChannel(Element properties) throws Exception {
            super(properties);
        }

        private MyChannel(URL properties) throws Exception {
            super(properties);
        }

        private MyChannel(String properties) throws Exception {
            super(properties);
        }

        private MyChannel(ProtocolStackConfigurator configurator) throws Exception {
            super(configurator);
        }

        public MyChannel(Collection<Protocol> protocols) throws Exception {
            super(protocols);
        }

        public MyChannel(Protocol... protocols) throws Exception {
            super(protocols);
        }

        private MyChannel(JChannel ch) throws Exception {
            super(ch);
        }


        public void setId(int id) {
            this.id=id;
        }

        protected MyChannel setAddress() {
            Address old_addr=local_addr;
            local_addr=new org.jgroups.util.UUID(id, id);

            if(old_addr != null)
                down(new Event(Event.REMOVE_ADDRESS, old_addr));
            if(name == null || name.isEmpty()) // generate a logical name if not set
                name=Util.generateLocalName();
            if(name != null && !name.isEmpty())
                org.jgroups.util.NameCache.add(local_addr, name);

            Event evt=new Event(Event.SET_LOCAL_ADDRESS, local_addr);
            down(evt);
            if(up_handler != null)
                up_handler.up(evt);
            return this;
        }
    }
}