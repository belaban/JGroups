package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.relay.*;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MyReceiver;
import org.jgroups.util.NameCache;
import org.jgroups.util.UUID;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.jgroups.tests.RelayTests.Data.Type.REQ;

/**
 * Various RELAY-related tests ({@link RELAY2} and {@link RELAY3})
 * @author Bela Ban
 * @since 3.2
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true,dataProvider="relayProvider")
public class RelayTest extends RelayTests {
    protected JChannel a, b, c;  // members in site "lon"
    protected JChannel d, e, f;  // used for other tests AB CD EF
    protected JChannel x, y, z;  // members in site "sfo

    protected static final String BRIDGE_CLUSTER = "global";
    protected static final String SFO            = "sfo", LON="lon", NYC="nyc";

    @DataProvider
    protected Object[][] relayProvider() {
        return new Object[][] {
          {RELAY2.class},
          {RELAY3.class}
        };
    }


    @AfterMethod protected void destroy() {Util.closeReverse(a,b,c,d,e,f,x,y,z);}

    /**
     * Test that RELAY can be added to an already connected channel.
     */
    public void testAddRelay2ToAnAlreadyConnectedChannel(Class<? extends RELAY> cl) throws Exception {
        // Create and connect a channel.
        a=new JChannel(defaultStack()).name("A").connect(SFO);
        System.out.printf("Channel %s is connected. View: %s\n", a.getName(), a.getView());

        // Add RELAY protocol to the already connected channel.
        RELAY relay=createSymmetricRELAY(cl, SFO, BRIDGE_CLUSTER, LON, SFO);

        a.getProtocolStack().insertProtocolAtTop(relay);
        for(Protocol p=relay; p != null; p=p.getDownProtocol())
            p.setAddress(a.getAddress());
        relay.setProtocolStack(a.getProtocolStack());
        relay.init(); // configure();
        relay.handleView(a.getView());

        // Check for RELAY2 presence
        RELAY ar=a.getProtocolStack().findProtocol(RELAY.class);
        assert ar != null;
        Util.waitUntilTrue(500, 50, () -> getRoute(a, SFO) != null);
        Route route=getRoute(a, SFO);
        if(cl.equals(RELAY3.class))
            // semantic change in RELAY3:
            assert route == null; // no other site master has joined this bridge cluster, so no route is found
        else
            assert route != null;
    }
    
    /**
     * Tests that routes are correctly registered after a partition and a subsequent merge
     * (https://issues.redhat.com/browse/JGRP-1524)
     */
    public void testMissingRouteAfterMerge(Class<? extends RELAY> cl) throws Exception {
        a=createNode(cl, LON, "A");
        b=createNode(cl, LON, "B");
        Util.waitUntilAllChannelsHaveSameView(30000, 1000, a, b);

        x=createNode(cl, SFO, "X");
        assert x.getView().size() == 1;

        RELAY ar=a.getProtocolStack().findProtocol(RELAY.class), xr=x.getProtocolStack().findProtocol(RELAY.class);
        assert ar != null && xr != null;
        Util.waitUntilTrue(10000, 500, () -> {
            JChannel ab=ar.getBridge(SFO), xb=xr.getBridge(LON);
            return ab != null && xb != null && ab.getView().size() == 2 && xb.getView().size() == 2;
        });
        JChannel a_bridge=ar.getBridge(SFO), x_bridge=xr.getBridge(LON);
        assert a_bridge != null && x_bridge != null;

        System.out.println("A's bridge channel: " + a_bridge.getView());
        System.out.println("X's bridge channel: " + x_bridge.getView());
        assert a_bridge.getView().size() == 2 : "bridge view is " + a_bridge.getView();
        assert x_bridge.getView().size() == 2 : "bridge view is " + x_bridge.getView();

        Route route=getRoute(x, LON);
        System.out.println("Route at sfo to lon: " + route);
        assert route != null;

        // Now inject a partition into site LON
        System.out.println("Creating partition between A and B:");
        injectSingletonPartitions(a, b);

        System.out.println("A's view: " + a.getView() + "\nB's view: " + b.getView());
        assert a.getView().size() == 1 && b.getView().size() == 1;

        route=getRoute(x, LON);
        System.out.println("Route at sfo to lon: " + route);
        assert route != null;

        View bridge_view=xr.getBridgeView(BRIDGE_CLUSTER);
        System.out.println("bridge_view = " + bridge_view);

        // Now make A and B form a cluster again:
        View merge_view=new MergeView(a.getAddress(), 10, Arrays.asList(a.getAddress(), b.getAddress()),
                                      Arrays.asList(View.create(a.getAddress(), 5, a.getAddress()),
                                                    View.create(b.getAddress(), 5, b.getAddress())));
        GMS gms=a.getProtocolStack().findProtocol(GMS.class);
        gms.installView(merge_view, null);
        gms=b.getProtocolStack().findProtocol(GMS.class);
        gms.installView(merge_view, null);

        Util.waitUntilAllChannelsHaveSameView(20000, 500, a, b);
        System.out.println("A's view: " + a.getView() + "\nB's view: " + b.getView());

        Util.waitUntilTrue(10000, 500, () -> {
            View bv=xr.getBridgeView(BRIDGE_CLUSTER);
            return bv != null && bv.size() == 2;
        });
        route=getRoute(x, LON);
        System.out.println("Route at sfo to lon: " + route);
        assert route != null;
    }


    /**
     * Tests sites LON and SFO, with SFO disconnecting (bridge view on LON should be 1) and reconnecting (bridge view on
     * LON and SFO should be 2)
     */
    public void testDisconnectAndReconnect(Class<? extends RELAY> cl) throws Exception {
        a=createNode(cl, LON, "A");
        x=createNode(cl, SFO, "X");

        System.out.println("Started A and X; waiting for bridge view of 2 on A and X");
        waitForBridgeView(2, 20000, 500, BRIDGE_CLUSTER, a, x);

        System.out.println("Disconnecting X; waiting for a bridge view on 1 on A");
        x.disconnect();
        waitForBridgeView(1, 20000, 500, BRIDGE_CLUSTER, a);

        System.out.println("Reconnecting X again; waiting for a bridge view of 2 on A and X");
        x.connect(SFO);
        waitForBridgeView(2, 20000, 500, BRIDGE_CLUSTER, a, x);
    }


    public void testCoordinatorShutdown(Class<? extends RELAY> cl) throws Exception {
        a=createNode(cl, LON, "A");
        b=createNode(cl, LON, "B");
        x=createNode(cl, SFO, "X");
        y=createNode(cl, SFO, "Y");
        Util.waitUntilAllChannelsHaveSameView(10000, 100, a, b);
        Util.waitUntilAllChannelsHaveSameView(10000, 100, x, y);
        waitForBridgeView(2, 20000, 100, BRIDGE_CLUSTER, a, x); // A and X are site masters

        long start=System.currentTimeMillis();
        a.close();
        long time=System.currentTimeMillis()-start;
        System.out.println("A took " + time + " ms");

        Util.waitUntilAllChannelsHaveSameView(10000, 100, b);
        waitForBridgeView(2, 20000, 100, BRIDGE_CLUSTER, b, x); // B and X are now site masters

        long start2=System.currentTimeMillis();
        b.close();
        long time2=System.currentTimeMillis() - start2;
        System.out.println("B took " + time2 + " ms");
        waitForBridgeView(1, 40000, 500, BRIDGE_CLUSTER, x);
    }



    /**
     * Tests the following scenario:
     * <ul>
     *     <li>Nodes A in LON and B in SFO, both are up</li>
     *     <li>B goes down</li>
     *     <li>The status of site SFO in LON is set to UNKNOWN and a task T is started which will set SFO's status
     *     to DOWN in site_down_timeout ms</li>
     *     <li>Before T kicks in, B in SFO is started again</li>
     *     <li>The status of site SFO in LON is now UP</li>
     *     <li>Make sure T is cancelled when transitioning from UNKNOWN to UP, or else it'll set the status
     *     of SFO to DOWN when it triggers</li>
     * </ul>
     */
    public void testUnknownAndUpStateTransitions(Class<? extends RELAY> cl) throws Exception {
        a=createNode(cl, LON, "A");
        x=createNode(cl, SFO, "X");
        waitForBridgeView(2, 20000, 500, BRIDGE_CLUSTER, a, x);

        System.out.println("Disconnecting X");
        x.disconnect();
        System.out.println("A: waiting for site SFO to be DOWN");
        waitUntilRoute(SFO, false, 20000, 500, a);

        System.out.println("Reconnecting X");
        x.connect(SFO);
        waitUntilRoute(SFO, true, 5000, 100, a);
        Route route=getRoute(a, SFO);
        assert route != null : "route is " + route + " (expected to be UP)";

        route=getRoute(x, LON);
        assert route != null : "route is " + route + " (expected to be UP)";
    }

    /** Tests https://issues.redhat.com/browse/JGRP-2554 and https://issues.redhat.com/browse/JGRP-2570*/
    public void testSiteUnreachableMessageBreaksSiteUUID(Class<? extends RELAY> cl) throws Exception {
        a=createNode(cl, LON, "A");
        b=createNode(cl, LON, "B");
        c=createNode(cl, LON, "C");
        x=createNode(cl, SFO, "X");
        waitForBridgeView(2, 10000, 500, BRIDGE_CLUSTER, a, x);

        final MyUphandler up_handler=new MyUphandler();
        b.setUpHandler(up_handler);

        log.debug("Disconnecting X");
        x.disconnect();
        log.debug("A: waiting for site SFO to be UNKNOWN");
        waitUntilRoute(SFO, false, 10000, 500, a);

        for (int j = 0; j < 100; j++)
            b.send(new SiteMaster(SFO), "to-sfo".getBytes());

        log.debug("Sending message from A to B");
        for (int j = 0; j < 100; j++)
            a.send(b.getAddress(), ("to-b-" + j).getBytes());

        for (int j = 0; j < 100; j++) {
            Message take = up_handler.getReceived().take();
            // all addresses are SiteUUIDs in RELAY3
            assert !cl.equals(RELAY3.class) || take.src() instanceof SiteUUID : "Address was " + take.src();
        }
        // https://issues.redhat.com/browse/JGRP-2586
        Util.waitUntilTrue(10000, 500, () -> up_handler.getSiteUnreachableEvents() > 0);
        assert up_handler.getSiteUnreachableEvents() > 0 && up_handler.getSiteUnreachableEvents() <= 100
          : "Expecting <= 100 site unreachable events on node B but got " + up_handler.getSiteUnreachableEvents();

        // drain site-unreachable events received after this point
        Util.waitUntilTrue(3000, 500, () -> up_handler.getSiteUnreachableEvents() > 10);

        MyUphandler h2=new MyUphandler();
        assert ((RELAY) a.getProtocolStack().findProtocol(RELAY.class)).isSiteMaster();
        a.setUpHandler(h2);

        // check if the site master receives the events
        for (int i = 0; i < 100; i++)
            a.send(new SiteMaster(SFO), "to-sfo-from-a".getBytes());
        assert h2.getSiteUnreachableEvents() == 100
          : "Expecting 100 site unreachable events on node A but got " + h2.getSiteUnreachableEvents();
    }



    /**
     * Cluster A,B,C in LON and X,Y,Z in SFO. A, B, X and Y are site masters (max_site_masters: 2).
     * Verifies that messages sent by C in the LON site are received in the correct order by all members of the SFO site
     * despite using multiple site masters. JIRA: https://issues.redhat.com/browse/JGRP-2112
     */
    public void testSenderOrderWithMultipleSiteMasters(Class<? extends RELAY> cl) throws Exception {
        MyReceiver<Object> rx=new MyReceiver<>().rawMsgs(true), ry=new MyReceiver<>().rawMsgs(true),
          rz=new MyReceiver<>().rawMsgs(true);
        final int NUM=512;
        final String sm_picker_impl=SiteMasterPickerImpl.class.getName();
        a=createNode(cl, LON, "A", 2, sm_picker_impl);
        b=createNode(cl, LON, "B", 2, sm_picker_impl);
        c=createNode(cl, LON, "C", 2, sm_picker_impl);
        Util.waitUntilAllChannelsHaveSameView(10000, 1000, a,b,c);

        x=createNode(cl, SFO, "X", 2, sm_picker_impl).setReceiver(rx);
        y=createNode(cl, SFO, "Y", 2, sm_picker_impl).setReceiver(ry);
        z=createNode(cl, SFO, "Z", 2, sm_picker_impl).setReceiver(rz);
        Util.waitUntilAllChannelsHaveSameView(10000, 1000, x,y,z);

        waitForBridgeView(4, 10000, 1000, BRIDGE_CLUSTER, a,b,x,y);

        // C in LON sends messages to the site master of SFO (via either SM A or B); everyone in SFO (x,y,z)
        // must receive them in correct order
        SiteMaster target_sm=new SiteMaster(SFO);
        System.out.printf("%s: sending %d messages to %s:\n", c.getAddress(), NUM, target_sm);
        for(int i=1; i <= NUM; i++) {
            Message msg=new BytesMessage(target_sm, i); // the seqno is in the payload of the message
            c.send(msg);
        }

        Util.waitUntilTrue(10000, 500, () -> Stream.of(rx,ry,rz).anyMatch(l -> l.size() >= NUM));
        System.out.printf("X: size=%d\nY: size=%d\nZ: size=%d\n", rx.size(), ry.size(), rz.size());
        assert rx.size() == NUM || ry.size() == NUM;
        assert rz.size() == 0;
    }

    public void testForwardingRoute(Class<? extends RELAY> ignored) {
        ForwardingRoute r1=new ForwardingRoute("hf", "net1"),
          r2=new ForwardingRoute("hf", "net3");
        assert !r1.equals(r2);
        Set<ForwardingRoute> routes=new HashSet<>();
        routes.add(r1);
        routes.add(r2);
        assert routes.size() == 2;
        r2.gateway("net1");
        assert r1.equals(r2);
    }

    /** Tests https://issues.redhat.com/browse/JGRP-2712 */
    public void testSitesUp(Class<? extends RELAY> cl) throws Exception {
        a=createNode(cl, LON, "A", BRIDGE_CLUSTER, LON, NYC, SFO);
        b=createNode(cl, LON, "B", BRIDGE_CLUSTER, LON, NYC, SFO);
        c=createNode(cl, LON, "C", BRIDGE_CLUSTER, LON, NYC, SFO);

        d=createNode(cl, NYC, "D", BRIDGE_CLUSTER, LON, NYC, SFO);
        e=createNode(cl, NYC, "E", BRIDGE_CLUSTER, LON, NYC, SFO);
        f=createNode(cl, NYC, "F", BRIDGE_CLUSTER, LON, NYC, SFO);

        try(JChannel _g=createNode(cl, SFO, "G", BRIDGE_CLUSTER, LON, NYC, SFO);
            JChannel _h=createNode(cl, SFO, "H", BRIDGE_CLUSTER, LON, NYC, SFO);
            JChannel _i=createNode(cl, SFO, "I", BRIDGE_CLUSTER, LON, NYC, SFO)) {
            Util.waitUntilAllChannelsHaveSameView(5000, 100, a,b,c);
            Util.waitUntilAllChannelsHaveSameView(5000, 100, d,e,f);
            Util.waitUntilAllChannelsHaveSameView(5000, 100, _g,_h,_i);

            waitUntilRoute(NYC, true, 5000, 500, a);
            waitUntilRoute(SFO, true, 5000, 500, a);
            waitUntilRoute(LON, true, 5000, 500, d);
            waitUntilRoute(SFO, true, 5000, 500, d);
            waitUntilRoute(LON, true, 5000, 500, _g);
            waitUntilRoute(NYC, true, 5000, 500, _g);

            assert Stream.of(a,b,c,d,e,f,_g,_h,_i).allMatch(c -> c.getView().size() == 3);
            assert Stream.of(a,d,_g).map(ch -> (RELAY)ch.getProtocolStack().findProtocol(RELAY.class))
              .allMatch(RELAY::isSiteMaster);

            Stream.of(d,e,f,_g,_h,_i)
              .map(ch -> (RELAY)ch.getProtocolStack().findProtocol(RELAY.class))
              .forEach(r -> r.setRouteStatusListener(new MyRouteStatusListener(r.getAddress()).verbose(false)));

            // now stop A; B will become new site master and we should get a site-down(NYC), then site-up(NYC)
            Util.close(a);
            Util.waitUntil(5000, 500, () -> Stream.of(d,e,f,_g,_h,_i)
              .map(ch -> (RELAY)ch.getProtocolStack().findProtocol(RELAY.class))
              .peek(r -> System.out.printf("%s: %s\n", r.getAddress(), r.getRouteStatusListener()))
              .map(r -> (MyRouteStatusListener)r.getRouteStatusListener())
              .allMatch(l -> l.down().contains(LON) && l.up().contains(LON)));
        }
    }



    /** Tests sending and receiving of messages across sites */
    public void testSendAndReceiveMulticasts(Class<? extends RELAY> cl) throws Exception {
        createSymmetricNetwork(cl, ch -> new MyReceiver<Message>().rawMsgs(true));

        // sends multicasts from site-master (_a) and non site masters (_b,_c, ...
        for(JChannel ch: allChannels())
            ch.send(null, String.format("%s", ch.getAddress()));
        Util.waitUntil(5000, 500,
                       () -> allChannels().stream().peek(RelayTests::printMessages)
                         .map(RelayTests::getReceiver)
                         .allMatch(r -> r.size() == 6));
    }

    /** Tests sending of multicasts and reception of responses */
    public void testSendAndReceiveMulticastsAndUnicastResponses(Class<? extends RELAY> cl) throws Exception {
        createSymmetricNetwork(cl, ch -> new ResponseSender<Message>(ch).rawMsgs(true));
        for(JChannel ch: allChannels())
            ch.send(null, String.format("%s", ch.getAddress()));
        Util.waitUntil(5000, 500,
                       () -> allChannels().stream().peek(RelayTests::printMessages)
                         .map(RelayTests::getReceiver)
                         .allMatch(r -> r.size() == 6 * 2));

        for(JChannel ch: allChannels()) {
            List<Message> list=getReceiver(ch).list();
            // assert that there are 6 multicasts and 6 unicasts
            assert expectedMulticasts(list,6);
            assert expectedUnicasts(list,6);

            Set<String> senders=list.stream()
              .map(Message::src).filter(Objects::nonNull)
              .map(a -> (a instanceof SiteUUID)? ((SiteUUID)a).getName() : NameCache.get(a))
              .collect(Collectors.toSet());
            Set<String> channels=allChannels().stream().map(JChannel::address)
              .map(a -> (a instanceof SiteUUID)? ((SiteUUID)a).getName() : NameCache.get(a)).collect(Collectors.toSet());
            assert senders.equals(channels);
        }
    }

    /** Tests sending to the 3 site masters A, C and E, and expecting responses */
    public void testSendingToSiteMasters(Class<? extends RELAY> cl) throws Exception {
        createSymmetricNetwork(cl, ch -> new ResponseSender<Message>(ch).rawMsgs(true));
        for(JChannel ch: allChannels()) {
            for(String site: new String[]{LON,NYC,SFO}) {
                Address target=new SiteMaster(site);
                ch.send(target, String.format("%s", ch.getAddress()));
            }
        }

        // each site-master received 6 messages and 3 responses, each non-SM received 3 responses only
        Util.waitUntil(5000, 200, () -> {
            for(JChannel ch: allChannels()) {
                List<Message> list=getReceiver(ch).list();
                int expected_size=isSiteMaster(ch)? 9 : 3;
                if(expected_size != list.size())
                    return false;
                printMessages(ch);
            }
            return true;
        });

        // check the address of the responses: all non site masters must have responses from site masters to them; the
        // site masters' addresses are the actual address of the members acting as site masters
        assert allChannels().stream().filter(ch -> !isSiteMaster(ch)).map(ch -> getReceiver(ch).list())
          .allMatch(l -> l.stream().allMatch(m -> m.dest() != null && m.src() != null));

        // check that we have 6 messages with dest=SiteMaster(S) where S is the current site (only in site masters)
        assert allChannels().stream().filter(RelayTests::isSiteMaster)
          .map(ch -> getReceiver(ch).list())
          .allMatch(l -> l.stream().filter(m -> m.dest() instanceof SiteMaster).count() == 6);
    }

    /** Same as above but with SiteMaster(null) as target */
    public void testSendingToAllSiteMasters(Class<? extends RELAY> cl) throws Exception {
        if(cl.equals(RELAY2.class)) {
            return; // SiteMaster(null) is not supported in RELAY2 (was added in RELAY3)
        }
        createSymmetricNetwork(cl, ch -> new ResponseSender<Message>(ch).rawMsgs(true));
        for(JChannel ch: allChannels()) {
            Address target=new SiteMaster(null);
            ch.send(target, String.format("%s", ch.getAddress()));
        }

        // each site-master received 6 messages and 3 responses, each non-SM received 3 responses only
        Util.waitUntil(5000, 200, () -> {
            for(JChannel ch: allChannels()) {
                List<Message> list=getReceiver(ch).list();
                int expected_size=isSiteMaster(ch)? 9 : 3;
                if(expected_size != list.size())
                    return false;
                printMessages(ch);
            }
            return true;
        });

        // check the address of the responses: all non site masters must have responses from site masters to them; the
        // site masters' addresses are the actual address of the members acting as site masters
        assert allChannels().stream().filter(ch -> !isSiteMaster(ch)).map(ch -> getReceiver(ch).list())
          .allMatch(l -> l.stream().allMatch(m -> m.dest() != null && m.src() != null));

        // check that we have 6 messages with dest=SiteMaster(S) where S is the current site (only in site masters)
        assert allChannels().stream().filter(RelayTests::isSiteMaster)
          .map(ch -> getReceiver(ch).list())
          .allMatch(l -> l.stream().filter(m -> m.dest() instanceof SiteMaster).count() == 6);
    }

    /** Sends a message to all members of the local site only */
    public void testMulticastsToLocalSiteOnly(Class<? extends RELAY> cl) throws Exception {
        createSymmetricNetwork(cl, ch -> new ResponseSender<Message>(ch).rawMsgs(true));
        // these messages won't get forwarded beyond site "LON" as flag NO_RELAY is set
        a.send(new ObjectMessage(null, "from-A").setFlag(Message.Flag.NO_RELAY));
        b.send(new ObjectMessage(null, "from-B").setFlag(Message.Flag.NO_RELAY));

        Util.waitUntil(5000, 200,
                       () -> Stream.of(a,b).peek(RelayTests::printMessages)
                         .map(RelayTests::getReceiver)
                         .allMatch(r -> r.list().size() == 4));

        assert Stream.of(a,b).map(ch -> getReceiver(ch).list())
          .allMatch(l -> l.stream().filter(m -> m.dest() == null).count() == 2 &&
            l.stream().filter(m -> m.dest() != null).count() == 2);

        List<JChannel> l=new ArrayList<>(allChannels());
        l.remove(a); l.remove(b);
        // make sure that the other channels did not receive any messages:
        assert l.stream().map(ch -> getReceiver(ch).list()).allMatch(List::isEmpty);
    }

    /** A local multicast from a non site-master is forwarded to all members of all sites */
    public void localMulticastForwardedToAllSites(Class<? extends RELAY> cl) throws Exception {
        createSymmetricNetwork(cl, ch -> new ResponseSender<Message>(ch).rawMsgs(true));
        b.send(null, "b-req"); // non site-master (A is SM)
        d.send(null, "d-req"); // non site-master (C is SM)

        // all members in all sites should received the 2 multicasts:
        Util.waitUntil(5000, 200, () -> allChannels().stream().peek(RelayTests::printMessages)
          .map(ch -> getReceiver(ch).list())
          .allMatch(l -> l.size() >= 2));

        Util.waitUntil(5000, 200,
                       () -> Stream.of(b,d)
                         .peek(RelayTests::printMessages)
                         .map(ch -> getReceiver(ch).list())
                         .allMatch(l -> l.size() == 2 /* mcasts */ + 6 /* unicast rsps */));
    }

    /** Tests sending of unicasts to different local members, varying between site masters and non site masters */
    public void testLocalUnicasts(Class<? extends RELAY> cl) throws Exception {
        createSymmetricNetwork(cl, ch -> new UnicastResponseSender<>(ch).rawMsgs(true));
        try(JChannel _c=createNode(cl, LON, "C", BRIDGE_CLUSTER, LON, NYC, SFO)){
            Util.waitUntilAllChannelsHaveSameView(5000, 100, a,b,_c);
            _c.setReceiver(new UnicastResponseSender<>(_c).rawMsgs(true));
            // site master to non SM
            a.send(b.getAddress(), new Data(REQ,"from A"));
            assertNumMessages(1, a,b);

            // should bypass RELAY
            a.send(new ObjectMessage(b.getAddress(), new Data(REQ,"from A")).setFlag(Message.Flag.NO_RELAY));
            assertNumMessages(1, a,b);

            // non-SM to SM
            b.send(a.getAddress(), new Data(REQ,"from B"));
            assertNumMessages(1, a,b);

            // bypassing RELAY
            b.send(new ObjectMessage(a.getAddress(), new Data(REQ,"from B")).setFlag(Message.Flag.NO_RELAY));
            assertNumMessages(1, a,b);

            // SM to self
            a.send(a.getAddress(), new Data(REQ,"from self"));
            assertNumMessages(2, a);

            // bypasses RELAY
            a.send(new ObjectMessage(a.getAddress(), new Data(REQ,"from self")).setFlag(Message.Flag.NO_RELAY));
            assertNumMessages(2, a);

            // non-SM to self
            b.send(b.getAddress(), new Data(REQ,"from self"));
            assertNumMessages(2, b);

            // bypasses RELAY
            b.send(new ObjectMessage(b.getAddress(), new Data(REQ,"from self")).setFlag(Message.Flag.NO_RELAY));
            assertNumMessages(2, b);

            // non-SM to non-SM
            b.send(_c.getAddress(), new Data(REQ,"from B"));
            assertNumMessages(1, b,_c);

            // bypasses RELAY
            b.send(new ObjectMessage(_c.getAddress(), new Data(REQ,"from B")).setFlag(Message.Flag.NO_RELAY));
            assertNumMessages(1, b,_c);
        }
    }

    /** Sends unicasts between members of different sites */
    public void testUnicasts(Class<? extends RELAY> cl) throws Exception {
        createSymmetricNetwork(cl, ch -> new UnicastResponseSender<>(ch).rawMsgs(true));
        boolean relay2=cl.equals(RELAY2.class);

        // because an address is not a SiteUUID in RELAY2 (just a regular address), A would not know C
        // we therefore wrap C's address into a SiteUUID (for RELAY2 only)
        Address target=relay2? makeSiteUUID(c.getAddress(), "nyc") : c.getAddress();

        a.send(target, new Data(REQ,"hello from A"));
        assertNumMessages(1, a,c);

        // SM to SM
        target=relay2? makeSiteUUID(a.getAddress(), "lon") : a.getAddress();
        c.send(target, new Data(REQ,"hello from C"));
        assertNumMessages(1,a,c);

        // non-SM to SM
        target=relay2? makeSiteUUID(c.getAddress(), "nyc") : c.getAddress();
        b.send(target, new Data(REQ,"hello from B"));
        assertNumMessages(1, b,c);

        // SM to non-SM
        target=relay2? makeSiteUUID(d.getAddress(), "nyc") : d.getAddress();
        a.send(target, new Data(REQ,"hello from A"));
        assertNumMessages(1, a,d);
    }

    /** Tests https://issues.redhat.com/browse/JGRP-2696 */
    public void testMulticastWithMultipleSiteMasters(Class<? extends RELAY> cl) throws Exception {
        if(cl.equals(RELAY2.class))
            return;
        a=createNode(cl, LON, "A", BRIDGE_CLUSTER, false, LON, NYC, SFO);
        b=createNode(cl, LON, "B", BRIDGE_CLUSTER, false, LON, NYC, SFO);
        c=createNode(cl, LON, "C", BRIDGE_CLUSTER, false, LON, NYC, SFO);

        d=createNode(cl, NYC, "D", BRIDGE_CLUSTER, false, LON, NYC, SFO);
        e=createNode(cl, NYC, "E", BRIDGE_CLUSTER, false, LON, NYC, SFO);
        f=createNode(cl, NYC, "F", BRIDGE_CLUSTER, false, LON, NYC, SFO);

        try(JChannel _g=createNode(cl, SFO, "G", BRIDGE_CLUSTER, false, LON, NYC, SFO);
            JChannel _h=createNode(cl, SFO, "H", BRIDGE_CLUSTER, false, LON, NYC, SFO);
            JChannel _i=createNode(cl, SFO, "I", BRIDGE_CLUSTER, false, LON, NYC, SFO)) {

            Supplier<Stream<JChannel>> generator=() -> Stream.concat(allChannels().stream(), Stream.of(_g, _h, _i));
            generator.get().forEach(RelayTest::changeRELAY);
            for(JChannel ch: Arrays.asList(a,b,c))
                ch.connect(LON);
            Util.waitUntilAllChannelsHaveSameView(5000, 100, a, b, c);

            for(JChannel ch: Arrays.asList(d,e,f))
                ch.connect(NYC);
            Util.waitUntilAllChannelsHaveSameView(5000, 100, d, e, f);

            for(JChannel ch: Arrays.asList(_g,_h,_i))
                ch.connect(SFO);
            Util.waitUntilAllChannelsHaveSameView(5000, 100, _g, _h, _i);

            waitUntilRoute(NYC, true, 5000, 500, a);
            waitUntilRoute(SFO, true, 5000, 500, a);
            waitUntilRoute(LON, true, 5000, 500, d);
            waitUntilRoute(SFO, true, 5000, 500, d);
            waitUntilRoute(LON, true, 5000, 500, _g);
            waitUntilRoute(NYC, true, 5000, 500, _g);

            assert Stream.of(a,b,d,e,_g,_h).map(ch -> ch.getProtocolStack().findProtocol(RELAY.class))
              .allMatch(r -> ((RELAY)r).isSiteMaster());
            assert Stream.of(c,f,_i).map(ch -> ch.getProtocolStack().findProtocol(RELAY.class))
              .noneMatch(r -> ((RELAY)r).isSiteMaster());


            generator.get().forEach(ch -> ch.setReceiver(new MyReceiver<Message>().rawMsgs(true)));

            // A and B (site masters) multicast 1 message each: every receiver should have exactly 2 messages
            a.send(null, "from A");
            b.send(null, "from B");

            Util.waitUntil(5000, 100,
                           () -> generator.get().map(RelayTests::getReceiver).allMatch(r -> r.size() == 2),
                           () -> printMessages(generator.get()));

            System.out.printf("received messages:\n%s\n", printMessages(generator.get()));
            generator.get().forEach(ch -> getReceiver(ch).reset());

            // destination of SiteMaster(null) is only available in RELAY3:
            if(cl.equals(RELAY3.class)) {
                // send to all site masters, but only *one* site master from each site is picked

                a.send(new SiteMaster(null), "from A");
                b.send(new SiteMaster(null), "from B");

                // A sends to itself, plus site masters from NYC (D or E) and SFO (G or H)
                // B sends to itself, plus site masters from NYC (D or E) and SFO (G or H)
                // -> the default SiteMasterPicker impl in RELAY pick a random site master / route; if we disabled
                // this and always picked the first site master / route in the list, only D and G would
                // receive messages (2 each); E and H would receive 0 messages
                Util.waitUntil(3000, 100,
                               () -> Stream.of(a,b).map(RelayTests::getReceiver).allMatch(r -> r.size() == 1));

                // all other site masters (D or E, G or H) get A's and B's message:
                Util.waitUntil(3000, 100,
                               () -> Stream.of(d,e,_g,_h)
                                 // a site master receives 0, 1 or 2 messages:
                                 .map(RelayTests::getReceiver).allMatch(r -> r.size() >= 0 && r.size() <= 2),
                               () -> printMessages(generator.get()));
                System.out.printf("-- received messages:\n%s\n", printMessages(generator.get()));
                generator.get().forEach(ch -> getReceiver(ch).reset());

                c.send(new SiteMaster(null), "from C");
                // same as above: A or B receives 1 message, D or E and G or H
                Util.waitUntil(3000, 100, () -> Stream.of(a,b,d,e,_g,_h).map(RelayTests::getReceiver)
                  .allMatch(r -> r.size() >= 0 && r.size() <= 2), () -> printMessages(generator.get()));
                System.out.printf("-- received messages:\n%s\n", printMessages(generator.get()));
                generator.get().forEach(ch -> getReceiver(ch).reset());
            }

            // C sends a multicast; A *or* B (but not both) should forward it to the other sites NYC and SFO
            c.send(null, "from C");
            Util.waitUntil(3000, 100,
                           () -> generator.get().map(RelayTests::getReceiver).allMatch(r -> r.size() == 1),
                           () -> printMessages(generator.get()));
            System.out.printf("-- received messages:\n%s\n", printMessages(generator.get()));
        }
    }

    protected static String printMessages(Stream<JChannel> s) {
        return s.map(ch -> String.format("%s: %d msgs (%s)", ch.address(), getReceiver(ch).size(),
                                         getReceiver(ch).list(Message::getObject)))
          .collect(Collectors.joining("\n"));
    }

    /** Set max_site_masters to 2 and relay_multicasts (in RELAY2) to true */
    protected static void changeRELAY(JChannel ch) {
        RELAY relay=ch.getProtocolStack().findProtocol(RELAY.class);
        relay.setMaxSiteMasters(2);
        if(relay instanceof RELAY2)
            ((RELAY2)relay).relayMulticasts(true);
    }

    protected static SiteUUID makeSiteUUID(Address addr, String site) {
        String name=NameCache.get(addr);
        return new SiteUUID((UUID)addr, name, site);
    }

    protected static JChannel createNode(Class<? extends RELAY> cl, String site_name, String node_name) throws Exception {
        return createNode(cl, site_name, node_name, 1, null);
    }

    protected static JChannel createNode(Class<? extends RELAY> cl, String site_name, String node_name,
                                         int num_site_masters, String sm_picker) throws Exception {
        RELAY relay=createSymmetricRELAY(cl, site_name, BRIDGE_CLUSTER, LON, SFO)
          .setMaxSiteMasters(num_site_masters).setSiteMasterPickerImpl(sm_picker);
        JChannel ch=new JChannel(defaultStack(relay)).name(node_name);
        if(site_name != null)
            ch.connect(site_name);
        return ch;
    }



    protected void createSymmetricNetwork(Class<? extends RELAY> cl, Function<JChannel,Receiver> r) throws Exception {
        a=createNode(cl, LON, "A", BRIDGE_CLUSTER, LON, NYC, SFO);
        b=createNode(cl, LON, "B", BRIDGE_CLUSTER, LON, NYC, SFO);

        c=createNode(cl, NYC, "C", BRIDGE_CLUSTER, LON, NYC, SFO);
        d=createNode(cl, NYC, "D", BRIDGE_CLUSTER, LON, NYC, SFO);

        e=createNode(cl, SFO, "E", BRIDGE_CLUSTER, LON, NYC, SFO);
        f=createNode(cl, SFO, "F", BRIDGE_CLUSTER, LON, NYC, SFO);

        if(r != null)
            allChannels().forEach(ch -> ch.setReceiver(r.apply(ch)));

        Util.waitUntilAllChannelsHaveSameView(5000, 200, a,b);
        Util.waitUntilAllChannelsHaveSameView(5000, 200, c,d);
        Util.waitUntilAllChannelsHaveSameView(5000, 200, e,f);
    }

    protected List<JChannel> allChannels() {
        return Arrays.asList(a,b,c,d,e,f);
    }


    protected class MyUphandler implements UpHandler {
        protected final BlockingQueue<Message> received=new LinkedBlockingDeque<>();
        protected final AtomicInteger          siteUnreachableEvents=new AtomicInteger(0);

        public BlockingQueue<Message> getReceived()              {return received;}
        public int                    getSiteUnreachableEvents() {return siteUnreachableEvents.get();}
        @Override public UpHandler    setLocalAddress(Address a) {return this;}
        public void                   clear() {received.clear(); siteUnreachableEvents.set(0);}

        @Override
        public Object up(Event evt) {
            if(evt.getType() == Event.SITE_UNREACHABLE) {
                log.debug("Site %s is unreachable", (Object) evt.getArg());
                siteUnreachableEvents.incrementAndGet();
            }
            return null;
        }

        @Override
        public Object up(Message msg) {
            log.debug("Received %s from %s\n", new String(msg.getArray(), StandardCharsets.UTF_8), msg.getSrc());
            received.add(msg);
            return null;
        }
    }




}
