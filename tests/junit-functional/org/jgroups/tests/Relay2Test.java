package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.relay.*;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MyReceiver;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.jgroups.tests.RelayTests.Data.Type.REQ;

/**
 * Various RELAY2-related tests
 * @author Bela Ban
 * @since 3.2
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class Relay2Test extends RelayTests {
    protected JChannel a, b, c;  // members in site "lon"
    protected JChannel d, e, f;  // used for other tests AB CD EF
    protected JChannel x, y, z;  // members in site "sfo

    protected static final String      BRIDGE_CLUSTER = "global";
    protected static final String      SFO            = "sfo", LON="lon", NYC="nyc";



    @AfterMethod protected void destroy() {Util.closeReverse(a,b,c,d,e,f,x,y,z);}

    /**
     * Test that RELAY2 can be added to an already connected channel.
     */
    public void testAddRelay2ToAnAlreadyConnectedChannel() throws Exception {
        // Create and connect a channel.
        a=new JChannel(defaultStack()).name("A").connect(SFO);
        System.out.println("Channel " + a.getName() + " is connected. View: " + a.getView());

        // Add RELAY2 protocol to the already connected channel.
        RELAY2 relayToInject = createSymmetricRELAY2(SFO, BRIDGE_CLUSTER, LON, SFO);

        a.getProtocolStack().insertProtocolAtTop(relayToInject);
        for(Protocol p=relayToInject; p != null; p=p.getDownProtocol())
            p.setAddress(a.getAddress());
        relayToInject.setProtocolStack(a.getProtocolStack());
        relayToInject.init(); // configure();
        relayToInject.handleView(a.getView());

        // Check for RELAY2 presence
        RELAY2 ar=a.getProtocolStack().findProtocol(RELAY2.class);
        assert ar != null;
        Util.waitUntilTrue(3000, 500, () -> getRoute(a, SFO) != null);
        Route route=getRoute(a, SFO);
        assert route == null; // no other site master has joined this bridge cluster, so no route is found
    }
    
    /**
     * Tests that routes are correctly registered after a partition and a subsequent merge
     * (https://issues.redhat.com/browse/JGRP-1524)
     */
    public void testMissingRouteAfterMerge() throws Exception {
        a=createNode(LON, "A");
        b=createNode(LON, "B");
        Util.waitUntilAllChannelsHaveSameView(30000, 1000, a, b);

        x=createNode(SFO, "X");
        assert x.getView().size() == 1;

        RELAY2 ar=a.getProtocolStack().findProtocol(RELAY2.class), xr=x.getProtocolStack().findProtocol(RELAY2.class);
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
    public void testDisconnectAndReconnect() throws Exception {
        a=createNode(LON, "A");
        x=createNode(SFO, "X");

        System.out.println("Started A and X; waiting for bridge view of 2 on A and X");
        waitForBridgeView(2, 20000, 500, BRIDGE_CLUSTER, a, x);

        System.out.println("Disconnecting X; waiting for a bridge view on 1 on A");
        x.disconnect();
        waitForBridgeView(1, 20000, 500, BRIDGE_CLUSTER, a);

        System.out.println("Reconnecting X again; waiting for a bridge view of 2 on A and X");
        x.connect(SFO);
        waitForBridgeView(2, 20000, 500, BRIDGE_CLUSTER, a, x);
    }


    public void testCoordinatorShutdown() throws Exception {
        a=createNode(LON, "A");
        b=createNode(LON, "B");
        x=createNode(SFO, "X");
        y=createNode(SFO, "Y");
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
    public void testUnknownAndUpStateTransitions() throws Exception {
        a=createNode(LON, "A");
        x=createNode(SFO, "X");
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
    public void testSiteUnreachableMessageBreaksSiteUUID() throws Exception {
        a=createNode(LON, "A");
        b=createNode(LON, "B");
        c=createNode(LON, "C");
        x=createNode(SFO, "X");
        waitForBridgeView(2, 10000, 500, BRIDGE_CLUSTER, a, x);

        final MyUphandler up_handler=new MyUphandler();
        b.setUpHandler(up_handler);

        log.debug("Disconnecting X");
        x.disconnect();
        log.debug("A: waiting for site SFO to be UNKNOWN");
        waitUntilRoute(SFO, false, 10000, 500, a);

        for (int j = 0; j < 100; j++)
            b.send(new SiteMaster(SFO), "to-sfo".getBytes(StandardCharsets.UTF_8));

        log.debug("Sending message from A to B");
        for (int j = 0; j < 100; j++)
            a.send(b.getAddress(), ("to-b-" + j).getBytes(StandardCharsets.UTF_8));

        for (int j = 0; j < 100; j++) {
            Message take = up_handler.getReceived().take();
            assert take.src() instanceof SiteUUID : "Address was " + take.src();
        }
        // https://issues.redhat.com/browse/JGRP-2586
        Util.waitUntilTrue(10000, 500, () -> up_handler.getSiteUnreachableEvents() > 0);
        assert up_handler.getSiteUnreachableEvents() > 0 && up_handler.getSiteUnreachableEvents() <= 100
          : "Expecting <= 100 site unreachable events on node B but got " + up_handler.getSiteUnreachableEvents();

        // drain site-unreachable events received after this point
        Util.waitUntilTrue(3000, 500, () -> up_handler.getSiteUnreachableEvents() > 10);

        MyUphandler h2=new MyUphandler();
        b.setUpHandler(h2);
        assert ((RELAY2) a.getProtocolStack().findProtocol(RELAY2.class)).isSiteMaster();
        a.setUpHandler(h2);

        // check if the site master receives the events
        for (int i = 0; i < 100; i++)
            a.send(new SiteMaster(SFO), "to-sfo-from-a".getBytes(StandardCharsets.UTF_8));

        assert h2.getSiteUnreachableEvents() == 100
          : "Expecting 100 site unreachable events on node A but got " + h2.getSiteUnreachableEvents();
    }



    /**
     * Cluster A,B,C in LON and X,Y,Z in SFO. A, B, X and Y are site masters (max_site_masters: 2).
     * Verifies that messages sent by C in the LON site are received in the correct order by all members of the SFO site
     * despite using multiple site masters. JIRA: https://issues.redhat.com/browse/JGRP-2112
     */
    public void testSenderOrderWithMultipleSiteMasters() throws Exception {
        MyReceiver<Object> rx=new MyReceiver<>().rawMsgs(true), ry=new MyReceiver<>().rawMsgs(true),
          rz=new MyReceiver<>().rawMsgs(true);
        final int NUM=512;
        final String sm_picker_impl=SiteMasterPickerImpl.class.getName();
        a=createNode(LON, "A", 2, sm_picker_impl);
        b=createNode(LON, "B", 2, sm_picker_impl);
        c=createNode(LON, "C", 2, sm_picker_impl);
        Util.waitUntilAllChannelsHaveSameView(10000, 1000, a,b,c);

        x=createNode(SFO, "X", 2, sm_picker_impl).setReceiver(rx);
        y=createNode(SFO, "Y", 2, sm_picker_impl).setReceiver(ry);
        z=createNode(SFO, "Z", 2, sm_picker_impl).setReceiver(rz);
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

    public void testForwardingRoute() {
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
    public void testSitesUp() throws Exception {
        a=createNode(LON, "A", BRIDGE_CLUSTER, LON, NYC, SFO);
        b=createNode(LON, "B", BRIDGE_CLUSTER, LON, NYC, SFO);
        c=createNode(LON, "C", BRIDGE_CLUSTER, LON, NYC, SFO);

        d=createNode(NYC, "D", BRIDGE_CLUSTER, LON, NYC, SFO);
        e=createNode(NYC, "E", BRIDGE_CLUSTER, LON, NYC, SFO);
        f=createNode(NYC, "F", BRIDGE_CLUSTER, LON, NYC, SFO);

        try(JChannel _g=createNode(SFO, "G", BRIDGE_CLUSTER, LON, NYC, SFO);
            JChannel _h=createNode(SFO, "H", BRIDGE_CLUSTER, LON, NYC, SFO);
            JChannel _i=createNode(SFO, "I", BRIDGE_CLUSTER, LON, NYC, SFO)) {
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
            assert Stream.of(a,d,_g).map(ch -> (RELAY2)ch.getProtocolStack().findProtocol(RELAY2.class))
              .allMatch(RELAY2::isSiteMaster);

            Stream.of(d,e,f,_g,_h,_i)
              .map(ch -> (RELAY2)ch.getProtocolStack().findProtocol(RELAY2.class))
              .forEach(r -> r.setRouteStatusListener(new MyRouteStatusListener(r.getAddress()).verbose(false)));

            // now stop A; B will become new site master and we should get a site-down(NYC), then site-up(NYC)
            Util.close(a);
            Util.waitUntil(5000, 500, () -> Stream.of(d,e,f,_g,_h,_i)
              .map(ch -> (RELAY2)ch.getProtocolStack().findProtocol(RELAY2.class))
              .peek(r -> System.out.printf("%s: %s\n", r.getAddress(), r.getRouteStatusListener()))
              .map(r -> (MyRouteStatusListener)r.getRouteStatusListener())
              .allMatch(l -> l.down().contains(LON) && l.up().contains(LON)));
        }
    }



    /** Tests sending and receiving of messages across sites */
    public void testSendAndReceiveMulticasts() throws Exception {
        createSymmetricNetwork(ch -> new MyReceiver<Message>().rawMsgs(true));

        // sends multicasts from site-master (_a) and non site masters (_b,_c, ...
        for(JChannel ch: allChannels())
            ch.send(null, String.format("%s", ch.getAddress()));
        Util.waitUntil(5000, 500,
                       () -> allChannels().stream().peek(Relay2Test::printMessages)
                         .map(Relay2Test::getReceiver)
                         .allMatch(r -> r.size() == 6));
    }

    /** Tests sending of multicasts and reception of responses */
    public void testSendAndReceiveMulticastsAndUnicastResponses() throws Exception {
        createSymmetricNetwork(ch -> new ResponseSender<Message>(ch).rawMsgs(true));
        for(JChannel ch: allChannels())
            ch.send(null, String.format("%s", ch.getAddress()));
        Util.waitUntil(5000, 500,
                       () -> allChannels().stream().peek(Relay2Test::printMessages)
                         .map(Relay2Test::getReceiver)
                         .allMatch(r -> r.size() == 6 * 2));

        for(JChannel ch: allChannels()) {
            List<Message> list=getReceiver(ch).list();
            // assert that there are 6 multicasts and 6 unicasts
            assert expectedMulticasts(list,6);
            assert expectedUnicasts(list,6);

            Set<Address> senders=list.stream().map(Message::src).filter(Objects::nonNull)
              .collect(Collectors.toSet());
            assert allChannels().stream().map(JChannel::getAddress).collect(Collectors.toSet()).equals(senders);
        }
    }

    /** Tests sending to the 3 site masters A, C and E, and expecting responses */
    public void testSendingToSiteMasters() throws Exception {
        createSymmetricNetwork(ch -> new ResponseSender<Message>(ch).rawMsgs(true));
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
          .map(ch -> ((MyReceiver<Message>)ch.getReceiver()).list())
          .allMatch(l -> l.stream().filter(m -> m.dest() instanceof SiteMaster).count() == 6);
    }

    /** Same as above but with SiteMaster(null) as target */
    public void testSendingToAllSiteMasters() throws Exception {
        createSymmetricNetwork(ch -> new ResponseSender<Message>(ch).rawMsgs(true));
        for(JChannel ch: allChannels()) {
            Address target=new SiteMaster(null);
            ch.send(target, String.format("%s", ch.getAddress()));
        }

        // each site-master received 6 messages and 3 responses, each non-SM received 3 responses only
        Util.waitUntil(5000, 200, () -> {
            for(JChannel ch: allChannels()) {
                List<Message> list=((MyReceiver<Message>)ch.getReceiver()).list();
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
    public void testMulticastsToLocalSiteOnly() throws Exception {
        createSymmetricNetwork(ch -> new ResponseSender<Message>(ch).rawMsgs(true));
        // these messages won't get forwarded beyond site "LON" as flag NO_RELAY is set
        a.send(new ObjectMessage(null, "from-A").setFlag(Message.Flag.NO_RELAY));
        b.send(new ObjectMessage(null, "from-B").setFlag(Message.Flag.NO_RELAY));

        Util.waitUntil(5000, 200,
                       () -> Stream.of(a,b).peek(Relay2Test::printMessages)
                         .map(Relay2Test::getReceiver)
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
    public void localMulticastForwardedToAllSites() throws Exception {
        createSymmetricNetwork(ch -> new ResponseSender<Message>(ch).rawMsgs(true));
        b.send(null, "b-req"); // non site-master (A is SM)
        d.send(null, "d-req"); // non site-master (C is SM)

        // all members in all sites should received the 2 multicasts:
        Util.waitUntil(5000, 200, () -> allChannels().stream().peek(Relay2Test::printMessages)
          .map(ch -> getReceiver(ch).list())
          .allMatch(l -> l.size() >= 2));

        Util.waitUntil(5000, 200,
                       () -> Stream.of(b,d)
                         .peek(Relay2Test::printMessages)
                         .map(ch -> getReceiver(ch).list())
                         .allMatch(l -> l.size() == 2 /* mcasts */ + 6 /* unicast rsps */));
    }

    /** Tests sending of unicasts to different local members, varying between site masters and non site masters */
    public void testLocalUnicasts() throws Exception {
        createSymmetricNetwork(ch -> new UnicastResponseSender<>(ch).rawMsgs(true));
        try(JChannel _c=createNode(LON, "C", BRIDGE_CLUSTER, LON, NYC, SFO)){
            Util.waitUntilAllChannelsHaveSameView(5000, 100, a,b,_c);
            _c.setReceiver(new UnicastResponseSender<>(_c).rawMsgs(true));
            // site master to non SM
            a.send(b.getAddress(), new Data(REQ,"from A"));
            assertNumMessages(1, a,b);

            // should bypass RELAY2
            a.send(new ObjectMessage(b.getAddress(), new Data(REQ,"from A")).setFlag(Message.Flag.NO_RELAY));
            assertNumMessages(1, a,b);

            // non-SM to SM
            b.send(a.getAddress(), new Data(REQ,"from B"));
            assertNumMessages(1, a,b);

            // bypassing RELAY2
            b.send(new ObjectMessage(a.getAddress(), new Data(REQ,"from B")).setFlag(Message.Flag.NO_RELAY));
            assertNumMessages(1, a,b);

            // SM to self
            a.send(a.getAddress(), new Data(REQ,"from self"));
            assertNumMessages(2, a);

            // bypasses RELAY2
            a.send(new ObjectMessage(a.getAddress(), new Data(REQ,"from self")).setFlag(Message.Flag.NO_RELAY));
            assertNumMessages(2, a);

            // non-SM to self
            b.send(b.getAddress(), new Data(REQ,"from self"));
            assertNumMessages(2, b);

            // bypasses RELAY2
            b.send(new ObjectMessage(b.getAddress(), new Data(REQ,"from self")).setFlag(Message.Flag.NO_RELAY));
            assertNumMessages(2, b);

            // non-SM to non-SM
            b.send(_c.getAddress(), new Data(REQ,"from B"));
            assertNumMessages(1, b,_c);

            // bypasses RELAY2
            b.send(new ObjectMessage(_c.getAddress(), new Data(REQ,"from B")).setFlag(Message.Flag.NO_RELAY));
            assertNumMessages(1, b,_c);
        }
    }

    /** Sends unicasts between members of different sites */
    public void testUnicasts() throws Exception {
        createSymmetricNetwork(ch -> new UnicastResponseSender<>(ch).rawMsgs(true));
        a.send(c.getAddress(), new Data(REQ,"hello from A"));
        assertNumMessages(1, a,c);

        // SM to SM
        c.send(a.getAddress(), new Data(REQ,"hello from C"));
        assertNumMessages(1,a,c);

        // non-SM to SM
        b.send(c.getAddress(), new Data(REQ,"hello from B"));
        assertNumMessages(1, b,c);

        // SM to non-SM
        a.send(d.getAddress(), new Data(REQ,"hello from A"));
        assertNumMessages(1, a,d);
    }

    protected static void assertNumMessages(int expected, JChannel ... channels) throws TimeoutException {
        try {
            Util.waitUntil(5000,100,
                           () -> Stream.of(channels).map(ch -> getReceiver(ch).list()).allMatch(l -> l.size() == expected),
                           () -> msgs(channels));
        }
        finally {
            Stream.of(channels).forEach(ch -> getReceiver(ch).reset());
        }
    }

    protected static MyReceiver<Message> getReceiver(JChannel ch) {
        return (MyReceiver<Message>)ch.getReceiver();
    }

    protected static int receivedMessages(JChannel ch) {
        return getReceiver(ch).list().size();
    }

    protected static boolean expectedUnicasts(List<Message> msgs,int expected) {
        return expectedDests(msgs,m -> m.dest() != null,expected);
    }

    protected static boolean expectedMulticasts(List<Message> msgs,int expected) {
        return expectedDests(msgs,m -> m.dest() == null,expected);
    }

    protected static boolean expectedDests(List<Message> msgs,Predicate<Message> p,int expected) {
        return msgs.stream().filter(p).count() == expected;
    }

    protected static void printMessages(JChannel ... channels) {
        System.out.println(msgs(channels));
    }

    protected static String msgs(JChannel... channels) {
        return Stream.of(channels)
          .map(ch -> String.format("%s: %s",ch.address(),getReceiver(ch).list(Message::getObject)))
          .collect(Collectors.joining("\n"));
    }


    protected static JChannel createNode(String site_name, String node_name) throws Exception {
        return createNode(site_name, node_name, 1, null);
    }

    protected static JChannel createNode(String site_name, String node_name, int num_site_masters,
                                         String sm_picker) throws Exception {
        RELAY2 relay=createSymmetricRELAY2(site_name, BRIDGE_CLUSTER, LON, SFO)
          .setMaxSiteMasters(num_site_masters).setSiteMasterPickerImpl(sm_picker);
        JChannel ch=new JChannel(defaultStack(relay)).name(node_name);
        if(site_name != null)
            ch.connect(site_name);
        return ch;
    }

    protected void createSymmetricNetwork(Function<JChannel,Receiver> r) throws Exception {
        a=createNode(LON, "A", BRIDGE_CLUSTER, LON, NYC, SFO);
        b=createNode(LON, "B", BRIDGE_CLUSTER, LON, NYC, SFO);

        c=createNode(NYC, "C", BRIDGE_CLUSTER, LON, NYC, SFO);
        d=createNode(NYC, "D", BRIDGE_CLUSTER, LON, NYC, SFO);

        e=createNode(SFO, "E", BRIDGE_CLUSTER, LON, NYC, SFO);
        f=createNode(SFO, "F", BRIDGE_CLUSTER, LON, NYC, SFO);

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

        @Override public UpHandler setLocalAddress(Address a) {return this;}

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
