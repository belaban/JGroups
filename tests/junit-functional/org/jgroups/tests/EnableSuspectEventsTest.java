package org.jgroups.tests;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.jgroups.Event;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Receiver;
import org.jgroups.View;
import org.jgroups.protocols.BasicTCP;
import org.jgroups.protocols.FD_ALL3;
import org.jgroups.protocols.FRAG4;
import org.jgroups.protocols.MERGE3;
import org.jgroups.protocols.NAKACK4;
import org.jgroups.protocols.RED;
import org.jgroups.protocols.TCP;
import org.jgroups.protocols.TCPPING;
import org.jgroups.protocols.UNICAST4;
import org.jgroups.protocols.VERIFY_SUSPECT2;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;



/**
 * Tests {@link BasicTCP#enableSuspectEvents(boolean)} to reproduce https://issues.redhat.com/browse/JGRP-2968 / https://issues.redhat.com/browse/WFLY-21236
 *
 * Increase the number of iterations {@link EnableSuspectEventsTest#NUM_ITERATIONS} and run with: mvn clean test -Dtest=EnableSuspectEventsTest
 *
 * @author Radoslav Husar
 */
@Test(groups = Global.TIME_SENSITIVE, singleThreaded = true)
public class EnableSuspectEventsTest {

    private static final String CLUSTER_NAME = EnableSuspectEventsTest.class.getSimpleName();
    private static final int NUM_NODES = 3;
    private static final int BASE_PORT = 7800;
    private static final int NUM_ITERATIONS = 10;
    private static final boolean FORCE_IPV6 = false;

    private JChannel[] channels;
    private TopologyMonitor[] monitors;

    @BeforeMethod
    protected void setup() throws Exception {
        String hostname = "localhost";

        // IPv6?
        if (FORCE_IPV6) {
            System.setProperty("java.net.preferIPv6Addresses", "true");
            hostname = "::1";
        }

        channels = new JChannel[NUM_NODES];
        monitors = new TopologyMonitor[NUM_NODES];

        InetAddress localhost = InetAddress.getByName(hostname);
        List<InetSocketAddress> initialHosts = new ArrayList<>();
        for (int i = 0; i < NUM_NODES; i++) {
            initialHosts.add(new InetSocketAddress(localhost, BASE_PORT + i));

        }

        // Create all channels
        for (int i = 0; i < NUM_NODES; i++) {
            channels[i] = createChannel(i, initialHosts, localhost);
            monitors[i] = new TopologyMonitor(channels[i].getName());
            channels[i].setReceiver(monitors[i]);
        }

        // Connect all channels
        for (int i = 0; i < NUM_NODES; i++) {
            channels[i].connect(CLUSTER_NAME);
        }

        // Wait for all channels to have the same view
        Util.waitUntilAllChannelsHaveSameView(10000, 500, channels);

        System.out.printf("-- Initial cluster formed with %d nodes (IPv6)\n", NUM_NODES);
        for (int i = 0; i < NUM_NODES; i++) {
            System.out.printf("   %s: %s, physical address: %s\n",
                    channels[i].getName(),
                    channels[i].getView(),
                    channels[i].down(new Event(Event.GET_PHYSICAL_ADDRESS, channels[i].getAddress())));
        }
    }

    @AfterMethod
    protected void tearDown() {
        Util.closeFast(channels);
    }

    @Test
    public void testEnableSuspects() throws Exception {
        for (int iteration = 0; iteration < NUM_ITERATIONS; iteration++) {
            JChannel coordinator = null;
            int coordIndex = -1;
            for (int i = 0; i < channels.length; i++) {
                if (channels[i] != null && channels[i].isConnected() &&
                        channels[i].getView() != null &&
                        channels[i].getAddress().equals(channels[i].getView().getCoord())) {
                    coordinator = channels[i];
                    coordIndex = i;
                    break;
                }
            }

            if (coordinator == null) {
                throw new IllegalStateException("No coordinator found!");
            }

            System.out.printf("\n=== Iteration %d: Coordinator %s leaving ===\n", iteration + 1, coordinator.getName());
            // Build list of stable channels (all except the coordinator)
            List<JChannel> stableChannelsList = new ArrayList<>();
            for (int i = 0; i < channels.length; i++) {
                if (i != coordIndex && channels[i] != null && channels[i].isConnected()) {
                    stableChannelsList.add(channels[i]);
                }
            }

            JChannel[] stableChannels = stableChannelsList.toArray(JChannel[]::new);

            // Reset monitoring state
            for (TopologyMonitor monitor : monitors) {
                if (monitor != null) {
                    monitor.reset();
                }
            }

            // The bug manifests when TCP connections close after LEAVE, triggering suspect events
            coordinator.disconnect();

            // Check the views on the remaining nodes
            Util.waitUntilAllChannelsHaveSameView(10000, 100, stableChannels);

            // Check the views on the remaining nodes
            System.out.printf("   Checking remaining nodes:\n");
            boolean foundBug = false;
            for (JChannel ch : stableChannels) {
                View view = ch.getView();
                System.out.printf("     %s: %s\n", ch.getName(), view);

                if (view.size() == 1) {
                    System.out.printf("   *** BUG REPRODUCED! Node %s formed singleton cluster!\n",
                            ch.getName());
                    foundBug = true;
                }
            }

            if (!foundBug) {
                // Verify all stable nodes have view of size NUM_NODES-1 (stayed together)
                for (JChannel ch : stableChannels) {
                    View view = ch.getView();
                    assert view.size() == NUM_NODES - 1 : String.format(
                            "FAILURE: Node %s should have view size 2, but got %d in view %s",
                            ch.getName(), view.size(), view);
                }

                // Verify they all have the same view
                Util.waitUntilAllChannelsHaveSameView(10000, 500, stableChannels);
            } else {
                throw new AssertionError("BUG REPRODUCED: Remaining nodes formed singleton clusters!");
            }

            System.out.printf("=== Node %d rejoining ===\n", coordIndex);

            // Reconnect the old coordinator (it was gracefully disconnected)
            coordinator.connect(CLUSTER_NAME);

            // Wait for all nodes to have the same view again (should be 3 nodes)
            Util.waitUntilAllChannelsHaveSameView(10000, 500, channels);
        }

        System.out.printf("Completed %d iterations without concurrent leaves\n", NUM_ITERATIONS);
    }

    /**
     * Stack mimicking WF stack - https://github.com/wildfly/wildfly/blob/39.0.0.Beta1/ee-feature-pack/galleon-shared/src/main/resources/feature_groups/jgroups.xml
     * With the matching defaults - https://github.com/wildfly/wildfly/blob/39.0.0.Beta1/clustering/jgroups/extension/src/main/resources/jgroups-defaults.xml
     */

    private static JChannel createChannel(int index, List<InetSocketAddress> initialHosts, InetAddress bindAddr) throws Exception {

        String name = "node-" + index;

        TCP tcp = new TCP();
        tcp.setBindAddress(bindAddr);  // Use IPv6 address
        tcp.setPortRange(0);
        tcp.setBindPort(BASE_PORT + index);
        tcp.setSendBufSize(640000);  // 640k
        tcp.setSockConnTimeout(300);  // 300ms
        tcp.enableSuspectEvents(true); // Critical: enable suspect events on connection close!
        tcp.setLevel("debug");


        RED red = new RED();

        TCPPING tcpping = new TCPPING()
                .setInitialHosts(initialHosts)
                .setPortRange(0)
                .numDiscoveryRuns(3);


        MERGE3 merge3 = new MERGE3()
                .setMinInterval(10000)  // 10s
                .setMaxInterval(30000);  // 30s

        FD_ALL3 fdAll3 = new FD_ALL3()
                .setInterval(15000)  // 15s
                .setTimeout(60000);  // 60s

        VERIFY_SUSPECT2 verifySuspect2 = new VERIFY_SUSPECT2()
                .setTimeout(1000);  // 1s

        NAKACK4 nakack4 = (NAKACK4) new NAKACK4()
                .setXmitInterval(100);  // 100ms

        UNICAST4 unicast4 = (UNICAST4) new UNICAST4()
                .setXmitInterval(100);  // 100ms

        GMS gms = new GMS()
                .printLocalAddress(false);

        FRAG4 frag4 = new FRAG4()
                .setFragSize(60000);  // 60k

        // Without VERIFY_SUSPECT2, suspect events will immediately cause removal
        return new JChannel(tcp, red, tcpping, merge3, fdAll3, verifySuspect2, nakack4, unicast4, gms, frag4).name(name);
    }

    /**
     * Monitors topology changes on a channel
     */
    private static class TopologyMonitor implements Receiver {
        private final String channelName;

        private final List<ViewChange> viewChanges = new CopyOnWriteArrayList<>();

        private View previousView;

        public TopologyMonitor(String channelName) {
            this.channelName = channelName;
        }

        @Override
        public void viewAccepted(View newView) {
            ViewChange change = new ViewChange(previousView, newView);
            System.out.printf("[%s] View changed: %s -> %s (delta=%d)\n",
                    channelName,
                    change.oldView != null ? change.oldView.size() : "null",
                    change.newView.size(),
                    change.oldView != null ? (change.oldView.size() - change.newView.size()) : 0);
            previousView = newView;
        }


        public void reset() {
            viewChanges.clear();
        }

        private record ViewChange(View oldView, View newView) {
        }
    }

}

