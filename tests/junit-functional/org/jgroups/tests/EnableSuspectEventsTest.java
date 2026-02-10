package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Tests {@link BasicTCP#enableSuspectEvents(boolean)} to reproduce https://issues.redhat.com/browse/JGRP-2968 / https://issues.redhat.com/browse/WFLY-21236
 *
 * Increase the number of iterations {@link EnableSuspectEventsTest#NUM_ITERATIONS} and run with: mvn clean test -Dtest=EnableSuspectEventsTest
 *
 * @author Radoslav Husar
 */
@Test(groups = Global.TIME_SENSITIVE, singleThreaded = true)
public class EnableSuspectEventsTest {

    private static final String  CLUSTER_NAME = EnableSuspectEventsTest.class.getSimpleName();
    private static final int     NUM_NODES = 3;
    private static final int     BASE_PORT = 7800;
    private static final int     NUM_ITERATIONS = 10;
    private static final boolean FORCE_IPV6 = false;

    private final LinkedList<JChannel> channels=new LinkedList<>();

    @BeforeMethod
    protected void setup() throws Exception {
        String hostname = "localhost";

        // IPv6?
        if (FORCE_IPV6) {
            System.setProperty("java.net.preferIPv6Addresses", "true");
            hostname = "::1";
        }

        InetAddress localhost = InetAddress.getByName(hostname);
        List<InetSocketAddress> initialHosts = new ArrayList<>();
        for (int i = 0; i < NUM_NODES; i++) {
            initialHosts.add(new InetSocketAddress(localhost, BASE_PORT + i));
        }

        // Create and connect all channels
        for (int i = 0; i < NUM_NODES; i++) {
            channels.add(createChannel(i, initialHosts, localhost).connect(CLUSTER_NAME));
        }

        // Wait for all channels to have the same view
        Util.waitUntilAllChannelsHaveSameView(10000, 500, channels);

        System.out.printf("-- Initial cluster formed with %d nodes\n", NUM_NODES);
        for (JChannel ch: channels) {
            System.out.printf("   %s: %s, physical address: %s\n",
                    ch.getName(),
                    ch.getView(),
                    ch.down(new Event(Event.GET_PHYSICAL_ADDRESS, ch.getAddress())));
        }
    }

    @AfterMethod
    protected void tearDown() {
        Util.close(channels);
    }

    public void testRepeatedLeaveAndJoin() throws Exception {
        for (int iteration = 0; iteration < NUM_ITERATIONS; iteration++) {
            JChannel coordinator = channels.removeFirst();
            Address coord_addr=coordinator.address();
            if (coordinator == null || !Objects.equals(coordinator.view().getCoord(), coordinator.address())) {
                throw new IllegalStateException("No coordinator found!");
            }

            System.out.printf("\n=== Iteration %d: Coordinator %s leaving ===\n", iteration + 1, coordinator.name());

            // Gracefully disconnect the coordinator simulating graceful node shutdown
            // The bug manifests when TCP connections close after LEAVE, triggering suspect events
            coordinator.disconnect();

            // Check the views on the remaining nodes
            System.out.print("   Checking remaining nodes:\n");
            Util.waitUntil(5000, 100,
                           () -> channels.stream().map(JChannel::view).allMatch(v -> v.size() == 2),
                           () -> String.format("all views must have 2 members: %s", print(channels)));
            System.out.printf("%s\n", print(channels));

            System.out.printf("=== Node %s rejoining ===\n", coord_addr);

            // Reconnect the old coordinator (it was gracefully disconnected)
            coordinator.connect(CLUSTER_NAME);
            channels.add(coordinator);

            // Wait for all nodes to have the same view again (should be 3 nodes)
            Util.waitUntilAllChannelsHaveSameView(10000, 500, channels);
            System.out.printf("after joining:\n%s\n", print(channels));
        }

        System.out.printf("Completed %d iterations without concurrent leaves\n", NUM_ITERATIONS);
    }

    private static String print(List<JChannel> l) {
        return l.stream().map(ch -> String.format("%s: %s", ch.address(), ch.view()))
          .collect(Collectors.joining("\n"));
    }

    /**
     * Stack mimicking WF stack - https://github.com/wildfly/wildfly/blob/39.0.0.Beta1/ee-feature-pack/galleon-shared/src/main/resources/feature_groups/jgroups.xml
     * With the matching defaults - https://github.com/wildfly/wildfly/blob/39.0.0.Beta1/clustering/jgroups/extension/src/main/resources/jgroups-defaults.xml
     */
    private static JChannel createChannel(int index, List<InetSocketAddress> initialHosts, InetAddress bindAddr) throws Exception {
        String name = String.valueOf((char)(index + 'A'));

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

}
