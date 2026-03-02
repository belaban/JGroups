package org.jgroups.tests.byteman;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.byteman.contrib.bmunit.BMNGRunner;
import org.jboss.byteman.contrib.bmunit.BMScript;
import org.jboss.byteman.rule.helper.Helper;
import org.jgroups.ChannelListener;
import org.jgroups.Global;
import org.jgroups.JChannel;
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

@Test(groups = Global.BYTEMAN, singleThreaded = true)
public class IncomingOutgoingConnectionLockTest extends BMNGRunner {
    private static final String CLUSTER_NAME = IncomingOutgoingConnectionLockTest.class.getName();
    private static final int NUM_NODES = 2;
    private static final int BASE_PORT = 7800;

    private final LinkedList<JChannel> channels = new LinkedList<>();

    @BeforeMethod
    protected void setup() throws Exception {
        Helper.activated();
        channels.clear();

        String hostname = "localhost";

        InetAddress localhost = InetAddress.getByName(hostname);
        List<InetSocketAddress> initialHosts = new ArrayList<>();
        for (int i = 0; i < NUM_NODES; i++) {
            initialHosts.add(new InetSocketAddress(localhost, BASE_PORT + i));
        }

        // Create and connect all channels
        for (int i = 0; i < NUM_NODES; i++) {
            channels.add(createChannel(i, initialHosts, localhost).connect(CLUSTER_NAME));
        }
        Util.waitUntilAllChannelsHaveSameView(10000, 500, channels);
    }

    @AfterMethod
    protected void tearDown() {
        channels.forEach(Util::close);
        Helper.deactivated();
    }

    @Test
    @BMScript(dir = "scripts/IncomingOutgoingConnectionLockTest", value = "testBasicInterlock")
    public void testBasicInterlock() throws Exception {
        logicTest("testBasicInterlock");
    }

    private void logicTest(String testName) throws Exception {
        System.out.println("---------------------------------8< " + testName + " --------------------------------");
        JChannel coordinator = channels.get(0);
        coordinator.disconnect();
        // wait for view of one channel
        Util.waitUntilAllChannelsHaveSameView(10000, 500, channels.get(1));
        System.out.printf("reconnect to the cluster %s\n", JChannel.getVersion());
        CountChannelListener counter = new CountChannelListener();
        coordinator.addChannelListener(counter);
        coordinator.connect(CLUSTER_NAME);
        Util.waitUntilAllChannelsHaveSameView(10000, 500, channels);
        assert counter.count() == 1;
        assert coordinator.getProtocolStack().<TCP>findProtocol(TCP.class).getOpenConnections() == 1;
    }

    private class CountChannelListener implements ChannelListener {
        AtomicInteger counter;

        public CountChannelListener() {
            counter = new AtomicInteger(0);
        }

        @Override
        public void channelConnected(JChannel channel) {
            System.out.printf("channel event %d connected %s \n", counter.incrementAndGet(), channel);
        }

        @Override
        public void channelDisconnected(JChannel channel) {
            System.out.printf("channel event %d disconnected %s \n", counter.incrementAndGet(), channel);
        }

        public int count() {
            return counter.intValue();
        }
    }

    private static JChannel createChannel(int index, List<InetSocketAddress> initialHosts, InetAddress bindAddr)
            throws Exception {
        String name = String.valueOf((char) (index + 'A'));

        TCP tcp = new TCP();
        tcp.setBindAddress(bindAddr); // Use IPv6 address
        tcp.setPortRange(0);
        tcp.setBindPort(BASE_PORT + index);
        tcp.setSendBufSize(640000); // 640k
        tcp.setSockConnTimeout(300); // 300ms
        tcp.enableSuspectEvents(true); // Critical: enable suspect events on connection close!
        tcp.setLevel("debug");

        RED red = new RED();

        TCPPING tcpping = new TCPPING().setInitialHosts(initialHosts).setPortRange(0).numDiscoveryRuns(3);

        MERGE3 merge3 = new MERGE3().setMinInterval(10000) // 10s
                .setMaxInterval(30000); // 30s

        FD_ALL3 fdAll3 = new FD_ALL3().setInterval(15000) // 15s
                .setTimeout(60000); // 60s

        VERIFY_SUSPECT2 verifySuspect2 = new VERIFY_SUSPECT2().setTimeout(1000); // 1s

        NAKACK4 nakack4 = (NAKACK4) new NAKACK4().setXmitInterval(100); // 100ms

        UNICAST4 unicast4 = (UNICAST4) new UNICAST4().setXmitInterval(100); // 100ms

        GMS gms = new GMS().printLocalAddress(false);

        FRAG4 frag4 = new FRAG4().setFragSize(60000); // 60k

        // Without VERIFY_SUSPECT2, suspect events will immediately cause removal
        return new JChannel(tcp, red, tcpping, merge3, fdAll3, verifySuspect2, nakack4, unicast4, gms, frag4).name(name);
    }
}
