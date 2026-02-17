package org.jgroups.tests.byteman;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.jboss.byteman.contrib.bmunit.BMNGRunner;
import org.jboss.byteman.contrib.bmunit.BMScript;
import org.jboss.byteman.rule.helper.Helper;
import org.jgroups.Address;
import org.jgroups.BytesMessage;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Receiver;
import org.jgroups.View;
import org.jgroups.protocols.FRAG4;
import org.jgroups.protocols.NAKACK4;
import org.jgroups.protocols.RED;
import org.jgroups.protocols.TCP;
import org.jgroups.protocols.TCPPING;
import org.jgroups.protocols.UNICAST4;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * Tests checks there is not dangling connection during close
 */
@Test(groups = Global.BYTEMAN, singleThreaded = true)
public class DanglingConnectionTest extends BMNGRunner {
    private static final byte[] MESSAGE = { 'd', 'u', 'm', 'b' };
    private static final String CLUSTER_NAME = DanglingConnectionTest.class.getSimpleName();
    private static final String LOCALHOST = "localhost";
    private static final int NUM_NODES = 2;
    private static final int BASE_PORT = 7800;

    @DataProvider(name = "parameter")
    public Object[] createSetup() {
        return new Object[] { Boolean.TRUE, Boolean.FALSE };
    }

    @BeforeMethod
    public void setup() {
        Helper.activated();
    }

    @AfterMethod
    public void tearDown() {
        Helper.deactivated();
    }

    @Test(dataProvider = "parameter")
    @BMScript(dir = "scripts/DanglingConnectionTest", value = "testDanglingConnectionTest_DanglingAfterClose")
    public void testDanglingConnectionTest_DanglingAfterClose(Boolean enableSuspects) throws Throwable {
        JChannel[] nodes = null;
        try {
            nodes = setup(NUM_NODES, enableSuspects);
            TCP tcp[] = new TCP[NUM_NODES];
            for (int i = 0; i < NUM_NODES; i++) {
                tcp[i] = nodes[i].getProtocolStack().findProtocol(TCP.class);
            }

            nodes[1].send(new BytesMessage(nodes[0].getAddress(), MESSAGE));
            Thread.sleep(200L); // give time to reach the rendevouz
            nodes[1].disconnect();

            Thread.sleep(200L);  // give time to reach the disconnection
            assertEquals(tcp[0].getOpenConnections(), 0);
            assertEquals(tcp[1].getOpenConnections(), 0);

            nodes[1].connect(CLUSTER_NAME);
            Util.waitUntilAllChannelsHaveSameView(1000L, 100, nodes);
        } finally {
            if (nodes != null) {
                Util.close(nodes);
            }
        }
    }

    @Test(dataProvider = "parameter")
    @BMScript(dir = "scripts/DanglingConnectionTest", value = "testDanglingConnectionTest_DanglingAfterClose")
    public void testDanglingConnectionTest_DanglingAfterCloseViewCheck(Boolean enableSuspects) throws Throwable {
        JChannel[] nodes = null;
        try {
            nodes = setup(NUM_NODES, enableSuspects);
            TCP tcp[] = new TCP[NUM_NODES];
            for (int i = 0; i < NUM_NODES; i++) {
                tcp[i] = nodes[i].getProtocolStack().findProtocol(TCP.class);
            }

            nodes[1].send(new BytesMessage(nodes[0].getAddress(), MESSAGE));
            Thread.sleep(200L); // give time to reach the rendevouz
            nodes[1].disconnect();

            Thread.sleep(200L);  // give time to reach the disconnection

            nodes[1].connect(CLUSTER_NAME);
            Util.waitUntilAllChannelsHaveSameView(1000L, 100, nodes);
        } finally {
            if (nodes != null) {
                Util.close(nodes);
            }
        }
    }

    protected JChannel[] setup(int numNodes, Boolean enableSuspects) throws Exception {
        JChannel[] nodes = new JChannel[numNodes];
        InetAddress localhost = InetAddress.getByName(LOCALHOST);
        List<InetSocketAddress> initialHosts = new ArrayList<>();
        for (int i = 0; i < numNodes; i++) {
            initialHosts.add(new InetSocketAddress(localhost, BASE_PORT + i));
        }
        for (int i = 0; i < numNodes; i++) {
            nodes[i] = createChannel(i, initialHosts, localhost, enableSuspects);
            nodes[i].connect(CLUSTER_NAME);
            Address local_addr = nodes[i].address();
            nodes[i].setReceiver(new Receiver() {
                @Override
                public void viewAccepted(View new_view) {
                    System.out.println(local_addr + " view accepted: " + new_view);
                }
            });

        }

        Util.waitUntilAllChannelsHaveSameView(1000L, 100, nodes);
        return nodes;
    }

    private static JChannel createChannel(int index, List<InetSocketAddress> initialHosts, InetAddress bindAddr, Boolean enableSuspects)
            throws Exception {
        String name = "node-" + index;

        TCP tcp = new TCP();
        tcp.setBindAddress(bindAddr);
        tcp.setPortRange(0);
        tcp.setBindPort(BASE_PORT + index);
        tcp.setSendBufSize(640000);
        tcp.setSockConnTimeout(300);
        tcp.enableSuspectEvents(enableSuspects);
        tcp.setLevel("debug");

        RED red = new RED();

        TCPPING tcpping = new TCPPING().setInitialHosts(initialHosts).setPortRange(0).numDiscoveryRuns(3);

        NAKACK4 nakack4 = (NAKACK4) new NAKACK4().setXmitInterval(100); // 100ms

        UNICAST4 unicast4 = (UNICAST4) new UNICAST4().setXmitInterval(100); // 100ms

        GMS gms = new GMS().printLocalAddress(false);

        FRAG4 frag4 = new FRAG4().setFragSize(60000); // 60k

        return new JChannel(tcp, red, tcpping, nakack4, unicast4, gms, frag4).name(name);
    }

}