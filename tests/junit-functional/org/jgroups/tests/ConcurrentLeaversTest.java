package org.jgroups.tests;

import java.net.InetAddress;
import java.util.stream.Stream;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.protocols.FD_ALL;
import org.jgroups.protocols.FD_SOCK;
import org.jgroups.protocols.MPING;
import org.jgroups.protocols.TCP;
import org.jgroups.protocols.UNICAST3;
import org.jgroups.protocols.VERIFY_SUSPECT;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

/**
 * Tests gracefully and concurrently leaving coordinator and a successor coordinator.
 * Nodes are leaving gracefully so no merging is expected.
 *
 * @author Radoslav Husar
 */
@Ignore("Reproducer for https://issues.jboss.org/browse/JGRP-2293")
@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class ConcurrentLeaversTest {

    protected static final int NUM = 10;
    protected static final int NUM_LEAVERS = 2;
    protected static final InetAddress LOOPBACK;

    static {
        try {
            LOOPBACK = Util.getLocalhost();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected JChannel[] channels = new JChannel[NUM];

    @BeforeMethod
    protected void setup() throws Exception {
        for (int i = 0; i < channels.length; i++) {
            channels[i] = create(String.valueOf(i + 1)).connect(ConcurrentLeaversTest.class.getSimpleName());
            Util.sleep(i < NUM_LEAVERS ? 2000 : 100);
        }
        Util.waitUntilAllChannelsHaveSameView(10000, 1000, channels);
    }

    @AfterMethod
    protected void destroy() {
        for (int i = channels.length - 2; i >= 0; i--)
            channels[i].close();
    }


    public void testGracefulLeave() throws Exception {
        System.out.println(Util.printViews(channels));
        System.out.println("\n");

        JChannel[] remaining_channels = new JChannel[channels.length - NUM_LEAVERS];
        System.arraycopy(channels, NUM_LEAVERS, remaining_channels, 0, channels.length - NUM_LEAVERS);

        Stream.of(channels).limit(NUM_LEAVERS).parallel().forEach(Util::close);
        Util.waitUntilAllChannelsHaveSameView(30000, 1000, remaining_channels);

        System.out.println(Util.printViews(remaining_channels));
    }

    protected static JChannel create(String name) throws Exception {
        return new JChannel(
                new TCP().setBindAddress(LOOPBACK),
                new MPING(),
                // omit MERGE3 from the stack -- nodes are leaving gracefully
                new FD_SOCK(),
                new FD_ALL(),
                new VERIFY_SUSPECT(),
                new NAKACK2().setUseMcastXmit(false),
                new UNICAST3(),
                new STABLE(),
                new GMS().joinTimeout(1000))
                .name(name);
    }
}
