package org.jgroups.protocols;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;

/**
 * @author Bela Ban
 * @since  4.0.3
 */
@Test(groups=Global.FUNCTIONAL)
public class TCPPING_Test {
    protected JChannel ch;

    @AfterMethod protected void destroy() {Util.close(ch);}

    /** Tests https://issues.jboss.org/browse/JGRP-2168 */
    public void testSettingInitialHostsProgrammatically() throws Exception {
        TCP transport=new TCP();
        transport.setBindAddress(InetAddress.getLoopbackAddress());
        transport.setBindPort(9600);
        TCPPING ping=new TCPPING();
        TCPGOSSIP gossip=new TCPGOSSIP();
        List<InetSocketAddress> gossip_router=Collections.singletonList(new InetSocketAddress(InetAddress.getLoopbackAddress(), 12000));
        gossip.setInitialHosts(gossip_router);
        ping.setInitialHosts2(Collections.singletonList(new IpAddress(transport.getBindAddress(), transport.getBindPort())));
        ch=new JChannel(transport, ping, gossip);
        assert !ping.getInitialHosts().isEmpty() : "No initial hosts!";
        assert !gossip.getInitialHosts().isEmpty(): "no initial hosts!";
    }
}
