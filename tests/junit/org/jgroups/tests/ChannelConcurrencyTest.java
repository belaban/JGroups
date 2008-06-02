package org.jgroups.tests;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.Properties;
import java.util.List;
import java.util.LinkedList;
import java.net.InetAddress;


import org.jgroups.JChannel;
import org.jgroups.Channel;
import org.jgroups.Global;
import org.jgroups.util.ResourceManager;
import org.jgroups.util.Util;
import org.jgroups.protocols.*;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.stack.Protocol;
import org.testng.annotations.Test;

/**
 * Tests concurrent startup
 * @author Brian Goose
 * @version $Id: ChannelConcurrencyTest.java,v 1.3 2008/06/02 14:55:35 belaban Exp $
 */
@Test(groups=Global.FLUSH)
public class ChannelConcurrencyTest {


    public static void test() throws Exception {
		final int count = 8;

		final Executor executor = Executors.newFixedThreadPool(count);
		final CountDownLatch latch = new CountDownLatch(count);
		final JChannel[] channels = new JChannel[count];
        JChannel ref=null;

        final long start = System.currentTimeMillis();
		for (int i = 0; i < count; i++) {
            if(ref == null) {
                channels[i] = new JChannel("flush-udp.xml");
                makeUnique(channels[i], count);
                ref=channels[i];
            }
            else {
                channels[i]=new JChannel(ref);
            }
            changeMergeInterval(channels[i]);
        }

		for (int i = 0; i < count; i++) {
			final int me = i;
			executor.execute(new Runnable() {

				public void run() {
					try {
						channels[me].connect("test");
						latch.countDown();
					} catch (final Exception e) {
						e.printStackTrace();
					}
				}
			});
		}

		// Wait for all channels to finish connecting
		latch.await();

		// Wait for all channels to have the correct number of members in their
		// current view
		for (;;) {
			boolean done = true;
			for (final JChannel channel : channels) {
				if (channel.getView().size() < count) {
					done = false;
				}
			}
			if (done) {
				break;
			} else {
				SECONDS.sleep(1);
			}
		}

        for(final Channel ch: channels) {
            System.out.println(ch.getView());
        }

        final long duration = System.currentTimeMillis() - start;
		System.out.println("Converged to a single group after " + duration + " ms");
	}

    private static void changeMergeInterval(JChannel channel) {
        ProtocolStack stack=channel.getProtocolStack();
        MERGE2 merge=(MERGE2)stack.findProtocol(MERGE2.class);
        if(merge != null) {
            merge.setMinInterval(5000);
            merge.setMaxInterval(10000);
        }
    }

    private static void makeUnique(Channel channel, int num) throws Exception {
        ProtocolStack stack=channel.getProtocolStack();
        TP transport=stack.getTransport();
        Properties props=new Properties();
        InetAddress bind_addr=transport.getBindAddressAsInetAddress();
        if(transport instanceof UDP) {
            String mcast_addr=ResourceManager.getNextMulticastAddress();
            short mcast_port=ResourceManager.getNextMulticastPort(bind_addr);
            props.setProperty("mcast_addr", mcast_addr);
            props.setProperty("mcast_port", String.valueOf(mcast_port));
            transport.setPropertiesInternal(props);
            ((UDP)transport).setMulticastAddress(mcast_addr);
            ((UDP)transport).setMulticastPort(mcast_port);
        }
        else if(transport instanceof BasicTCP) {
            List<Short> ports=ResourceManager.getNextTcpPorts(bind_addr, num);
            props.setProperty("bind_port", String.valueOf(ports.get(0)));
            props.setProperty("port_range", String.valueOf(num));
            transport.setPropertiesInternal(props);
            transport.setBindPort(ports.get(0));
            transport.setPortRange(num);

            Protocol ping=stack.findProtocol(TCPPING.class);
            if(ping == null)
                throw new IllegalStateException("TCP stack must consist of TCP:TCPPING - other config are not supported");

            List<String> initial_hosts=new LinkedList<String>();
            for(short port: ports) {
                initial_hosts.add(bind_addr + "[" + port + "]");
            }
            String tmp=Util.printListWithDelimiter(initial_hosts, ",");
            ((TCPPING)ping).setInitialHosts(tmp);
        }
        else {
            throw new IllegalStateException("Only UDP and TCP are supported as transport protocols");
        }
    }
}