package org.jgroups.tests;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.List;
import java.util.LinkedList;
import java.net.InetAddress;


import org.jgroups.JChannel;
import org.jgroups.Channel;
import org.jgroups.Global;
import org.jgroups.util.ResourceManager;
import org.jgroups.util.Util;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.stack.Protocol;
import org.testng.annotations.Test;

/**
 * Tests concurrent startup
 * @author Brian Goose
 * @version $Id: ChannelConcurrencyTest.java,v 1.8 2008/06/06 14:51:21 vlada Exp $
 */
@Test(groups=Global.FLUSH)
public class ChannelConcurrencyTest  extends ChannelTestBase{


    public void test() throws Throwable {
        final int count=8;

        final Executor executor=Executors.newFixedThreadPool(count);
        final CountDownLatch latch=new CountDownLatch(count);
        final JChannel[] channels=new JChannel[count];
        final Task[] tasks=new Task[count];

        JChannel ref = null;
        final long start=System.currentTimeMillis();
        for(int i=0;i < count;i++) {
            if(ref == null) {
                channels[i]=new JChannel("flush-udp.xml");
                makeUnique(channels[i], count);
                ref=channels[i];
            }
            else {
                channels[i]=new JChannel(ref);
            }
            tasks[i]=new Task(latch, channels[i]);
            changeMergeInterval(channels[i]);
            changeViewBundling(channels[i]);
        }

        for(final Task t:tasks) {
            executor.execute(t);
        }

        try {
            // Wait for all channels to finish connecting
            latch.await();

            for(Task t:tasks) {
                Throwable ex=t.getException();
                if(ex != null)
                    throw ex;
            }

            // Wait for all channels to have the correct number of members in their
            // current view
            boolean converged=false;
            for(int timeoutToConverge=120,counter=0;counter < timeoutToConverge && !converged;SECONDS.sleep(1),counter++) {
                for(final JChannel channel:channels) {
                    converged = channel.getView().size() == count;
                    if(!converged)
                        break;
                }                
            }

            final long duration=System.currentTimeMillis() - start;
            System.out.println("Converged to a single group after " + duration + " ms; group is:\n");
            for(int i=0;i < channels.length;i++) {
                System.out.println("#" + (i + 1) + ": " + channels[i].getLocalAddress() + ": " + channels[i].getView());
            }

            for(final JChannel channel:channels) {
                assertEquals("View ok for channel " + channel.getLocalAddress(), count, channel.getView().size());
            }
        }
        finally {
            System.out.print("closing channels: ");
            for(int i=channels.length -1; i>= 0; i--) {
                Channel channel=channels[i];
                channel.close();
                Util.sleep(250);
                int tries=0;
                while((channel.isConnected() || channel.isOpen()) && tries++ < 10) {
                    Util.sleep(1000);
                }
            }
            System.out.println("OK");

            for(final JChannel channel: channels) {
                assertFalse("Channel connected", channel.isConnected());
            }
        }
    }

    private static void changeViewBundling(JChannel channel) {
        ProtocolStack stack=channel.getProtocolStack();
        GMS gms=(GMS)stack.findProtocol(GMS.class);
        if(gms != null) {
            gms.setViewBundling(true);
            gms.setMaxBundlingTime(500);
        }
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
        InetAddress bind_addr=transport.getBindAddressAsInetAddress();
        if(transport instanceof UDP) {
            String mcast_addr=ResourceManager.getNextMulticastAddress();
            short mcast_port=ResourceManager.getNextMulticastPort(bind_addr);
            ((UDP)transport).setMulticastAddress(mcast_addr);
            ((UDP)transport).setMulticastPort(mcast_port);
        }
        else if(transport instanceof BasicTCP) {
            List<Short> ports=ResourceManager.getNextTcpPorts(bind_addr, num);
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
    
    private static class Task implements Runnable {
        private final Channel c;
        private final CountDownLatch latch;
        private Throwable exception=null;


        public Task(CountDownLatch latch, Channel c) {
            this.latch=latch;
            this.c=c;
        }

        public Throwable getException() {
            return exception;
        }

        public void run() {
            try {
                c.connect("test");
            }
            catch(final Exception e) {
                exception=e;
                e.printStackTrace();
            }
            finally {
                latch.countDown();
            }
        }
    }
}