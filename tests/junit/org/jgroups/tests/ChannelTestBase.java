package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.PhysicalAddress;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.BasicTCP;
import org.jgroups.protocols.TCPPING;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.UDP;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.ResourceManager;
import org.jgroups.util.Util;
import org.testng.annotations.*;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Bela Ban
 * @author Vladimir Blagojevic
 * @author <a href="mailto://brian.stansberry@jboss.com">Brian Stansberry</a>
 */
@Test(groups = "base", singleThreaded = true)
public class ChannelTestBase {
    protected String      channel_conf = "udp.xml";
    protected InetAddress bind_addr;
    protected Log         log;

    @BeforeClass
    @Parameters(value = { "channel.conf"})
    protected void initializeBase(@Optional("udp.xml") String chconf) throws Exception {
        log=LogFactory.getLog(this.getClass());
        Test annotation = this.getClass().getAnnotation(Test.class);
        // this should never ever happen!
        if (annotation == null)
            throw new Exception("Test is not marked with @Test annotation");

        bind_addr=Util.getLoopback();
        this.channel_conf = chconf;
    }


    @AfterClass(alwaysRun = true)
    protected void nullifyInstanceFields() {
        for (Class<?> current = this.getClass(); current.getSuperclass() != null; current = current.getSuperclass()) {
            Field[] fields = current.getDeclaredFields();
            for (Field f : fields) {
                try {
                    if (!Modifier.isStatic(f.getModifiers()) && !f.getDeclaringClass().isPrimitive()) {
                        f.setAccessible(true);
                        f.set(this, null);
                    }
                } catch (Exception e) {
                }
            }
        }
    }


    protected JChannel createChannel() throws Exception {
        JChannel ch=new JChannel(channel_conf);
        ch.getProtocolStack().getTransport().setBindAddress(bind_addr);
        return ch;
    }

    protected void makeUnique(List<JChannel> channels) throws Exception {
        JChannel[] list=new JChannel[channels.size()];
        for(int i=0; i < list.length; i++)
            list[i]=channels.get(i);
        makeUnique(list);
    }

    protected void makeUnique(JChannel ... channels) throws Exception {
        String mcast_addr=Util.getProperty(new String[]{ Global.UDP_MCAST_ADDR, "jboss.partition.udpGroup" },
                                           null, "mcast_addr", null);
        boolean is_udp=Stream.of(channels).anyMatch(ch -> ch.getProtocolStack().getTransport() instanceof UDP),
          is_tcp=Stream.of(channels).anyMatch(ch -> ch.getProtocolStack().getTransport() instanceof BasicTCP);

        if(is_udp) {
            InetAddress mcast=mcast_addr != null? InetAddress.getByName(mcast_addr)
              : InetAddress.getByName(ResourceManager.getNextMulticastAddress());
            int mcast_port=ResourceManager.getNextMulticastPort(bind_addr);
            for(JChannel ch: channels) {
                UDP udp=(UDP)ch.getProtocolStack().getTransport();
                udp.setBindAddress(bind_addr);
                udp.setMulticastAddress(mcast).setMulticastPort(mcast_port);
            }
        }
        else if(is_tcp) {
            List<Integer> ports = ResourceManager.getNextTcpPorts(bind_addr, channels.length);
            Collection<InetSocketAddress> hosts=ports.stream()
              .map(p -> new InetSocketAddress(bind_addr, p)).collect(Collectors.toList());
            for(int i=0; i < channels.length; i++) {
                ProtocolStack stack=channels[i].getProtocolStack();
                TP tp=stack.getTransport();
                tp.setBindPort(ports.get(i));
                TCPPING ping=stack.findProtocol(TCPPING.class);
                if(ping == null)
                    throw new IllegalStateException("TCP stack must consist of TCP:TCPPING - other configs are not supported");

                ping.setInitialHosts(hosts);
            }
        }
        else
            throw new IllegalStateException("Only UDP and TCP are supported as transport protocols");
    }

    /** Get the bind_addr and bind_port or all channels and create an initial_hosts, to be set in TCPPING */
    protected static void sameInitialHosts(JChannel... channels) {
        List<PhysicalAddress> addrs=Stream.of(channels).map(ch -> ch.getProtocolStack().getTransport())
          .map(TP::localPhysicalAddress).collect(Collectors.toList());

        for(JChannel ch: channels) {
            TCPPING tcpping=ch.getProtocolStack().findProtocol(TCPPING.class);
            if(tcpping != null)
                tcpping.setInitialHosts2(addrs);
        }
    }


}
