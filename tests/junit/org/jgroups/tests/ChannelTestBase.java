package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.BasicTCP;
import org.jgroups.protocols.TCPPING;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.UDP;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.ResourceManager;
import org.jgroups.util.StackType;
import org.jgroups.util.Util;
import org.testng.annotations.*;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

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

        StackType type=Util.getIpStackType();
        bind_addr=InetAddress.getByName(type == StackType.IPv6 ? "::1" : "127.0.0.1");
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


    protected final static void assertTrue(boolean condition) {
        Util.assertTrue(condition);
    }

    protected final static void assertTrue(String message, boolean condition) {
        Util.assertTrue(message,condition);
    }

    protected final static void assertFalse(boolean condition) {
        Util.assertFalse(condition);
    }

    protected final static void assertFalse(String message, boolean condition) {
        Util.assertFalse(message,condition);
    }

    protected final static void assertEquals(String message, Object val1, Object val2) {
        Util.assertEquals(message,val1,val2);
    }

    protected final static void assertEquals(Object val1, Object val2) {
        Util.assertEquals(null,val1,val2);
    }

    protected final static void assertNotNull(String message, Object val) {
        Util.assertNotNull(message,val);
    }

    protected final static void assertNotNull(Object val) {
        Util.assertNotNull(null,val);
    }

    protected final static void assertNull(String message, Object val) {
        Util.assertNull(message,val);
    }

    protected final static void assertNull(Object val) {
        Util.assertNull(null, val);
    }

    /**
     * Creates a channel and modifies the configuration such that no other channel will able to join
     * this one even if they have the same cluster name (if unique = true). This is done by
     * modifying mcast_addr and mcast_port with UDP, and by changing TCP.start_port, TCP.port_range
     * and TCPPING.initial_hosts with TCP. Mainly used to run TestNG tests concurrently.
     * 
     * @param num The number of channels we will create. Only important (for port_range) with TCP, ignored by UDP
     */
    protected JChannel createChannel(boolean unique, int num) throws Exception {
        return new DefaultChannelTestFactory().createChannel(unique, num);
    }

    protected JChannel createChannel(boolean unique, int num, String name) throws Exception {
        return new DefaultChannelTestFactory().createChannel(unique, num).name(name);
    }

    protected JChannel createChannel() throws Exception {
        return new DefaultChannelTestFactory().createChannel();
    }

    protected JChannel createChannel(boolean unique) throws Exception {
        return createChannel(unique,2);
    }

    protected JChannel createChannel(JChannel ch) throws Exception {
        return new DefaultChannelTestFactory().createChannel(ch);
    }

    protected JChannel createChannel(JChannel ch, String name) throws Exception {
        return new DefaultChannelTestFactory().createChannel(ch).name(name);
    }

    protected static String getUniqueClusterName() {
        return getUniqueClusterName(null);
    }

    protected static String getUniqueClusterName(String base_name) {
        return ResourceManager.getUniqueClusterName(base_name);
    }

    /**
     * Default channel factory used in junit tests
     */
    protected class DefaultChannelTestFactory {

        public JChannel createChannel() throws Exception {
            return createChannel(channel_conf);
        }

        public JChannel createChannel(boolean unique, int num) throws Exception {
            JChannel c = createChannel(channel_conf);
            if(unique)
                makeUnique(c, num);
            return c;
        }

        public JChannel createChannel(final JChannel ch) throws Exception {
            return new JChannel(ch);
        }

        private JChannel createChannel(String configFile) throws Exception {
            return new JChannel(configFile);
        }

        protected void makeUnique(JChannel channel, int num) throws Exception {
            String str = Util.getProperty(new String[]{ Global.UDP_MCAST_ADDR, "jboss.partition.udpGroup" },
                                          null, "mcast_addr", null);
            makeUnique(channel, num, str);
        }

        protected void makeUnique(JChannel channel, int num, String mcast_address) throws Exception {
            ProtocolStack stack = channel.getProtocolStack();
            TP transport = stack.getTransport();
            transport.setBindAddress(bind_addr);
            if (transport instanceof UDP) {
                short mcast_port = ResourceManager.getNextMulticastPort(bind_addr);
                ((UDP) transport).setMulticastPort(mcast_port);
                if (mcast_address != null) {
                    ((UDP) transport).setMulticastAddress(InetAddress.getByName(mcast_address));
                } else {
                    String mcast_addr = ResourceManager.getNextMulticastAddress();
                    ((UDP) transport).setMulticastAddress(InetAddress.getByName(mcast_addr));
                }
            } else if (transport instanceof BasicTCP) {
                List<Integer> ports = ResourceManager.getNextTcpPorts(bind_addr, num);
                transport.setBindPort(ports.get(0));
                Protocol ping = stack.findProtocol(TCPPING.class);
                if (ping == null)
                    throw new IllegalStateException("TCP stack must consist of TCP:TCPPING - other config are not supported");
                Collection<InetSocketAddress> hosts=ports.stream()
                  .map(p -> new InetSocketAddress(bind_addr, p)).collect(Collectors.toList());
                ((TCPPING)ping).setInitialHosts(hosts);
            } else {
                throw new IllegalStateException("Only UDP and TCP are supported as transport protocols");
            }
        }
    }



}
