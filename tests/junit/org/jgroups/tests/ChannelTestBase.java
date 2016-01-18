package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.*;
import org.jgroups.stack.AbstractProtocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.ResourceManager;
import org.jgroups.util.StackType;
import org.jgroups.util.Util;
import org.testng.annotations.*;

import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.InetAddress;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Bela Ban
 * @author Vladimir Blagojevic
 * @author <a href="mailto://brian.stansberry@jboss.com">Brian Stansberry</a>
 */
@Test(groups = "base", sequential = true)
public class ChannelTestBase {
    protected String  channel_conf = "udp.xml";
    protected String  bind_addr = null;
    protected Log     log;

    @BeforeClass
    @Parameters(value = { "channel.conf", "use_blocking" })
    protected void initializeBase(@Optional("udp.xml") String chconf, @Optional("false") String use_blocking) throws Exception {
        log=LogFactory.getLog(this.getClass());
        Test annotation = this.getClass().getAnnotation(Test.class);
        // this should never ever happen!
        if (annotation == null)
            throw new Exception("Test is not marked with @Test annotation");

        StackType type=Util.getIpStackType();
        bind_addr=type == StackType.IPv6 ? "::1" : "127.0.0.1";
        this.channel_conf = chconf;
        bind_addr = Util.getProperty(new String[]{Global.BIND_ADDR}, null, "bind_addr", bind_addr);
        System.setProperty(Global.BIND_ADDR, bind_addr);
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

    protected String getBindAddress() {
        return bind_addr;
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
     * and TCPPING.initial_hosts with TCP. Mainly used to run TestNG tests concurrently. Note that
     * MuxChannels are not currently supported.
     * 
     * @param num
     *            The number of channels we will create. Only important (for port_range) with TCP,
     *            ignored by UDP
     * @return
     * @throws Exception
     */
    protected JChannel createChannel(boolean unique, int num) throws Exception {
        return (JChannel) new DefaultChannelTestFactory().createChannel(unique, num);
    }

    protected JChannel createChannel(boolean unique, int num, String name) throws Exception {
        return (JChannel)new DefaultChannelTestFactory().createChannel(unique, num).name(name);
    }

    protected JChannel createChannel() throws Exception {
        return new DefaultChannelTestFactory().createChannel();
    }

    protected JChannel createChannel(boolean unique) throws Exception {
        return createChannel(unique,2);
    }

    protected JChannel createChannel(JChannel ch) throws Exception {
        return (JChannel) new DefaultChannelTestFactory().createChannel(ch);
    }

    protected JChannel createChannel(JChannel ch, String name) throws Exception {
        return (JChannel) new DefaultChannelTestFactory().createChannel(ch).name(name);
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

        public AbstractChannel createChannel(boolean unique, int num) throws Exception {
            JChannel c = createChannel(channel_conf);
            UNICAST2 uni=(UNICAST2)c.getProtocolStack().findProtocol(UNICAST2.class);
            if(uni != null) {
                uni.setValue("stable_interval", 5000);
                uni.setValue("max_bytes", 1000000);
            }
            if(unique)
                makeUnique(c, num);
            return c;
        }

        public AbstractChannel createChannel(final JChannel ch) throws Exception {
            return new JChannel(ch);
        }

        private JChannel createChannel(String configFile) throws Exception {
            return new JChannel(configFile);
        }

        protected void makeUnique(AbstractChannel channel, int num) throws Exception {
            String str = Util.getProperty(new String[]{ Global.UDP_MCAST_ADDR, "jboss.partition.udpGroup" },
                                          null, "mcast_addr", null);
            makeUnique(channel, num, str);
        }

        protected void makeUnique(AbstractChannel channel, int num, String mcast_address) throws Exception {
            ProtocolStack stack = channel.getProtocolStack();
            AbstractProtocol transport = stack.getTransport();

            if (transport instanceof UDP) {
                short mcast_port = ResourceManager.getNextMulticastPort(InetAddress.getByName(bind_addr));
                ((UDP) transport).setMulticastPort(mcast_port);
                if (mcast_address != null) {
                    ((UDP) transport).setMulticastAddress(InetAddress.getByName(mcast_address));
                } else {
                    String mcast_addr = ResourceManager.getNextMulticastAddress();
                    ((UDP) transport).setMulticastAddress(InetAddress.getByName(mcast_addr));
                }
            } else if (transport instanceof AbstractBasicTCP) {
                List<Short> ports = ResourceManager.getNextTcpPorts(InetAddress.getByName(bind_addr), num);
                ((AbstractTP) transport).setBindPort(ports.get(0));
                ((AbstractTP) transport).setPortRange(num);

                AbstractProtocol ping = stack.findProtocol(TCPPING.class);
                if (ping == null)
                    throw new IllegalStateException("TCP stack must consist of TCP:TCPPING - other config are not supported");

                List<String> initial_hosts = new LinkedList<>();
                for (short port : ports) {
                    initial_hosts.add(bind_addr + "[" + port + "]");
                }
                String tmp = Util.printListWithDelimiter(initial_hosts, ",", 2000, false);
                List<PhysicalAddress> init_hosts = Util.parseCommaDelimitedHosts(tmp, 0);
                ((TCPPING)ping).setInitialHosts(init_hosts);
            } else {
                throw new IllegalStateException("Only UDP and TCP are supported as transport protocols");
            }
        }
    }

  

    interface EventSequence {
        /** Return an event string. Events are translated as follows: get state='g', set state='s',
         *  block='b', unlock='u', view='v' */
        String getEventSequence();
        String getName();
    }

    /**
     * Base class for all aplications using channel
     */
    protected abstract class ChannelApplication extends ReceiverAdapter implements EventSequence, Runnable {
        protected AbstractChannel channel;
        protected Thread thread;
        protected Throwable exception;
        protected StringBuilder events;

        public ChannelApplication(String name) throws Exception {
            channel = createChannel(true, 4);
            init(name);
        }

        public ChannelApplication(JChannel copySource, String name) throws Exception {
            channel = createChannel(copySource);
            init(name);
        }

        protected void init(String name) {
            events = new StringBuilder();
            channel.setName(name);
            channel.setReceiver(this);
        }

        /**
         * Method allowing implementation of specific test application level logic
         * 
         * @throws Exception
         */
        protected abstract void useChannel() throws Exception;

        public void run() {
            try {
                useChannel();
            } catch (Exception e) {
                log.error(channel.getName() + ": " + e.getLocalizedMessage(), e);
                exception = e; // Save it for the test to check
            }
        }

        public List<Address> getMembers() {
            List<Address> result = null;
            View v = channel.getView();
            if (v != null) {
                result = v.getMembers();
            }
            return result;
        }

        public Address getLocalAddress() {
            return channel.getAddress();
        }

        public void start() {
            thread = new Thread(this, getName());
            thread.start();
        }

        public AbstractChannel getChannel() {
            return channel;
        }

        public String getName() {
            return channel != null ? channel.getName() : "n/a";
        }

        public void cleanup() {
            if (thread != null && thread.isAlive())
                thread.interrupt();
            Util.close(channel);
        }

        public String getEventSequence()                              {return events.toString();}
        public void   block()                                         {events.append('b');}
        public void   getState(OutputStream ostream) throws Exception {events.append('g');}
        public void   setState(InputStream istream) throws Exception  {events.append('s');}
        public void   unblock()                                       {events.append('u');}
        public void   viewAccepted(View new_view)                     {events.append('v');}
    }

}
