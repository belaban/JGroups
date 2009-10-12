package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.BasicTCP;
import org.jgroups.protocols.TCPPING;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.UDP;
import org.jgroups.protocols.pbcast.FLUSH;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.ResourceManager;
import org.jgroups.util.Util;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Bela Ban
 * @author Vladimir Blagojevic
 * @author <a href="mailto://brian.stansberry@jboss.com">Brian Stansberry</a>
 * @version $Id$
 */
@Test(groups = "base", sequential = true)
public class ChannelTestBase {

    protected String channel_conf = "udp.xml";

    protected boolean use_blocking = false;

    protected boolean use_flush = false;

    private String bind_addr = "127.0.0.1";

    protected final Log log = LogFactory.getLog(this.getClass());

    @BeforeClass
    @Parameters(value = { "channel.conf", "use_blocking" })
    protected void initializeBase(@Optional("udp.xml") String chconf, @Optional("false") String use_blocking) throws Exception {
        Test annotation = this.getClass().getAnnotation(Test.class);
        // this should never ever happen!
        if (annotation == null)
            throw new Exception("Test is not marked with @Test annotation");

        List<String> groups = Arrays.asList(annotation.groups());
        boolean testRequiresFlush = groups.contains(Global.FLUSH);

        this.use_blocking = testRequiresFlush || Boolean.parseBoolean(use_blocking);
        this.use_flush = testRequiresFlush;
        this.channel_conf = chconf;
        bind_addr = Util.getBindAddress(null).getHostAddress();
    }

    @BeforeMethod
    protected void startTestHeader(java.lang.reflect.Method m) {
        System.out.println("\n================ Starting test " + m.getName()
                        + " ================\n");
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

    protected boolean useBlocking() {
        return use_blocking;
    }

    protected void setUseBlocking(boolean flag) {
        use_blocking = flag;
    }

    protected boolean useFlush() {
        return use_flush;
    }

    protected final static void assertTrue(boolean condition) {
        Util.assertTrue(condition);
    }

    protected final static void assertTrue(String message, boolean condition) {
        Util.assertTrue(message, condition);
    }

    protected final static void assertFalse(boolean condition) {
        Util.assertFalse(condition);
    }

    protected final static void assertFalse(String message, boolean condition) {
        Util.assertFalse(message, condition);
    }

    protected final static void assertEquals(String message, Object val1, Object val2) {
        Util.assertEquals(message, val1, val2);
    }

    protected final static void assertEquals(Object val1, Object val2) {
        Util.assertEquals(null, val1, val2);
    }

    protected final static void assertNotNull(String message, Object val) {
        Util.assertNotNull(message, val);
    }

    protected final static void assertNotNull(Object val) {
        Util.assertNotNull(null, val);
    }

    protected final static void assertNull(String message, Object val) {
        Util.assertNull(message, val);
    }

    protected final static void assertNull(Object val) {
        Util.assertNotNull(null, val);
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

    protected JChannel createChannel() throws Exception {
        return new DefaultChannelTestFactory().createChannel();
    }

    protected JChannel createChannel(boolean unique) throws Exception {
        return createChannel(unique, 2);
    }

    protected JChannel createChannel(JChannel ch) throws Exception {
        return (JChannel) new DefaultChannelTestFactory().createChannel(ch);
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

        public Channel createChannel(boolean unique, int num) throws Exception {
            JChannel c = createChannel(channel_conf);
            if (unique) {
                makeUnique(c, num);
            }

            if (log.isDebugEnabled()) {
                log.debug("Modifying channel resources:");
                logChannelResources(c);
            }
            return c;
        }

        public Channel createChannel(final JChannel ch) throws Exception {
            Map<Integer, Object> channelOptions = new HashMap<Integer, Object>();
            boolean useBlocking = (Boolean) ch.getOpt(Channel.BLOCK);
            channelOptions.put(Channel.BLOCK, useBlocking);

            log.info("Using configuration file " + channel_conf);
            JChannel retval = new JChannel(ch);
            for (Map.Entry<Integer, Object> entry : channelOptions.entrySet()) {
                Integer key = entry.getKey();
                Object value = entry.getValue();
                retval.setOpt(key, value);
            }
            if (useFlush())
                Util.addFlush(retval, new FLUSH());

            if (log.isDebugEnabled()) {
                log.debug("Creating cloned channel with resources:");
                logChannelResources(retval);
            }
            return retval;
        }

        private JChannel createChannel(String configFile) throws Exception {
            Map<Integer, Object> channelOptions = new HashMap<Integer, Object>();
            channelOptions.put(Channel.BLOCK, useBlocking());

            log.info("Using configuration file " + configFile);
            JChannel ch = new JChannel(configFile);
            for (Map.Entry<Integer, Object> entry : channelOptions.entrySet()) {
                Integer key = entry.getKey();
                Object value = entry.getValue();
                ch.setOpt(key, value);
            }
            if (useFlush())
                Util.addFlush(ch, new FLUSH());

            if (log.isDebugEnabled()) {
                log.debug("Creating channel with resources:");
                logChannelResources(ch);
            }
            return ch;
        }

        protected void makeUnique(Channel channel, int num) throws Exception {
            String str = Util.getProperty(new String[] { Global.UDP_MCAST_ADDR,
                            "jboss.partition.udpGroup" }, null, "mcast_addr", false, null);
            if (str != null)
                makeUnique(channel, num, str);
            else
                makeUnique(channel, num, null);

        }

        protected void makeUnique(Channel channel, int num, String mcast_address) throws Exception {
            ProtocolStack stack = channel.getProtocolStack();
            Protocol transport = stack.getTransport();

            if (transport instanceof UDP) {
                short mcast_port = ResourceManager.getNextMulticastPort(InetAddress.getByName(bind_addr));
                ((UDP) transport).setMulticastPort(mcast_port);
                if (mcast_address != null) {
                    ((UDP) transport).setMulticastAddress(mcast_address);
                } else {
                    String mcast_addr = ResourceManager.getNextMulticastAddress();
                    ((UDP) transport).setMulticastAddress(mcast_addr);
                }
            } else if (transport instanceof BasicTCP) {
                List<Short> ports = ResourceManager.getNextTcpPorts(InetAddress.getByName(bind_addr), num);
                ((TP) transport).setBindPort(ports.get(0));
                ((TP) transport).setPortRange(num);

                Protocol ping = stack.findProtocol(TCPPING.class);
                if (ping == null)
                    throw new IllegalStateException(
                                    "TCP stack must consist of TCP:TCPPING - other config are not supported");

                List<String> initial_hosts = new LinkedList<String>();
                for (short port : ports) {
                    initial_hosts.add(bind_addr + "[" + port + "]");
                }
                String tmp = Util.printListWithDelimiter(initial_hosts, ",");
                List<IpAddress> init_hosts = Util.parseCommaDelimitedHosts(tmp, 1);
                ((TCPPING) ping).setInitialHosts(init_hosts);
            } else {
                throw new IllegalStateException(
                                "Only UDP and TCP are supported as transport protocols");
            }
        }
    }

    /*
     * Writes the shared channel resources used by this channel to the log (debugging).
     */
    protected void logChannelResources(Channel ch) {

        ProtocolStack stack = ch.getProtocolStack();
        Protocol transport = stack.getTransport();
        if (transport instanceof UDP) {
            log.debug("(udp.mcast_addr, udp.mcast_port) = ("
                            + ((UDP) transport).getMulticastAddress() + ", "
                            + ((UDP) transport).getMulticastPort() + ")");
        } else if (transport instanceof BasicTCP) {
            log.debug("(tcp.bind_port, tcp.port_range) = (" + ((TP) transport).getBindPort() + ", "
                            + ((TP) transport).getPortRange() + ")");
        } else {
            throw new IllegalStateException("Only UDP and TCP are supported as transport protocols");
        }
    }

    interface EventSequence {
        List<Object> getEvents();

        String getName();
    }

    /**
     * Base class for all aplications using channel
     */
    protected abstract class ChannelApplication extends ExtendedReceiverAdapter implements
                    EventSequence, Runnable {
        protected Channel channel;
        protected Thread thread;
        protected Throwable exception;
        protected List<Object> events;

        public ChannelApplication(String name) throws Exception {
            channel = createChannel(true, 4);
            init(name);
        }

        public ChannelApplication(JChannel copySource, String name) throws Exception {
            channel = createChannel(copySource);
            init(name);
        }

        protected void init(String name) {
            events = Collections.synchronizedList(new LinkedList<Object>());
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

        public Channel getChannel() {
            return channel;
        }

        public String getName() {
            return channel != null ? channel.getName() : "n/a";
        }

        public void cleanup() {
            if (thread != null && thread.isAlive())
                thread.interrupt();
            try {
                channel.close();
            } catch (Throwable t) {
                log.warn("Exception while closing channel " + getName(), t);
            }
        }

        public List<Object> getEvents() {
            return events;
        }

        public void block() {
            events.add(new BlockEvent());
        }

        public byte[] getState() {
            events.add(new GetStateEvent(null, null));
            return null;
        }

        public void getState(OutputStream ostream) {
            events.add(new GetStateEvent(null, null));
        }

        public byte[] getState(String state_id) {
            events.add(new GetStateEvent(null, state_id));
            return null;
        }

        public void getState(String state_id, OutputStream ostream) {
            events.add(new GetStateEvent(null, state_id));
        }

        public void setState(byte[] state) {
            events.add(new SetStateEvent(null, null));
        }

        public void setState(InputStream istream) {
            events.add(new SetStateEvent(null, null));
        }

        public void setState(String state_id, byte[] state) {
            events.add(new SetStateEvent(null, null));
        }

        public void setState(String state_id, InputStream istream) {
            events.add(new SetStateEvent(null, null));
        }

        public void unblock() {
            events.add(new UnblockEvent());
        }

        public void viewAccepted(View new_view) {
            events.add(new_view);
            log.info(getLocalAddress() + ": view=" + new_view);
        }
    }

    /**
     * Channel with semaphore allows application to go through fine-grained synchronous step
     * control.
     * <p/>
     * PushChannelApplicationWithSemaphore application will not proceed to useChannel() until it
     * acquires permit from semphore. After useChannel() completes the acquired permit will be
     * released. Test driver should control how semaphore tickets are given and acquired.
     */
    protected abstract class PushChannelApplicationWithSemaphore extends ChannelApplication {
        protected Semaphore semaphore;

        public PushChannelApplicationWithSemaphore(String name, Semaphore semaphore)
                        throws Exception {
            super(name);
            this.semaphore = semaphore;
        }

        public PushChannelApplicationWithSemaphore(JChannel copySource, String name,
                        Semaphore semaphore) throws Exception {
            super(copySource, name);
            this.semaphore = semaphore;
        }

        public void run() {
            boolean acquired = false;
            try {
                acquired = semaphore.tryAcquire(60000L, TimeUnit.MILLISECONDS);
                if (!acquired)
                    throw new Exception(channel.getAddress() + ": cannot acquire semaphore");

                useChannel();
            } catch (Exception e) {
                log.error(channel.getName() + ": " + e.getLocalizedMessage(), e);
                exception = e; // Save it for the test to check
            } finally {
                if (acquired) {
                    semaphore.release();
                }
            }
        }
    }

    protected static void checkEventStateTransferSequence(EventSequence receiver) {
        List<Object> events = receiver.getEvents();
        assertNotNull(events);
        final String validSequence = "([b][vgs]*[u])+";
        // translate the eventTrace to an eventString
        try {
            assertTrue("Invalid event sequence " + events, validateEventString(
                            translateEventTrace(events), validSequence));
        } catch (Exception e) {
            AssertJUnit.fail("Invalid event sequence " + events);
        }
    }

    /**
     * Method for translating event traces into event strings, where each event in the trace is
     * represented by a letter.
     */
    protected static String translateEventTrace(List<Object> et) throws Exception {
        StringBuilder eventString = new StringBuilder();
        for (Iterator<Object> it = et.iterator(); it.hasNext();) {
            Object obj = it.next();
            if (obj instanceof BlockEvent)
                eventString.append("b");
            else if (obj instanceof UnblockEvent)
                eventString.append("u");
            else if (obj instanceof SetStateEvent)
                eventString.append("s");
            else if (obj instanceof GetStateEvent)
                eventString.append("g");
            else if (obj instanceof View)
                eventString.append("v");
            else
                throw new Exception("Unrecognized event type in event trace");
        }
        String s = eventString.toString();
        // if it ends with block, strip it out because it will be regarded as error sequence
        while (s.endsWith("b")) {
            s = s.substring(0, s.length() - 1);
        }
        return s;
    }

    /**
     * Method for validating event strings against event string specifications, where a
     * specification is a regular expression involving event symbols. e.g. [b]([sgv])[u]
     */
    protected static boolean validateEventString(String eventString, String spec) {
        Pattern pattern = null;
        Matcher matcher = null;

        // set up the regular expression specification
        pattern = Pattern.compile(spec);
        // set up the actual event string
        matcher = pattern.matcher(eventString);

        // check if the actual string satisfies the specification
        if (matcher.find()) {
            // a match has been found, but we need to check that the whole event string
            // matches, and not just a substring
            if (!(matcher.start() == 0 && matcher.end() == eventString.length())) {
                // match on full eventString not found
                System.err
                                .println("event string invalid (proper substring matched): event string = "
                                                + eventString
                                                + ", specification = "
                                                + spec
                                                + "matcher.start() "
                                                + matcher.start()
                                                + " matcher.end() " + matcher.end());
                return false;
            }
        } else {
            return false;
        }
        return true;
    }
}
