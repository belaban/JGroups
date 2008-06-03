package org.jgroups.tests;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.*;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.protocols.BasicTCP;
import org.jgroups.protocols.TCPPING;
import org.jgroups.protocols.UDP;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.stack.GossipRouter;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.ResourceManager;
import org.jgroups.util.Util;
import org.testng.annotations.*;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * @author Bela Ban
 * @author Vladimir Blagojevic
 * @author <a href="mailto://brian.stansberry@jboss.com">Brian Stansberry</a>
 * @version $Id$
 */
@Test(groups=Global.STACK_DEPENDENT, sequential=true)
public class ChannelTestBase {

    protected String channel_conf="udp.xml";

    protected int active_threads=0;

    protected boolean compare_thread_count=false;

    protected String before_threads=null;

    protected boolean use_blocking=false;

    private int router_port=12001;

    private String bind_addr="127.0.0.1";

    GossipRouter router=null;

    protected final Log log=LogFactory.getLog(this.getClass());




    @BeforeClass
    @Parameters(value={"channel.conf", "compare_thread_count", "use_blocking"})
    protected void initialize(@Optional("udp.xml") String channel_conf,
                              @Optional("false") String compare_thread_count,
                              @Optional("false") String use_blocking) throws Exception {
        if(channel_conf != null)
            this.channel_conf=channel_conf;

        if(compare_thread_count != null)
            this.compare_thread_count=Boolean.parseBoolean(compare_thread_count);

        if(use_blocking != null)
            this.use_blocking=Boolean.parseBoolean(use_blocking);

        if(isTunnelUsed()) {
            router=new GossipRouter(router_port, bind_addr);
            router.start();
        }

        if(shouldCompareThreadCount()) {
            active_threads=Thread.activeCount();
            before_threads="active threads before (" + active_threads + "):\n" + Util.activeThreads();
        }
    }

    protected boolean isTunnelUsed() {
        return channel_conf.contains("tunnel");
    }

    
    @AfterClass
    protected void terminate() throws Exception {
        if(router != null) {
            router.stop();
            Util.sleep(100);
        }

        if(shouldCompareThreadCount()) {
            // at the moment Thread.activeCount() is called it might count in threads that are just being
            // excluded from active count. Therefore we include a slight delay of 20 msec

            Util.sleep(20);
            int current_active_threads=Thread.activeCount();

            String msg="";
            if(active_threads != current_active_threads) {
                System.out.println(before_threads);
                System.out.println("active threads after (" + current_active_threads + "):\n" + Util.activeThreads());
                msg="active threads:\n" + Util.dumpThreads();
            }
            assert active_threads == current_active_threads : msg;
        }
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
     * Creates a channel and modifies the configuration such that no other channel will able to join this
     * one even if they have the same cluster name (if unique = true). This is done by modifying mcast_addr and mcast_port with UDP,
     * and by changing TCP.start_port, TCP.port_range and TCPPING.initial_hosts with TCP. Mainly used to
     * run TestNG tests concurrently. Note that MuxChannels are not currently supported.
     * @param num The number of channels we will create. Only important (for port_range) with TCP, ignored by UDP
     * @return
     * @throws Exception
     */
    protected JChannel createChannel(boolean unique, int num) throws Exception {
        return (JChannel)new DefaultChannelTestFactory().createChannel(unique, num);
    }

    protected JChannel createChannel() throws Exception {
        return new DefaultChannelTestFactory().createChannel();
    }

    protected JChannel createChannel(boolean unique) throws Exception {
        return createChannel(unique, 1);
    }

    protected JChannel createChannel(JChannel ch) throws Exception {
        return (JChannel)new DefaultChannelTestFactory().createChannel(ch);
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
            return createChannel(channel_conf, useBlocking());
        }

        public Channel createChannel(boolean unique, int num) throws Exception {
            JChannel c=createChannel(channel_conf, useBlocking());
            if(unique) {
                makeUnique(c, num);
            }
            muteLocalAddress(c);
            return c;
        }

        public Channel createChannel(final JChannel ch) throws Exception {
            Map<Integer, Object> channelOptions=new HashMap<Integer, Object>();
            boolean useBlocking=(Boolean)ch.getOpt(Channel.BLOCK);
            channelOptions.put(Channel.BLOCK, useBlocking);

            log.info("Using configuration file " + channel_conf);
            JChannel retval=new JChannel(ch);
            for(Map.Entry<Integer, Object> entry : channelOptions.entrySet()) {
                Integer key=entry.getKey();
                Object value=entry.getValue();
                retval.setOpt(key, value);
            }
            return retval;
        }

        private JChannel createChannel(String configFile, boolean useBlocking) throws Exception {
            Map<Integer, Object> channelOptions=new HashMap<Integer, Object>();
            channelOptions.put(Channel.BLOCK, useBlocking);

            log.info("Using configuration file " + configFile);
            JChannel ch=new JChannel(configFile);
            for(Map.Entry<Integer, Object> entry : channelOptions.entrySet()) {
                Integer key=entry.getKey();
                Object value=entry.getValue();
                ch.setOpt(key, value);
            }
            return ch;
        }

        private void muteLocalAddress(Channel c) {
            ProtocolStack stack=c.getProtocolStack();
            Protocol gms=stack.findProtocol(GMS.class);
            if(gms != null) {
                ((GMS)gms).setPrintLocalAddress(false);
            }
        }

        protected void makeUnique(Channel channel, int num) throws Exception {
            ProtocolStack stack=channel.getProtocolStack();
            Protocol transport=stack.getTransport();
            if(transport instanceof UDP) {
                String mcast_addr=ResourceManager.getNextMulticastAddress();
                short mcast_port=ResourceManager.getNextMulticastPort(InetAddress.getByName(bind_addr));
                ((UDP)transport).setMulticastAddress(mcast_addr);
                ((UDP)transport).setMulticastPort(mcast_port);
            }
            else if(transport instanceof BasicTCP) {
                List<Short> ports=ResourceManager.getNextTcpPorts(InetAddress.getByName(bind_addr), num);
                ((TP)transport).setBindPort(ports.get(0));
                ((TP)transport).setPortRange(num);

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




    interface EventSequence {
        List<Object> getEvents();
        String getName();
    }

    /**
     * Base class for all aplications using channel
     */
    protected abstract class ChannelApplication implements EventSequence, Runnable, MemberRetrievable {
        protected Channel   channel;
        protected Thread    thread;
        protected Throwable exception;
        protected String    name;


        /**
         * Creates a unconnected channel and assigns a name to it.
         * @param name    name of this channel
         * @throws ChannelException
         */
        public ChannelApplication(String name) throws Exception {
            this.name=name;
            channel=createChannel();
        }

        /**
         * Method allowing implementation of specific test application level logic
         * @throws Exception
         */
        protected abstract void useChannel() throws Exception;

        public void run() {
            try {
                useChannel();
            }
            catch(Exception e) {
                log.error(name + ": " + e.getLocalizedMessage(), e);
                exception=e; // Save it for the test to check
            }
        }

        public List getMembers() {
            List result=null;
            View v=channel.getView();
            if(v != null) {
                result=v.getMembers();
            }
            return result;
        }


        public Address getLocalAddress() {
            return channel.getLocalAddress();
        }

        public void start() {
            thread=new Thread(this, getName());
            thread.start();
            Address a=getLocalAddress();
            boolean connected=a != null;
            if(connected) {
                log.info("Thread for channel " + a + "[" + getName() + "] started");
            }
            else {
                log.info("Thread for channel [" + getName() + "] started");
            }
        }


        public Channel getChannel() {
            return channel;
        }

        public String getName() {
            return name;
        }

        public void cleanup() {
            if(thread != null && thread.isAlive()) {
                thread.interrupt();
            }
            Address a=getLocalAddress();
            boolean connected=a != null;
            if(connected) {
                log.info("Closing channel " + a + "[" + getName() + "]");
            }
            else {
                log.info("Closing channel [" + getName() + "]");
            }
            try {
                channel.close();
                log.info("Closed channel " + a + "[" + getName() + "]");
            }
            catch(Throwable t) {
                log.warn("Got exception while closing channel " + a + "[" + getName() + "]");
            }
        }
    }

    protected abstract class PushChannelApplication extends ChannelApplication implements ExtendedReceiver {
        RpcDispatcher dispatcher;
        List<Object> events;


        public PushChannelApplication(String name,  boolean useDispatcher) throws Exception {
            super(name);
            events=Collections.synchronizedList(new LinkedList<Object>());
            if(useDispatcher) {
                dispatcher=new RpcDispatcher(channel, this, this, this);
            }
            else {
                channel.setReceiver(this);
            }
        }

        public List<Object> getEvents() {
            return events;
        }


        public void block() {
            events.add(new BlockEvent());
            log.debug("Channel " + getLocalAddress() + "[" + getName() + "] in blocking");
        }

        public byte[] getState() {
            events.add(new GetStateEvent(null, null));
            log.debug("Channel getState " + getLocalAddress() + "[" + getName() + "] ");
            return null;
        }

        public void getState(OutputStream ostream) {
            events.add(new GetStateEvent(null, null));
            log.debug("Channel getState " + getLocalAddress() + "[" + getName() + "]");
        }

        public byte[] getState(String state_id) {
            events.add(new GetStateEvent(null, state_id));
            log.debug("Channel getState " + getLocalAddress() + "[" + getName() + " state id =" + state_id);
            return null;
        }

        public void getState(String state_id, OutputStream ostream) {
            events.add(new GetStateEvent(null, state_id));
            log.debug("Channel getState " + getLocalAddress() + "[" + getName() + "] state id =" + state_id);
        }

        public void receive(Message msg) {
        }

        public void setState(byte[] state) {
            events.add(new SetStateEvent(null, null));
            log.debug("Channel setState " + getLocalAddress() + "[" + getName() + "] ");
        }

        public void setState(InputStream istream) {
            events.add(new SetStateEvent(null, null));
            log.debug("Channel setState " + getLocalAddress() + "[" + getName() + "]");
        }

        public void setState(String state_id, byte[] state) {
            events.add(new SetStateEvent(null, null));
            log.debug("Channel setState " + getLocalAddress()
                    + "["
                    + getName()
                    + "] state id ="
                    + state_id
                    + ", state size is "
                    + state.length);
        }

        public void setState(String state_id, InputStream istream) {
            events.add(new SetStateEvent(null, null));
            log.debug("Channel setState " + getLocalAddress() + "[" + getName() + "] state id " + state_id);
        }

        public void suspect(Address suspected_mbr) {
            log.debug("Channel " + getLocalAddress()
                    + "["
                    + getName()
                    + "] suspecting "
                    + suspected_mbr);
        }

        public void unblock() {
            events.add(new UnblockEvent());
            log.debug("Channel " + getLocalAddress() + "[" + getName() + "] unblocking");
        }

        public void viewAccepted(View new_view) {
            events.add(new_view);
            log.info("Channel " + getLocalAddress()
                    + "["
                    + getName()
                    + "] accepted view "
                    + new_view);
        }
    }

    /**
     * Channel with semaphore allows application to go through fine-grained
     * synchronous step control.
     * <p/>
     * PushChannelApplicationWithSemaphore application will not proceed to
     * useChannel() until it acquires permit from semphore. After useChannel()
     * completes the acquired permit will be released. Test driver should
     * control how semaphore tickets are given and acquired.
     */
    protected abstract class PushChannelApplicationWithSemaphore extends PushChannelApplication {
        protected Semaphore semaphore;

        public PushChannelApplicationWithSemaphore(String name,
                                                   Semaphore semaphore,
                                                   boolean useDispatcher) throws Exception {
            super(name, useDispatcher);
            this.semaphore=semaphore;
        }

        protected PushChannelApplicationWithSemaphore(String name, Semaphore semaphore) throws Exception {
            this(name, semaphore, false);
        }


        public void run() {
            boolean acquired=false;
            try {
                acquired=semaphore.tryAcquire(60000L, TimeUnit.MILLISECONDS);
                if(!acquired) {
                    throw new Exception(name + " cannot acquire semaphore");
                }

                useChannel();
            }
            catch(Exception e) {
                log.error(name + ": " + e.getLocalizedMessage(), e);
                exception=e; // Save it for the test to check
            }
            finally {
                if(acquired) {
                    semaphore.release();
                }
            }
        }
    }

    protected void checkEventStateTransferSequence(EventSequence receiver) {
        List<Object> events=receiver.getEvents();
        String eventString="[" + receiver.getName() + ",events:" + events;
        log.info(eventString);
        assert events != null;
        assert events.size() > 1;
        assert events.get(0) instanceof BlockEvent : "First event is not block but " + events.get(0);
        assert events.get(events.size() - 1) instanceof UnblockEvent : "Last event not unblock but " + events.get(events.size() - 1);
        int size=events.size();

        for(int i=0; i < size; i++) {
            Object event=events.get(i);
            if(event instanceof BlockEvent) {
                if(i + 1 < size) {
                    Object o=events.get(i + 1);
                    assert o instanceof SetStateEvent || o instanceof GetStateEvent || o instanceof UnblockEvent
                            || o instanceof View
                            : "After Block should be state|unblock|view, but it is " + o.getClass() + ",events= " + eventString;
                }
                if(i > 0) {
                    Object o=events.get(i - 1);
                    assert o instanceof UnblockEvent
                            : "Before Block should be state or Unblock , but it is " + o.getClass() + ",events= " + eventString;
                }
            }
            else if(event instanceof SetStateEvent) {
                if(i + 1 < size) {
                    Object o=events.get(i + 1);
                    assert o instanceof UnblockEvent
                            : "After setstate should be unblock , but it is " + o.getClass() + ",events= " + eventString;
                }
                Object o=events.get(i - 1);
                assert o instanceof BlockEvent || o instanceof View
                        : "Before setstate should be block|view, but it is " + o.getClass() + ",events= " + eventString;
            }
            else if(event instanceof GetStateEvent) {
                if(i + 1 < size) {
                    Object o=events.get(i + 1);
                    assert o instanceof UnblockEvent || o instanceof View || o instanceof GetStateEvent
                            : "After getstate should be view/unblock/getstate , but it is " + o.getClass() + ",events= " + eventString;
                }
                Object o=events.get(i - 1);
                assert o instanceof BlockEvent || o instanceof View || o instanceof GetStateEvent
                        : "Before state should be block/view/getstate , but it is " + o.getClass() + ",events= " + eventString;
            }
            else if(event instanceof UnblockEvent) {
                if(i + 1 < size) {
                    Object o=events.get(i + 1);
                    assert o instanceof BlockEvent
                            : "After UnBlock should be Block , but it is " + o.getClass() + ",events= " + eventString;
                }
                if(i > 0) {
                    Object o=events.get(i - 1);
                    assert o instanceof SetStateEvent || o instanceof GetStateEvent || o instanceof BlockEvent
                            || o instanceof View
                            : "Before UnBlock should be block|state|view , but it is " + o.getClass() + ",events= " + eventString;
                }
            }

        }
    }

    

    protected interface MemberRetrievable {
        public List getMembers();
        public Address getLocalAddress();
    }

    protected boolean shouldCompareThreadCount() {
        return compare_thread_count;
    }

    protected boolean useBlocking() {
        return use_blocking;
    }

    /**
     * Checks each channel in the parameter array to see if it has the exact same view as other channels in an array.
     */
    protected static boolean areViewsComplete(MemberRetrievable[] channels, int memberCount) {
        for(int i=0; i < memberCount; i++) {
            if(!isViewComplete(channels[i], memberCount)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Loops, continually calling
     * {@link #areViewsComplete(org.jgroups.tests.ChannelTestBase.MemberRetrievable[], int)}
     * until it either returns true or <code>timeout</code> ms have elapsed.
     * @param channels channels which must all have consistent views
     * @param timeout  max number of ms to loop
     * @throws RuntimeException if <code>timeout</code> ms have elapse without all channels
     *                          having the same number of members.
     */
    protected static void blockUntilViewsReceived(MemberRetrievable[] channels, long timeout) {
        blockUntilViewsReceived(channels, channels.length, timeout);
    }

    protected static void blockUntilViewsReceived(Collection channels, long timeout) {
        blockUntilViewsReceived(channels, channels.size(), timeout);
    }

    /**
     * Loops, continually calling
     * {@link #areViewsComplete(org.jgroups.tests.ChannelTestBase.MemberRetrievable[], int)}
     * until it either returns true or <code>timeout</code> ms have elapsed.
     * @param channels channels which must all have consistent views
     * @param timeout  max number of ms to loop
     * @throws RuntimeException if <code>timeout</code> ms have elapse without all channels
     *                          having the same number of members.
     */
    protected static void blockUntilViewsReceived(MemberRetrievable[] channels, int count, long timeout) {
        long failTime=System.currentTimeMillis() + timeout;

        while(System.currentTimeMillis() < failTime) {
            Util.sleep(100);
            if(areViewsComplete(channels, count)) {
                return;
            }
        }

        StringBuilder sb=new StringBuilder();
        for(MemberRetrievable c : channels) {
            sb.append(c.getLocalAddress() + ",view=" + c.getMembers() + "|");
        }
        throw new RuntimeException("timed out before caches had complete views. Views are " + sb.toString());
    }

    protected static void blockUntilViewsReceived(Collection channels, int count, long timeout) {
        long failTime=System.currentTimeMillis() + timeout;

        while(System.currentTimeMillis() < failTime) {
            Util.sleep(100);
            if(areViewsComplete((MemberRetrievable[])channels.toArray(new MemberRetrievable[channels.size()]), count)) {
                return;
            }
        }

        throw new RuntimeException("timed out before caches had complete views");
    }

    protected static boolean isViewComplete(MemberRetrievable channel, int memberCount) {

        List members=channel.getMembers();
        if(members == null || memberCount > members.size()) {
            return false;
        }
        else if(memberCount < members.size()) {
            // This is an exceptional condition
            StringBuilder sb=new StringBuilder("Channel at address ");
            sb.append(channel.getLocalAddress());
            sb.append(" had ");
            sb.append(members.size());
            sb.append(" members; expecting ");
            sb.append(memberCount);
            sb.append(". Members were (");
            for(int j=0; j < members.size(); j++) {
                if(j > 0) {
                    sb.append(", ");
                }
                sb.append(members.get(j));
            }
            sb.append(')');

            throw new IllegalStateException(sb.toString());
        }

        return true;
    }


}
