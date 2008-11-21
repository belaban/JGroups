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
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.ResourceManager;
import org.jgroups.util.Util;
import org.testng.AssertJUnit;
import org.testng.annotations.*;

import java.io.InputStream;
import java.io.OutputStream;
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
@Test(groups="base", sequential=true)
public class ChannelTestBase {

    protected String channel_conf="udp.xml";

    protected boolean use_blocking=false;

    private String bind_addr="127.0.0.1";

    protected final Log log=LogFactory.getLog(this.getClass());




    @BeforeClass
	@Parameters(value = { "channel.conf", "use_blocking" })
	protected void initializeBase(@Optional("udp.xml") String chconf,
			@Optional("false") String use_blocking) throws Exception {
		Test annotation = this.getClass().getAnnotation(Test.class);
		// this should never ever happen!
		if (annotation == null)
			throw new Exception("Test is not marked with @Test annotation");

		List<String> groups = Arrays.asList(annotation.groups());
		boolean testRequiresFlush = groups.contains(Global.FLUSH);
		
		//don't change chconf - should be equal to optional parameter above
		if (testRequiresFlush && chconf.equals("udp.xml")) {
			this.channel_conf = "flush-udp.xml";
			this.use_blocking = true;
		} else {
			this.channel_conf = chconf;
			this.use_blocking = Boolean.parseBoolean(use_blocking);
		}

		bind_addr = Util.getBindAddress(null).getHostAddress();
	}


    
    protected String getBindAddress(){
        return bind_addr;
    }

    protected boolean useBlocking() {
          return use_blocking;
    }

    protected void setUseBlocking(boolean flag) {
        use_blocking=flag;
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
        return createChannel(unique, 2);
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
            String str=Util.getProperty(new String[] { Global.UDP_MCAST_ADDR,
                                                      "jboss.partition.udpGroup" },
                                        null,
                                        "mcast_addr",
                                        false,
                                        null);
            if(str != null)
                makeUnique(channel, num, str);
            else
                makeUnique(channel, num, null);

        }
        
        protected void makeUnique(Channel channel, int num, String mcast_address) throws Exception {
            ProtocolStack stack=channel.getProtocolStack();
            Protocol transport=stack.getTransport();
            if(transport instanceof UDP) {
                short mcast_port=ResourceManager.getNextMulticastPort(InetAddress.getByName(bind_addr));
                ((UDP)transport).setMulticastPort(mcast_port);
                if(mcast_address != null) {
                    ((UDP)transport).setMulticastAddress(mcast_address);
                }
                else {
                    String mcast_addr=ResourceManager.getNextMulticastAddress();
                    ((UDP)transport).setMulticastAddress(mcast_addr);
                }
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
    protected abstract class ChannelApplication implements EventSequence, Runnable, MemberRetrievable,ExtendedReceiver {
        protected Channel   channel;
        protected Thread    thread;
        protected Throwable exception;
        protected String    name;
        protected RpcDispatcher dispatcher;
        protected List<Object> events;
       
        public ChannelApplication(String name,  boolean useDispatcher) throws Exception {
            this.name=name;
            channel=createChannel(true,4);
            events=Collections.synchronizedList(new LinkedList<Object>());
            if(useDispatcher) {
                dispatcher=new RpcDispatcher(channel, this, this, this);
            }
            else {
                channel.setReceiver(this);
            }
        }
        
        public ChannelApplication(JChannel copySource,String name,  boolean useDispatcher) throws Exception {
            this.name=name;
            channel=createChannel(copySource);
            events=Collections.synchronizedList(new LinkedList<Object>());
            if(useDispatcher) {
                dispatcher=new RpcDispatcher(channel, this, this, this);
            }
            else {
                channel.setReceiver(this);
            }
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

        public List<Address> getMembers() {
            List<Address> result=null;
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
    protected abstract class PushChannelApplicationWithSemaphore extends ChannelApplication {
        protected Semaphore semaphore;

        public PushChannelApplicationWithSemaphore(String name,
                                                   Semaphore semaphore,
                                                   boolean useDispatcher) throws Exception {
            super(name, useDispatcher);
            this.semaphore=semaphore;
        }
        
        public PushChannelApplicationWithSemaphore(JChannel copySource, String name,
                                                   Semaphore semaphore,
                                                   boolean useDispatcher) throws Exception {
            super(copySource,name, useDispatcher);
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
        assertNotNull(events);
        final String validSequence="([b][vgs]*[u])+";     
        // translate the eventTrace to an eventString   
        try {           
            assertTrue("Invalid event sequence " + events, validateEventString(translateEventTrace(events), validSequence));          
        }
        catch(Exception e) {
            AssertJUnit.fail("Invalid event sequence " + events);
        }        
    }     
    
    /**
     * Method for translating event traces into event strings, where each event
     * in the trace is represented by a letter.
     */
    protected static String translateEventTrace(List<Object> et) throws Exception {
        StringBuffer eventString=new StringBuffer();
        for(Iterator<Object> it=et.iterator();it.hasNext();) {
            Object obj=it.next();
            if(obj instanceof BlockEvent)
                eventString.append("b");
            else if(obj instanceof UnblockEvent)
                eventString.append("u");
            else if(obj instanceof SetStateEvent)
                eventString.append("s");
            else if(obj instanceof GetStateEvent)
                eventString.append("g");
            else if(obj instanceof View)
                eventString.append("v");
            else
                throw new Exception("Unrecognized event type in event trace");
        }
        String s = eventString.toString();
        //if it ends with block, strip it out because it will be regarded as error sequence
        while(s.endsWith("b")){
        	s = s.substring(0, s.length()-1);
        }
        return s;
    }
  
    /**
     * Method for validating event strings against event string specifications,
     * where a specification is a regular expression involving event symbols.
     * e.g. [b]([sgv])[u]
     */
    protected static boolean validateEventString(String eventString, String spec) {
        Pattern pattern=null;
        Matcher matcher=null;

        // set up the regular expression specification
        pattern=Pattern.compile(spec);
        // set up the actual event string
        matcher=pattern.matcher(eventString);

        // check if the actual string satisfies the specification
        if(matcher.find()) {
            // a match has been found, but we need to check that the whole event string
            // matches, and not just a substring
            if(!(matcher.start() == 0 && matcher.end() == eventString.length())) {
                // match on full eventString not found
                System.err.println("event string invalid (proper substring matched): event string = " + eventString
                                   + ", specification = "
                                   + spec
                                   + "matcher.start() "
                                   + matcher.start()
                                   + " matcher.end() "
                                   + matcher.end());
                return false;
            }
        }
        else {          
            return false;
        }
        return true;
    }
    
    protected interface MemberRetrievable {
        public List<Address> getMembers();
        public Address getLocalAddress();
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
