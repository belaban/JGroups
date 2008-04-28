package org.jgroups.tests;

import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.*;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.mux.MuxChannel;
import org.jgroups.stack.GossipRouter;
import org.jgroups.util.Util;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * 
 * @author Bela Ban
 * @author Vladimir Blagojevic
 * @author <a href="mailto://brian.stansberry@jboss.com">Brian Stansberry</a>
 * @version $Revision$
 */
public class ChannelTestBase extends TestCase {

    protected final static Random RANDOM = new Random();

    private static final int LETTER_A = 64;

    protected final static String DEFAULT_MUX_FACTORY_COUNT = "4";

    protected static String CHANNEL_CONFIG = "udp.xml";

    protected static String MUX_CHANNEL_CONFIG = "stacks.xml";

    protected static String MUX_CHANNEL_CONFIG_STACK_NAME = "udp";

    protected int active_threads = 0;

    protected JChannelFactory muxFactory[] = null;

    protected String thread_dump = null;

    protected int currentChannelGeneratedName = LETTER_A;

    private static final int ROUTER_PORT = 12001;

    private static final String BIND_ADDR = "127.0.0.1";

    GossipRouter router = null;

    protected final Log log = LogFactory.getLog(this.getClass());

    public ChannelTestBase(){
        super();
    }

    public ChannelTestBase(String name){
        super(name);
    }

    protected void setUp() throws Exception {
        super.setUp();
        MUX_CHANNEL_CONFIG = System.getProperty("mux.conf", MUX_CHANNEL_CONFIG);
        MUX_CHANNEL_CONFIG_STACK_NAME = System.getProperty("mux.conf.stack",
                                                           MUX_CHANNEL_CONFIG_STACK_NAME);
        CHANNEL_CONFIG = System.getProperty("channel.conf", CHANNEL_CONFIG);

        currentChannelGeneratedName = LETTER_A;

        if(isTunnelUsed()){
            router = new GossipRouter(ROUTER_PORT, BIND_ADDR);
            router.start();
        }

        if(isMuxChannelUsed()){
            muxFactory = new JChannelFactory[getMuxFactoryCount()];

            for(int i = 0;i < muxFactory.length;i++){
                muxFactory[i] = new JChannelFactory();
                muxFactory[i].setMultiplexerConfig(MUX_CHANNEL_CONFIG);
            }
        }

        if(shouldCompareThreadCount()){
            active_threads = Thread.activeCount();
            thread_dump = "active threads before (" + active_threads
                          + "):\n"
                          + Util.activeThreads();
        }
    }

    protected static boolean isTunnelUsed() {

        String channelConf = System.getProperty("channel.conf","");
        String channelFlushConf = System.getProperty("channel.conf.flush","");
        // TODO add maybe a bit more foolproof check later
        return channelConf.contains("tunnel") || channelFlushConf.contains("tunnel");
    }

    protected void tearDown() throws Exception {
        super.tearDown();

        if(isMuxChannelUsed()){
            for(int i = 0;i < muxFactory.length;i++){
                muxFactory[i].destroy();
            }
        }

        if(router != null){
            router.stop();
            // TODO ensure proper thread/socket cleanup when stopping
            // GossipRouter
            Util.sleep(100);
        }

        if(shouldCompareThreadCount()){
            // at the moment Thread.activeCount() is called
            // it might count in threads that are just being
            // excluded from active count.

            // Therefore we include a slight delay of 20 msec

            Util.sleep(20);
            int current_active_threads = Thread.activeCount();

            String msg = "";
            if(active_threads != current_active_threads){
                System.out.println(thread_dump);
                System.out.println("active threads after (" + current_active_threads
                                   + "):\n"
                                   + Util.activeThreads());
                msg = "active threads:\n" + Util.dumpThreads();
            }
            assertEquals(msg, active_threads, current_active_threads);
        }
    }

    /**
     * Returns an array of mux application/service names with a guarantee that:
     * <p> - there are no application/service name collissions on top of one
     * channel (i.e cannot have two application/service(s) with the same name on
     * top of one channel)
     * <p> - each generated application/service name is guaranteed to have a
     * corresponding pair application/service with the same name on another
     * channel
     * 
     * @param muxApplicationstPerChannelCount
     * @return
     */
    protected String[] createMuxApplicationNames(int muxApplicationstPerChannelCount) {
        return createMuxApplicationNames(muxApplicationstPerChannelCount, getMuxFactoryCount());
    }

    /**
     * Returns an array of mux application/service names with a guarantee that:
     * <p> - there are no application/service name collissions on top of one
     * channel (i.e cannot have two application/service(s) with the same name on
     * top of one channel)
     * <p> - each generated application/service name is guaranteed to have a
     * corresponding pair application/service with the same name on another
     * channel
     * 
     * @param muxApplicationstPerChannelCount
     * @param muxFactoryCount
     *            how many mux factories should be used (has to be less than
     *            getMuxFactoryCount())
     * @return array of mux application id's represented as String objects
     */
    protected String[] createMuxApplicationNames(int muxApplicationstPerChannelCount,
                                                 int muxFactoryCount) {
        if(muxFactoryCount > getMuxFactoryCount()){
            throw new IllegalArgumentException("Parameter muxFactoryCount hs to be less than or equal to getMuxFactoryCount()");
        }

        int startLetter = LETTER_A;
        String names[] = null;
        int totalMuxAppCount = muxFactoryCount * muxApplicationstPerChannelCount;
        names = new String[totalMuxAppCount];

        boolean pickNextLetter = false;
        for(int i = 0;i < totalMuxAppCount;i++){
            pickNextLetter = (i % muxFactoryCount == 0);
            if(pickNextLetter){
                startLetter++;
            }
            names[i] = Character.toString((char) startLetter);
        }
        return names;
    }

    /**
     * Returns channel name as String next in alphabetic sequence since
     * getNextChannelName() has been called last. Sequence is restarted to
     * letter "A" after each setUp call.
     * 
     * @return
     */
    protected String getNextChannelName() {
        return Character.toString((char) ++currentChannelGeneratedName);
    }

    protected String[] createApplicationNames(int applicationCount) {
        String names[] = new String[applicationCount];
        for(int i = 0;i < applicationCount;i++){
            names[i] = getNextChannelName();
        }
        return names;
    }

    protected JChannel createChannel(Object id) throws Exception {
       return (JChannel) new DefaultChannelTestFactory().createChannel(id);
    }

    protected JChannel createChannel() throws Exception {
        return createChannel("A");
    }

    /**
     * Default channel factory used in junit tests
     */
    protected class DefaultChannelTestFactory implements ChannelTestFactory {
       

        protected JChannel createChannel(String configFile, boolean useBlocking)
                throws Exception {
            Map<Integer,Object> channelOptions = new HashMap<Integer,Object>();
            channelOptions.put(Channel.BLOCK, useBlocking);
            return createChannel(configFile, channelOptions);
        }

        protected JChannel createChannel(String configFile, Map<Integer,Object> channelOptions)
                throws Exception {
            JChannel ch = null;
            log.info("Using configuration file " + configFile);
            ch = new JChannel(configFile);
            for (Map.Entry<Integer,Object> entry: channelOptions.entrySet()) {
                Integer key = entry.getKey();
                Object value = entry.getValue();
                ch.setOpt(key, value);
            }
            return ch;
        }

        public Channel createChannel(Object id) throws Exception {
            JChannel c = null;
            if (isMuxChannelUsed()) {
                synchronized(muxFactory) {
                    log.info("Using configuration file " + MUX_CHANNEL_CONFIG
                             + ", stack is "
                             + MUX_CHANNEL_CONFIG_STACK_NAME);
                    for(int i=0;i < muxFactory.length;i++) {
                        if(!muxFactory[i].hasMuxChannel(MUX_CHANNEL_CONFIG_STACK_NAME,
                                                        id.toString())) {
                            c=(JChannel)muxFactory[i].createMultiplexerChannel(MUX_CHANNEL_CONFIG_STACK_NAME,
                                                                               id.toString());
                            if(useBlocking()) {
                                c.setOpt(Channel.BLOCK, Boolean.TRUE);
                            }
                            return c;
                        }
                    }
                }

                throw new Exception(
                        "Cannot create mux channel with id "
                                + id
                                + " since all currently used channels have already registered service with that id");
            } else {
                c = createChannel(CHANNEL_CONFIG, useBlocking());
            }
            return c;
        }
    }

    public class NextAvailableMuxChannelTestFactory implements ChannelTestFactory {
        public Channel createChannel(Object id) throws Exception {
            return ChannelTestBase.this.createChannel(id);
        }
    }

    /**
     * Decouples channel creation for junit tests
     */
    protected interface ChannelTestFactory {
        public Channel createChannel(Object id) throws Exception;
    }
    
    interface EventSequence{
        List<Object> getEvents();
        String getName();
    }

    /**
     * Base class for all aplications using channel
     */
    protected abstract class ChannelApplication implements EventSequence, Runnable, ChannelRetrievable {
        protected Channel channel;

        protected Thread thread;

        protected Throwable exception;

        protected String name;

        public ChannelApplication(String name) throws Exception{
            ChannelTestBase.this.createChannel(name);
        }      

        /**
         * Creates a unconnected channel and assigns a name to it.
         * 
         * @param name
         *            name of this channel
         * @param factory
         *            factory to create Channel
         * @throws ChannelException
         */       
        public ChannelApplication (String name, ChannelTestFactory factory) throws Exception {
           this.name = name;
           channel = factory.createChannel(name);
        }

        /**
         * Method allowing implementation of specific test application level
         * logic
         * 
         * @throws Exception
         */
        protected abstract void useChannel() throws Exception;

        public void run() {
            try{
                useChannel();
            }catch(Exception e){
                log.error(name + ": " + e.getLocalizedMessage(), e);
                exception = e; // Save it for the test to check
            }
        }
            
        public boolean isUsingMuxChannel() {
            return channel instanceof MuxChannel;
        }

        public Address getLocalAddress() {
            return channel.getLocalAddress();
        }

        public void start() {
            thread = new Thread(this, getName());
            thread.start();
            Address a = getLocalAddress();
            boolean connected = a != null;
            if(connected){
                log.info("Thread for channel " + a + "[" + getName() + "] started");
            }else{
                log.info("Thread for channel [" + getName() + "] started");
            }
        }

        public void setChannel(Channel ch) {
            this.channel = ch;
        }

        public Channel getChannel() {
            return channel;
        }

        public String getName() {
            return name;
        }

        public void cleanup() {
            if(thread != null && thread.isAlive()){
                thread.interrupt();
            }
            Address a = getLocalAddress();
            boolean connected = a != null;
            if(connected){
                log.info("Closing channel " + a + "[" + getName() + "]");
            }else{
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

        public PushChannelApplication(String name) throws Exception{
            this(name, false);
        }        

        public PushChannelApplication(String name,boolean useDispatcher) throws Exception{
            this(name, new DefaultChannelTestFactory(), useDispatcher);
        }

        public PushChannelApplication(String name,ChannelTestFactory factory,boolean useDispatcher) throws Exception{
            super(name, factory);
            events = Collections.synchronizedList(new LinkedList<Object>());
            if(useDispatcher){
                dispatcher = new RpcDispatcher(channel, this, this, this);
            }else{
                channel.setReceiver(this);
            }
        }
        
        public List<Object> getEvents(){
            return events;
        }

        public RpcDispatcher getDispatcher() {
            return dispatcher;
        }

        public boolean hasDispatcher() {
            return dispatcher != null;
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

        public void receive(Message msg) {}

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
     * <p>
     * PushChannelApplicationWithSemaphore application will not proceed to
     * useChannel() until it acquires permit from semphore. After useChannel()
     * completes the acquired permit will be released. Test driver should
     * control how semaphore tickets are given and acquired.
     * 
     */
    protected abstract class PushChannelApplicationWithSemaphore extends PushChannelApplication {
        protected Semaphore semaphore;

        public PushChannelApplicationWithSemaphore(String name,
                                                   ChannelTestFactory factory,
                                                   Semaphore semaphore,
                                                   boolean useDispatcher) throws Exception{
            super(name, factory, useDispatcher);
            this.semaphore = semaphore;
        }

        protected PushChannelApplicationWithSemaphore(String name,Semaphore semaphore) throws Exception{
            this(name,semaphore,false);
        }      

        protected PushChannelApplicationWithSemaphore(String name,
                                                      Semaphore semaphore,
                                                      boolean useDispatcher) throws Exception{
            this(name, new DefaultChannelTestFactory(), semaphore, useDispatcher);
        }
        
        public void run() {
            boolean acquired = false;
            try{
                acquired = semaphore.tryAcquire(60000L, TimeUnit.MILLISECONDS);
                if(!acquired){
                    throw new Exception(name + " cannot acquire semaphore");
                }

                useChannel();
            }catch(Exception e){
                log.error(name + ": " + e.getLocalizedMessage(), e);
                // Save it for the test to check
                exception = e;
            }finally{
                if(acquired){
                    semaphore.release();
                }
            }
        }
    }  

    protected void checkEventStateTransferSequence(EventSequence receiver) {
        
        List<Object> events = receiver.getEvents();
        String eventString = "[" + receiver.getName() + ",events:" + events;
        log.info(eventString);        
        assertNotNull(events);
        assertTrue(events.size()>1);
        assertTrue("First event is not block but " + events.get(0),events.get(0) instanceof BlockEvent);
        assertTrue("Last event not unblock but " + events.get(events.size()-1),events.get(events.size()-1) instanceof UnblockEvent);
        int size = events.size();
        for(int i = 0;i < size;i++){
            Object event = events.get(i);
            if(event instanceof BlockEvent){
                if(i + 1 < size){
                    Object o = events.get(i + 1);
                    assertTrue("After Block should be state|unblock|view, but it is " + o.getClass() + ",events= "+ eventString,
                               o instanceof SetStateEvent || o instanceof GetStateEvent
                                       || o instanceof UnblockEvent
                                       || o instanceof View);
                }
                if(i > 0){
                    Object o = events.get(i - 1);
                    assertTrue("Before Block should be state or Unblock , but it is " + o.getClass() + ",events= " + eventString, 
                               o instanceof UnblockEvent);
                }
            }
            else if(event instanceof SetStateEvent){
                if(i + 1 < size){
                    Object o = events.get(i + 1);
                    assertTrue("After setstate should be unblock , but it is " + o.getClass() + ",events= " + eventString,
                               o instanceof UnblockEvent);
                }
                Object o = events.get(i - 1);
                assertTrue("Before setstate should be block|view, but it is " + o.getClass() + ",events= " + eventString,
                           o instanceof BlockEvent || o instanceof View);
            }
            else if(event instanceof GetStateEvent){
                if(i + 1 < size){
                    Object o = events.get(i + 1);
                    assertTrue("After getstate should be view/unblock/getstate , but it is " + o.getClass() + ",events= " + eventString,
                               o instanceof UnblockEvent || o instanceof View || o instanceof GetStateEvent); 
                }
                Object o = events.get(i - 1);
                assertTrue("Before state should be block/view/getstate , but it is " + o.getClass() + ",events= " + eventString,
                           o instanceof BlockEvent || o instanceof View || o instanceof GetStateEvent);
            }
            else if(event instanceof UnblockEvent){
                if(i + 1 < size){
                    Object o = events.get(i + 1);
                    assertTrue("After UnBlock should be Block , but it is " + o.getClass() + ",events= " + eventString,
                               o instanceof BlockEvent);
                }
                if(i > 0){
                    Object o = events.get(i - 1);
                    assertTrue("Before UnBlock should be block|state|view , but it is " + o.getClass() + ",events= " + eventString,
                               o instanceof SetStateEvent || o instanceof GetStateEvent
                                       || o instanceof BlockEvent
                                       || o instanceof View);
                }
            }

        }        
    }   

    protected void checkEventSequence(PushChannelApplication receiver, boolean isMuxUsed) {
        List<Object> events = receiver.getEvents();
        String eventString = "[" + receiver.getName()
                             + "|"
                             + receiver.getLocalAddress()
                             + ",events:"
                             + events;
        log.info(eventString);        
        assertNotNull(events);
        assertTrue(events.size()>1);
        assertTrue("First event is not block but " + events.get(0),events.get(0) instanceof BlockEvent);
        assertTrue("Last event not unblock but " + events.get(events.size()-1),events.get(events.size()-1) instanceof UnblockEvent);
        int size = events.size();
        for(int i = 0;i < size;i++){
            Object event = events.get(i);
            if(event instanceof BlockEvent){
                if(i + 1 < size){
                    Object ev = events.get(i + 1);
                    if(isMuxUsed){
                        assertTrue("After Block should be View or Unblock but it is " + ev.getClass() + ",events= " + eventString,
                                   ev instanceof View || ev instanceof UnblockEvent);
                    }else{
                        assertTrue("After Block should be View but it is " + ev.getClass() + ",events= " + eventString,
                                   ev instanceof View);
                    }
                }
                if(i > 0){
                    Object ev = events.get(i - 1);
                    assertTrue("Before Block should be Unblock but it is " + ev.getClass() + ",events= " + eventString,
                               ev instanceof UnblockEvent);
                }
            }
            else if(event instanceof View){
                if(i + 1 < size){
                    Object ev = events.get(i + 1);
                    assertTrue("After View should be Unblock but it is " + ev.getClass() + ",events= " + eventString,
                               ev instanceof UnblockEvent);
                }
                Object ev = events.get(i - 1);
                assertTrue("Before View should be Block but it is " + ev.getClass() + ",events= " + eventString,
                           ev instanceof BlockEvent);
            }
            else if(event instanceof UnblockEvent){
                if(i + 1 < size){
                    Object ev = events.get(i + 1);
                    assertTrue("After UnBlock should be Block but it is " + ev.getClass() + ",events= " + eventString,
                               ev instanceof BlockEvent);
                }

                Object ev = events.get(i - 1);
                if(isMuxUsed){
                    assertTrue("Before UnBlock should be View or Block but it is " + ev.getClass() + ",events= " + eventString,
                               ev instanceof View || ev instanceof BlockEvent);
                }else{
                    assertTrue("Before UnBlock should be View but it is " + ev.getClass() + ",events= " + eventString,
                               ev instanceof View);
                }
            }
        }       
    }
    

    public interface ChannelRetrievable {
        Channel getChannel();
    }

    /**
     * Returns true if JVM has been started with mux.on system property set to
     * true, false otherwise.
     * 
     * @return
     */
    protected static boolean isMuxChannelUsed() {
        return Boolean.valueOf(System.getProperty("mux.on", "false")).booleanValue();
    }

    /**
     * Returns true if JVM has been started with threadcount system property set
     * to true, false otherwise.
     * 
     * @return
     */
    protected static boolean shouldCompareThreadCount() {
        return Boolean.valueOf(System.getProperty("threadcount", "false")).booleanValue();
    }

    /**
     * Returns value of mux.factorycount system property has been set, otherwise
     * returns DEFAULT_MUX_FACTORY_COUNT.
     * 
     * @return
     */
    protected int getMuxFactoryCount() {
        return Integer.parseInt(System.getProperty("mux.factorycount", DEFAULT_MUX_FACTORY_COUNT));
    }

    /**
     * Returns true if JVM has been started with useBlocking system property set
     * to true, false otherwise.
     * 
     * @return
     */
    protected boolean useBlocking() {
        return Boolean.valueOf(System.getProperty("useBlocking", "false")).booleanValue();
    }

    /**
     * Checks each channel in the parameter array to see if it has the exact
     * same view as other channels in an array.
     */
    public static boolean areViewsComplete(Channel[] channels, int memberCount) {
        for(int i = 0;i < memberCount;i++){
            if(!isViewComplete(channels[i], memberCount)){
                return false;
            }
        }

        return true;
    }

    /**
     * Loops, continually calling
     * {@link #areViewsComplete(org.jgroups.tests.ChannelTestBase.ChannelRetrievable[], int)}
     * until it either returns true or <code>timeout</code> ms have elapsed.
     * 
     * @param channels
     *            channels which must all have consistent views
     * @param timeout
     *            max number of ms to loop
     * @throws RuntimeException
     *             if <code>timeout</code> ms have elapse without all channels
     *             having the same number of members.
     */
    public static void blockUntilViewsReceived(Channel[] channels, long timeout) {
        blockUntilViewsReceived(channels, channels.length, timeout);
    }   
    
    public static void blockUntilViewsReceived(List<? extends ChannelRetrievable> channels, int count, long timeout) {
        Channel[] cs = new Channel[channels.size()];
        int i = 0;
        for(ChannelRetrievable channel:channels) {
            cs[i++] = channel.getChannel();
        }
        blockUntilViewsReceived(cs, count, timeout);
    }
    
    public static void blockUntilViewsReceived(List<? extends ChannelRetrievable> channels,long timeout) {
        Channel[] cs = new Channel[channels.size()];
        int i = 0;
        for(ChannelRetrievable channel:channels) {
            cs[i++] = channel.getChannel();
        }
        blockUntilViewsReceived(cs, cs.length, timeout);
    }

    /**
     * Loops, continually calling
     * {@link #areViewsComplete(org.jgroups.tests.ChannelTestBase.ChannelRetrievable[], int)}
     * until it either returns true or <code>timeout</code> ms have elapsed.
     * 
     * @param channels
     *            channels which must all have consistent views
     * @param timeout
     *            max number of ms to loop
     * @throws RuntimeException
     *             if <code>timeout</code> ms have elapse without all channels
     *             having the same number of members.
     */
    public static void blockUntilViewsReceived(Channel[] channels, int count, long timeout) {
        long failTime = System.currentTimeMillis() + timeout;

        while(System.currentTimeMillis() < failTime){
            Util.sleep(100);
            if(areViewsComplete(channels, count)){
                return;
            }
        }

        StringBuilder sb = new StringBuilder();
        for (Channel c : channels) {            
            sb.append(c.getLocalAddress()+ ",view=" +c.getView().getMembers()+"|");
        }
        throw new RuntimeException("timed out before caches had complete views. Views are " + sb.toString());
    }
    
    public static void blockUntilViewsReceived(Channel channel, int count, long timeout) {
        long failTime=System.currentTimeMillis() + timeout;
        boolean timeouted = false;
        while(!isViewComplete(channel, count) && !timeouted) {            
            timeouted=System.currentTimeMillis() > failTime;
            Util.sleep(100);
        }        
        if(timeouted) {
            StringBuilder sb=new StringBuilder();
            sb.append(channel.getLocalAddress() + ",view=" + channel.getView().getMembers() + "|");
            throw new RuntimeException("timed out before caches had complete views. Views are " + sb.toString());
        }
    }
    
    /**
     * Loops, continually calling
     * {@link #areViewsComplete(org.jgroups.tests.ChannelTestBase.ChannelRetrievable[], int)}
     * until it either returns true or <code>timeout</code> ms have elapsed.
     * 
     * @param channels
     *                channels which must all have consistent views
     * @param timeout
     *                max number of ms to loop
     * @throws RuntimeException
     *                 if <code>timeout</code> ms have elapse without all
     *                 channels having the same number of members.
     */
    public static void blockUntilViewsReceived(ChannelRetrievable[] channels, int count, long timeout) {
        List<ChannelRetrievable> list=Arrays.asList(channels);
        blockUntilViewsReceived(list,count,timeout);
    }
    
    /**
     * Loops, continually calling
     * {@link #areViewsComplete(org.jgroups.tests.ChannelTestBase.ChannelRetrievable[], int)}
     * until it either returns true or <code>timeout</code> ms have elapsed.
     * 
     * @param channels
     *            channels which must all have consistent views
     * @param timeout
     *            max number of ms to loop
     * @throws RuntimeException
     *             if <code>timeout</code> ms have elapse without all channels
     *             having the same number of members.
     */
    public static void blockUntilViewsReceived(ChannelRetrievable[] channels, long timeout) {
        List<ChannelRetrievable> list=Arrays.asList(channels);
        blockUntilViewsReceived(list,list.size(),timeout);
    }

    public static void blockUntilViewsReceived(Collection<Channel> channels, int count, long timeout) {
        long failTime = System.currentTimeMillis() + timeout;

        while(System.currentTimeMillis() < failTime){
            Util.sleep(100);
            if(areViewsComplete(channels.toArray(new Channel[]{}),
                                count)){
                return;
            }
        }

        throw new RuntimeException("timed out before caches had complete views");
    }

    public static boolean isViewComplete(Channel channel, int memberCount) {

        List<Address> members = channel.getView().getMembers();
        if(members == null || memberCount > members.size()){
            return false;
        }else if(memberCount < members.size()){
            // This is an exceptional condition
            StringBuilder sb = new StringBuilder("Channel at address ");
            sb.append(channel.getLocalAddress());
            sb.append(" had ");
            sb.append(members.size());
            sb.append(" members; expecting ");
            sb.append(memberCount);
            sb.append(". Members were (");
            for(int j = 0;j < members.size();j++){
                if(j > 0){
                    sb.append(", ");
                }
                sb.append(members.get(j));
            }
            sb.append(')');

            throw new IllegalStateException(sb.toString());
        }

        return true;
    }

    public static void sleepRandom(int minTime,int maxTime) {
        int nextInt = RANDOM.nextInt(maxTime);
        if (nextInt <minTime)
            nextInt = minTime;
        Util.sleep(nextInt);
    }

}
