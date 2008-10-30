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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

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

    protected int active_threads = 0;

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
        CHANNEL_CONFIG = System.getProperty("channel.conf", CHANNEL_CONFIG);

        currentChannelGeneratedName = LETTER_A;

        if(isTunnelUsed()){
            router = new GossipRouter(ROUTER_PORT, BIND_ADDR);
            router.start();
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

        if(router != null){
            router.stop();
            // TODO ensure proper thread/socket cleanup when stopping GossipRouter
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
            return createChannel(CHANNEL_CONFIG, useBlocking());
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
    
    /**
     * Method for translating event traces into event strings, where each event
     * in the trace is represented by a letter.
     */
    public static String translateEventTrace(List<Object> et) throws Exception {

        String eventString="";

        for(Iterator it=et.iterator();it.hasNext();) {

            Object obj=it.next();

            if(obj instanceof BlockEvent)
                eventString+="b";
            else if(obj instanceof UnblockEvent)
                eventString+="u";
            else if(obj instanceof SetStateEvent)
                eventString+="s";
            else if(obj instanceof GetStateEvent)
                eventString+="g";
            else if(obj instanceof View)
                eventString+="v";
            else
                throw new Exception("Unrecognized event type in event trace");
        }
        return eventString;
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

    /**
     * Method for validating event traces.
     */
    public static boolean validateEventTrace(List<Object> eventTrace) {


        final String validSequence="([b][vgs]*[u])+";

        String eventString=null;
        boolean result = false;

        // translate the eventTrace to an eventString
        try {
            eventString=translateEventTrace(eventTrace);
            result = validateEventString(eventString, validSequence);
        }
        catch(Exception e) {
            e.printStackTrace();
        }      
        return result;
        
    } 

    protected static void checkEventStateTransferSequence(EventSequence receiver) {

        List<Object> events=receiver.getEvents();
        assertNotNull(events);
        final String validSequence="([b][vgs]*[u])+";     
        // translate the eventTrace to an eventString   
        try {           
            assertTrue("Invalid event sequence " + events, validateEventString(translateEventTrace(events), validSequence));          
        }
        catch(Exception e) {
            fail("Invalid event sequence " + events);
        }        
    }     
    

    public interface ChannelRetrievable {
        Channel getChannel();
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

        View view=channel.getView();
        List<Address> members = view !=null? view.getMembers(): null;
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
