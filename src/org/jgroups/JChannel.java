package org.jgroups;

import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.blocks.MethodCall;
import org.jgroups.conf.ConfiguratorFactory;
import org.jgroups.conf.ProtocolConfiguration;
import org.jgroups.conf.ProtocolStackConfigurator;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.TP;
import org.jgroups.stack.Configurator;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.stack.StateTransferInfo;
import org.jgroups.util.*;
import org.w3c.dom.Element;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Exchanger;


/**
 * JChannel is a pure Java implementation of Channel.
 * When a JChannel object is instantiated it automatically sets up the
 * protocol stack.
 * <p>
 * <B>Properties</B>
 * <P>
 * Properties are used to configure a channel, and are accepted in
 * several forms; the String form is described here.
 * A property string consists of a number of properties separated by
 * colons.  For example:
 * <p>
 * <pre>"&lt;prop1&gt;(arg1=val1):&lt;prop2&gt;(arg1=val1;arg2=val2):&lt;prop3&gt;:&lt;propn&gt;"</pre>
 * <p>
 * Each property relates directly to a protocol layer, which is
 * implemented as a Java class. When a protocol stack is to be created
 * based on the above property string, the first property becomes the
 * bottom-most layer, the second one will be placed on the first, etc.:
 * the stack is created from the bottom to the top, as the string is
 * parsed from left to right. Each property has to be the name of a
 * Java class that resides in the
 * {@link org.jgroups.protocols} package.
 * <p>
 * Note that only the base name has to be given, not the fully specified
 * class name (e.g., UDP instead of org.jgroups.protocols.UDP).
 * <p>
 * Each layer may have 0 or more arguments, which are specified as a
 * list of name/value pairs in parentheses directly after the property.
 * In the example above, the first protocol layer has 1 argument,
 * the second 2, the third none. When a layer is created, these
 * properties (if there are any) will be set in a layer by invoking
 * the layer's setProperties() method
 * <p>
 * As an example the property string below instructs JGroups to create
 * a JChannel with protocols UDP, PING, FD and GMS:<p>
 * <pre>"UDP(mcast_addr=228.10.9.8;mcast_port=5678):PING:FD:GMS"</pre>
 * <p>
 * The UDP protocol layer is at the bottom of the stack, and it
 * should use mcast address 228.10.9.8. and port 5678 rather than
 * the default IP multicast address and port. The only other argument
 * instructs FD to output debug information while executing.
 * Property UDP refers to a class {@link org.jgroups.protocols.UDP},
 * which is subsequently loaded and an instance of which is created as protocol layer.
 * If any of these classes are not found, an exception will be thrown and
 * the construction of the stack will be aborted.
 *
 * @author Bela Ban
 */
@MBean(description="JGroups channel")
public class JChannel extends Channel {

    /** The default protocol stack used by the default constructor  */
    public static final String DEFAULT_PROTOCOL_STACK="udp.xml";

    /*the address of this JChannel instance*/
    protected UUID local_addr=null;

    protected String name=null;

    /*the channel (also know as group) name*/
    private String cluster_name=null;  // group name
    /*the latest view of the group membership*/
    private View my_view=null;
    /*the queue that is used to receive messages (events) from the protocol stack*/
    private final Queue mq=new Queue();
    /*the protocol stack, used to send and receive messages from the protocol stack*/
    private ProtocolStack prot_stack=null;

    private final Promise<Boolean> state_promise=new Promise<Boolean>();

    private final Exchanger<StateTransferInfo> applstate_exchanger=new Exchanger<StateTransferInfo>();
    
    /*flag to indicate whether to receive blocks, if this is set to true, receive_views is set to true*/
    @ManagedAttribute(description="Flag indicating whether to receive blocks",writable=true)
    private boolean receive_blocks=false;
    
    /*flag to indicate whether to receive local messages
     *if this is set to false, the JChannel will not receive messages sent by itself*/
    @ManagedAttribute(description="Flag indicating whether to receive this channel's own messages",writable=true)
    private boolean receive_local_msgs=true;
    
    /*channel connected flag*/
    protected volatile boolean connected=false;

    /*channel closed flag*/
    protected volatile boolean closed=false;      // close() has been called, channel is unusable

    /** True if a state transfer protocol is available, false otherwise */
    private boolean state_transfer_supported=false; // set by CONFIG event from STATE_TRANSFER protocol

    /** True if a flush protocol is available, false otherwise */
    private volatile boolean flush_supported=false; // set by CONFIG event from FLUSH protocol

    /** Provides storage for arbitrary objects. Protocols can send up CONFIG events, and all key-value pairs of
     * a CONFIG event will be added to additional_data. On reconnect, a CONFIG event will be sent down by the channel,
     * containing all key-value pairs of additional_data
     */
    protected final Map<String,Object> additional_data=new HashMap<String,Object>();
    
    protected final ConcurrentMap<String,Object> config=Util.createConcurrentMap(16);

    protected final Log log=LogFactory.getLog(JChannel.class);

    /** Collect statistics */
    @ManagedAttribute(description="Collect channel statistics",writable=true)
    protected boolean stats=true;

    protected long sent_msgs=0, received_msgs=0, sent_bytes=0, received_bytes=0;

    private final TP.ProbeHandler probe_handler=new MyProbeHandler();



    /**
     * Creates a JChannel without a protocol stack; used for programmatic creation of channel and protocol stack
     * @param create_protocol_stack If true, tthe default configuration will be used. If false, no protocol stack
     * will be created
     */
    public JChannel(boolean create_protocol_stack) {
        if(create_protocol_stack) {
            try {
                init(ConfiguratorFactory.getStackConfigurator(DEFAULT_PROTOCOL_STACK));
            }
            catch(ChannelException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Constructs a <code>JChannel</code> instance with the protocol stack
     * specified by the <code>DEFAULT_PROTOCOL_STACK</code> member.
     *
     * @throws ChannelException if problems occur during the initialization of
     *                          the protocol stack.
     */
    public JChannel() throws ChannelException {
        this(DEFAULT_PROTOCOL_STACK);
    }

    /**
     * Constructs a <code>JChannel</code> instance with the protocol stack
     * configuration contained by the specified file.
     *
     * @param properties a file containing a JGroups XML protocol stack
     *                   configuration.
     *
     * @throws ChannelException if problems occur during the configuration or
     *                          initialization of the protocol stack.
     */
    public JChannel(File properties) throws ChannelException {
        this(ConfiguratorFactory.getStackConfigurator(properties));
    }

    /**
     * Constructs a <code>JChannel</code> instance with the protocol stack
     * configuration contained by the specified XML element.
     *
     * @param properties a XML element containing a JGroups XML protocol stack
     *                   configuration.
     *
     * @throws ChannelException if problems occur during the configuration or
     *                          initialization of the protocol stack.
     */
    public JChannel(Element properties) throws ChannelException {
        this(ConfiguratorFactory.getStackConfigurator(properties));
    }

    /**
     * Constructs a <code>JChannel</code> instance with the protocol stack
     * configuration indicated by the specified URL.
     *
     * @param properties a URL pointing to a JGroups XML protocol stack
     *                   configuration.
     *
     * @throws ChannelException if problems occur during the configuration or
     *                          initialization of the protocol stack.
     */
    public JChannel(URL properties) throws ChannelException {
        this(ConfiguratorFactory.getStackConfigurator(properties));
    }

    /**
     * Constructs a <code>JChannel</code> instance with the protocol stack
     * configuration based upon the specified properties parameter.
     *
     * @param properties an old style property string, a string representing a
     *                   system resource containing a JGroups XML configuration,
     *                   a string representing a URL pointing to a JGroups XML
     *                   XML configuration, or a string representing a file name
     *                   that contains a JGroups XML configuration.
     *
     * @throws ChannelException if problems occur during the configuration and
     *                          initialization of the protocol stack.
     */
    public JChannel(String properties) throws ChannelException {
        this(ConfiguratorFactory.getStackConfigurator(properties));
    }

    /**
     * Constructs a <code>JChannel</code> instance with the protocol stack
     * configuration contained by the protocol stack configurator parameter.
     * <p>
     * All of the public constructors of this class eventually delegate to this
     * method.
     *
     * @param configurator a protocol stack configurator containing a JGroups
     *                     protocol stack configuration.
     *
     * @throws ChannelException if problems occur during the initialization of
     *                          the protocol stack.
     */
    public JChannel(ProtocolStackConfigurator configurator) throws ChannelException {
        init(configurator);
    }




    /**
     * Creates a new JChannel with the protocol stack as defined in the properties
     * parameter. an example of this parameter is<BR>
     * "UDP:PING:FD:STABLE:NAKACK:UNICAST:FRAG:FLUSH:GMS:VIEW_ENFORCER:STATE_TRANSFER:QUEUE"<BR>
     * Other examples can be found in the ./conf directory<BR>
     * @param properties the protocol stack setup; if null, the default protocol stack will be used.
     * 					 The properties can also be a java.net.URL object or a string that is a URL spec.
     *                   The JChannel will validate any URL object and String object to see if they are a URL.
     *                   In case of the parameter being a url, the JChannel will try to load the xml from there.
     *                   In case properties is a org.w3c.dom.Element, the ConfiguratorFactory will parse the
     *                   DOM tree with the element as its root element.
     * @deprecated Use the constructors with specific parameter types instead.
     */
    public JChannel(Object properties) throws ChannelException {
        if (properties == null)
            properties = DEFAULT_PROTOCOL_STACK;

        ProtocolStackConfigurator c;

        try {
            c=ConfiguratorFactory.getStackConfigurator(properties);
        }
        catch(Exception x) {
            throw new ChannelException("unable to load protocol stack", x);
        }
        init(c);
    }


    /**
     * Creates a channel with the same configuration as the channel passed to this constructor. This is used by
     * testing code, and should not be used by any other code !
     * @param ch
     * @throws ChannelException
     */
    public JChannel(JChannel ch) throws ChannelException {
        init(ch);
        receive_blocks=ch.receive_blocks;
        receive_local_msgs=ch.receive_local_msgs;
    }


 
    /**
     * Returns the protocol stack
     */
    public ProtocolStack getProtocolStack() {
        return prot_stack;
    }

    public void setProtocolStack(ProtocolStack stack) {
        this.prot_stack=stack;
        if(prot_stack != null)
            prot_stack.setChannel(this);
    }

    protected Log getLog() {
        return log;
    }

    /**
     * Returns the protocol stack configuration in string format. An example of this property is<br/>
     * "UDP:PING:FD:STABLE:NAKACK:UNICAST:FRAG:FLUSH:GMS:VIEW_ENFORCER:STATE_TRANSFER:QUEUE"
     */
    public String getProperties() {
        return prot_stack != null? prot_stack.printProtocolSpec(true) : null;
    }

    public boolean statsEnabled() {
        return stats;
    }

    public void enableStats(boolean stats) {
        this.stats=stats;
    }

    @ManagedOperation
    public void resetStats() {
        sent_msgs=received_msgs=sent_bytes=received_bytes=0;
    }

    @ManagedAttribute
    public long getSentMessages() {return sent_msgs;}
    @ManagedAttribute
    public long getSentBytes() {return sent_bytes;}
    @ManagedAttribute
    public long getReceivedMessages() {return received_msgs;}
    @ManagedAttribute
    public long getReceivedBytes() {return received_bytes;}
    @ManagedAttribute
    public int getNumberOfTasksInTimer() {
        TimeScheduler timer=getTimer();
        return timer != null? timer.size() : -1;
    }

    @ManagedAttribute
    public int getTimerThreads() {
        TimeScheduler timer=getTimer();
        return timer != null? timer.getMinThreads() : -1;
    }

    @ManagedOperation
    public String dumpTimerQueue() {
        TimeScheduler timer=getTimer();
        return timer != null? timer.dumpTimerTasks() : "<n/a";
    }

    /**
     * Returns a pretty-printed form of all the protocols. If include_properties
     * is set, the properties for each protocol will also be printed.
     */
    @ManagedOperation
    public String printProtocolSpec(boolean include_properties) {
        ProtocolStack ps=getProtocolStack();
        return ps != null? ps.printProtocolSpec(include_properties) : null;
    }


    /**
     * Connects the channel to a group.
     * If the channel is already connected, an error message will be printed to the error log.
     * If the channel is closed a ChannelClosed exception will be thrown.
     * This method starts the protocol stack by calling ProtocolStack.start,
     * then it sends an Event.CONNECT event down the stack and waits for the return value.
     * Once the call returns, the channel listeners are notified and the channel is considered connected.
     *
     * @param cluster_name A <code>String</code> denoting the group name. Cannot be null.
     * @exception ChannelException The protocol stack cannot be started
     * @exception ChannelClosedException The channel is closed and therefore cannot be used any longer.
     *                                   A new channel has to be created first.
     */
    @ManagedOperation(description="Connects the channel to a group")
    public synchronized void connect(String cluster_name) throws ChannelException {
    	connect(cluster_name,true);
    }

    /**
     * Connects the channel to a group.
     * If the channel is already connected, an error message will be printed to the error log.
     * If the channel is closed a ChannelClosed exception will be thrown.
     * This method starts the protocol stack by calling ProtocolStack.start,
     * then it sends an Event.CONNECT event down the stack and waits for the return value.
     * Once the call returns, the channel listeners are notified and the channel is considered connected.
     *
     * @param cluster_name A <code>String</code> denoting the group name. Cannot be null.
     * @exception ChannelException The protocol stack cannot be started
     * @exception ChannelClosedException The channel is closed and therefore cannot be used any longer.
     *                                   A new channel has to be created first.
     */
    @ManagedOperation(description="Connects the channel to a group")
    public synchronized void connect(String cluster_name, boolean useFlushIfPresent) throws ChannelException {
        if (connected) {
            if (log.isTraceEnabled())
                log.trace("already connected to " + cluster_name);
            return;
        }

        setAddress();
        startStack(cluster_name);

        if (cluster_name != null) { // only connect if we are not a unicast channel
            
            Event connect_event;
            if (useFlushIfPresent) {
                connect_event = new Event(Event.CONNECT_USE_FLUSH, cluster_name);
            } else {
                connect_event = new Event(Event.CONNECT, cluster_name);
            }

            // waits forever until connected (or channel is closed)
            Object res = downcall(connect_event); 
            if (res != null && res instanceof Exception) {
                // the JOIN was rejected by the coordinator
                stopStack(true, false);
                init();
                throw new ChannelException("connect() failed", (Throwable) res);
            }            
        }
        connected = true;
        notifyChannelConnected(this);
    }

    /**
     * Connects this channel to a group and gets a state from a specified state
     * provider.
     * <p>
     * 
     * This method essentially invokes
     * <code>connect<code> and <code>getState<code> methods successively. 
     * If FLUSH protocol is in channel's stack definition only one flush is executed for both connecting and 
     * fetching state rather than two flushes if we invoke <code>connect<code> and <code>getState<code> in succesion. 
     *   
     * If the channel is already connected, an error message will be printed to the error log.
     * If the channel is closed a ChannelClosed exception will be thrown.
     * 
     *                                       
     * @param cluster_name  the cluster name to connect to. Cannot be null.
     * @param target the state provider. If null state will be fetched from coordinator, unless this channel is coordinator.
     * @param state_id the substate id for partial state transfer. If null entire state will be transferred. 
     * @param timeout the timeout for state transfer.      
     * 
     * @exception ChannelException The protocol stack cannot be started
     * @exception ChannelException Connecting to cluster was not successful 
     * @exception ChannelClosedException The channel is closed and therefore cannot be used any longer.
     *                                   A new channel has to be created first.
     * @exception StateTransferException State transfer was not successful
     *
     */
    public synchronized void connect(String cluster_name,
                                     Address target,
                                     String state_id,
                                     long timeout) throws ChannelException {
    	connect(cluster_name, target, state_id, timeout,true);
    }

    
    /**
     * Connects this channel to a group and gets a state from a specified state
     * provider.
     * <p>
     * 
     * This method essentially invokes
     * <code>connect<code> and <code>getState<code> methods successively. 
     * If FLUSH protocol is in channel's stack definition only one flush is executed for both connecting and 
     * fetching state rather than two flushes if we invoke <code>connect<code> and <code>getState<code> in succesion. 
     *   
     * If the channel is already connected, an error message will be printed to the error log.
     * If the channel is closed a ChannelClosed exception will be thrown.
     * 
     *                                       
     * @param cluster_name  the cluster name to connect to. Cannot be null.
     * @param target the state provider. If null state will be fetched from coordinator, unless this channel is coordinator.
     * @param state_id the substate id for partial state transfer. If null entire state will be transferred. 
     * @param timeout the timeout for state transfer.      
     * 
     * @exception ChannelException The protocol stack cannot be started
     * @exception ChannelException Connecting to cluster was not successful 
     * @exception ChannelClosedException The channel is closed and therefore cannot be used any longer.
     *                                   A new channel has to be created first.
     * @exception StateTransferException State transfer was not successful
     *
     */
    public synchronized void connect(String cluster_name,
                                     Address target,
                                     String state_id,
                                     long timeout,
                                     boolean useFlushIfPresent) throws ChannelException {

        if(connected) {
            if(log.isTraceEnabled()) log.trace("already connected to " + this.cluster_name);
            return;
        }

        setAddress();
        startStack(cluster_name);

        boolean stateTransferOk;
        boolean joinSuccessful;
        boolean canFetchState=false;
        // only connect if we are not a unicast channel
        if(cluster_name == null)
            return;

        try {
            Event connect_event;
            if(useFlushIfPresent)
                connect_event=new Event(Event.CONNECT_WITH_STATE_TRANSFER_USE_FLUSH, cluster_name);
            else
                connect_event=new Event(Event.CONNECT_WITH_STATE_TRANSFER, cluster_name);

            Object res=downcall(connect_event); // waits forever until connected (or channel is closed)
            joinSuccessful=!(res != null && res instanceof Exception);
            if(!joinSuccessful) {
                stopStack(true, false);
                init();
                throw new ChannelException("connect() failed", (Throwable)res);
            }

            connected=true;
            notifyChannelConnected(this);
            canFetchState=getView() != null && getView().size() > 1;

            // if I am not the only member in cluster then
            if(canFetchState) {
                try {
                    // fetch state from target
                    stateTransferOk=getState(target, state_id, timeout, false);
                    if(!stateTransferOk) {
                        throw new StateTransferException(getAddress() + " could not fetch state "
                            + (state_id == null ? "(full)" : state_id) + " from "
                            + (target == null ? "(all)" : target));
                    }
                }
                catch(Exception e) {
                    throw new StateTransferException(getAddress() + " could not fetch state "
                        + (state_id == null ? "(full)" : state_id) + " from "
                        + (target == null ? "(all)" : target), e);
                }
            }

        }
        finally {
            if (flushSupported() && useFlushIfPresent){
                //stopFlush if we fetched the state or failed to connect...
                if(canFetchState || !connected)            
                    stopFlush();                             
            }
        }
    }


    /**
     * Disconnects the channel if it is connected. If the channel is closed,
     * this operation is ignored<BR>
     * Otherwise the following actions happen in the listed order<BR>
     * <ol>
     * <li> The JChannel sends a DISCONNECT event down the protocol stack<BR>
     * <li> Blocks until the event has returned<BR>
     * <li> Sends a STOP_QUEING event down the stack<BR>
     * <li> Stops the protocol stack by calling ProtocolStack.stop()<BR>
     * <li> Notifies the listener, if the listener is available<BR>
     * </ol>
     */
    @ManagedOperation(description="Disconnects the channel if connected")
    public synchronized void disconnect() {
        if(closed) return;

        if(connected) {

            if(cluster_name != null) {
                // Send down a DISCONNECT event, which travels down to the GMS, where a response is returned
                Event disconnect_event=new Event(Event.DISCONNECT, local_addr);
                down(disconnect_event);   // DISCONNECT is handled by each layer
            }
            connected=false;
            stopStack(true, false);            
            notifyChannelDisconnected(this);
            init(); // sets local_addr=null; changed March 18 2003 (bela) -- prevented successful rejoining
        }
    }


    /**
     * Destroys the channel.
     * After this method has been called, the channel us unusable.<BR>
     * This operation will disconnect the channel and close the channel receive queue immediately<BR>
     */
    @ManagedOperation(description="Disconnects and destroys the channel")
    public synchronized void close() {
        _close(true, true); // by default disconnect before closing channel and close mq
    }

    /**
     * Shuts down a channel without disconnecting. To be used by tests only, don't use for application purposes
     * @deprecated Use {@link Util#shutdown(Channel)} instead. This method will be removed in 3.0
     */
    @ManagedOperation(description="Shuts down the channel without disconnecting")
    @Deprecated
    public synchronized void shutdown() {
        try {
            Util.shutdown(this);
        }
        catch(Exception e) {
            log.error("failed shutting down channel " + getAddress(), e);
        }
    }

    /**
     * Opens the channel. Note that the channel is only open, but <em>not connected</em>.
     * This does the following actions:
     * <ol>
     * <li> Resets the receiver queue by calling Queue.reset
     * <li> Sets up the protocol stack by calling ProtocolStack.setup
     * <li> Sets the closed flag to false
     * </ol>
     * @deprecated With the removal of shunning, this method should not be used anymore
     */
    @Deprecated
    public synchronized void open() throws ChannelException {
        if(!closed)
            throw new ChannelException("channel is already open");

        try {
            mq.reset();

            String props=getProperties();
            List<ProtocolConfiguration> configs=Configurator.parseConfigurations(props);

            // new stack is created on open() - bela June 12 2003
            prot_stack=new ProtocolStack(this);
            prot_stack.setup(configs);
            closed=false;
        }
        catch(Exception e) {
            throw new ChannelException("failed to open channel" , e);
        }
    }

    /**
     * returns true if the Open operation has been called successfully
     */
    @ManagedAttribute
    public boolean isOpen() {
        return !closed;
    }


    /**
     * returns true if the Connect operation has been called successfully
     */
    @ManagedAttribute
    public boolean isConnected() {
        return connected;
    }

    @ManagedAttribute
    public int getNumMessages() {
        return mq.size();
    }

    @ManagedOperation
    public String dumpQueue() {
        return Util.dumpQueue(mq);
    }

    /**
     * Returns a map of statistics of the various protocols and of the channel itself.
     * @return Map<String,Map>. A map where the keys are the protocols ("channel" pseudo key is
     * used for the channel itself") and the values are property maps.
     */
    @ManagedOperation
    public Map<String,Object> dumpStats() {
        Map<String,Object> retval=prot_stack.dumpStats();
        if(retval != null) {
            Map<String,Long> tmp=dumpChannelStats();
            if(tmp != null)
                retval.put("channel", tmp);
        }
        return retval;
    }

    public Map<String,Object> dumpStats(String protocol_name, List<String> attrs) {
        return prot_stack.dumpStats(protocol_name, attrs);
    }

    @ManagedOperation
    public Map<String,Object> dumpStats(String protocol_name) {
        return prot_stack.dumpStats(protocol_name, null);
    }

    protected Map<String,Long> dumpChannelStats() {
        Map<String,Long> retval=new HashMap<String,Long>();
        retval.put("sent_msgs",      sent_msgs);
        retval.put("sent_bytes",     sent_bytes);
        retval.put("received_msgs",  received_msgs);
        retval.put("received_bytes", received_bytes);
        return retval;
    }


    /**
     * Sends a message through the protocol stack.
     * Implements the Transport interface.
     *
     * @param msg the message to be sent through the protocol stack,
     *        the destination of the message is specified inside the message itself
     * @exception ChannelNotConnectedException
     * @exception ChannelClosedException
     */
    @ManagedOperation
    public void send(Message msg) throws ChannelNotConnectedException, ChannelClosedException {
        checkClosedOrNotConnected();
        if(msg == null)
            throw new NullPointerException("msg is null");
        if(stats) {
            sent_msgs++;
            sent_bytes+=msg.getLength();
        }

        down(new Event(Event.MSG, msg));
    }


    /**
     * creates a new message with the destination address, and the source address
     * and the object as the message value
     * @param dst - the destination address of the message, null for all members
     * @param src - the source address of the message
     * @param obj - the value of the message
     * @exception ChannelNotConnectedException
     * @exception ChannelClosedException
     * @see JChannel#send
     */
    @ManagedOperation
    public void send(Address dst, Address src, Serializable obj) throws ChannelNotConnectedException, ChannelClosedException {
        send(new Message(dst, src, obj));
    }

    public void send(Address dst, Address src, byte[] buf) throws ChannelNotConnectedException, ChannelClosedException {
        send(new Message(dst, src, buf));
    }

    public void send(Address dst, Address src, byte[] buf, int offset, int length) throws ChannelNotConnectedException, ChannelClosedException {
        send(new Message(dst, src, buf, offset, length));
    }

    /**
     * Blocking receive method.
     * This method returns the object that was first received by this JChannel and that has not been
     * received before. After the object is received, it is removed from the receive queue.<BR>
     * If you only want to inspect the object received without removing it from the queue call
     * JChannel.peek<BR>
     * If no messages are in the receive queue, this method blocks until a message is added or the operation times out<BR>
     * By specifying a timeout of 0, the operation blocks forever, or until a message has been received.
     * @param timeout the number of milliseconds to wait if the receive queue is empty. 0 means wait forever
     * @exception TimeoutException if a timeout occured prior to a new message was received
     * @exception ChannelNotConnectedException
     * @exception ChannelClosedException
     * @see JChannel#peek
     * @deprecated Use a {@link Receiver} instead
     */
    public Object receive(long timeout) throws ChannelNotConnectedException, ChannelClosedException, TimeoutException {

        checkClosedOrNotConnected();

        try {
            Event evt=(timeout <= 0)? (Event)mq.remove() : (Event)mq.remove(timeout);
            Object retval=getEvent(evt);
            evt=null;
            return retval;
        }
        catch(QueueClosedException queue_closed) {
            throw new ChannelClosedException();
        }
        catch(TimeoutException t) {
            throw t;
        }
        catch(Exception e) {
            if(log.isErrorEnabled()) log.error("exception: " + e);
            return null;
        }
    }


    /**
     * Just peeks at the next message, view or block. Does <em>not</em> install
     * new view if view is received<BR>
     * Does the same thing as JChannel.receive but doesn't remove the object from the
     * receiver queue
     * * @deprecated Use a {@link Receiver} instead
     */
    public Object peek(long timeout) throws ChannelNotConnectedException, ChannelClosedException, TimeoutException {

        checkClosedOrNotConnected();

        try {
            Event evt=(timeout <= 0)? (Event)mq.peek() : (Event)mq.peek(timeout);
            Object retval=getEvent(evt);
            evt=null;
            return retval;
        }
        catch(QueueClosedException queue_closed) {
            if(log.isErrorEnabled()) log.error("exception: " + queue_closed);
            return null;
        }
        catch(TimeoutException t) {
            return null;
        }
        catch(Exception e) {
            if(log.isErrorEnabled()) log.error("exception: " + e);
            return null;
        }
    }




    /**
     * Returns the current view.
     * <BR>
     * If the channel is not connected or if it is closed it will return null.
     * <BR>
     * @return returns the current group view, or null if the channel is closed or disconnected
     */
    public View getView() {
        return closed || !connected ? null : my_view;
    }
    
    @ManagedAttribute(name="View")
    public String getViewAsString() {
        View v=getView();
        return v != null ? v.toString() : "n/a";
    }
    
    @ManagedAttribute
    public static String getVersion() {
        return Version.printDescription();
    }

    public Address getLocalAddress() {
        return getAddress();
    }

    /**
     * Returns the local address of the channel (null if the channel is closed)
     */
    public Address getAddress() {
        return closed ? null : local_addr;
    }

    @ManagedAttribute(name="Address")
    public String getAddressAsString() {
        return local_addr != null? local_addr.toString() : "n/a";
    }

    @ManagedAttribute(name="Address (UUID)")
    public String getAddressAsUUID() {
        return local_addr != null? local_addr.toStringLong() : null;
    }

    public String getName() {
        return name;
    }

    public String getName(Address member) {
        return member != null? UUID.get(member) : null;
    }

    /**
     * Sets the logical name for the channel. The name will stay associated with this channel for the channel's
     * lifetime (until close() is called). This method should be called <em>before</em> calling connect().<br/>
     * @param name
     */
    @ManagedAttribute(writable=true, description="The logical name of this channel. Stays with the channel until " +
            "the channel is closed")
    public void setName(String name) {
        if(name != null) {
            this.name=name;
            if(local_addr != null) {
                UUID.add(local_addr, this.name);
            }
        }
    }

    /**
     * returns the name of the channel
     * if the channel is not connected or if it is closed it will return null
     * @deprecated Use {@link #getClusterName()} instead
     */
    public String getChannelName() {
        return closed ? null : !connected ? null : cluster_name;
    }

    @ManagedAttribute(description="Returns cluster name this channel is connected to")
    public String getClusterName() {
        return closed ? null : !connected ? null : cluster_name;
    }


    /**
     * Sets a channel option.  The options can be one of the following:
     * <UL>
     * <LI>    Channel.BLOCK
     * <LI>    Channel.LOCAL
     * <LI>    Channel.AUTO_RECONNECT
     * <LI>    Channel.AUTO_GETSTATE
     * </UL>
     * <P>
     * There are certain dependencies between the options that you can set,
     * I will try to describe them here.
     * <P>
     * Option: Channel.BLOCK<BR>
     * Value:  java.lang.Boolean<BR>
     * Result: set to true will set setOpt(VIEW, true) and the JChannel will receive BLOCKS and VIEW events<BR>
     *<BR>
     * Option: LOCAL<BR>
     * Value:  java.lang.Boolean<BR>
     * Result: set to true the JChannel will receive messages that it self sent out.<BR>
     *<BR>
     * Option: AUTO_RECONNECT<BR>
     * Value:  java.lang.Boolean<BR>
     * Result: set to true and the JChannel will try to reconnect when it is being closed<BR>
     *<BR>
     * Option: AUTO_GETSTATE<BR>
     * Value:  java.lang.Boolean<BR>
     * Result: set to true, the AUTO_RECONNECT will be set to true and the JChannel will try to get the state after a close and reconnect happens<BR>
     * <BR>
     *
     * @param option the parameter option Channel.VIEW, Channel.SUSPECT, etc
     * @param value the value to set for this option
     *
     */
    public void setOpt(int option, Object value) {
        if(closed) {
            if(log.isWarnEnabled()) log.warn("channel is closed; option not set !");
            return;
        }

        switch(option) {
            case VIEW:
            case SUSPECT:
            case GET_STATE_EVENTS:
            case AUTO_RECONNECT:
            case AUTO_GETSTATE:
                break;
            case BLOCK:
                if(value instanceof Boolean)
                    receive_blocks=((Boolean)value).booleanValue();
                else
                    if(log.isErrorEnabled()) log.error("option " + Channel.option2String(option) +
                            " (" + value + "): value has to be Boolean");
                break;

            case LOCAL:
                if(value instanceof Boolean)
                    receive_local_msgs=((Boolean)value).booleanValue();
                else
                    if(log.isErrorEnabled()) log.error("option " + Channel.option2String(option) +
                            " (" + value + "): value has to be Boolean");
                break;

            default:
                if(log.isErrorEnabled()) log.error("option " + Channel.option2String(option) + " not known");
                break;
        }
    }


    /**
     * returns the value of an option.
     * @param option the option you want to see the value for
     * @return the object value, in most cases java.lang.Boolean
     * @see JChannel#setOpt
     */
    public Object getOpt(int option) {
        switch(option) {
            case VIEW:
            	return Boolean.TRUE;
            case BLOCK:
            	return receive_blocks;
            case SUSPECT:
            	return Boolean.TRUE;
            case AUTO_RECONNECT:
                return false;
            case AUTO_GETSTATE:
                return false;
            case GET_STATE_EVENTS:
                return Boolean.TRUE;
            case LOCAL:
            	return receive_local_msgs ? Boolean.TRUE : Boolean.FALSE;
            default:
                if(log.isErrorEnabled()) log.error("option " + Channel.option2String(option) + " not known");
                return null;
        }
    }


    /**
     * Called to acknowledge a block() (callback in <code>MembershipListener</code> or
     * <code>BlockEvent</code> received from call to <code>receive()</code>).
     * After sending blockOk(), no messages should be sent until a new view has been received.
     * Calling this method on a closed channel has no effect.
     */
    public void blockOk() {
        
    }


    /**
     * Retrieves a full state from the target member.
     * <p>
     * 
     * State transfer is initiated by invoking getState on this channel, state
     * receiver, and sending a GET_STATE message to a target member - state
     * provider. State provider passes GET_STATE message to application that is
     * using the state provider channel which in turn provides an application
     * state to a state receiver. Upon successful installation of a state at
     * state receiver this method returns true.
     * 
     * 
     * @param target
     *                State provider. If null, coordinator is used
     * @param timeout
     *                the number of milliseconds to wait for the operation to
     *                complete successfully. 0 waits until the state has been
     *                received  
     * 
     * @see ExtendedMessageListener#getState(OutputStream)
     * @see ExtendedMessageListener#setState(InputStream)
     * @see MessageListener#getState()
     * @see MessageListener#setState(byte[])
     * 
     * 
     * @return true if state transfer was successful, false otherwise
     * @throws ChannelNotConnectedException
     *                 if channel was not connected at the time state retrieval
     *                 was initiated
     * @throws ChannelClosedException
     *                 if channel was closed at the time state retrieval was
     *                 initiated
     * @throws IllegalStateException
     *                 if one of state transfer protocols is not present in this
     *                 channel
     * @throws IllegalStateException
     *                 if flush is used in this channel and cluster could not be
     *                 flushed
     */
    public boolean getState(Address target, long timeout) throws ChannelNotConnectedException, ChannelClosedException {
        return getState(target,null,timeout);
    }

    /**
     * Retrieves a substate (or partial state) indicated by state_id from the target member.
     * <p>
     * 
     * State transfer is initiated by invoking getState on this channel, state
     * receiver, and sending a GET_STATE message to a target member - state
     * provider. State provider passes GET_STATE message to application that is
     * using the state provider channel which in turn provides an application
     * state to a state receiver. Upon successful installation of a state at
     * state receiver this method returns true.
     * 
     * 
     * @param target
     *                State provider. If null, coordinator is used
     * @param state_id
     *                The ID of the substate. If null, the entire state will be
     *                transferred
     * @param timeout
     *                the number of milliseconds to wait for the operation to
     *                complete successfully. 0 waits until the state has been
     *                received
     *                    
     * @see ExtendedMessageListener#getState(OutputStream)
     * @see ExtendedMessageListener#setState(InputStream)
     * @see MessageListener#getState()
     * @see MessageListener#setState(byte[])
     * 
     * 
     * @return true if state transfer was successful, false otherwise
     * @throws ChannelNotConnectedException
     *                 if channel was not connected at the time state retrieval
     *                 was initiated
     * @throws ChannelClosedException
     *                 if channel was closed at the time state retrieval was
     *                 initiated
     * @throws IllegalStateException
     *                 if one of state transfer protocols is not present in this
     *                 channel
     * @throws IllegalStateException
     *                 if flush is used in this channel and cluster could not be
     *                 flushed
     */
    public boolean getState(Address target, String state_id, long timeout) throws ChannelNotConnectedException, ChannelClosedException {
        return getState(target, state_id, timeout, true);
    }
    
    /**
     * Retrieves a substate (or partial state) indicated by state_id from the target member.
     * <p>
     * 
     * State transfer is initiated by invoking getState on this channel, state
     * receiver, and sending a GET_STATE message to a target member - state
     * provider. State provider passes GET_STATE message to application that is
     * using the state provider channel which in turn provides an application
     * state to a state receiver. Upon successful installation of a state at
     * state receiver this method returns true.
     * 
     * 
     * @param target
     *                State provider. If null, coordinator is used
     * @param state_id
     *                The ID of the substate. If null, the entire state will be
     *                transferred
     * @param timeout
     *                the number of milliseconds to wait for the operation to
     *                complete successfully. 0 waits until the state has been
     *                received
     * @param useFlushIfPresent
     *                whether channel should be flushed prior to state retrieval
     * 
     * @see ExtendedMessageListener#getState(OutputStream)
     * @see ExtendedMessageListener#setState(InputStream)
     * @see MessageListener#getState()
     * @see MessageListener#setState(byte[])
     * 
     * 
     * @return true if state transfer was successful, false otherwise
     * @throws ChannelNotConnectedException
     *                 if channel was not connected at the time state retrieval
     *                 was initiated
     * @throws ChannelClosedException
     *                 if channel was closed at the time state retrieval was
     *                 initiated
     * @throws IllegalStateException
     *                 if one of state transfer protocols is not present in this
     *                 channel
     * @throws IllegalStateException
     *                 if flush is used in this channel and cluster could not be
     *                 flushed
     */    
    public boolean getState(Address target, String state_id, long timeout,
			boolean useFlushIfPresent) throws ChannelNotConnectedException,
			ChannelClosedException {
		
    	Callable<Boolean> flusher = new Callable<Boolean>() {
			public Boolean call() throws Exception {
				return Util.startFlush(JChannel.this);
			}
		};
		return getState(target, state_id, timeout, useFlushIfPresent?flusher:null);
	}
    
    /**
     * Retrieves a substate (or partial state) indicated by state_id from the target member.
     * <p>
     * 
     * State transfer is initiated by invoking getState on this channel, state
     * receiver, and sending a GET_STATE message to a target member - state
     * provider. State provider passes GET_STATE message to application that is
     * using the state provider channel which in turn provides an application
     * state to a state receiver. Upon successful installation of a state at
     * state receiver this method returns true.
     * 
     * 
     * @param target
     *                State provider. If null, coordinator is used
     * @param state_id
     *                The ID of the substate. If null, the entire state will be
     *                transferred
     * @param timeout
     *                the number of milliseconds to wait for the operation to
     *                complete successfully. 0 waits until the state has been
     *                received
     * @param flushInvoker
     *                algorithm invoking flush
     * 
     * @see ExtendedMessageListener#getState(OutputStream)
     * @see ExtendedMessageListener#setState(InputStream)
     * @see MessageListener#getState()
     * @see MessageListener#setState(byte[])
     * 
     * 
     * @return true if state transfer was successful, false otherwise
     * @throws ChannelNotConnectedException
     *                 if channel was not connected at the time state retrieval
     *                 was initiated
     * @throws ChannelClosedException
     *                 if channel was closed at the time state retrieval was
     *                 initiated
     * @throws IllegalStateException
     *                 if one of state transfer protocols is not present in this
     *                 channel
     * @throws IllegalStateException
     *                 if flush is used in this channel and cluster could not be
     *                 flushed
     */    
    protected boolean getState(Address target, String state_id, long timeout,Callable<Boolean> flushInvoker) throws ChannelNotConnectedException, ChannelClosedException {
        checkClosedOrNotConnected();
        if(!state_transfer_supported) {
            throw new IllegalStateException("fetching state will fail as state transfer is not supported. "
                    + "Add one of the STATE_TRANSFER protocols to your protocol configuration");
        }
        
        if(target == null)
            target=determineCoordinator();
        if(target != null && local_addr != null && target.equals(local_addr)) {
            if(log.isTraceEnabled())
                log.trace("cannot get state from myself (" + target + "): probably the first member");
            return false;
        }
              
        boolean initiateFlush = flushSupported() && flushInvoker!=null;
        
        if (initiateFlush) {
			boolean successfulFlush = false;
			try {
				successfulFlush = flushInvoker.call();
			} 
			catch (Exception e) {
				successfulFlush = false;
				// http://jira.jboss.com/jira/browse/JGRP-759
			} 
			finally {
				if (!successfulFlush) {
					throw new IllegalStateException("Node "+ local_addr+ " could not flush the cluster for state retrieval");
				}
			}
		}

        state_promise.reset();
        StateTransferInfo state_info=new StateTransferInfo(target, state_id, timeout);
        down(new Event(Event.GET_STATE, state_info));
        Boolean b=state_promise.getResult(state_info.timeout);
        
        if(initiateFlush)
            stopFlush();
        
        boolean state_transfer_successfull = b != null && b.booleanValue();
        if(!state_transfer_successfull)
            down(new Event(Event.RESUME_STABLE));
        return state_transfer_successfull;
    }


    /**
     * Retrieves the current group state. Sends GET_STATE event down to STATE_TRANSFER layer.
     * Blocks until STATE_TRANSFER sends up a GET_STATE_OK event or until <code>timeout</code>
     * milliseconds have elapsed. The argument of GET_STATE_OK should be a vector of objects.
     * @param targets - the target members to receive the state from ( an Address list )
     * @param timeout - the number of milliseconds to wait for the operation to complete successfully
     * @return true of the state was received, false if the operation timed out
     * @deprecated Not really needed - we always want to get the state from a single member,
     * use {@link #getState(org.jgroups.Address, long)} instead
     */
    public boolean getAllStates(Vector targets, long timeout) throws ChannelNotConnectedException, ChannelClosedException {
        throw new UnsupportedOperationException("use getState() instead");
    }


    /**
     * Called by the application is response to receiving a <code>getState()</code> object when
     * calling <code>receive()</code>.
     * When the application receives a getState() message on the receive() method,
     * it should call returnState() to reply with the state of the application
     * @param state The state of the application as a byte buffer
     *              (to send over the network).
     */
    public void returnState(byte[] state) {
        try {
            StateTransferInfo state_info=new StateTransferInfo(null, null, 0L, state);
            applstate_exchanger.exchange(state_info);
        }
        catch(InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Returns a substate as indicated by state_id
     * @param state
     * @param state_id
     */
    public void returnState(byte[] state, String state_id) {
        try {
            StateTransferInfo state_info=new StateTransferInfo(null, state_id, 0L, state);
            applstate_exchanger.exchange(state_info);
        }
        catch(InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }





    /**
     * Callback method <BR>
     * Called by the ProtocolStack when a message is received.
     * It will be added to the message queue from which subsequent
     * <code>Receive</code>s will dequeue it.
     * @param evt the event carrying the message from the protocol stack
     */
    public Object up(Event evt) {
        int     type=evt.getType();
        Message msg;


        switch(type) {

            case Event.MSG:
                msg=(Message)evt.getArg();
                if(stats) {
                    received_msgs++;
                    received_bytes+=msg.getLength();
                }

                if(!receive_local_msgs) {  // discard local messages (sent by myself to me)
                    if(local_addr != null && msg.getSrc() != null)
                        if(local_addr.equals(msg.getSrc()))
                            return null;
                }
                break;

            case Event.VIEW_CHANGE:
                View tmp=(View)evt.getArg();
                if(tmp instanceof MergeView)
                    my_view=new View(tmp.getVid(), tmp.getMembers());
                else
                    my_view=tmp;

                /*
             * Bela&Vladimir Oct 27th,2006 (JGroups 2.4)- we need to switch to
             * connected=true because client can invoke channel.getView() in
             * viewAccepted() callback invoked on this thread
             * (see Event.VIEW_CHANGE handling below)
             */

                // not good: we are only connected when we returned from connect() - bela June 22 2007
                // Changed: when a channel gets a view of which it is a member then it should be
                // connected even if connect() hasn't returned yet ! (bela Noc 2010)
                if(connected == false) {
                    connected=true;
                }
                break;

            case Event.CONFIG:
                Map<String,Object> cfg=(Map<String,Object>)evt.getArg();
                if(cfg != null) {
                    if(cfg.containsKey("state_transfer")) {
                        state_transfer_supported=((Boolean)cfg.get("state_transfer")).booleanValue();
                    }
                    if(cfg.containsKey("flush_supported")) {
                        flush_supported=((Boolean)cfg.get("flush_supported")).booleanValue();
                    }
                    cfg.putAll(cfg);
                }
                break;
                
            case Event.GET_STATE_OK:
                StateTransferInfo state_info=(StateTransferInfo)evt.getArg();
                byte[] state=state_info.state;

                try {
                    if(up_handler != null) {
                        return up_handler.up(evt);
                    }

                    if(state != null) {
                        String state_id=state_info.state_id;
                        if(receiver != null) {
                            try {
                                if(receiver instanceof ExtendedReceiver && state_id != null)
                                    ((ExtendedReceiver)receiver).setState(state_id, state);
                                else
                                    receiver.setState(state);
                            }
                            catch(Throwable t) {
                                if(log.isWarnEnabled())
                                    log.warn("failed calling setState() in receiver", t);
                            }
                        }
                        else {
                            try {
                                mq.add(new Event(Event.STATE_RECEIVED, state_info));
                            }
                            catch(Exception e) {
                            }
                        }
                    }
                }
                finally {
                    state_promise.setResult(state != null? Boolean.TRUE : Boolean.FALSE);
                }
                break;
            case Event.STATE_TRANSFER_INPUTSTREAM_CLOSED:
                state_promise.setResult(Boolean.TRUE);
                break;

            case Event.STATE_TRANSFER_INPUTSTREAM:
                StateTransferInfo sti=(StateTransferInfo)evt.getArg();
                InputStream is=sti.inputStream;
                //Oct 13,2006 moved to down() when Event.STATE_TRANSFER_INPUTSTREAM_CLOSED is received
                //state_promise.setResult(is != null? Boolean.TRUE : Boolean.FALSE);

                if(up_handler != null) {
                    return up_handler.up(evt);
                }

                if(is != null) {
                    if(receiver instanceof ExtendedReceiver) {
                        try {
                            if(sti.state_id == null)
                                ((ExtendedReceiver)receiver).setState(is);
                            else
                                ((ExtendedReceiver)receiver).setState(sti.state_id, is);
                        }
                        catch(Throwable t) {
                            if(log.isWarnEnabled())
                                log.warn("failed calling setState() in receiver", t);
                        }
                    }
                    else if(receiver instanceof Receiver){
                        if(log.isWarnEnabled()){
                            log.warn("Channel has STREAMING_STATE_TRANSFER, however," +
                                    " application does not implement ExtendedMessageListener. State is not transfered");
                            Util.close(is);
                        }
                    }
                    else {
                        try {
                            mq.add(new Event(Event.STATE_TRANSFER_INPUTSTREAM, sti));
                        }
                        catch(Exception e) {
                        }
                    }
                }
                break;

            case Event.GET_LOCAL_ADDRESS:
                return local_addr;

            default:
                break;
        }


        // If UpHandler is installed, pass all events to it and return (UpHandler is e.g. a building block)
        if(up_handler != null)
            return up_handler.up(evt);

        switch(type) {
            case Event.MSG:
                if(receiver != null) {
                    try {
                        receiver.receive((Message)evt.getArg());
                    }
                    catch(Throwable t) {
                        if(log.isWarnEnabled())
                            log.warn("failed calling receive() in receiver", t);
                    }
                    return null;
                }
                break;
            case Event.VIEW_CHANGE:
                if(receiver != null) {
                    try {
                        receiver.viewAccepted((View)evt.getArg());
                    }
                    catch(Throwable t) {
                        if(log.isWarnEnabled())
                            log.warn("failed calling viewAccepted() in receiver", t);
                    }
                    return null;
                }
                break;
            case Event.SUSPECT:
                if(receiver != null) {
                    try {
                        receiver.suspect((Address)evt.getArg());
                    }
                    catch(Throwable t) {
                        if(log.isWarnEnabled())
                            log.warn("failed calling suspect() in receiver", t);
                    }
                    return null;
                }
                break;
            case Event.GET_APPLSTATE:
                if(receiver != null) {
                    StateTransferInfo state_info=(StateTransferInfo)evt.getArg();
                    byte[] tmp_state=null;
                    String state_id=state_info.state_id;
                    try {
                        if(receiver instanceof ExtendedReceiver && state_id!=null) {
                            tmp_state=((ExtendedReceiver)receiver).getState(state_id);
                        }
                        else {
                            tmp_state=receiver.getState();
                        }
                    }
                    catch(Throwable t) {
                        if(log.isWarnEnabled())
                            log.warn("failed calling getState() in receiver", t);
                    }
                    return new StateTransferInfo(null, state_id, 0L, tmp_state);
                }
                break;
            case Event.STATE_TRANSFER_OUTPUTSTREAM:
                StateTransferInfo sti=(StateTransferInfo)evt.getArg();
                OutputStream os=sti.outputStream;
                if(receiver instanceof ExtendedReceiver) {                    
                    if(os != null) {
                        try {
                            if(sti.state_id == null)
                                ((ExtendedReceiver)receiver).getState(os);
                            else
                                ((ExtendedReceiver)receiver).getState(sti.state_id, os);
                        }
                        catch(Throwable t) {
                            if(log.isWarnEnabled())
                                log.warn("failed calling getState() in receiver", t);
                        }                       
                    }                    
                }
                else if(receiver instanceof Receiver){
                    if(log.isWarnEnabled()){
                        log.warn("Channel has STREAMING_STATE_TRANSFER, however," +
                                " application does not implement ExtendedMessageListener. State is not transfered");
                        Util.close(os);
                    }
                }
                break;

            case Event.BLOCK:
                if(!receive_blocks) {  // discard if client has not set 'receiving blocks' to 'on'
                    return true;
                }

                if(receiver != null) {
                    try {
                        receiver.block();
                    }
                    catch(Throwable t) {
                        if(log.isErrorEnabled())
                            log.error("failed calling block() in receiver", t);
                    }                     
                    return true;
                }
                break;
            case Event.UNBLOCK:
                //invoke receiver if block receiving is on
                if(receive_blocks && receiver instanceof ExtendedReceiver) {                                                     
                    try {
                        ((ExtendedReceiver)receiver).unblock();
                    }
                    catch(Throwable t) {
                        if(log.isErrorEnabled())
                            log.error("failed calling unblock() in receiver", t);
                    }                                                            
                }                       
                return null;                
            default:
                break;
        }

        if(type == Event.MSG || type == Event.VIEW_CHANGE || type == Event.SUSPECT ||
                type == Event.GET_APPLSTATE || type== Event.STATE_TRANSFER_OUTPUTSTREAM
                || type == Event.BLOCK || type == Event.UNBLOCK) {
            try {
                mq.add(evt);
            }
            catch(QueueClosedException queue_closed) {
                ; // ignore
            }
            catch(Exception e) {
                if(log.isWarnEnabled()) log.warn("exception adding event " + evt + " to message queue", e);
            }
        }

        if(type == Event.GET_APPLSTATE) {
            try {
                return applstate_exchanger.exchange(null);
            }
            catch(InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            }
        }
        return null;
    }


    /**
     * Sends a message through the protocol stack if the stack is available
     * @param evt the message to send down, encapsulated in an event
     */
    public void down(Event evt) {
        if(evt == null) return;

        switch(evt.getType()) {
            case Event.CONFIG:
                try {
                    Map<String,Object> m=(Map<String,Object>)evt.getArg();
                    if(m != null) {
                        additional_data.putAll(m);
                        if(m.containsKey("additional_data")) {
                            byte[] tmp=(byte[])m.get("additional_data");
                            if(local_addr != null)
                                local_addr.setAdditionalData(tmp);
                        }
                    }
                }
                catch(Throwable t) {
                    if(log.isErrorEnabled()) log.error("CONFIG event did not contain a hashmap: " + t);
                }
                break;            
        }

        prot_stack.down(evt);
    }


    public Object downcall(Event evt) {
        if(evt == null) return null;

        switch(evt.getType()) {
            case Event.CONFIG:
                try {
                    Map<String,Object> m=(Map<String,Object>)evt.getArg();
                    if(m != null) {
                        additional_data.putAll(m);
                        if(m.containsKey("additional_data")) {
                            byte[] tmp=(byte[])m.get("additional_data");
                            if(local_addr != null)
                                local_addr.setAdditionalData(tmp);
                        }
                    }
                }
                catch(Throwable t) {
                    if(log.isErrorEnabled()) log.error("CONFIG event did not contain a hashmap: " + t);
                }
                break;           
        }

        return prot_stack.down(evt);
    }



    @ManagedOperation
    public String toString(boolean details) {
        StringBuilder sb=new StringBuilder();
        sb.append("local_addr=").append(local_addr).append('\n');
        sb.append("cluster_name=").append(cluster_name).append('\n');
        sb.append("my_view=").append(my_view).append('\n');
        sb.append("connected=").append(connected).append('\n');
        sb.append("closed=").append(closed).append('\n');
        sb.append("incoming queue size=").append(mq.size()).append('\n');
        if(details) {
            sb.append("receive_blocks=").append(receive_blocks).append('\n');
            sb.append("receive_local_msgs=").append(receive_local_msgs).append('\n');
            sb.append("state_transfer_supported=").append(state_transfer_supported).append('\n');
            sb.append("props=").append(getProperties()).append('\n');
        }

        return sb.toString();
    }


    /* ----------------------------------- Private Methods ------------------------------------- */


    protected final void init(ProtocolStackConfigurator configurator) throws ChannelException {
        if(log.isInfoEnabled())
            log.info("JGroups version: " + Version.description);

        List<ProtocolConfiguration> configs;
        try {
            configs=configurator.getProtocolStack();
            for(ProtocolConfiguration config: configs) 
                config.substituteVariables();  // replace vars with system props
        }
        catch(Exception e) {
            throw new ChannelException("unable to parse the protocol configuration", e);
        }

        synchronized(Channel.class) {
            prot_stack=new ProtocolStack(this);
            try {
                prot_stack.setup(configs); // Setup protocol stack (creates protocol, calls init() on them)
            }
            catch(Throwable e) {
                throw new ChannelException("unable to setup the protocol stack", e);
            }
        }
    }

    protected final void init(JChannel ch) throws ChannelException {
        if(ch == null)
            throw new IllegalArgumentException("channel is null");
        if(log.isInfoEnabled())
            log.info("JGroups version: " + Version.description);

        synchronized(JChannel.class) {
            prot_stack=new ProtocolStack(this);
            try {
                prot_stack.setup(ch.getProtocolStack()); // Setup protocol stack (creates protocol, calls init() on them)
            }
            catch(Throwable e) {
                throw new ChannelException("unable to setup the protocol stack: " + e.getMessage(), e);
            }
        }
    }


    /**
     * Initializes all variables. Used after <tt>close()</tt> or <tt>disconnect()</tt>,
     * to be ready for new <tt>connect()</tt>
     */
    private void init() {
        if(local_addr != null)
            down(new Event(Event.REMOVE_ADDRESS, local_addr));
        local_addr=null;
        cluster_name=null;
        my_view=null;

        // changed by Bela Sept 25 2003
        //if(mq != null && mq.closed())
          //  mq.reset();
        connected=false;
    }


    private void startStack(String cluster_name) throws ChannelException {
        /*make sure the channel is not closed*/
        checkClosed();

        /*make sure we have a valid channel name*/
        if(cluster_name == null) {
            if(log.isDebugEnabled()) log.debug("cluster_name is null, assuming unicast channel");
        }
        else
            this.cluster_name=cluster_name;

        try {
            prot_stack.startStack(cluster_name, local_addr); // calls start() in all protocols, from top to bottom
        }
        catch(Throwable e) {
            throw new ChannelException("failed to start protocol stack", e);
        }

        if(socket_factory != null) {
            prot_stack.getTopProtocol().setSocketFactory(socket_factory);
        }
        

        /*create a temporary view, assume this channel is the only member and is the coordinator*/
        Vector<Address> t=new Vector<Address>(1);
        t.addElement(local_addr);
        my_view=new View(local_addr, 0, t);  // create a dummy view

        TP transport=prot_stack.getTransport();
        transport.registerProbeHandler(probe_handler);
    }

    /**
     * Generates new UUID and sets local address. Sends down a REMOVE_ADDRESS (if existing address was present) and
     * a SET_LOCAL_ADDRESS
     */
    protected void setAddress() {
        UUID old_addr=local_addr;
        local_addr=UUID.randomUUID();

        byte[] buf=(byte[])additional_data.get("additional_data");
        if(buf != null)
            local_addr.setAdditionalData(buf);

        if(old_addr != null)
            down(new Event(Event.REMOVE_ADDRESS, old_addr));
        if(name == null || name.length() == 0) // generate a logical name if not set
            name=Util.generateLocalName();
        if(name != null && name.length() > 0)
            UUID.add(local_addr, name);

        Event evt=new Event(Event.SET_LOCAL_ADDRESS, local_addr);
        down(evt);
        if(up_handler != null)
            up_handler.up(evt);
    }


    /**
     * health check<BR>
     * throws a ChannelClosed exception if the channel is closed
     */
    protected void checkClosed() throws ChannelClosedException {
        if(closed)
            throw new ChannelClosedException();
    }


    protected void checkClosedOrNotConnected() throws ChannelNotConnectedException, ChannelClosedException {
        if(closed)
            throw new ChannelClosedException();
        if(!connected)
            throw new ChannelNotConnectedException();
    }


    /**
     * returns the value of the event<BR>
     * These objects will be returned<BR>
     * <PRE>
     * <B>Event Type    - Return Type</B>
     * Event.MSG           - returns a Message object
     * Event.VIEW_CHANGE   - returns a View object
     * Event.SUSPECT       - returns a SuspectEvent object
     * Event.BLOCK         - returns a new BlockEvent object
     * Event.GET_APPLSTATE - returns a GetStateEvent object
     * Event.STATE_RECEIVED- returns a SetStateEvent object
     * Event.Exit          - returns an ExitEvent object
     * All other           - return the actual Event object
     * </PRE>
     * @param   evt - the event of which you want to extract the value
     * @return the event value if it matches the select list,
     *         returns null if the event is null
     *         returns the event itself if a match (See above) can not be made of the event type
     */
    static Object getEvent(Event evt) {
        if(evt == null)
            return null; // correct ?

        switch(evt.getType()) {
            case Event.MSG:
                return evt.getArg();
            case Event.VIEW_CHANGE:
                return evt.getArg();
            case Event.SUSPECT:
                return new SuspectEvent(evt.getArg());
            case Event.BLOCK:
                return new BlockEvent();
            case Event.UNBLOCK:
                return new UnblockEvent();
            case Event.GET_APPLSTATE:
                StateTransferInfo info=(StateTransferInfo)evt.getArg();
                return new GetStateEvent(info.target, info.state_id);
            case Event.STATE_RECEIVED:
                info=(StateTransferInfo)evt.getArg();
                return new SetStateEvent(info.state, info.state_id);
            case Event.STATE_TRANSFER_OUTPUTSTREAM:
            	info = (StateTransferInfo)evt.getArg();
                return new StreamingGetStateEvent(info.outputStream,info.state_id);
            case Event.STATE_TRANSFER_INPUTSTREAM:
            	info = (StateTransferInfo)evt.getArg();
                return new StreamingSetStateEvent(info.inputStream,info.state_id);
            default:
                return evt;
        }
    }

    /**
     * Disconnects and closes the channel.
     * This method does the following things
     * <ol>
     * <li>Calls <code>this.disconnect</code> if the disconnect parameter is true
     * <li>Calls <code>Queue.close</code> on mq if the close_mq parameter is true
     * <li>Calls <code>ProtocolStack.stop</code> on the protocol stack
     * <li>Calls <code>ProtocolStack.destroy</code> on the protocol stack
     * <li>Sets the channel closed and channel connected flags to true and false
     * <li>Notifies any channel listener of the channel close operation
     * </ol>
     */
    protected void _close(boolean disconnect, boolean close_mq) {
        UUID old_addr=local_addr;
        if(closed)
            return;

        if(disconnect)
            disconnect();                     // leave group if connected

        if(close_mq) 
            closeMessageQueue(false);       

        stopStack(true, true);
        closed=true;
        connected=false;
        notifyChannelClosed(this);
        init(); // sets local_addr=null; changed March 18 2003 (bela) -- prevented successful rejoining
        if(old_addr != null)
            UUID.remove(old_addr);
    }

    protected void stopStack(boolean stop, boolean destroy) {
        if(prot_stack != null) {
            try {
                if(stop)
                    prot_stack.stopStack(cluster_name);

                if(destroy)
                    prot_stack.destroy();
            }
            catch(Exception e) {
                if(log.isErrorEnabled())
                    log.error("failed destroying the protocol stack", e);
            }

            TP transport=prot_stack.getTransport();
            if(transport != null)
                transport.unregisterProbeHandler(probe_handler);
        }
    }


    public final void closeMessageQueue(boolean flush_entries) {
        mq.close(flush_entries);
    }


    public boolean flushSupported() {
        return flush_supported;
    }

    /**
     * Will perform a flush of the system, ie. all pending messages are flushed out of the 
     * system and all members ack their reception. After this call returns, no member will
     * be sending any messages until {@link #stopFlush()} is called.
     * <p>
     * In case of flush collisions, random sleep time backoff algorithm is employed and
     * flush is reattempted for numberOfAttempts. Therefore this method is guaranteed 
     * to return after timeout x numberOfAttempts miliseconds.
     * 
     * @param automatic_resume Call {@link #stopFlush()} after the flush
     * @return true if FLUSH completed within the timeout
     */
    public boolean startFlush(boolean automatic_resume) {
        if(!flushSupported()) {
            throw new IllegalStateException("Flush is not supported, add pbcast.FLUSH protocol to your configuration");
        }           	  
        boolean successfulFlush = (Boolean) downcall(new Event(Event.SUSPEND));
        
        if(automatic_resume)
            stopFlush();

        return successfulFlush;              
    }
    
    /**
     * Performs a partial flush in a cluster for flush participants. 
     * <p>
     * All pending messages are flushed out only for flush participants.
     * Remaining members in a cluster are not included in flush.
     * Flush participants should be a proper subset of a current view. 
     * 
     * <p>
     * In case of flush collisions, random sleep time backoff algorithm is employed and
     * flush is reattempted for numberOfAttempts. Therefore this method is guaranteed 
     * to return after timeout x numberOfAttempts miliseconds.
     * 
     * @param automatic_resume Call {@link #stopFlush()} after the flush
     * @return true if FLUSH completed within the timeout
     */
    public boolean startFlush(List<Address> flushParticipants,boolean automatic_resume) {
        boolean successfulFlush;
        if(!flushSupported()){
            throw new IllegalStateException("Flush is not supported, add pbcast.FLUSH protocol to your configuration");
        }
        View v = getView();
        if(v != null && v.getMembers().containsAll(flushParticipants)){
            successfulFlush = (Boolean) downcall(new Event(Event.SUSPEND, flushParticipants));
        }else{
            throw new IllegalArgumentException("Current view " + v
                                               + " does not contain all flush participants "
                                               + flushParticipants);
        }
        
        if(automatic_resume)
            stopFlush(flushParticipants);

        return successfulFlush;              
    }
    
    /**
     * Will perform a flush of the system, ie. all pending messages are flushed out of the 
     * system and all members ack their reception. After this call returns, no member will
     * be sending any messages until {@link #stopFlush()} is called.
     * <p>
     * In case of flush collisions, random sleep time backoff algorithm is employed and
     * flush is reattempted for numberOfAttempts. Therefore this method is guaranteed 
     * to return after timeout x numberOfAttempts miliseconds.
     * @param timeout
     * @param automatic_resume Call {@link #stopFlush()} after the flush
     * @return true if FLUSH completed within the timeout
     */
    public boolean startFlush(long timeout, boolean automatic_resume) {        
        return startFlush(automatic_resume);       
    }

    public void stopFlush() {
        if(!flushSupported()) {
            throw new IllegalStateException("Flush is not supported, add pbcast.FLUSH protocol to your configuration");
        }      
        down(new Event(Event.RESUME));      
    }

    public void stopFlush(List<Address> flushParticipants) {
        if(!flushSupported()) {
            throw new IllegalStateException("Flush is not supported, add pbcast.FLUSH protocol to your configuration");
        }       
        down(new Event(Event.RESUME, flushParticipants));
    }
    
    @Override
    public Map<String, Object> getInfo(){
       return new HashMap<String, Object>(config);
    }

    public void setInfo(String key, Object value) {
        if(key != null)
            config.put(key, value);
    }

    Address determineCoordinator() {
        Vector<Address> mbrs=my_view != null? my_view.getMembers() : null;
        if(mbrs == null)
            return null;
        if(!mbrs.isEmpty())
            return mbrs.firstElement();
        return null;
    }

    private TimeScheduler getTimer() {
        if(prot_stack != null) {
            TP transport=prot_stack.getTransport();
            if(transport != null) {
                return transport.getTimer();
            }
        }
        return null;
    }

    /* ------------------------------- End of Private Methods ---------------------------------- */

    class MyProbeHandler implements TP.ProbeHandler {

        public Map<String, String> handleProbe(String... keys) {
            Map<String, String> map=new HashMap<String, String>(2);
            for(String key: keys) {
                if(key.startsWith("jmx")) {
                    Map<String, Object> tmp_stats;
                    int index=key.indexOf("=");
                    if(index > -1) {
                        List<String> list=null;
                        String protocol_name=key.substring(index +1);
                        index=protocol_name.indexOf(".");
                        if(index > -1) {
                            String rest=protocol_name;
                            protocol_name=protocol_name.substring(0, index);
                            String attrs=rest.substring(index +1); // e.g. "num_sent,msgs,num_received_msgs"
                            list=Util.parseStringList(attrs, ",");
                        }

                        tmp_stats=dumpStats(protocol_name, list);
                    }
                    else
                        tmp_stats=dumpStats();

                    map.put("jmx", tmp_stats != null? Util.mapToString(tmp_stats) : "null");
                    continue;
                }
                if(key.equals("info")) {
                    Map<String, Object> tmp_info=getInfo();
                    map.put("info", tmp_info != null? Util.mapToString(tmp_info) : "null");
                }
                if(key.equals("socks")) {
                    map.put("socks", getOpenSockets());
                }
                if(key.startsWith("invoke") || key.startsWith("op")) {
                    int index=key.indexOf("=");
                    if(index != -1) {
                        try {
                            handleOperation(map, key.substring(index+1));
                        }
                        catch(Throwable throwable) {
                            log.error("failed invoking operation " + key.substring(index+1), throwable);
                        }
                    }
                }
            }

            map.put("version", Version.description);
            if(my_view != null && !map.containsKey("view"))
                map.put("view", my_view.toString());
            map.put("local_addr", getAddressAsString() + " [" + getAddressAsUUID() + "]");
            PhysicalAddress physical_addr=(PhysicalAddress)downcall(new Event(Event.GET_PHYSICAL_ADDRESS, local_addr));
            if(physical_addr != null)
                map.put("physical_addr", physical_addr.toString());
            map.put("cluster", getClusterName());
            return map;
        }

        public String[] supportedKeys() {
            return new String[]{"jmx", "info", "invoke", "op", "socks"};
        }

        String getOpenSockets() {
            Map<Object, String> socks=getSocketFactory().getSockets();
            TP transport=getProtocolStack().getTransport();
            if(transport != null && transport.isSingleton()) {
                Map<Object,String> tmp=transport.getSocketFactory().getSockets();
                if(tmp != null)
                    socks.putAll(tmp);
            }

            StringBuilder sb=new StringBuilder();
            if(socks != null) {
                for(Map.Entry<Object,String> entry: socks.entrySet()) {
                    Object key=entry.getKey();
                    if(key instanceof ServerSocket) {
                        ServerSocket tmp=(ServerSocket)key;
                        sb.append(tmp.getInetAddress()).append(":").append(tmp.getLocalPort())
                                .append(" ").append(entry.getValue()).append(" [tcp]");
                    }
                    else if(key instanceof DatagramSocket) {
                        DatagramSocket sock=(DatagramSocket)key;
                        sb.append(sock.getLocalAddress()).append(":").append(sock.getLocalPort())
                                .append(" ").append(entry.getValue()).append(" [udp]");
                    }
                    else {
                        sb.append(key).append(" ").append(entry.getValue());
                    }
                    sb.append("\n");
                }
            }
            return sb.toString();
        }

        /**
         * Invokes an operation and puts the return value into map
         * @param map
         * @param operation Protocol.OperationName[args], e.g. STABLE.foo[arg1 arg2 arg3]
         */
        private void handleOperation(Map<String, String> map, String operation) throws Throwable {
            int index=operation.indexOf(".");
            if(index == -1)
                throw new IllegalArgumentException("operation " + operation + " is missing the protocol name");
            String prot_name=operation.substring(0, index);
            Protocol prot=prot_stack.findProtocol(prot_name);
            if(prot == null)
                throw new IllegalArgumentException("protocol " + prot_name + " not found");

            int args_index=operation.indexOf("[");
            String method_name;
            if(args_index != -1)
                method_name=operation.substring(index +1, args_index).trim();
            else
                method_name=operation.substring(index+1).trim();

            String[] args=null;
            if(args_index != -1) {
                int end_index=operation.indexOf("]");
                if(end_index == -1)
                    throw new IllegalArgumentException("] not found");
                List<String> str_args=Util.parseCommaDelimitedStrings(operation.substring(args_index + 1, end_index));
                Object[] strings=str_args.toArray();
                args=new String[strings.length];
                for(int i=0; i < strings.length; i++)
                    args[i]=(String)strings[i];
            }

            Method method=MethodCall.findMethod(prot.getClass(), method_name, args);
            MethodCall call=new MethodCall(method);
            Object[] converted_args=null;
            if(args != null) {
                converted_args=new Object[args.length];
                Class<?>[] types=method.getParameterTypes();
                for(int i=0; i < args.length; i++)
                    converted_args[i]=MethodCall.convert(args[i], types[i]);
            }
            Object retval=call.invoke(prot, converted_args);
            if(retval != null)
                map.put(prot_name + "." + method_name, retval.toString());
        }
    }

  
}
