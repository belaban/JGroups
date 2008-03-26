
package org.jgroups;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.conf.ConfiguratorFactory;
import org.jgroups.conf.ProtocolStackConfigurator;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.stack.StateTransferInfo;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.*;
import org.w3c.dom.Element;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

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
 * @version $Id: JChannel.java,v 1.106.2.6 2008/03/26 08:17:02 belaban Exp $
 */
public class JChannel extends Channel {

    /**
     * The default protocol stack used by the default constructor.
     */
    public static final String DEFAULT_PROTOCOL_STACK=
            "UDP(down_thread=false;mcast_send_buf_size=640000;mcast_port=45566;discard_incompatible_packets=true;" +
                    "ucast_recv_buf_size=20000000;mcast_addr=228.10.10.10;up_thread=false;loopback=false;" +
                    "mcast_recv_buf_size=25000000;max_bundle_size=64000;max_bundle_timeout=30;" +
                    "use_incoming_packet_handler=true;use_outgoing_packet_handler=false;" +
                    "ucast_send_buf_size=640000;tos=16;enable_bundling=true;ip_ttl=2):" +
            "PING(timeout=2000;down_thread=false;num_initial_members=3;up_thread=false):" +
            "MERGE2(max_interval=10000;down_thread=false;min_interval=5000;up_thread=false):" +
            "FD(timeout=2000;max_tries=3;down_thread=false;up_thread=false):" +
            "VERIFY_SUSPECT(timeout=1500;down_thread=false;up_thread=false):" +
            "pbcast.NAKACK(max_xmit_size=60000;down_thread=false;use_mcast_xmit=false;gc_lag=0;" +
                    "discard_delivered_msgs=true;up_thread=false;retransmit_timeout=100,200,300,600,1200,2400,4800):" +
            "UNICAST(timeout=300,600,1200,2400,3600;down_thread=false;up_thread=false):" +
            "pbcast.STABLE(stability_delay=1000;desired_avg_gossip=50000;max_bytes=400000;down_thread=false;" +
                    "up_thread=false):" +
            "VIEW_SYNC(down_thread=false;avg_send_interval=60000;up_thread=false):" +
            "pbcast.GMS(print_local_addr=true;join_timeout=3000;down_thread=false;" +
                    "join_retry_timeout=2000;up_thread=false;shun=true):" +
            "FC(max_credits=2000000;down_thread=false;up_thread=false;min_threshold=0.10):" +
            "FRAG2(frag_size=60000;down_thread=false;up_thread=false):" +
            "pbcast.STATE_TRANSFER(down_thread=false;up_thread=false)";

    static final String FORCE_PROPS="force.properties";

    /* the protocol stack configuration string */
    private String props=null;

    /*the address of this JChannel instance*/
    private Address local_addr=null;
    /*the channel (also know as group) name*/
    private String cluster_name=null;  // group name
    /*the latest view of the group membership*/
    private View my_view=null;
    /*the queue that is used to receive messages (events) from the protocol stack*/
    private final Queue mq=new Queue();
    /*the protocol stack, used to send and receive messages from the protocol stack*/
    private ProtocolStack prot_stack=null;

    /** Thread responsible for closing a channel and potentially reconnecting to it (e.g., when shunned). */
    protected CloserThread closer=null;

    /** To wait until a local address has been assigned */
    private final Promise local_addr_promise=new Promise();

    /** To wait until we have connected successfully */
    private final Promise connect_promise=new Promise();

    /** To wait until we have been disconnected from the channel */
    private final Promise disconnect_promise=new Promise();

    private final Promise state_promise=new Promise();

    private final Promise flush_unblock_promise=new Promise();

    private final Promise flush_promise=new Promise();

    /** wait until we have a non-null local_addr */
    private long LOCAL_ADDR_TIMEOUT=30000; //=Long.parseLong(System.getProperty("local_addr.timeout", "30000"));
    /*if the states is fetched automatically, this is the default timeout, 5 secs*/
    private static final long GET_STATE_DEFAULT_TIMEOUT=5000;
    /*if FLUSH is used channel waits for UNBLOCK event, this is the default timeout, 10 secs*/
    private static final long FLUSH_UNBLOCK_TIMEOUT=10000;
    /*flag to indicate whether to receive blocks, if this is set to true, receive_views is set to true*/
    private boolean receive_blocks=false;
    /*flag to indicate whether to receive local messages
     *if this is set to false, the JChannel will not receive messages sent by itself*/
    private boolean receive_local_msgs=true;
    /*flag to indicate whether the channel will reconnect (reopen) when the exit message is received*/
    private boolean auto_reconnect=false;
    /*flag t indicate whether the state is supposed to be retrieved after the channel is reconnected
     *setting this to true, automatically forces auto_reconnect to true*/
    private boolean auto_getstate=false;
    /*channel connected flag*/
    protected boolean connected=false;

    /*channel closed flag*/
    protected boolean closed=false;      // close() has been called, channel is unusable

    /** True if a state transfer protocol is available, false otherwise */
    private boolean state_transfer_supported=false; // set by CONFIG event from STATE_TRANSFER protocol

    /** True if a flush protocol is available, false otherwise */
    private volatile boolean flush_supported=false; // set by CONFIG event from FLUSH protocol

    /** Used to maintain additional data across channel disconnects/reconnects. This is a kludge and will be remove
     * as soon as JGroups supports logical addresses
     */
    private byte[] additional_data=null;

    protected final Log log=LogFactory.getLog(getClass());

    /** Collect statistics */
    protected boolean stats=true;

    protected long sent_msgs=0, received_msgs=0, sent_bytes=0, received_bytes=0;




    /** Used by subclass to create a JChannel without a protocol stack, don't use as application programmer */
    protected JChannel(boolean no_op) {
        ;
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
    protected JChannel(ProtocolStackConfigurator configurator) throws ChannelException {
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

        ProtocolStackConfigurator c=null;

        try {
            c=ConfiguratorFactory.getStackConfigurator(properties);
        }
        catch(Exception x) {
            throw new ChannelException("unable to load protocol stack", x);
        }
        init(c);
    }


    /**
     * Returns the protocol stack.
     * Currently used by Debugger.
     * Specific to JChannel, therefore
     * not visible in Channel
     */
    public ProtocolStack getProtocolStack() {
        return prot_stack;
    }

    protected Log getLog() {
        return log;
    }

    /**
     * returns the protocol stack configuration in string format.
     * an example of this property is<BR>
     * "UDP:PING:FD:STABLE:NAKACK:UNICAST:FRAG:FLUSH:GMS:VIEW_ENFORCER:STATE_TRANSFER:QUEUE"
     */
    public String getProperties() {
        return props;
    }

    public boolean statsEnabled() {
        return stats;
    }

    public void enableStats(boolean stats) {
        this.stats=stats;
    }

    public void resetStats() {
        sent_msgs=received_msgs=sent_bytes=received_bytes=0;
    }

    public long getSentMessages() {return sent_msgs;}
    public long getSentBytes() {return sent_bytes;}
    public long getReceivedMessages() {return received_msgs;}
    public long getReceivedBytes() {return received_bytes;}
    public int  getNumberOfTasksInTimer() {return prot_stack != null ? prot_stack.timer.size() : -1;}

    public String dumpTimerQueue() {
        return prot_stack != null? prot_stack.dumpTimerQueue() : "<n/a";
    }

    /**
     * Returns a pretty-printed form of all the protocols. If include_properties is set,
     * the properties for each protocol will also be printed.
     */
    public String printProtocolSpec(boolean include_properties) {
        return prot_stack != null ? prot_stack.printProtocolSpec(include_properties) : null;
    }


    /**
     * Connects the channel to a group.
     * If the channel is already connected, an error message will be printed to the error log.
     * If the channel is closed a ChannelClosed exception will be thrown.
     * This method starts the protocol stack by calling ProtocolStack.start,
     * then it sends an Event.CONNECT event down the stack and waits to receive a CONNECT_OK event.
     * Once the CONNECT_OK event arrives from the protocol stack, any channel listeners are notified
     * and the channel is considered connected.
     *
     * @param cluster_name A <code>String</code> denoting the group name. Cannot be null.
     * @exception ChannelException The protocol stack cannot be started
     * @exception ChannelClosedException The channel is closed and therefore cannot be used any longer.
     *                                   A new channel has to be created first.
     */
    public synchronized void connect(String cluster_name) throws ChannelException, ChannelClosedException {
        /*make sure the channel is not closed*/
        checkClosed();

        /*if we already are connected, then ignore this*/
        if(connected) {
            if(log.isTraceEnabled()) log.trace("already connected to " + cluster_name);
            return;
        }

        /*make sure we have a valid channel name*/
        if(cluster_name == null) {
            if(log.isInfoEnabled()) log.info("cluster_name is null, assuming unicast channel");
        }
        else
            this.cluster_name=cluster_name;

        try {
            prot_stack.startStack(); // calls start() in all protocols, from top to bottom
        }
        catch(Throwable e) {
            throw new ChannelException("failed to start protocol stack", e);
        }

        String tmp=Util.getProperty(new String[]{Global.CHANNEL_LOCAL_ADDR_TIMEOUT, "local_addr.timeout"},
                                    null, null, false, "30000");
        LOCAL_ADDR_TIMEOUT=Long.parseLong(tmp);

		/* Wait LOCAL_ADDR_TIMEOUT milliseconds for local_addr to have a non-null value (set by SET_LOCAL_ADDRESS) */
        local_addr=(Address)local_addr_promise.getResult(LOCAL_ADDR_TIMEOUT);
        if(local_addr == null) {
            log.fatal("local_addr is null; cannot connect");
            throw new ChannelException("local_addr is null");
        }


        /*create a temporary view, assume this channel is the only member and
         *is the coordinator*/
        Vector t=new Vector(1);
        t.addElement(local_addr);
        my_view=new View(local_addr, 0, t);  // create a dummy view

        // only connect if we are not a unicast channel
        if(cluster_name != null) {
            connect_promise.reset();

            if(flush_supported)
               flush_unblock_promise.reset();

            Event connect_event=new Event(Event.CONNECT, cluster_name);
            down(connect_event);

            Object res=connect_promise.getResult();  // waits forever until connected (or channel is closed)
            if(res != null && res instanceof Exception) { // the JOIN was rejected by the coordinator
                throw new ChannelException("connect() failed", (Throwable)res);
            }

            //if FLUSH is used do not return from connect() until UNBLOCK event is received
            boolean singletonMember = my_view != null && my_view.size() == 1;
            boolean shouldWaitForUnblock = flush_supported && receive_blocks && !singletonMember && !flush_unblock_promise.hasResult();
            if(shouldWaitForUnblock){
               try{
                  flush_unblock_promise.getResultWithTimeout(FLUSH_UNBLOCK_TIMEOUT);
               }
               catch (TimeoutException te){
                  if(log.isWarnEnabled())
                     log.warn("waiting on UNBLOCK after connect timed out");
               }
            }
        }
        connected=true;
        notifyChannelConnected(this);
    }


    public synchronized boolean connect(String cluster_name, Address target, String state_id, long timeout) throws ChannelException {
        throw new UnsupportedOperationException("not yet implemented");
    }


    /**
     * Disconnects the channel if it is connected. If the channel is closed, this operation is ignored<BR>
     * Otherwise the following actions happen in the listed order<BR>
     * <ol>
     * <li> The JChannel sends a DISCONNECT event down the protocol stack<BR>
     * <li> Blocks until the channel to receives a DISCONNECT_OK event<BR>
     * <li> Sends a STOP_QUEING event down the stack<BR>
     * <li> Stops the protocol stack by calling ProtocolStack.stop()<BR>
     * <li> Notifies the listener, if the listener is available<BR>
     * </ol>
     */
    public synchronized void disconnect() {
        if(closed) return;

        if(connected) {

            if(cluster_name != null) {

                /* Send down a DISCONNECT event. The DISCONNECT event travels down to the GMS, where a
                *  DISCONNECT_OK response is generated and sent up the stack. JChannel blocks until a
                *  DISCONNECT_OK has been received, or until timeout has elapsed.
                */
                Event disconnect_event=new Event(Event.DISCONNECT, local_addr);
                disconnect_promise.reset();
                down(disconnect_event);   // DISCONNECT is handled by each layer
                disconnect_promise.getResult(); // wait for DISCONNECT_OK
            }

            // Just in case we use the QUEUE protocol and it is still blocked...
            down(new Event(Event.STOP_QUEUEING));

            connected=false;
            try {
                prot_stack.stopStack(); // calls stop() in all protocols, from top to bottom
            }
            catch(Exception e) {
                if(log.isErrorEnabled()) log.error("exception: " + e);
            }
            notifyChannelDisconnected(this);
            init(); // sets local_addr=null; changed March 18 2003 (bela) -- prevented successful rejoining
        }
    }


    /**
     * Destroys the channel.
     * After this method has been called, the channel us unusable.<BR>
     * This operation will disconnect the channel and close the channel receive queue immediately<BR>
     */
    public synchronized void close() {
        _close(true, true); // by default disconnect before closing channel and close mq
    }


    /** Shuts down the channel without disconnecting */
    public synchronized void shutdown() {
        _close(false, true); // by default disconnect before closing channel and close mq
    }

    /**
     * Opens the channel.
     * This does the following actions:
     * <ol>
     * <li> Resets the receiver queue by calling Queue.reset
     * <li> Sets up the protocol stack by calling ProtocolStack.setup
     * <li> Sets the closed flag to false
     * </ol>
     */
    public synchronized void open() throws ChannelException {
        if(!closed)
            throw new ChannelException("channel is already open");

        try {
            mq.reset();

            // new stack is created on open() - bela June 12 2003
            prot_stack=new ProtocolStack(this, props);
            prot_stack.setup();
            closed=false;
        }
        catch(Exception e) {
            throw new ChannelException("failed to open channel" , e);
        }
    }

    /**
     * returns true if the Open operation has been called successfully
     */
    public boolean isOpen() {
        return !closed;
    }


    /**
     * returns true if the Connect operation has been called successfully
     */
    public boolean isConnected() {
        return connected;
    }

    public int getNumMessages() {
        return mq != null? mq.size() : -1;
    }


    public String dumpQueue() {
        return Util.dumpQueue(mq);
    }

    /**
     * Returns a map of statistics of the various protocols and of the channel itself.
     * @return Map<String,Map>. A map where the keys are the protocols ("channel" pseudo key is
     * used for the channel itself") and the values are property maps.
     */
    public Map dumpStats() {
        Map retval=prot_stack.dumpStats();
        if(retval != null) {
            Map tmp=dumpChannelStats();
            if(tmp != null)
                retval.put("channel", tmp);
        }
        return retval;
    }

    private Map dumpChannelStats() {
        Map retval=new HashMap();
        retval.put("sent_msgs", new Long(sent_msgs));
        retval.put("sent_bytes", new Long(sent_bytes));
        retval.put("received_msgs", new Long(received_msgs));
        retval.put("received_bytes", new Long(received_bytes));
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
    public void send(Message msg) throws ChannelNotConnectedException, ChannelClosedException {
        checkClosed();
        checkNotConnected();
        if(stats) {
            sent_msgs++;
            sent_bytes+=msg.getLength();
        }
        if(msg == null)
            throw new NullPointerException("msg is null");
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
    public void send(Address dst, Address src, Serializable obj) throws ChannelNotConnectedException, ChannelClosedException {
        send(new Message(dst, src, obj));
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
     */
    public Object receive(long timeout) throws ChannelNotConnectedException, ChannelClosedException, TimeoutException {

        checkClosed();
        checkNotConnected();

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
     */
    public Object peek(long timeout) throws ChannelNotConnectedException, ChannelClosedException, TimeoutException {

        checkClosed();
        checkNotConnected();

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


    /**
     * returns the local address of the channel
     * returns null if the channel is closed
     */
    public Address getLocalAddress() {
        return closed ? null : local_addr;
    }


    /**
     * returns the name of the channel
     * if the channel is not connected or if it is closed it will return null
     * @deprecated Use {@link #getClusterName()} instead
     */
    public String getChannelName() {
        return closed ? null : !connected ? null : cluster_name;
    }

    public String getClusterName() {
        return cluster_name;
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
                if(log.isWarnEnabled())
                    log.warn("option VIEW has been deprecated (it is always true now); this option is ignored");
                break;
            case SUSPECT:
                if(log.isWarnEnabled())
                    log.warn("option SUSPECT has been deprecated (it is always true now); this option is ignored");
                break;
            case BLOCK:
                if(value instanceof Boolean)
                    receive_blocks=((Boolean)value).booleanValue();
                else
                    if(log.isErrorEnabled()) log.error("option " + Channel.option2String(option) +
                                                     " (" + value + "): value has to be Boolean");
                break;

            case GET_STATE_EVENTS:
                if(log.isTraceEnabled())
                    log.trace("option GET_STATE_EVENTS has been deprecated (it is always true now); this option is ignored");
                break;

            case LOCAL:
                if(value instanceof Boolean)
                    receive_local_msgs=((Boolean)value).booleanValue();
                else
                    if(log.isErrorEnabled()) log.error("option " + Channel.option2String(option) +
                                                     " (" + value + "): value has to be Boolean");
                break;

            case AUTO_RECONNECT:
                if(value instanceof Boolean)
                    auto_reconnect=((Boolean)value).booleanValue();
                else
                    if(log.isErrorEnabled()) log.error("option " + Channel.option2String(option) +
                                                     " (" + value + "): value has to be Boolean");
                break;

            case AUTO_GETSTATE:
                if(value instanceof Boolean) {
                    auto_getstate=((Boolean)value).booleanValue();
                    if(auto_getstate)
                        auto_reconnect=true;
                }
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
            	return receive_blocks ? Boolean.TRUE : Boolean.FALSE;
            case SUSPECT:
            	return Boolean.TRUE;
            case AUTO_RECONNECT:
                return auto_reconnect ? Boolean.TRUE : Boolean.FALSE;
            case AUTO_GETSTATE:
                return auto_getstate ? Boolean.TRUE : Boolean.FALSE;
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
        down(new Event(Event.BLOCK_OK));
        down(new Event(Event.START_QUEUEING));
    }


    /**
     * Retrieves the current group state. Sends GET_STATE event down to STATE_TRANSFER layer.
     * Blocks until STATE_TRANSFER sends up a GET_STATE_OK event or until <code>timeout</code>
     * milliseconds have elapsed. The argument of GET_STATE_OK should be a single object.
     * @param target the target member to receive the state from. if null, state is retrieved from coordinator
     * @param timeout the number of milliseconds to wait for the operation to complete successfully. 0 waits until
     * the state has been received
     * @return true of the state was received, false if the operation timed out
     */
    public boolean getState(Address target, long timeout) throws ChannelNotConnectedException, ChannelClosedException {
        return getState(target,null,timeout);
    }

    /**
     * Retrieves a substate (or partial state) from the target.
     * @param target State provider. If null, coordinator is used
     * @param state_id The ID of the substate. If null, the entire state will be transferred
     * @param timeout the number of milliseconds to wait for the operation to complete successfully. 0 waits until
     * the state has been received
     * @return
     * @throws ChannelNotConnectedException
     * @throws ChannelClosedException
     */
    public boolean getState(Address target, String state_id, long timeout) throws ChannelNotConnectedException, ChannelClosedException {
        if(target == null)
            target=determineCoordinator();
        if(target != null && local_addr != null && target.equals(local_addr)) {
            if(log.isTraceEnabled())
                log.trace("cannot get state from myself (" + target + "): probably the first member");
            return false;
        }

        StateTransferInfo info=new StateTransferInfo(target, state_id, timeout);
        boolean rc=_getState(new Event(Event.GET_STATE, info), info);
        if(rc == false)
            down(new Event(Event.RESUME_STABLE));
        return rc;
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
        StateTransferInfo info=new StateTransferInfo(null, null, 0L, state);
        down(new Event(Event.GET_APPLSTATE_OK, info));
    }

    /**
     * Returns a substate as indicated by state_id
     * @param state
     * @param state_id
     */
    public void returnState(byte[] state, String state_id) {
        StateTransferInfo info=new StateTransferInfo(null, state_id, 0L, state);
        down(new Event(Event.GET_APPLSTATE_OK, info));
    }





    /**
     * Callback method <BR>
     * Called by the ProtocolStack when a message is received.
     * It will be added to the message queue from which subsequent
     * <code>Receive</code>s will dequeue it.
     * @param evt the event carrying the message from the protocol stack
     */
    public void up(Event evt) {
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
                        return;
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
             * 
             * We do not set connect_promise because we want to wait for
             * CONNECT_OK and then return from user's JChannel.connect() call. 
             * This is important since we have to wait for Event.UNBLOCK after 
             * CONNECT_OK if blocks are turned on. See JChannel.connect() for 
             * details.
             *
             */
            if(connected == false) {
                connected=true;                
            }

            // unblock queueing of messages due to previous BLOCK event:
            down(new Event(Event.STOP_QUEUEING));
            break;

        case Event.CONFIG:
            HashMap config=(HashMap)evt.getArg();
            if(config != null) {
                if(config.containsKey("state_transfer")) {
                    state_transfer_supported=((Boolean)config.get("state_transfer")).booleanValue();
                }
                if(config.containsKey("flush_supported")) {
                    flush_supported=((Boolean)config.get("flush_supported")).booleanValue();
                }
            }
            break;

        case Event.CONNECT_OK:
            connect_promise.setResult(evt.getArg());
            break;

        case Event.SUSPEND_OK:
        	flush_promise.setResult(Boolean.TRUE);
        	break;

        case Event.DISCONNECT_OK:
            disconnect_promise.setResult(Boolean.TRUE);
            break;

        case Event.GET_STATE_OK:
            StateTransferInfo info=(StateTransferInfo)evt.getArg();
            byte[] state=info.state;

            state_promise.setResult(state != null? Boolean.TRUE : Boolean.FALSE);
            if(up_handler != null) {
                up_handler.up(evt);
                return;
            }

            if(state != null) {
                String state_id=info.state_id;
                if(receiver != null) {
                    if(receiver instanceof ExtendedReceiver && state_id!=null)
                        ((ExtendedReceiver)receiver).setState(state_id, state);
                    else
                        receiver.setState(state);
                }
                else {
                    try {mq.add(new Event(Event.STATE_RECEIVED, info));} catch(Exception e) {}
                }
            }
            break;

        case Event.STATE_TRANSFER_INPUTSTREAM:
            StateTransferInfo sti=(StateTransferInfo)evt.getArg();
            InputStream is=sti.inputStream;
            //Oct 13,2006 moved to down() when Event.STATE_TRANSFER_INPUTSTREAM_CLOSED is received
            //state_promise.setResult(is != null? Boolean.TRUE : Boolean.FALSE);

            if(up_handler != null) {
                up_handler.up(evt);
                return;
            }

            if(is != null) {
                if(receiver instanceof ExtendedReceiver) {
                    if(sti.state_id == null)
                        ((ExtendedReceiver)receiver).setState(is);
                    else
                        ((ExtendedReceiver)receiver).setState(sti.state_id, is);
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

        case Event.SET_LOCAL_ADDRESS:
            local_addr_promise.setResult(evt.getArg());
            break;

        case Event.EXIT:
            handleExit(evt);
            return;  // no need to pass event up; already done in handleExit()

        default:
            break;
        }


        // If UpHandler is installed, pass all events to it and return (UpHandler is e.g. a building block)
        if(up_handler != null) {
            up_handler.up(evt);

            if(type == Event.UNBLOCK){
               flush_unblock_promise.setResult(Boolean.TRUE);
            }
            return;
        }

        switch(type) {
            case Event.MSG:
                if(receiver != null) {
                    receiver.receive((Message)evt.getArg());
                    return;
                }
                break;
            case Event.VIEW_CHANGE:
                if(receiver != null) {
                    receiver.viewAccepted((View)evt.getArg());
                    return;
                }
                break;
            case Event.SUSPECT:
                if(receiver != null) {
                    receiver.suspect((Address)evt.getArg());
                    return;
                }
                break;
            case Event.GET_APPLSTATE:
                if(receiver != null) {
                    StateTransferInfo info=(StateTransferInfo)evt.getArg();
                    byte[] tmp_state;
                    String state_id=info.state_id;
                    if(receiver instanceof ExtendedReceiver && state_id!=null) {
                        tmp_state=((ExtendedReceiver)receiver).getState(state_id);
                    }
                    else {
                        tmp_state=receiver.getState();
                    }
                    returnState(tmp_state, state_id);
                    return;
                }
                break;
            case Event.STATE_TRANSFER_OUTPUTSTREAM:
                if(receiver != null) {
                    StateTransferInfo sti=(StateTransferInfo)evt.getArg();
                    OutputStream os=sti.outputStream;
                    if(os != null && receiver instanceof ExtendedReceiver) {
                        if(sti.state_id == null)
                            ((ExtendedReceiver)receiver).getState(os);
                        else
                            ((ExtendedReceiver)receiver).getState(sti.state_id, os);
                    }
                    return;
                }
				break;

            case Event.BLOCK:
                if(!receive_blocks) {  // discard if client has not set 'receiving blocks' to 'on'
                    down(new Event(Event.BLOCK_OK));
                    down(new Event(Event.START_QUEUEING));
                    return;
                }

                if(receiver != null) {
                    try {
                        receiver.block();
                    }
                    catch(Throwable t) {
                        if(log.isErrorEnabled())
                            log.error("failed calling block() on Receiver", t);
                    }
                    finally {
                        blockOk();
                    }
                    return;
                }
                break;
            case Event.UNBLOCK:
                //discard if client has not set 'receiving blocks' to 'on'
                if(!receive_blocks) {
                    return;
                }
                if(receiver instanceof ExtendedReceiver) {
                    try {
                        ((ExtendedReceiver)receiver).unblock();
                    }
                    catch(Throwable t) {
                        if(log.isErrorEnabled())
                            log.error("failed calling unblock() on Receiver", t);
                    }
                    finally{
                       flush_unblock_promise.setResult(Boolean.TRUE);
                    }
                    return;
                }
                break;
            default:
                break;
        }

        if(type == Event.MSG || type == Event.VIEW_CHANGE || type == Event.SUSPECT ||
                type == Event.GET_APPLSTATE || type== Event.STATE_TRANSFER_OUTPUTSTREAM
                || type == Event.BLOCK || type == Event.UNBLOCK) {
            try {
                mq.add(evt);
            }
            catch(Exception e) {
                if(log.isErrorEnabled()) log.error("exception adding event " + evt + " to message queue", e);
            }
        }
    }


    /**
     * Sends a message through the protocol stack if the stack is available
     * @param evt the message to send down, encapsulated in an event
     */
    public void down(Event evt) {
        if(evt == null) return;

        // handle setting of additional data (kludge, will be removed soon)
        if(evt.getType() == Event.CONFIG) {
            try {
                Map m=(Map)evt.getArg();
                if(m != null && m.containsKey("additional_data")) {
                    additional_data=(byte[])m.get("additional_data");
                    if(local_addr instanceof IpAddress)
                        ((IpAddress)local_addr).setAdditionalData(additional_data);
                }
            }
            catch(Throwable t) {
                if(log.isErrorEnabled()) log.error("CONFIG event did not contain a hashmap: " + t);
            }
        }

        if(evt.getType() ==  Event.STATE_TRANSFER_INPUTSTREAM_CLOSED){
           state_promise.setResult(Boolean.TRUE);
        }

        if(prot_stack != null)
            prot_stack.down(evt);
        else
            if(log.isErrorEnabled()) log.error("no protocol stack available");
    }



    public String toString(boolean details) {
        StringBuffer sb=new StringBuffer();
        sb.append("local_addr=").append(local_addr).append('\n');
        sb.append("cluster_name=").append(cluster_name).append('\n');
        sb.append("my_view=").append(my_view).append('\n');
        sb.append("connected=").append(connected).append('\n');
        sb.append("closed=").append(closed).append('\n');
        if(mq != null)
            sb.append("incoming queue size=").append(mq.size()).append('\n');
        if(details) {
            sb.append("receive_blocks=").append(receive_blocks).append('\n');
            sb.append("receive_local_msgs=").append(receive_local_msgs).append('\n');
            sb.append("auto_reconnect=").append(auto_reconnect).append('\n');
            sb.append("auto_getstate=").append(auto_getstate).append('\n');
            sb.append("state_transfer_supported=").append(state_transfer_supported).append('\n');
            sb.append("props=").append(props).append('\n');
        }

        return sb.toString();
    }


    /* ----------------------------------- Private Methods ------------------------------------- */


    protected final void init(ProtocolStackConfigurator configurator) throws ChannelException {
        if(log.isInfoEnabled())
            log.info("JGroups version: " + Version.description);
        ConfiguratorFactory.substituteVariables(configurator); // replace vars with system props
        props=configurator.getProtocolStackString();
        prot_stack=new ProtocolStack(this, props);
        try {
            prot_stack.setup(); // Setup protocol stack (create layers, queues between them
        }
        catch(Throwable e) {
            throw new ChannelException("unable to setup the protocol stack", e);
        }
    }


    /**
     * Initializes all variables. Used after <tt>close()</tt> or <tt>disconnect()</tt>,
     * to be ready for new <tt>connect()</tt>
     */
    private void init() {
        local_addr=null;
        cluster_name=null;
        my_view=null;

        // changed by Bela Sept 25 2003
        //if(mq != null && mq.closed())
          //  mq.reset();

        connect_promise.reset();
        disconnect_promise.reset();
        connected=false;
    }


    /**
     * health check.<BR>
     * throws a ChannelNotConnected exception if the channel is not connected
     */
    protected void checkNotConnected() throws ChannelNotConnectedException {
        if(!connected)
            throw new ChannelNotConnectedException();
    }

    /**
     * health check<BR>
     * throws a ChannelClosed exception if the channel is closed
     */
    protected void checkClosed() throws ChannelClosedException {
        if(closed)
            throw new ChannelClosedException();
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
            case Event.EXIT:
                return new ExitEvent();
            default:
                return evt;
        }
    }


    /**
     * Receives the state from the group and modifies the JChannel.state object<br>
     * This method initializes the local state variable to null, and then sends the state
     * event down the stack. It waits for a GET_STATE_OK event to bounce back
     * @param evt the get state event, has to be of type Event.GET_STATE
     * @param info Information about the state transfer, e.g. target member and timeout
     * @return true of the state was received, false if the operation timed out
     */
    private boolean _getState(Event evt, StateTransferInfo info) throws ChannelNotConnectedException, ChannelClosedException {
        checkClosed();
        checkNotConnected();
        if(!state_transfer_supported) {
            throw new IllegalStateException("fetching state will fail as state transfer is not supported. "
                    + "Add one of the STATE_TRANSFER protocols to your protocol configuration");
        }

        if(flush_supported)
           flush_unblock_promise.reset();

        state_promise.reset();
        down(evt);
        Boolean state_transfer_successfull=(Boolean)state_promise.getResult(info.timeout);

        //if FLUSH is used do not return from getState() until UNBLOCK event is received
        boolean shouldWaitForUnblock = flush_supported && receive_blocks;
        if(shouldWaitForUnblock){
           try{
              flush_unblock_promise.getResultWithTimeout(FLUSH_UNBLOCK_TIMEOUT);
           }
           catch (TimeoutException te){
              if(log.isWarnEnabled())
                 log.warn("Waiting on UNBLOCK after getState timed out");
           }
        }

        return state_transfer_successfull != null && state_transfer_successfull.booleanValue();
    }


    /**
     * Disconnects and closes the channel.
     * This method does the folloing things
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
        if(closed)
            return;

        if(disconnect)
            disconnect();                     // leave group if connected

        if(close_mq) {
            try {
                if(mq != null)
                    mq.close(false);              // closes and removes all messages
            }
            catch(Exception e) {
                if(log.isErrorEnabled()) log.error("exception: " + e);
            }
        }

        if(prot_stack != null) {
            try {
                prot_stack.stopStack();
                prot_stack.destroy();
            }
            catch(Exception e) {
                if(log.isErrorEnabled()) log.error("failed destroying the protocol stack", e);
            }
        }
        closed=true;
        connected=false;
        notifyChannelClosed(this);
        init(); // sets local_addr=null; changed March 18 2003 (bela) -- prevented successful rejoining
    }


    public final void closeMessageQueue(boolean flush_entries) {
        if(mq != null)
            mq.close(flush_entries);
    }


    /**
     * Creates a separate thread to close the protocol stack.
     * This is needed because the thread that called JChannel.up() with the EXIT event would
     * hang waiting for up() to return, while up() actually tries to kill that very thread.
     * This way, we return immediately and allow the thread to terminate.
     */
    private void handleExit(Event evt) {
        notifyChannelShunned();
        if(closer != null && !closer.isAlive())
            closer=null;
        if(closer == null) {
            if(log.isInfoEnabled())
                log.info("received an EXIT event, will leave the channel");
            closer=new CloserThread(evt);
            closer.start();
        }
    }

    public boolean flushSupported() {
        return flush_supported;
    }

    /**
     * Will perform a flush of the system, ie. all pending messages are flushed out of the 
     * system and all members ack their reception. After this call return, no member will 
     * be sending any messages until {@link #stopFlush()} is called.
     * <p>
     * 
     * In case of flush collisions random sleep time backoff algorithm is employed and 
     * flush is reattempted for numberOfAttempts. Therefore this method is guaranteed 
     * to return after timeout*numberOfAttempts miliseconds.
     * 
     * 
     * @param timeout
     * @param numberOfAttempts if flush was unsuccessful attempt again until numberOfAttempts is 0
     * @param automatic_resume Call {@link #stopFlush()} after the flush
     * @return true if FLUSH completed within the timeout
     */
    public boolean startFlush(long timeout, int numberOfAttempts, boolean automatic_resume) {
        if(!flush_supported) {
            throw new IllegalStateException("Flush is not supported, add pbcast.FLUSH protocol to your configuration");
        }

        boolean successfulFlush = false;
        flush_promise.reset();        
        down(new Event(Event.SUSPEND));
        try{  
           Boolean r = null;
           if(flush_promise.hasResult()){              
              r = (Boolean)flush_promise.getResult();
              successfulFlush = r.booleanValue();
           }
           else{              
              r = (Boolean) flush_promise.getResultWithTimeout(timeout);
              successfulFlush = r.booleanValue();
           }
        }
        catch (TimeoutException e){
           //it is normal to get timeouts - it is the final outcome that counts 
           //we will just retry below
           
           if(log.isInfoEnabled())
              log.info("JChannel.startFlush requested by " + local_addr
                 + " timed out waiting for flush responses after " + timeout + " msec");
        }

        if (!successfulFlush && numberOfAttempts > 0){
           long backOffSleepTime = Util.random(5000);
           if(log.isInfoEnabled())               
              log.info("Flush in progress detected at " + local_addr + ". Backing off for "
                    + backOffSleepTime + " ms. Attempts left " + numberOfAttempts);
           
           Util.sleepRandom(backOffSleepTime);      
           successfulFlush = startFlush(timeout, --numberOfAttempts ,automatic_resume);
        }     

        if(automatic_resume)
            stopFlush();

        return successfulFlush;              
    }
    
    /**
     * Will perform a flush of the system, ie. all pending messages are flushed out of the 
     * system and all members ack their reception. After this call return, no member will 
     * be sending any messages until {@link #stopFlush()} is called.
     * <p>
     * 
     * In case of flush collisions random sleep time backoff algorithm is employed and 
     * flush is reattempted for a default of three times. Therefore this method is guaranteed 
     * to return after timeout*3 miliseconds.
     *     
     * @param timeout
     * @param automatic_resume Call {@link #stopFlush()} after the flush
     * @return true if FLUSH completed within the timeout
     */
    public boolean startFlush(long timeout, boolean automatic_resume) {       
       int defaultNumberOfFlushAttempts = 3;
       return startFlush(timeout,defaultNumberOfFlushAttempts, automatic_resume);
    }

    public void stopFlush() {
        if(!flush_supported) {
            throw new IllegalStateException("Flush is not supported, add pbcast.FLUSH protocol to your configuration");
        }
        
        flush_unblock_promise.reset();
        down(new Event(Event.RESUME));
        
        //do not return until UNBLOCK event is received        
        boolean shouldWaitForUnblock = receive_blocks;        
        if(shouldWaitForUnblock){
           try{              
              flush_unblock_promise.getResultWithTimeout(5000);
           }
           catch (TimeoutException te){              
           }
        }
    }

    Address determineCoordinator() {
        Vector mbrs=my_view != null? my_view.getMembers() : null;
        if(mbrs == null)
            return null;
        if(mbrs.size() > 0)
            return (Address)mbrs.firstElement();
        return null;
    }

    /* ------------------------------- End of Private Methods ---------------------------------- */


    class CloserThread extends Thread {
        final Event evt;
        final Thread t=null;


        CloserThread(Event evt) {
            super(Util.getGlobalThreadGroup(), "CloserThread");
            this.evt=evt;
            setDaemon(true);
        }


        public void run() {
            try {
                String old_cluster_name=cluster_name; // remember because close() will null it
                if(log.isInfoEnabled())
                    log.info("closing the channel");
                _close(false, false); // do not disconnect before closing channel, do not close mq (yet !)

                if(up_handler != null)
                    up_handler.up(this.evt);
                else {
                    try {
                        if(receiver == null)
                            mq.add(this.evt);
                    }
                    catch(Exception ex) {
                        if(log.isErrorEnabled()) log.error("exception: " + ex);
                    }
                }

                if(mq != null) {
                    Util.sleep(500); // give the mq thread a bit of time to deliver EXIT to the application
                    try {
                        mq.close(false);
                    }
                    catch(Exception ex) {
                    }
                }

                if(auto_reconnect) {
                    try {
                        if(log.isInfoEnabled()) log.info("reconnecting to group " + old_cluster_name);
                        open();
                    }
                    catch(Exception ex) {
                        if(log.isErrorEnabled()) log.error("failure reopening channel: " + ex);
                        return;
                    }
                    try {
                        if(additional_data != null) {
                            // send previously set additional_data down the stack - other protocols (e.g. TP) use it
                            Map m=new HashMap(11);
                            m.put("additional_data", additional_data);
                            down(new Event(Event.CONFIG, m));
                        }
                        connect(old_cluster_name);
                        notifyChannelReconnected(local_addr);
                    }
                    catch(Exception ex) {
                        if(log.isErrorEnabled()) log.error("failure reconnecting to channel: " + ex);
                        return;
                    }
                }

                if(auto_getstate && state_transfer_supported) {
                    if(log.isInfoEnabled())
                        log.info("fetching the state (auto_getstate=true)");
                    boolean rc=JChannel.this.getState(null, GET_STATE_DEFAULT_TIMEOUT);
                    if(log.isInfoEnabled()) {
                        if(rc)
                            log.info("state was retrieved successfully");
                        else
                            log.info("state transfer failed");
                    }
                }

            }
            catch(Exception ex) {
                if(log.isErrorEnabled()) log.error("exception: " + ex);
            }
            finally {
                closer=null;
            }
        }
    }

}
