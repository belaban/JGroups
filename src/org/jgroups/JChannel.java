// $Id: JChannel.java,v 1.29 2004/09/23 16:29:59 belaban Exp $

package org.jgroups;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.conf.ConfiguratorFactory;
import org.jgroups.conf.ProtocolStackConfigurator;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.stack.StateTransferInfo;
import org.jgroups.util.*;
import org.w3c.dom.Element;

import java.io.File;
import java.io.Serializable;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

/**
 * JChannel is a pure Java implementation of Channel
 * When a JChannel object is instantiated it automatically sets up the
 * protocol stack
 * @author Bela Ban
 * @author Filip Hanik
 * @version $Revision: 1.29 $
 */
public class JChannel extends Channel {

    /**
     * The default protocol stack used by the default constructor.
     */
    public static final String DEFAULT_PROTOCOL_STACK=
            "UDP(mcast_addr=228.1.2.3;mcast_port=45566;ip_ttl=32):" +
            "PING(timeout=3000;num_initial_members=6):" +
            "FD(timeout=3000):" +
            "VERIFY_SUSPECT(timeout=1500):" +
            "pbcast.NAKACK(gc_lag=10;retransmit_timeout=600,1200,2400,4800):" +
            "UNICAST(timeout=600,1200,2400,4800):" +
            "pbcast.STABLE(desired_avg_gossip=10000):" +
            "FRAG:" +
            "pbcast.GMS(join_timeout=5000;join_retry_timeout=2000;" +
            "shun=true;print_local_addr=true)";

    static final String FORCE_PROPS="force.properties";

    /* the protocol stack configuration string */
    private String props=null;

    /*the address of this JChannel instance*/
    private Address local_addr=null;
    /*the channel (also know as group) name*/
    private String channel_name=null;  // group name
    /*the latest view of the group membership*/
    private View my_view=null;
    /*the queue that is used to receive messages (events) from the protocol stack*/
    private final Queue mq=new Queue();
    /*the protocol stack, used to send and receive messages from the protocol stack*/
    private ProtocolStack prot_stack=null;

    /** Thread responsible for closing a channel and potentially reconnecting to it (e.g. when shunned) */
    protected CloserThread closer=null;

    /** To wait until a local address has been assigned */
    private final Promise local_addr_promise=new Promise();

    /** To wait until we have connected successfully */
    private final Promise connect_promise=new Promise();

    /** To wait until we have been disconnected from the channel */
    private final Promise disconnect_promise=new Promise();

    private final Promise state_promise=new Promise();

    // private final Promise flow_control_promise=new Promise();
    // private final Object  flow_control_mutex=new Object();

    /** wait until we have a non-null local_addr */
    private long LOCAL_ADDR_TIMEOUT=30000; //=Long.parseLong(System.getProperty("local_addr.timeout", "30000"));
    /*if the states is fetched automatically, this is the default timeout, 5 secs*/
    private static final long GET_STATE_DEFAULT_TIMEOUT=5000;
    /*flag to indicate whether to receive views from the protocol stack*/
    private boolean receive_views=true;
    /*flag to indicate whether to receive suspect messages*/
    private boolean receive_suspects=true;
    /*flag to indicate whether to receive blocks, if this is set to true, receive_views is set to true*/
    private boolean receive_blocks=false;
    /*flag to indicate whether to receive local messages
     *if this is set to false, the JChannel will not receive messages sent by itself*/
    private boolean receive_local_msgs=true;
    /*flag to indicate whether to receive a state message or not*/
    private boolean receive_get_states=false;
    /*flag to indicate whether the channel will reconnect (reopen) when the exit message is received*/
    private boolean auto_reconnect=false;
    /*flag t indicate whether the state is supposed to be retrieved after the channel is reconnected
     *setting this to true, automatically forces auto_reconnect to true*/
    private boolean auto_getstate=false;
    /*channel connected flag*/
    private boolean connected=false;

    /** block send()/down() if true (unlocked by UNBLOCK_SEND event) */
    private final CondVar block_sending=new CondVar("block_sending", Boolean.FALSE);

    /*channel closed flag*/
    private boolean closed=false;      // close() has been called, channel is unusable

    /** True if a state transfer protocol is available, false otherwise */
    private boolean state_transfer_supported=false; // set by CONFIG event from STATE_TRANSFER protocol

    /** Used to maintain additional data across channel disconnects/reconnects. This is a kludge and will be remove
     * as soon as JGroups supports logical addresses
     */
    private byte[] additional_data=null;

    protected final Log log=LogFactory.getLog(getClass());

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
        props = configurator.getProtocolStackString();

        /*create the new protocol stack*/
        prot_stack=new ProtocolStack(this, props);

        /* Setup protocol stack (create layers, queues between them */
        try {
            prot_stack.setup();
        }
        catch(Throwable e) {
            throw new ChannelException("unable to setup the protocol stack", e);
        }
    }

    /**
     * Creates a new JChannel with the protocol stack as defined in the properties
     * parameter. an example of this parameter is<BR>
     * "UDP:PING:FD:STABLE:NAKACK:UNICAST:FRAG:FLUSH:GMS:VIEW_ENFORCER:STATE_TRANSFER:QUEUE"<BR>
     * Other examples can be found in the ./conf directory<BR>
     * @param properties the protocol stack setup, if null, the default protocol stack will be used
     * @param properties the properties can also be a java.net.URL object or a string that is a URL spec.
     *                   The JChannel will validate any URL object and String object to see if they are a URL.
     *                   In case of the parameter being a url, the JChannel will try to load the xml from there.
     *                   In case properties is a org.w3c.dom.Element, the ConfiguratorFactory will parse the
     *                   DOM tree with the element as its root element.
     * @deprecated Use the constructors with specific parameter types instead.
     */
    public JChannel(Object properties) throws ChannelException {
        if (properties == null) {
            properties = DEFAULT_PROTOCOL_STACK;
        }

        try {
            ProtocolStackConfigurator c=ConfiguratorFactory.getStackConfigurator(properties);
            props=c.getProtocolStackString();
        }
        catch(Exception x) {
            String strace=Util.getStackTrace(x);
            if(log.isErrorEnabled()) log.error(strace);
            throw new ChannelException("unable to load protocol stack: {" + x.getMessage() + ';' + strace + '}');
        }

        /*create the new protocol stack*/
        prot_stack=new ProtocolStack(this, props);

        /* Setup protocol stack (create layers, queues between them */
        try {
            prot_stack.setup();
        }
        catch(Throwable e) {
            throw new ChannelException("JChannel(): " + e);
        }
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


    /**
     * returns the protocol stack configuration in string format.
     * an example of this property is<BR>
     * "UDP:PING:FD:STABLE:NAKACK:UNICAST:FRAG:FLUSH:GMS:VIEW_ENFORCER:STATE_TRANSFER:QUEUE"
     */
    public String getProperties() {
        return props;
    }


    /**
     * Returns a pretty-printed form of all the protocols. If include_properties is set,
     * the properties for each protocol will also be printed.
     */
    public String printProtocolSpec(boolean include_properties) {
        return prot_stack != null ? prot_stack.printProtocolSpec(include_properties) : null;
    }


    /**
     * Connects the channel to a group.<BR>
     * If the channel is already connected, an error message will be printed to the error log<BR>
     * If the channel is closed a ChannelClosed exception will be thrown<BR>
     * This method starts the protocol stack by calling ProtocolStack.start<BR>
     * then it sends an Event.CONNECT event down the stack and waits to receive a CONNECT_OK event<BR>
     * Once the CONNECT_OK event arrives from the protocol stack, any channel listeners are notified<BR>
     * and the channel is considered connected<BR>
     *
     * @param channel_name A <code>String</code> denoting the group name. Cannot be null.
     * @exception ChannelException The protocol stack cannot be started
     * @exception ChannelClosedException The channel is closed and therefore cannot be used any longer.
     *                                   A new channel has to be created first.
     */
    public synchronized void connect(String channel_name) throws ChannelException, ChannelClosedException {
        /*make sure the channel is not closed*/
        checkClosed();

        /*if we already are connected, then ignore this*/
        if(connected) {
            if(log.isErrorEnabled()) log.error("already connected to " + channel_name);
            return;
        }

        /*make sure we have a valid channel name*/
        if(channel_name == null) {
            if(log.isInfoEnabled()) log.info("channel_name is null, assuming unicast channel");
        }
        else
            this.channel_name=channel_name;

        try {
            prot_stack.startStack(); // calls start() in all protocols, from top to bottom
        }
        catch(Throwable e) {
            if(log.isErrorEnabled()) log.error("exception: " + e);
            throw new ChannelException(e.toString());
        }

        /* try to get LOCAL_ADDR_TIMEOUT. Catch SecurityException if called in an untrusted environment (e.g. using JNLP) */
        try {
            LOCAL_ADDR_TIMEOUT=Long.parseLong(System.getProperty("local_addr.timeout","30000"));
        }
        catch (SecurityException e1) {
            /* Use the default value specified above*/
        }

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
        if(channel_name != null) {
            connect_promise.reset();
            Event connect_event=new Event(Event.CONNECT, channel_name);
            down(connect_event);
            connect_promise.getResult();  // waits forever until connected (or channel is closed)
        }

        /*notify any channel listeners*/
        connected=true;
        if(channel_listener != null)
            channel_listener.channelConnected(this);
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

            if(channel_name != null) {

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

            if(channel_listener != null)
                channel_listener.channelDisconnected(this);

            init(); // sets local_addr=null; changed March 18 2003 (bela) -- prevented successful rejoining
        }
    }


    /**
     * Destroys the channel.<BR>
     * After this method has been called, the channel us unusable.<BR>
     * This operation will disconnect the channel and close the channel receive queue immediately<BR>
     */
    public synchronized void close() {
        _close(true, true); // by default disconnect before closing channel and close mq
    }


    /**
     * Opens the channel.<BR>
     * this does the following actions<BR>
     * 1. Resets the receiver queue by calling Queue.reset<BR>
     * 2. Sets up the protocol stack by calling ProtocolStack.setup<BR>
     * 3. Sets the closed flag to false.<BR>
     */
    public synchronized void open() throws ChannelException {
        if(!closed)
            throw new ChannelException("JChannel.open(): channel is already open");

        try {
            mq.reset();

            // new stack is created on open() - bela June 12 2003
            prot_stack=new ProtocolStack(this, props);
            prot_stack.setup();
            closed=false;
        }
        catch(Exception e) {
            throw new ChannelException("JChannel().open(): " + e.getMessage());
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


    /**
     * implementation of the Transport interface.<BR>
     * Sends a message through the protocol stack<BR>
     * @param msg the message to be sent through the protocol stack,
     *        the destination of the message is specified inside the message itself
     * @exception ChannelNotConnectedException
     * @exception ChannelClosedException
     */
    public void send(Message msg) throws ChannelNotConnectedException, ChannelClosedException {
        checkClosed();
        checkNotConnected();
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
        Object retval=null;
        Event evt;

        checkClosed();
        checkNotConnected();

        try {
            evt=(timeout <= 0) ? (Event)mq.remove() : (Event)mq.remove(timeout);
            retval=getEvent(evt);
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
        Object retval=null;
        Event evt;

        checkClosed();
        checkNotConnected();

        try {
            evt=(timeout <= 0) ? (Event)mq.peek() : (Event)mq.peek(timeout);
            retval=getEvent(evt);
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
     * returns the current view.<BR>
     * if the channel is not connected or if it is closed it will return null<BR>
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
     */
    public String getChannelName() {
        return closed ? null : !connected ? null : channel_name;
    }


    /**
     * sets a channel option
     * the options can be either
     * <PRE>
     *     Channel.BLOCK
     *     Channel.VIEW
     *     Channel.SUSPECT
     *     Channel.LOCAL
     *     Channel.GET_STATE_EVENTS
     *     Channel.AUTO_RECONNECT
     *     Channel.AUTO_GETSTATE
     * </PRE>
     * There are certain dependencies between the options that you can set, I will try to describe them here<BR>
     * Option: Channel.VIEW option<BR>
     * Value:  java.lang.Boolean<BR>
     * Result: set to true the JChannel will receive VIEW change events<BR>
     *<BR>
     * Option: Channel.SUSPECT<BR>
     * Value:  java.lang.Boolean<BR>
     * Result: set to true the JChannel will receive SUSPECT events<BR>
     *<BR>
     * Option: Channel.BLOCK<BR>
     * Value:  java.lang.Boolean<BR>
     * Result: set to true will set setOpt(VIEW, true) and the JChannel will receive BLOCKS and VIEW events<BR>
     *<BR>
     * Option: GET_STATE_EVENTS<BR>
     * Value:  java.lang.Boolean<BR>
     * Result: set to true the JChannel will receive state events<BR>
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
                if(value instanceof Boolean)
                    receive_views=((Boolean)value).booleanValue();
                else
                    if(log.isErrorEnabled()) log.error("option " + Channel.option2String(option) +
                                                     " (" + value + "): value has to be Boolean");
                break;
            case SUSPECT:
                if(value instanceof Boolean)
                    receive_suspects=((Boolean)value).booleanValue();
                else
                    if(log.isErrorEnabled()) log.error("option " + Channel.option2String(option) +
                                                     " (" + value + "): value has to be Boolean");
                break;
            case BLOCK:
                if(value instanceof Boolean)
                    receive_blocks=((Boolean)value).booleanValue();
                else
                    if(log.isErrorEnabled()) log.error("option " + Channel.option2String(option) +
                                                     " (" + value + "): value has to be Boolean");
                if(receive_blocks)
                    receive_views=true;
                break;

            case GET_STATE_EVENTS:
                if(value instanceof Boolean)
                    receive_get_states=((Boolean)value).booleanValue();
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
//                return Boolean.valueOf(receive_views);
            	return receive_views ? Boolean.TRUE : Boolean.FALSE;
            case BLOCK:
//                return Boolean.valueOf(receive_blocks);
            	return receive_blocks ? Boolean.TRUE : Boolean.FALSE;
            case SUSPECT:
//                return Boolean.valueOf(receive_suspects);
            	return receive_suspects ? Boolean.TRUE : Boolean.FALSE;
            case GET_STATE_EVENTS:
//                return Boolean.valueOf(receive_get_states);
            	return receive_get_states ? Boolean.TRUE : Boolean.FALSE;
            case LOCAL:
//                return Boolean.valueOf(receive_local_msgs);
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
     * @param target - the target member to receive the state from. if null, state is retrieved from coordinator
     * @param timeout - the number of milliseconds to wait for the operation to complete successfully
     * @return true of the state was received, false if the operation timed out
     */
    public boolean getState(Address target, long timeout) throws ChannelNotConnectedException, ChannelClosedException {
        StateTransferInfo info=new StateTransferInfo(StateTransferInfo.GET_FROM_SINGLE, target);
        info.timeout=timeout;
        return _getState(new Event(Event.GET_STATE, info), timeout);
    }


    /**
     * Retrieves the current group state. Sends GET_STATE event down to STATE_TRANSFER layer.
     * Blocks until STATE_TRANSFER sends up a GET_STATE_OK event or until <code>timeout</code>
     * milliseconds have elapsed. The argument of GET_STATE_OK should be a vector of objects.
     * @param targets - the target members to receive the state from ( an Address list )
     * @param timeout - the number of milliseconds to wait for the operation to complete successfully
     * @return true of the state was received, false if the operation timed out
     */
    public boolean getAllStates(Vector targets, long timeout) throws ChannelNotConnectedException, ChannelClosedException {
        StateTransferInfo info=new StateTransferInfo(StateTransferInfo.GET_FROM_MANY, targets);
        return _getState(new Event(Event.GET_STATE, info), timeout);
    }


    /**
     * Called by the application is response to receiving a <code>getState()</code> object when
     * calling <code>receive()</code>.<br>
     * When the application receives a getState() message on the receive() method,
     * it should call returnState() to reply with the state of the application
     * @param state The state of the application as a byte buffer
     *              (to send over the network).
     */
    public void returnState(byte[] state) {
        down(new Event(Event.GET_APPLSTATE_OK, state));
    }





    /**
     * Callback method <BR>
     * Called by the ProtocolStack when a message is received.
     * It will be added to the message queue from which subsequent
     * <code>Receive</code>s will dequeue it.
     * @param evt the event carrying the message from the protocol stack
     */
    public void up(Event evt) {
        int type=evt.getType();
        Message msg;

        /*if the queue is not available, there is no point in
         *processing the message at all*/
        if(mq == null) {
            if(log.isErrorEnabled()) log.error("message queue is null");
            return;
        }

        switch(type) {

            case Event.MSG:
                msg=(Message)evt.getArg();
                if(!receive_local_msgs) {  // discard local messages (sent by myself to me)
                    if(local_addr != null && msg.getSrc() != null)
                        if(local_addr.equals(msg.getSrc()))
                            return;
                }
                break;

            case Event.VIEW_CHANGE:
                my_view=(View)evt.getArg();

                // crude solution to bug #775120: if we get our first view *before* the CONNECT_OK,
                // we simply set the state to connected
                if(connected == false) {
                    connected=true;
                    connect_promise.setResult(Boolean.TRUE);
                }

                // unblock queueing of messages due to previous BLOCK event:
                down(new Event(Event.STOP_QUEUEING));
                if(!receive_views)  // discard if client has not set receving views to on
                    return;
                //if(connected == false)
                //  my_view=(View)evt.getArg();
                break;

            case Event.SUSPECT:
                if(!receive_suspects)
                    return;
                break;

            case Event.GET_APPLSTATE:  // return the application's state
                if(!receive_get_states) {  // if not set to handle state transfers, send null state
                    down(new Event(Event.GET_APPLSTATE_OK, null));
                    return;
                }
                break;

            case Event.CONFIG:
                HashMap config=(HashMap)evt.getArg();
                if(config != null && config.containsKey("state_transfer"))
                    state_transfer_supported=((Boolean)config.get("state_transfer")).booleanValue();
                break;

            case Event.BLOCK:
                // If BLOCK is received by application, then we trust the application to not send
                // any more messages until a VIEW_CHANGE is received. Otherwise (BLOCKs are disabled),
                // we queue any messages sent until the next VIEW_CHANGE (they will be sent in the
                // next view)

                if(!receive_blocks) {  // discard if client has not set 'receiving blocks' to 'on'
                    down(new Event(Event.BLOCK_OK));
                    down(new Event(Event.START_QUEUEING));
                    return;
                }
                break;

            case Event.CONNECT_OK:
                connect_promise.setResult(Boolean.TRUE);
                break;

            case Event.DISCONNECT_OK:
                disconnect_promise.setResult(Boolean.TRUE);
                break;

            case Event.GET_STATE_OK:
                try {
                    mq.add(new Event(Event.STATE_RECEIVED, evt.getArg()));
                }
                catch(Exception e) {
                }
                state_promise.setResult(evt.getArg());
                break;

            case Event.SET_LOCAL_ADDRESS:
                local_addr_promise.setResult(evt.getArg());
                break;

            case Event.EXIT:
                handleExit(evt);
                return;  // no need to pass event up; already done in handleExit()

            case Event.BLOCK_SEND: // emitted by FLOW_CONTROL
                if(log.isInfoEnabled()) log.info("received BLOCK_SEND");
                block_sending.set(Boolean.TRUE);
                break;

            case Event.UNBLOCK_SEND:  // emitted by FLOW_CONTROL
                if(log.isInfoEnabled()) log.info("received UNBLOCK_SEND");
                block_sending.set(Boolean.FALSE);
                break;

            default:
                break;
        }


        // If UpHandler is installed, pass all events to it and return (UpHandler is e.g. a building block)
        if(up_handler != null) {
            up_handler.up(evt);
            return;
        }

        if(type == Event.MSG || type == Event.VIEW_CHANGE || type == Event.SUSPECT ||
                type == Event.GET_APPLSTATE || type == Event.BLOCK) {
            try {
                mq.add(evt);
            }
            catch(Exception e) {
                if(log.isErrorEnabled()) log.error("exception: " + e);
            }
        }
    }


    /**
     * Sends a message through the protocol stack if the stack is available
     * @param evt the message to send down, encapsulated in an event
     */
    public void down(Event evt) {
        if(evt == null) return;

        // only block for messages; all other events are passed through
        // we use double-checked locking; it is okay to 'lose' one or more messages because block_sending changes
        // to true after an initial false value
        if(evt.getType() == Event.MSG && block_sending.get().equals(Boolean.TRUE)) {
            if(log.isInfoEnabled()) log.info("down() blocks because block_sending == true");
            block_sending.waitUntil(Boolean.FALSE);
        }

        // handle setting of additional data (kludge, will be removed soon)
        if(evt.getType() == Event.CONFIG) {
            try {
                Map m=(Map)evt.getArg();
                if(m != null && m.containsKey("additional_data")) {
                    additional_data=(byte[])m.get("additional_data");
                }
            }
            catch(Throwable t) {
                if(log.isErrorEnabled()) log.error("CONFIG event did not contain a hashmap: " + t);
            }

        }

        if(prot_stack != null)
            prot_stack.down(evt);
        else
            if(log.isErrorEnabled()) log.error("no protocol stack available");
    }


    public String toString(boolean details) {
        StringBuffer sb=new StringBuffer();
        sb.append("local_addr=").append(local_addr).append('\n');
        sb.append("channel_name=").append(channel_name).append('\n');
        sb.append("my_view=").append(my_view).append('\n');
        sb.append("connected=").append(connected).append('\n');
        sb.append("closed=").append(closed).append('\n');
        if(mq != null)
            sb.append("incoming queue size=").append(mq.size()).append('\n');
        if(details) {
            sb.append("block_sending=").append(block_sending).append('\n');
            sb.append("receive_views=").append(receive_views).append('\n');
            sb.append("receive_suspects=").append(receive_suspects).append('\n');
            sb.append("receive_blocks=").append(receive_blocks).append('\n');
            sb.append("receive_local_msgs=").append(receive_local_msgs).append('\n');
            sb.append("receive_get_states=").append(receive_get_states).append('\n');
            sb.append("auto_reconnect=").append(auto_reconnect).append('\n');
            sb.append("auto_getstate=").append(auto_getstate).append('\n');
            sb.append("state_transfer_supported=").append(state_transfer_supported).append('\n');
            sb.append("props=").append(props).append('\n');
        }

        return sb.toString();
    }


    /* ----------------------------------- Private Methods ------------------------------------- */


    /**
     * Initializes all variables. Used after <tt>close()</tt> or <tt>disconnect()</tt>,
     * to be ready for new <tt>connect()</tt>
     */
    private void init() {
        local_addr=null;
        channel_name=null;
        my_view=null;

        // changed by Bela Sept 25 2003
        //if(mq != null && mq.closed())
          //  mq.reset();

        connect_promise.reset();
        disconnect_promise.reset();
        connected=false;
        block_sending.set(Boolean.FALSE);
    }


    /**
     * health check.<BR>
     * throws a ChannelNotConnected exception if the channel is not connected
     */
    void checkNotConnected() throws ChannelNotConnectedException {
        if(!connected)
            throw new ChannelNotConnectedException();
    }

    /**
     * health check<BR>
     * throws a ChannelClosed exception if the channel is closed
     */
    void checkClosed() throws ChannelClosedException {
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
            case Event.GET_APPLSTATE:
                return new GetStateEvent(evt.getArg());
            case Event.STATE_RECEIVED:
                return new SetStateEvent((byte[])evt.getArg());
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
     * @param timeout the number of milliseconds to wait for the GET_STATE_OK response
     * @return true of the state was received, false if the operation timed out
     */
    boolean _getState(Event evt, long timeout) throws ChannelNotConnectedException, ChannelClosedException {
        checkClosed();
        checkNotConnected();
        if(!state_transfer_supported) {
            log.error("fetching state will fail as state transfer is not supported. "
                      + "Add one of the STATE_TRANSFER protocols to your protocol configuration");
            return false;
        }

        state_promise.reset();
        down(evt);
        byte[] state=(byte[])state_promise.getResult(timeout);
        if(state != null) // state set by GET_STATE_OK event
            return true;
        else
            return false;
    }


    /**
     * Disconnects and closes the channel.
     * This method does the folloing things
     * 1. Calls <code>this.disconnect</code> if the disconnect parameter is true
     * 2. Calls <code>Queue.close</code> on mq if the close_mq parameter is true
     * 3. Calls <code>ProtocolStack.stop</code> on the protocol stack
     * 4. Calls <code>ProtocolStack.destroy</code> on the protocol stack
     * 5. Sets the channel closed and channel connected flags to true and false
     * 6. Notifies any channel listener of the channel close operation
     */
    void _close(boolean disconnect, boolean close_mq) {
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
                if(log.isErrorEnabled()) log.error("exception: " + e);
            }
        }
        closed=true;
        connected=false;
        if(channel_listener != null)
            channel_listener.channelClosed(this);
        init(); // sets local_addr=null; changed March 18 2003 (bela) -- prevented successful rejoining
    }


    /**
     * Creates a separate thread to close the protocol stack.
     * This is needed because the thread that called JChannel.up() with the EXIT event would
     * hang waiting for up() to return, while up() actually tries to kill that very thread.
     * This way, we return immediately and allow the thread to terminate.
     */
    void handleExit(Event evt) {
        if(channel_listener != null)
            channel_listener.channelShunned();

        if(closer != null && !closer.isAlive())
            closer=null;
        if(closer == null) {
            if(log.isInfoEnabled())
                log.info("received an EXIT event, will leave the channel");
            closer=new CloserThread(evt);
            closer.start();
        }
    }

    /* ------------------------------- End of Private Methods ---------------------------------- */


    class CloserThread extends Thread {
        final Event evt;
        final Thread t=null;


        CloserThread(Event evt) {
            this.evt=evt;
            setName("CloserThread");
            setDaemon(true);
        }


        public void run() {
            try {
                String old_channel_name=channel_name; // remember because close() will null it
                if(log.isInfoEnabled())
                    log.info("closing the channel");
                _close(false, false); // do not disconnect before closing channel, do not close mq (yet !)

                if(up_handler != null)
                    up_handler.up(this.evt);
                else {
                    try {
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
                        if(log.isInfoEnabled()) log.info("reconnecting to group " + old_channel_name);
                        open();
                    }
                    catch(Exception ex) {
                        if(log.isErrorEnabled()) log.error("failure reopening channel: " + ex);
                        return;
                    }
                    try {
                        if(additional_data != null) {
                            // set previously set additional data
                            Map m=new HashMap(11);
                            m.put("additional_data", additional_data);
                            down(new Event(Event.CONFIG, m));
                        }
                        connect(old_channel_name);
                        if(channel_listener != null)
                            channel_listener.channelReconnected(local_addr);
                    }
                    catch(Exception ex) {
                        if(log.isErrorEnabled()) log.error("failure reconnecting to channel: " + ex);
                        return;
                    }
                }

                if(auto_getstate) {
                    if(log.isInfoEnabled())
                        log.info("fetching the state (auto_getstate=true)");
                    boolean rc=JChannel.this.getState(null, GET_STATE_DEFAULT_TIMEOUT);
                    if(rc)
                        if(log.isInfoEnabled()) log.info("state was retrieved successfully");
                    else
                        if(log.isInfoEnabled()) log.info("state transfer failed");
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
