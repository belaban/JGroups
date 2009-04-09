// $Id: Channel.java,v 1.47 2009/04/09 09:11:29 belaban Exp $

package org.jgroups;


import org.apache.commons.logging.Log;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.stack.ProtocolStack;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;


/**
 A channel represents a group communication endpoint (like BSD datagram sockets). A
 client joins a group by connecting the channel to a group address and leaves it by
 disconnecting. Messages sent over the channel are received by all group members that
 are connected to the same group (that is, all members that have the same group
 address).<p>

 The FSM for a channel is roughly as follows: a channel is created
 (<em>unconnected</em>). The channel is connected to a group
 (<em>connected</em>). Messages can now be sent and received. The channel is
 disconnected from the group (<em>unconnected</em>). The channel could now be connected to a
 different group again. The channel is closed (<em>closed</em>).<p>

 Only a single sender is allowed to be connected to a channel at a time, but there can be
 more than one channel in an application.<p>

 Messages can be sent to the group members using the <em>send</em> method and messages
 can be received using <em>receive</em> (pull approach).<p>

 A channel instance is created using either a <em>ChannelFactory</em> or the public
 constructor. Each implementation of a channel must provide a subclass of
 <code>Channel</code> and an implementation of <code>ChannelFactory</code>.  <p>
 Various degrees of sophistication in message exchange can be achieved using building
 blocks on top of channels; e.g., light-weight groups, synchronous message invocation,
 or remote method calls. Channels are on the same abstraction level as sockets, and
 should really be simple to use. Higher-level abstractions are all built on top of
 channels.

 @author  Bela Ban
 @see     java.net.DatagramPacket
 @see     java.net.MulticastSocket
 */
@MBean(description="Channel")
public abstract class Channel implements Transport {
    public static final int BLOCK=0;
    public static final int VIEW=1;
    public static final int SUSPECT=2;
    public static final int LOCAL=3;
    public static final int GET_STATE_EVENTS=4;
    public static final int AUTO_RECONNECT=5;
    public static final int AUTO_GETSTATE=6;


    protected UpHandler            up_handler=null;   // when set, <em>all</em> events are passed to it !
    protected Set<ChannelListener> channel_listeners=null;
    protected Receiver             receiver=null;


    protected abstract Log getLog();


    public abstract ProtocolStack getProtocolStack();

    /**
     Connects the channel to a group. The client is now able to receive group
     messages, views and block events (depending on the options set) and to send
     messages to (all or single) group members.  This is a null operation if already
     connected.<p>

     All channels with the same name form a group, that means all messages
     sent to the group will be received by all channels connected to the same
     channel name.<p>

     @param cluster_name The name of the chanel to connect to.
     @exception ChannelException The protocol stack cannot be started
     @exception ChannelClosedException The channel is closed and therefore cannot be used any longer.
     A new channel has to be created first.
     @see Channel#disconnect
     */
    abstract public void connect(String cluster_name) throws ChannelException;


    /**
     * Connects the channel to a group <em>and</em> fetches the state
     * 
     * @param cluster_name
     *            The name of the cluster to connect to.
     * @param target
     *            The address of the member from which the state is to be
     *            retrieved. If it is null, the state is retrieved from coordinator is contacted.
     * @param state_id
     *            The ID of a substate. If the full state is to be fetched, set
     *            this parameter to null
     * @param timeout
     *            Milliseconds to wait for the state response (0 = wait indefinitely).
     * 
     * @throws ChannelException thrown if connecting to cluster was not successful 
     * @throws StateTransferException thrown if state transfer was not successful
     * 
     */
    abstract public void connect(String cluster_name, Address target, String state_id, long timeout) throws ChannelException;


    /** Disconnects the channel from the current group (if connected), leaving the group.
     It is a null operation if not connected. It is a null operation if the channel is closed.

     @see #connect(String) */
    abstract public void disconnect();


    /**
     Destroys the channel and its associated resources (e.g., the protocol stack). After a channel
     has been closed, invoking methods on it throws the <code>ChannelClosed</code> exception
     (or results in a null operation). It is a null operation if the channel is already closed.<p>
     If the channel is connected to a group, <code>disconnec()t</code> will be called first.
     */
    abstract public void close();


    /** Shuts down the channel without disconnecting if connected, stops all the threads */
    abstract public void shutdown();


    /**
     Re-opens a closed channel. Throws an exception if the channel is already open. After this method
     returns, connect() may be called to join a group. The address of this member will be different from
     the previous incarnation.
     */
    public void open() throws ChannelException {
        ;
    }


    /**
     Determines whether the channel is open; 
     i.e., the protocol stack has been created (may not be connected though).
     */
    abstract public boolean isOpen();


    /**
     Determines whether the channel is connected to a group. This implies it is open. If true is returned,
     then the channel can be used to send and receive messages.
     */
    abstract public boolean isConnected();


    /**
     * Returns the number of messages that are waiting. Those messages can be
     * removed by {@link #receive(long)}. Note that this number could change after
     * calling this method and before calling <tt>receive()</tt> (e.g. the latter
     * method might be called by a different thread).
     * @return The number of messages on the queue, or -1 if the queue/channel
     * is closed/disconnected.
     */
    public int getNumMessages() {
        return -1;
    }

    public String dumpQueue() {
        return "";
    }


    /**
     * Returns a map of statistics of the various protocols and of the channel itself.
     * @return Map<String,Map>. A map where the keys are the protocols ("channel" pseudo key is
     * used for the channel itself") and the values are property maps.
     */
    public abstract Map<String,Object> dumpStats();

    /** Sends a message to a (unicast) destination. The message contains
     <ol>
     <li>a destination address (Address). A <code>null</code> address sends the message
     to all group members.
     <li>a source address. Can be left empty. Will be filled in by the protocol stack.
     <li>a byte buffer. The message contents.
     <li>several additional fields. They can be used by application programs (or patterns). E.g.
     a message ID, a <code>oneway</code> field which determines whether a response is
     expected etc.
     </ol>
     @param msg The message to be sent. Destination and buffer should be set. A null destination
     means to send to all group members.

     @exception ChannelNotConnectedException The channel must be connected to send messages.

     @exception ChannelClosedException The channel is closed and therefore cannot be used any longer.
     A new channel has to be created first.

     */
    abstract public void send(Message msg) throws ChannelNotConnectedException, ChannelClosedException;


    /**
     Helper method. Will create a Message(dst, src, obj) and use send(Message).
     @param dst Destination address for message. If null, message will be sent to all current group members
     @param src Source (sender's) address. If null, it will be set by the protocol's transport layer before
     being put on the wire. Can usually be set to null.
     @param obj Serializable object. Will be serialized into the byte buffer of the Message. If it is <em>
     not</em> serializable, the byte buffer will be null.
     */
    abstract public void send(Address dst, Address src, Serializable obj) throws ChannelNotConnectedException,
                                                                                 ChannelClosedException;


    /**
     Access to event mechanism of channels. Enables to send and receive events, used by building
     blocks to communicate with (building block) specific protocol layers. Currently useful only
     with JChannel.
     */
    public void down(Event evt) {
    }

    /**
     * Can be used instead of down() when a return value is expected. This will be removed in 3.0 when we change
     * the signature of down() to return Object rather than void
     * @param evt
     * @return
     */
    public Object downcall(Event evt) {
        return null;
    }


    /** Receives a message, a view change or a block event. By using <code>setOpt</code>, the
     type of objects to be received can be determined (e.g., not views and blocks, just
     messages).

     The possible types returned can be:
     <ol>
     <li><code>Message</code>. Normal message
     <li><code>Event</code>. All other events (used by JChannel)
     <li><code>View</code>. A view change.
     <li><code>BlockEvent</code>. A block event indicating that a flush protocol has been started, and we should not
     send any more messages. This event should be ack'ed by calling {@link org.jgroups.Channel#blockOk()} .
     Any messages sent after blockOk() returns might get blocked until the flush protocol has completed.
     <li><code>UnblockEvent</code>. An unblock event indicating that the flush protocol has completed and we can resume
     sending messages
     <li><code>SuspectEvent</code>. A notification of a suspected member.
     <li><code>GetStateEvent</code>. The current state of the application should be
     returned using <code>ReturnState</code>.
     <li><code>SetStateEvent</code>. The state of a single/all members as requested previously
     by having called <code>Channel.getState(s).
     <li><code>ExitEvent</code>. Signals that this member was forced to leave the group 
     (e.g., caused by the member being suspected.) The member can rejoin the group by calling
     open(). If the AUTO_RECONNECT is set (see setOpt()), the reconnect will be done automatically.
     </ol>
     The <code>instanceof</code> operator can be used to discriminate between different types
     returned.
     @param timeout Value in milliseconds. Value <= 0 means wait forever
     @return A Message, View, BlockEvent, SuspectEvent, GetStateEvent, SetStateEvent or
     ExitEvent, depending on what is on top of the internal queue.

     @exception ChannelNotConnectedException The channel must be connected to receive messages.

     @exception ChannelClosedException The channel is closed and therefore cannot be used any longer.
     A new channel has to be created first.

     @exception TimeoutException Thrown when a timeout has occurred.
     @deprecated Use a {@link Receiver} instead
     */
    abstract public Object receive(long timeout) throws ChannelNotConnectedException,
                                                        ChannelClosedException, TimeoutException;


    /** Returns the next message, view, block, suspect or other event <em>without removing
     it from the queue</em>.
     @param timeout Value in milliseconds. Value <= 0 means wait forever
     @return A Message, View, BlockEvent, SuspectEvent, GetStateEvent or SetStateEvent object,
     depending on what is on top of the internal queue.

     @exception ChannelNotConnectedException The channel must be connected to receive messages.

     @exception ChannelClosedException The channel is closed and therefore cannot be used any longer.
     A new channel has to be created first.

     @exception TimeoutException Thrown when a timeout has occurred.

     @see #receive(long)
     @deprecated Use a {@link Receiver} instead, this method will not be available in JGroups 3.0
     */
    abstract public Object peek(long timeout) throws ChannelNotConnectedException, ChannelClosedException, TimeoutException;


    /**
     * Gets the current view. This does <em>not</em> retrieve a new view, use
     <code>receive()</code> to do so. The view may only be available after a successful
     <code>connect()</code>. The result of calling this method on an unconnected channel
     is implementation defined (may return null). Calling it on a channel that is not
     enabled to receive view events (via <code>setOpt</code>) returns
     <code>null</code>. Calling this method on a closed channel returns a null view.
     @return The current view.  
     */
    abstract public View getView();


    /**
     Returns the channel's own address. The result of calling this method on an unconnected
     channel is implementation defined (may return null). Calling this method on a closed
     channel returns null. Addresses can be used as destination in the <code>send()</code> operation.
     @return The channel's address (opaque)
     @deprecated Use {@link #getAddress()} instead
     */
    abstract public Address getLocalAddress();



    /**
     Returns the channel's own address. The result of calling this method on an unconnected
     channel is implementation defined (may return null). Calling this method on a closed
     channel returns null. Successor to {@link #getAddress()}. Addresses can be used as destination
     in the <code>send()</code> operation.
     @return The channel's address (opaque)
     */
    abstract public Address getAddress();

    /**
     * Returns the logical name of this channel if set.
     * @return The logical name or null (if not set)
     */
    abstract public String getName();


    /**
     Returns the group address of the group of which the channel is a member. This is
     the object that was the argument to <code>connect()</code>. Calling this method on a closed
     channel returns <code>null</code>.
     @return The group address
     @deprecated Use {@link #getClusterName()} instead */
    abstract public String getChannelName();

    /**
     Returns the cluster name of the group of which the channel is a member. This is
     the object that was the argument to <code>connect()</code>. Calling this method on a closed
     channel returns <code>null</code>.
     @return The cluster name */
    abstract public String getClusterName();


    public String getProperties() {
        return "n/a";
    }

    /**
     When up_handler is set, all events will be passed to it directly. These will not be received
     by the channel (except connect/disconnect, state retrieval and the like). This can be used by
     building blocks on top of a channel; thus the channel is used as a pass-through medium, and
     the building blocks take over some of the channel's tasks. However, tasks such as connection
     management and state transfer is still handled by the channel.
     */
    public void setUpHandler(UpHandler up_handler) {
        this.up_handler=up_handler;
    }


    /**
     Allows to be notified when a channel event such as connect, disconnect or close occurs.
     E.g. a PullPushAdapter may choose to stop when the channel is closed, or to start when
     it is opened.
     @deprecated Use addChannelListener() instead
     */
    public void setChannelListener(ChannelListener channel_listener) {
        addChannelListener(channel_listener);
    }

    /**
     Allows to be notified when a channel event such as connect, disconnect or close occurs.
     E.g. a PullPushAdapter may choose to stop when the channel is closed, or to start when
     it is opened.
     */
    @ManagedOperation
    public synchronized void addChannelListener(ChannelListener listener) {
        if(listener == null)
            return;
        if(channel_listeners == null)
            channel_listeners=new CopyOnWriteArraySet<ChannelListener>();
        channel_listeners.add(listener);
    }

    @ManagedOperation
    public synchronized void removeChannelListener(ChannelListener listener) {
        if(channel_listeners != null && listener != null)
            channel_listeners.remove(listener);
    }

    public synchronized void clearChannelListeners() {
        if(channel_listeners != null)
            channel_listeners.clear();
    }

    /** Sets the receiver, which will handle all messages, view changes etc */
    public void setReceiver(Receiver r) {
        receiver=r;
    }

    /**
     Sets an option. The following options are currently recognized:
     <ol>
     <li><code>BLOCK</code>. Turn the reception of BLOCK events on/off (value is Boolean).
     Default is off
     <li><code>LOCAL</code>. Receive its own broadcast messages to the group
     (value is Boolean). Default is on.
     <li><code>AUTO_RECONNECT</code>. Turn auto-reconnection on/off. If on, when a member if forced out
     of a group (EXIT event), then we will reconnect.
     <li><code>AUTO_GETSTATE</code>. Turn automatic fetching of state after an auto-reconnect on/off.
     This also sets AUTO_RECONNECT to true (if not yet set).
     </ol>
     This method can be called on an unconnected channel. Calling this method on a
     closed channel has no effect.
     */
    abstract public void setOpt(int option, Object value);


    /**
     Gets an option. This method can be called on an unconnected channel.  Calling this
     method on a closed channel returns <code>null</code>.

     @param option  The option to be returned.
     @return The object associated with an option.
     */
    abstract public Object getOpt(int option);

    abstract public boolean flushSupported();
    
    abstract public boolean startFlush(List<Address> flushParticipants,boolean automatic_resume);

    abstract public boolean startFlush(boolean automatic_resume);
    
    abstract public boolean startFlush(long timeout, boolean automatic_resume);

    abstract public void stopFlush();
    
    abstract public void stopFlush(List<Address> flushParticipants);


    /** Called to acknowledge a block() (callback in <code>MembershipListener</code> or
     <code>BlockEvent</code> received from call to <code>Receive</code>).
     After sending BlockOk, no messages should be sent until a new view has been received.
     Calling this method on a closed channel has no effect.
     */
    abstract public void blockOk();


    /**
     Retrieve the state of the group. Will usually contact the oldest group member to get
     the state. When the method returns true, a <code>SetStateEvent</code> will have been
     added to the channel's queue, causing <code>receive()</code> to return the state in one of
     the next invocations. If false, no state will be retrieved by <code>receive()</code>.
     @param target The address of the member from which the state is to be retrieved. If it is
     null, the coordinator is contacted.
     @param timeout Milliseconds to wait for the response (0 = wait indefinitely).
     @return boolean True if the state was retrieved successfully, otherwise false.
     @exception ChannelNotConnectedException The channel must be connected to receive messages.

     @exception ChannelClosedException The channel is closed and therefore cannot be used
     any longer. A new channel has to be created first.

     */
    abstract public boolean getState(Address target, long timeout)
            throws ChannelNotConnectedException, ChannelClosedException;


    /**
     * Fetches a partial state identified by state_id.
     * @param target
     * @param state_id
     * @param timeout
     * @return
     * @throws ChannelNotConnectedException
     * @throws ChannelClosedException
     */
    abstract public boolean getState(Address target, String state_id, long timeout)
            throws ChannelNotConnectedException, ChannelClosedException;

    /**
     Retrieve all states of the group members. Will contact all group members to get
     the states. When the method returns true, a <code>SetStateEvent</code> will have been
     added to the channel's queue, causing <code>Receive</code> to return the states in one of
     the next invocations. If false, no states will be retrieved by <code>Receive</code>.
     @param targets A list of members which are contacted for states. If the list is null,
     all the current members of the group will be contacted.
     @param timeout Milliseconds to wait for the response (0 = wait indefinitely).
     @return boolean True if the state was retrieved successfully, otherwise false.
     @exception ChannelNotConnectedException The channel must be connected to
     receive messages.
     @exception ChannelClosedException The channel is closed and therefore cannot be used
     any longer. A new channel has to be created first.
     @deprecated Not really needed - we always want to get the state from a single member
     */
    abstract public boolean getAllStates(Vector targets, long timeout)
            throws ChannelNotConnectedException, ChannelClosedException;


    /**
     * Called by the application is response to receiving a
     * <code>getState()</code> object when calling <code>receive()</code>.
     * @param state The state of the application as a byte buffer
     *              (to send over the network).
     */
    public abstract void returnState(byte[] state);

    /** Returns a given substate (state_id of null means return entire state) */
    public abstract void returnState(byte[] state, String state_id);
    
    public abstract Map<String,Object> getInfo();
    public abstract void setInfo(String key, Object value);


    public static String option2String(int option) {
        switch(option) {
            case BLOCK:
                return "BLOCK";
            case VIEW:
                return "VIEW";
            case SUSPECT:
                return "SUSPECT";
            case LOCAL:
                return "LOCAL";
            case GET_STATE_EVENTS:
                return "GET_STATE_EVENTS";
            case AUTO_RECONNECT:
                return "AUTO_RECONNECT";
            case AUTO_GETSTATE:
                return "AUTO_GETSTATE";
            default:
                return "unknown (" + option + ')';
        }
    }

    protected void notifyChannelConnected(Channel c) {
        if(channel_listeners == null) return;
        for(ChannelListener channelListener: channel_listeners) {
            try {
                channelListener.channelConnected(c);
            }
            catch(Throwable t) {
                getLog().error("exception in channelConnected() callback", t);
            }
        }
    }

    protected void notifyChannelDisconnected(Channel c) {
        if(channel_listeners == null) return;
        for(ChannelListener channelListener: channel_listeners) {
            try {
                channelListener.channelDisconnected(c);
            }
            catch(Throwable t) {
                getLog().error("exception in channelDisonnected() callback", t);
            }
        }
    }

    protected void notifyChannelClosed(Channel c) {
        if(channel_listeners == null) return;
        for(ChannelListener channelListener: channel_listeners) {
            try {
                channelListener.channelClosed(c);
            }
            catch(Throwable t) {
                getLog().error("exception in channelClosed() callback", t);
            }
        }
    }

    protected void notifyChannelShunned() {
        if(channel_listeners == null) return;
        for(ChannelListener channelListener: channel_listeners) {
            try {
                channelListener.channelShunned();
            }
            catch(Throwable t) {
                getLog().error("exception in channelShunned() callback", t);
            }
        }
    }

    protected void notifyChannelReconnected(Address addr) {
        if(channel_listeners == null) return;
        for(ChannelListener channelListener: channel_listeners) {
            try {
                channelListener.channelReconnected(addr);
            }
            catch(Throwable t) {
                getLog().error("exception in channelReconnected() callback", t);
            }
        }
    }


}
