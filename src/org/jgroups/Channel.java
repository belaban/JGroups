
package org.jgroups;


import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.logging.Log;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.DefaultSocketFactory;
import org.jgroups.util.SocketFactory;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;


/**
 A channel represents a group communication endpoint (like BSD datagram sockets). A
 client joins a group by connecting the channel to a group and leaves it by
 disconnecting. Messages sent over the channel are received by all group members that
 are connected to the same group (that is, all members that have the same group name).<p/>

 The FSM for a channel is roughly as follows: a channel is created (<em>unconnected</em>). The channel is connected to
 a group (<em>connected</em>). Messages can now be sent and received. The channel is disconnected from the group
 (<em>unconnected</em>). The channel could now be connected to a different group again.
 The channel is closed (<em>closed</em>).<p/>

 Only a single sender is allowed to be connected to a channel at a time, but there can be
 more than one channel in an application.<p/>

 Messages can be sent to the group members using the <em>send</em> method and messages
 can be received setting a {@link Receiver} in {@link #setReceiver(Receiver)} and implementing the
 {@link Receiver#receive(Message)} callback.<p>

 A channel instance is created using the public constructor.<p/>
 Various degrees of sophistication in message exchange can be achieved using building
 blocks on top of channels; e.g., light-weight groups, synchronous message invocation,
 or remote method calls. Channels are on the same abstraction level as sockets, and
 should really be simple to use. Higher-level abstractions are all built on top of channels.

 @author  Bela Ban
 @see     java.net.DatagramPacket
 @see     java.net.MulticastSocket
 @see     JChannel
 */
@MBean(description="Channel")
public abstract class Channel /* implements Transport */ {
    protected UpHandler            up_handler=null;   // when set, <em>all</em> events are passed to it !
    protected Set<ChannelListener> channel_listeners=null;
    protected Receiver             receiver=null;
    protected SocketFactory        socket_factory=new DefaultSocketFactory();

    @ManagedAttribute(description="Whether or not to discard messages sent by this channel",writable=true)
    protected boolean discard_own_messages=false;

    protected abstract Log getLog();


    public abstract ProtocolStack getProtocolStack();

    public SocketFactory getSocketFactory() {
        return socket_factory;
    }

    public void setSocketFactory(SocketFactory factory) {
        socket_factory=factory;
        ProtocolStack stack=getProtocolStack();
        Protocol prot=stack != null? stack.getTopProtocol() : null;
        if(prot != null)
            prot.setSocketFactory(factory);
    }

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
     @see #disconnect()
     */
    abstract public void connect(String cluster_name) throws ChannelException;


    /**
     * Connects this channel to a group and gets a state from a specified member
     * provider.
     * <p/>
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
     * @param timeout the timeout for state transfer.
     *
     * @exception ChannelException The protocol stack cannot be started
     * @exception ChannelException Connecting to cluster was not successful
     * @exception ChannelClosedException The channel is closed and therefore cannot be used any longer.
     *                                   A new channel has to be created first.
     * @exception StateTransferException State transfer was not successful
     *
     */
    abstract public void connect(String cluster_name, Address target, long timeout) throws ChannelException;


    /**
     * Disconnects the channel if it is connected. If the channel is closed or disconnected, this operation is ignored<br/>
     * The channel can then be connected to the same or a different cluster again.
     * @see #connect(String) */
    abstract public void disconnect();


    /**
     Destroys the channel and its associated resources (e.g., the protocol stack). After a channel
     has been closed, invoking methods on it throws the <code>ChannelClosed</code> exception
     (or results in a null operation). It is a null operation if the channel is already closed.<p>
     If the channel is connected to a group, <code>disconnec()t</code> will be called first.
     */
    abstract public void close();



    /**
     Determines whether the channel is open; ie. the protocol stack has been created (may not be connected though).
     */
    abstract public boolean isOpen();


    /**
     Determines whether the channel is connected to a group. This implies it is open. If true is returned,
     then the channel can be used to send and receive messages.
     */
    abstract public boolean isConnected();



    /**
     * Returns a map of statistics of the various protocols and of the channel itself.
     * @return Map<String,Map>. A map where the keys are the protocols ("channel" pseudo key is
     * used for the channel itself") and the values are property maps.
     */
    public abstract Map<String,Object> dumpStats();

    /** Sends a message. The message contains
     <ol>
     <li>a destination address (Address). A <code>null</code> address sends the message
     to all group members.
     <li>a source address. Can be left empty. Will be filled in by the protocol stack.
     <li>a byte buffer. The message contents.
     <li>several additional fields. They can be used by application programs (or patterns). E.g.
     a message ID, flags etc
     </ol>
     @param msg The message to be sent. Destination and buffer should be set. A null destination
     means to send to all group members.
     @exception ChannelNotConnectedException The channel must be connected to send messages.
     @exception ChannelClosedException The channel is closed and therefore cannot be used any longer.
     A new channel has to be created first.
     */
    abstract public void send(Message msg) throws ChannelException;


    /**
     Helper method. Will create a Message(dst, src, obj) and use send(Message).
     @param dst Destination address for message. If null, message will be sent to all current group members
     @param obj Serializable object. Will be serialized into the byte buffer of the Message. If it is <em>
     not</em> serializable, the byte buffer will be null.
     */
    abstract public void send(Address dst, Serializable obj) throws ChannelException;

    /**
     * Sends a message. See {@link #send(Address,byte[],int,int)} for details
     * @param dst
     * @param buf
     * @throws ChannelException
     */
    abstract public void send(Address dst, byte[] buf) throws ChannelException;

    /**
     * Sends a message to a destination.
     * @param dst The destination address. If null, the message will be sent to all cluster nodes (= group members)
     * @param buf The buffer to be sent
     * @param offset The offset into the buffer
     * @param length The length of the data to be sent. Has to be <= buf.length - offset. This will send
     *        <code>length</code> bytes starting at <code>offset</code>
     * @throws ChannelException If send() failed
     */
    abstract public void send(Address dst, byte[] buf, int offset, int length) throws ChannelException;


    /**
     Access to event mechanism of channels. Enables to send and receive events, used by building
     blocks to communicate with (building block) specific protocol layers. Currently useful only
     with JChannel.
     */
    public Object down(Event evt) {
        return null;
    }


    /**
     * Gets the current view. The view may only be available after a successful
     * <code>connect()</code>. The result of calling this method on an unconnected channel
     * is implementation defined (may return null). Calling this method on a closed channel returns a null view.
     * @return The current view.
     */
    abstract public View getView();



    /**
     Returns the channel's own address. The result of calling this method on an unconnected
     channel is implementation defined (may return null). Calling this method on a closed
     channel returns null. Addresses can be used as destination in the <code>send()</code> operation.
     @return The channel's address (opaque)
     */
    abstract public Address getAddress();

    /**
     * Returns the logical name of this channel if set.
     * @return The logical name or null (if not set)
     */
    abstract public String getName();

    /**
     * Returns the logical name of a given member. The lookup is from the local cache of logical address
     * / logical name mappings and no remote communication is performed.
     * @param member
     * @return The logical name for <code>member</code>
     */
    abstract public String getName(Address member);


    /**
     * Sets the logical name for the channel. The name will stay associated with this channel for the channel's
     * lifetime (until close() is called). This method should be called <em>before</em> calling connect().
     * @param name
     */
    abstract public void setName(String name);


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

    public UpHandler getUpHandler() {
        return up_handler;
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

    public Receiver getReceiver() {
        return receiver;
    }

    /**
     * When set to true, all messages sent a member A will be discarded by A.
     * @param flag
     */
    public void setDiscardOwnMessages(boolean flag) {discard_own_messages=flag;}
    
    public boolean getDiscardOwnMessages() {return discard_own_messages;}

    abstract public boolean flushSupported();

    /**
     * Performs a partial flush in a cluster for flush participants.
     * <p/>
     * All pending messages are flushed out only for the flush participants. The remaining members
     * in a cluster are not included in the flush. The flush participants should be a proper subset
     * of a current view.
     * <p/>
     * 
     * @param automatic_resume
     *            Call {@link #stopFlush()} after the flush
     * @see #startFlush(boolean)
     */
    abstract public void startFlush(List<Address> flushParticipants, boolean automatic_resume)
                throws Exception;

    /**
     * Will perform a flush of the system, ie. all pending messages are flushed out of the system
     * and all members ack their reception. After this call returns, no member will be sending any
     * messages until {@link #stopFlush()} is called.
     * <p/>
     * In case of flush collisions, a random sleep time backoff algorithm is employed and the flush
     * is reattempted for numberOfAttempts. Therefore this method is guaranteed to return after
     * timeout x numberOfAttempts miliseconds.
     * 
     * @param automatic_resume
     *            Call {@link #stopFlush()} after the flush
     */
    abstract public void startFlush(boolean automatic_resume) throws Exception;
    
    abstract public void stopFlush();
    
    abstract public void stopFlush(List<Address> flushParticipants);


    /**
     * Retrieves the full state from the target member.
     * <p>
     * State transfer is initiated by invoking getState on this channel, the state requester, and sending a GET_STATE
     * message to a target member - the state provider. The state provider passes a GET_STATE message to the application
     * that is using the the state provider channel which in turn provides an application state to the state requester.
     * Upon successful installation of a state at the state receiver, this method returns true.
     *
     * @param target State provider. If null (default), the coordinator is used
     * @param timeout The number of milliseconds to wait for the operation to complete successfully.
     *                0 waits until the state has been received
     *
     * @see MessageListener#getState(java.io.OutputStream)
     * @see MessageListener#setState(java.io.InputStream)
     * @see MessageListener#getState()
     * @see MessageListener#setState(byte[])
     *
     * @return true If the state transfer was successful, false otherwise
     * @exception ChannelNotConnectedException The channel must be connected to receive messages.
     * @exception ChannelClosedException The channel is closed and therefore cannot be used
     *            any longer. A new channel has to be created first.
     * @exception StateTransferException When there was a problem during the state transfer. The exact subtype of
     *            the exception, plus the exception message will allow a caller to determine what went wrong, and to possbly retry.
     * @throws IllegalStateException If the flush is used in this channel and the cluster could not be flushed
     */
    abstract public boolean getState(Address target, long timeout) throws ChannelException;




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

   

}
