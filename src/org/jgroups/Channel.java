
package org.jgroups;


import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.DefaultSocketFactory;
import org.jgroups.util.SocketFactory;
import org.jgroups.util.Util;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Consumer;

/**
 * A channel represents a group communication endpoint (like a socket). An application joins a cluster by connecting
 * the channel to a cluster name and leaves it by disconnecting. Messages sent over the channel are received by all
 * cluster members that are connected to the same cluster (that is, all members that have the same cluster name).
 * <p/>
 * The state machine for a channel is as follows: a channel is created (<em>unconnected</em>). The
 * channel is connected to a cluster (<em>connected</em>). Messages can now be sent and received. The
 * channel is disconnected from the cluster (<em>unconnected</em>). The channel could now be connected
 * to a different cluster again. The channel is closed (<em>closed</em>).
 * <p/>
 * Only a single sender is allowed to be connected to a channel at a time, but there can be more than one channel
 * in an application.
 * <p/>
 * Messages can be sent to the cluster members using the <em>send</em> method and messages can be received by setting
 * a {@link Receiver} in {@link #setReceiver(Receiver)} and implementing the {@link Receiver#receive(Message)} callback,
 * or extending {@link ReceiverAdapter} and overriding the {@link ReceiverAdapter#receive(Message)} method.
 *
 * @author Bela Ban
 * @since 2.0
 * @see JChannel
 */
@MBean(description="Channel")
public abstract class Channel implements Closeable {

    public enum State {
        OPEN,       // initial state, after channel has been created, or after a disconnect()
        CONNECTING, // when connect() is called
        CONNECTED,  // after successful connect()
        CLOSED      // after close() has been called
    }


    /** The current state of the channel */
    protected volatile State       state=State.OPEN;
    protected UpHandler            up_handler;   // when set, all events are passed to the UpHandler
    protected Set<ChannelListener> channel_listeners;
    protected Receiver             receiver;
    protected SocketFactory        socket_factory=new DefaultSocketFactory();
    protected final Log            log=LogFactory.getLog(getClass());

    @ManagedAttribute(description="Whether or not to discard messages sent by this channel",writable=true)
    protected boolean              discard_own_messages;


    @ManagedAttribute(description="The current state")
    public String getState() {return state.toString();}

    public abstract ProtocolStack getProtocolStack();

    public SocketFactory getSocketFactory() {
        return socket_factory;
    }

    public Channel setSocketFactory(SocketFactory factory) {
        socket_factory=factory;
        ProtocolStack stack=getProtocolStack();
        Protocol prot=stack != null? stack.getTopProtocol() : null;
        if(prot != null)
            prot.setSocketFactory(factory);
        return this;
    }

   /**
    * Connects the channel to a cluster. The client is now able to receive messages from cluster members, views and
    * to send messages to (all or single) cluster members. This is a no-op if already connected.
    * <p>
    * All channels connecting to the same cluster name form a cluster, that means all messages sent to the cluster will
    * be received by all channels connected to the same cluster.
    * <p>
    * @param cluster_name The name of the cluster to connect to.
    * @exception Exception The protocol stack cannot be started
    * @exception IllegalStateException The channel is closed
    */
    abstract public Channel connect(String cluster_name) throws Exception;


    /**
     * Connects this channel to a cluster and gets the state from a specified state provider.
     * <p/>
     * This method essentially invokes <code>connect<code> and <code>getState<code> methods successively.
     * If FLUSH protocol is in channel's stack definition only one flush is executed for both connecting and
     * fetching state rather than two flushes if we invoke <code>connect<code> and <code>getState<code> in succession.
     * <p/>
     * If the channel is closed an exception will be thrown.
     *
     * @param cluster_name  the cluster name to connect to. Cannot be null.
     * @param target the state provider. If null state will be fetched from coordinator, unless this channel is coordinator.
     * @param timeout the timeout for state transfer.
     * @exception Exception Connecting to the cluster or state transfer was not successful
     * @exception IllegalStateException The channel is closed and therefore cannot be used
     */
    abstract public Channel connect(String cluster_name, Address target, long timeout) throws Exception;


   /**
    * Disconnects the channel if it is connected. If the channel is closed or disconnected, this operation is ignored.
    * The channel can then be connected to the same or a different cluster again.
    * @see #connect(String)
    */
    abstract public Channel disconnect();


   /**
    * Destroys the channel and its associated resources (e.g., the protocol stack). After a channel has been closed,
    * invoking methods on it throws the {@code ChannelClosed} exception (or results in a null operation).
    * It is a no-op if the channel is already closed.
    * <p>
    * If the channel is connected to a cluster, {@code disconnect()} will be called first.
    */
    abstract public void close();



   /**
    * Determines whether the channel is open; ie. the protocol stack has been created (may not be connected though).
    * @return true is channel is open, false otherwise
    */
   @ManagedAttribute public boolean isOpen()       {return state != State.CLOSED;}


   /**
    * Determines whether the channel is connected to a group.
    * @return true if channel is connected to cluster and can send/receive messages, false otherwise
    */
   @ManagedAttribute public boolean isConnected()  {return state == State.CONNECTED;}

    /**
     * Determines whether the channel is in the connecting state; this means {@link Channel#connect(String)} has been
     * called, but hasn't returned yet
     * @return true if the channel is in the connecting state, false otherwise
     */
    @ManagedAttribute public boolean isConnecting() {return state == State.CONNECTING;}


    /** Determines whether the channel is in the closed state */
    @ManagedAttribute public boolean isClosed()     {return state == State.CLOSED;}


   /**
    * Returns a map of statistics of the various protocols and of the channel itself.
    * @return Map<String,Map>. A map where the keys are the protocols ("channel" pseudo key is used
    *         for the channel itself") and the values are property maps.
    */
    public abstract Map<String,Object> dumpStats();

   /**
    * Sends a message. The message contains
    * <ol>
    * <li>a destination address (Address). A {@code null} address sends the message to all cluster members.
    * <li>a source address. Can be left empty as it will be assigned automatically
    * <li>a byte buffer. The message contents.
    * <li>several additional fields. They can be used by application programs (or patterns). E.g. a message ID, flags etc
    * </ol>
    * 
    * @param msg the message to be sent. Destination and buffer should be set. A null destination
    *           means to send to all group members.
    * @exception IllegalStateException thrown if the channel is disconnected or closed
    */
    abstract public Channel send(Message msg) throws Exception;


   /**
    * Helper method to create a Message with given parameters and invoke {@link #send(Message)}.
    * @param dst destination address for the message. If null, the message will be sent to all cluster members
    * @param obj a serializable object. Will be marshalled into the byte buffer of the message. If it
    *           is <em>not</em> serializable, an exception will be thrown
    * @throws Exception exception thrown if message sending was not successful
    */
    abstract public Channel send(Address dst, Object obj) throws Exception;

   /**
    * Sends a message. See {@link #send(Address,byte[],int,int)} for details
    * @param dst destination address for the message. If null, the message will be sent to all cluster members
    * @param buf buffer message payload
    * @throws Exception exception thrown if the message sending was not successful
    */
    abstract public Channel send(Address dst, byte[] buf) throws Exception;

   /**
    * Sends a message to a destination.
    * * @param dst the destination address. If null, the message will be sent to all cluster nodes (= cluster members)
    * @param buf the buffer to be sent
    * @param offset the offset into the buffer
    * @param length the length of the data to be sent. Has to be <= buf.length - offset. This will send
    *           {@code length} bytes starting at {@code offset}
    * @throws Exception thrown if send() failed
    */
    abstract public Channel send(Address dst, byte[] buf, int offset, int length) throws Exception;


   /**
    * Enables access to the event mechanism of a channel and is normally not used by clients directly.
    * @param evt sends an Event to a specific protocol layer and receives a response.
    * @return a response from a particular protocol layer targeted by Event parameter
    */
    public Object down(Event evt) {
        return null;
    }


   /**
    * Gets the current view. The view may only be available after a successful {@code connect()}. The result of calling
    * this method on an unconnected channel is implementation defined (may return null).
    * Calling this method on a closed channel returns a null view.
    * @return The current view.
    */
    abstract public View getView();


   /**
    * Returns the channel's own address. The result of calling this method on an unconnected channel
    * is implementation defined (may return null). Calling this method on a closed channel returns
    * null. Addresses can be used as destination in the {@code send()} operation.
    * @return The channel's address (opaque)
    */
    abstract public Address getAddress();

   /**
    * Returns the logical name of this channel.
    * @return The logical name
    */
    abstract public String getName();

   /**
    * Returns the logical name of a given member. The lookup is from the local cache of logical
    * address / logical name mappings and no remote communication is performed.
    * @param member
    * @return The logical name for {@code member}
    */
    abstract public String getName(Address member);


   /**
    * Sets the logical name for the channel. The name will stay associated with this channel for the channel's lifetime
    * (until close() is called). This method should be called <em>before</em> calling connect().
    * @param name
    */
    abstract public Channel setName(String name);

    /** Names a channel, same as {@link #setName(String)} */
    abstract public Channel name(String name);


   /**
    * Returns the cluster name of the group of which the channel is a member. This is the object that was the argument
    * to {@code connect()}. Calling this method on a closed channel returns {@code null}.
    * @return The cluster name
    */
    abstract public String getClusterName();


    public String getProperties() {
        return "n/a";
    }

   /**
    * Sets this channel event handler to be a recipient of all events . These will not be received by the channel
    * (except connect/disconnect, state retrieval and the like). This can be used by building blocks on top of a channel;
    * thus the channel is used as a pass-through medium, and the building blocks take over some of the channel's tasks.
    * However, tasks such as connection management and state transfer is still handled by the channel.
    * @param up_handler handler to handle channel events
    */
    public Channel setUpHandler(UpHandler up_handler) {
        this.up_handler=up_handler;
        return this;
    }

    /**
    * Returns the UpHandler installed for this channel
    * @return the installed UpHandler implementation
    */
    public UpHandler getUpHandler() {
        return up_handler;
    }

   /**
    * Adds a ChannelListener instance that will be notified when a channel event such as connect,
    * disconnect or close occurs.
    * @param listener to be notified
    */
    public synchronized Channel addChannelListener(ChannelListener listener) {
        if(listener == null)
            return this;
        if(channel_listeners == null)
            channel_listeners=new CopyOnWriteArraySet<>();
        channel_listeners.add(listener);
        return this;
    }

    /**
    * Removes a ChannelListener previously installed
    * @param listener to be removed
    */
    public synchronized Channel removeChannelListener(ChannelListener listener) {
        if(channel_listeners != null && listener != null)
            channel_listeners.remove(listener);
        return this;
    }

    /** Clears all installed ChannelListener instances */
    public synchronized Channel clearChannelListeners() {
        if(channel_listeners != null)
            channel_listeners.clear();
        return this;
    }

   /**
    * Sets the receiver for this channel. The eceiver will handle all messages, view changes, implement state transfer
    * logic and so on.
    * @param r the receiver instance for this channel
    * @see Receiver
    */
    public Channel setReceiver(Receiver r) {
        if(receiver != null && r != null)
            log.warn("%s: receiver already set");
        receiver=r;
        return this;
    }

   /**
    * Returns the receiver for this channel if it has been installed using {@link Channel#setReceiver(Receiver)}, null otherwise
    * @return the receiver installed on this channel
    */
   public Receiver getReceiver() {
        return receiver;
    }

   /**
    * When set to true, all messages sent by this member will be discarded when received.
    */
    public Channel setDiscardOwnMessages(boolean flag) {discard_own_messages=flag; return this;}
    
    /**
    * Returns true if this channel will discard its own messages, false otherwise
    */
    public boolean getDiscardOwnMessages() {return discard_own_messages;}

    abstract public boolean flushSupported();

   /**
    * Performs the flush of the cluster but only for the specified flush participants.
    * <p/>
    * All pending messages are flushed out but only for the flush participants. The remaining members in the cluster
    * are not included in the flush. The list of flush participants should be a proper subset of the current view.
    * <p/>
    * If this flush is not automatically resumed it is an obligation of the application to invoke the matching
    * {@link #stopFlush(List)} method with the same list of members used in {@link #startFlush(List, boolean)}.
    *
    * @param automatic_resume if true call {@link #stopFlush()} after the flush
    * @see #startFlush(boolean)
    * @see Util#startFlush(Channel, List, int, long, long)
    */
    abstract public Channel startFlush(List<Address> flushParticipants, boolean automatic_resume) throws Exception;

   /**
    * Performs the flush of the cluster, ie. all pending application messages are flushed out of the cluster and
    * all members ack their reception. After this call returns, no member will be allowed to send any
    * messages until {@link #stopFlush()} is called.
    * <p/>
    * In the case of flush collisions (another member attempts flush at roughly the same time) start flush will
    * fail by throwing an Exception. Applications can re-attempt flushing after certain back-off period.
    * <p/>
    * JGroups provides a helper random sleep time backoff algorithm for flush using Util class.
    * @param automatic_resume if true call {@link #stopFlush()} after the flush
    * @see Util#startFlush(Channel, List, int, long, long)
    */
    abstract public Channel startFlush(boolean automatic_resume) throws Exception;
    
   /**
    * Stops the current flush of the cluster. Cluster members are unblocked and allowed to send new
    * and pending messages.
    * @see Channel#startFlush(boolean)
    * @see Channel#startFlush(List, boolean)
    */
   abstract public Channel stopFlush();
    
   /**
    * Stops the current flush of the cluster for the specified flush participants. Flush
    * participants are unblocked and allowed to send new and pending messages.
    * <p>
    * It is an obligation of the application to invoke the matching {@link #startFlush(List, boolean)} method with the
    * same list of members prior to invocation of this method.
    * @param flushParticipants the flush participants
    */
   abstract public Channel stopFlush(List<Address> flushParticipants);


   /**
    * Retrieves the full state from the target member.
    * <p>
    * The state transfer is initiated by invoking getState() on this channel. The state provider in turn invokes the
    * {@link MessageListener#getState(java.io.OutputStream)} callback and sends the state to this node, the state receiver.
    * After the state arrives at the state receiver, the {@link MessageListener#setState(java.io.InputStream)} callback
    * is invoked to install the state.
    * @param target the state provider. If null the coordinator is used by default
    * @param timeout the number of milliseconds to wait for the operation to complete successfully. 0
    *           waits forever until the state has been received
    * @see MessageListener#getState(java.io.OutputStream)
    * @see MessageListener#setState(java.io.InputStream)
    * @exception IllegalStateException the channel was closed or disconnected, or the flush (if present) failed
    * @exception StateTransferException raised if there was a problem during the state transfer
    */
    abstract public Channel getState(Address target, long timeout) throws Exception;




    protected Channel notifyChannelConnected(Channel c) {
        return notifyListeners(l -> l.channelConnected(c), "channelConnected");
    }

    protected Channel notifyChannelDisconnected(Channel c) {
        return notifyListeners(l -> l.channelDisconnected(c), "channelDisconnected()");
    }

    protected Channel notifyChannelClosed(Channel c) {
        return notifyListeners(l -> l.channelClosed(c), "channelClosed()");
    }

    protected Channel notifyListeners(Consumer<ChannelListener> func, String msg) {
        if(channel_listeners != null) {
            try {
                channel_listeners.forEach(func::accept);
            }
            catch(Throwable t) {
                log.error(Util.getMessage("CallbackException"), msg, t);
            }
        }
        return this;
    }
   

}
