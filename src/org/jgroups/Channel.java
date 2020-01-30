
package org.jgroups;


import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.NameCache;
import org.jgroups.util.SocketFactory;
import org.jgroups.util.Util;

import java.io.Closeable;
import java.util.List;
import java.util.Map;

/**
 * A channel represents a group communication endpoint (like BSD datagram sockets). A client joins a
 * group by connecting the channel to a group and leaves it by disconnecting. Messages sent over the
 * channel are received by all group members that are connected to the same group (that is, all
 * members that have the same group name).
 * <p/>
 * 
 * The FSM for a channel is roughly as follows: a channel is created (<em>unconnected</em>). The
 * channel is connected to a group (<em>connected</em>). Messages can now be sent and received. The
 * channel is disconnected from the group (<em>unconnected</em>). The channel could now be connected
 * to a different group again. The channel is closed (<em>closed</em>).
 * <p/>
 * 
 * Only a single sender is allowed to be connected to a channel at a time, but there can be more
 * than one channel in an application.
 * <p/>
 * 
 * Messages can be sent to the group members using the <em>send</em> method and messages can be
 * received setting a {@link Receiver} in {@link #setReceiver(Receiver)} and implementing the
 * {@link Receiver#receive(Message)} callback.
 * <p>
 * 
 * A channel instance is created using the public constructor.
 * <p/>
 * Various degrees of sophistication in message exchange can be achieved using building blocks on
 * top of channels; e.g., light-weight groups, synchronous message invocation, or remote method
 * calls. Channels are on the same abstraction level as sockets, and should really be simple to use.
 * Higher-level abstractions are all built on top of channels.
 * 
 * @author Bela Ban
 * @since 2.0
 * @see java.net.DatagramPacket
 * @see java.net.MulticastSocket
 * @see JChannel
 */
@MBean(description="Channel")
public class Channel implements Closeable {
    protected final JChannel ch;

    public Channel(JChannel ch) {
        this.ch=ch;
    }

    public enum State {
        OPEN,       // initial state, after channel has been created, or after a disconnect()
        CONNECTING, // when connect() is called
        CONNECTED,  // after successful connect()
        CLOSED      // after close() has been called
    }




    @ManagedAttribute(description="The current state")
    public String getState() {return ch.getState();}

    public ProtocolStack getProtocolStack() {return ch.getProtocolStack();}

    public SocketFactory getSocketFactory() {return ch.getProtocolStack().getBottomProtocol().getSocketFactory();}

    public void setSocketFactory(SocketFactory factory) {
        ProtocolStack stack=getProtocolStack();
        Protocol prot=stack != null? stack.getTopProtocol() : null;
        if(prot != null)
            prot.setSocketFactory(factory);
    }

   /**
    * Connects the channel to a group. The client is now able to receive group messages, views and
    * to send messages to (all or single) group members. This is a null operation if already
    * connected.
    * <p>
    * 
    * All channels with the same name form a group, that means all messages sent to the group will
    * be received by all channels connected to the same cluster name.
    * <p>
    * 
    * @param cluster_name
    *           The name of the channel to connect to.
    * @exception Exception
    *               The protocol stack cannot be started
    * @exception IllegalStateException
    *               The channel is closed
    */
    public void connect(String cluster_name) throws Exception {
        ch.connect(cluster_name);
    }


    /**
     * Connects this channel to a group and gets a state from a specified state provider.
     * <p/>
     *
     * This method essentially invokes
     * <code>connect<code> and <code>getState<code> methods successively.
     * If FLUSH protocol is in channel's stack definition only one flush is executed for both connecting and
     * fetching state rather than two flushes if we invoke <code>connect<code> and <code>getState<code> in succession.
     *
     * If the channel is closed an exception will be thrown.
     *
     *
     * @param cluster_name  the cluster name to connect to. Cannot be null.
     * @param target the state provider. If null state will be fetched from coordinator, unless this channel is coordinator.
     * @param timeout the timeout for state transfer.
     *
     * @exception Exception Connecting to the cluster or state transfer was not successful
     * @exception IllegalStateException The channel is closed and therefore cannot be used
     *
     */
    public void connect(String cluster_name, Address target, long timeout) throws Exception {
        ch.connect(cluster_name, target, timeout);
    }


   /**
    * Disconnects the channel if it is connected. If the channel is closed or disconnected, this
    * operation is ignored<br/>
    * The channel can then be connected to the same or a different cluster again.
    * 
    * @see #connect(String)
    */
    public void disconnect() {
        ch.disconnect();
    }


   /**
    * Destroys the channel and its associated resources (e.g., the protocol stack). After a channel
    * has been closed, invoking methods on it throws the <code>ChannelClosed</code> exception (or
    * results in a null operation). It is a null operation if the channel is already closed.
    * <p>
    * If the channel is connected to a group, <code>disconnect()</code> will be called first.
    */
    public void close() {
        ch.close();
    }



   /**
    * Determines whether the channel is open; ie. the protocol stack has been created (may not be connected though).
    * @return true is channel is open, false otherwise
    */
   @ManagedAttribute public boolean isOpen()       {return ch.isOpen();}


   /**
    * Determines whether the channel is connected to a group.
    * @return true if channel is connected to cluster (group) and can send/receive messages, false otherwise
    */
   @ManagedAttribute public boolean isConnected()  {return ch.isConnected();}

    /**
     * Determines whether the channel is in the connecting state; this means {@link Channel#connect(String)} has been
     * called, but hasn't returned yet
     * @return true if the channel is in the connecting state, false otherwise
     */
    @ManagedAttribute public boolean isConnecting() {return ch.isConnecting();}


    /**
     * Determines whether the channel is in the closed state.
     * @return
     */
    @ManagedAttribute public boolean isClosed()     {return ch.isClosed();}


   /**
    * Returns a map of statistics of the various protocols and of the channel itself.
    * 
    * @return Map<String,Map>. A map where the keys are the protocols ("channel" pseudo key is used
    *         for the channel itself") and the values are property maps.
    */
    public Map<String,Map<String,Object>> dumpStats() {return ch.dumpStats();}

   /**
    * Sends a message. The message contains
    * <ol>
    * <li>a destination address (Address). A <code>null</code> address sends the message to all
    * group members.
    * <li>a source address. Can be left empty as it will be assigned automatically
    * <li>a byte buffer. The message contents.
    * <li>several additional fields. They can be used by application programs (or patterns). E.g. a
    * message ID, flags etc
    * </ol>
    * 
    * @param msg
    *           The message to be sent. Destination and buffer should be set. A null destination
    *           means to send to all group members.
    * @exception IllegalStateException
    *               thrown if the channel is disconnected or closed
    */
    public void send(Message msg) throws Exception {
        ch.send(msg);
    }


   /**
    * Helper method to create a Message with given parameters and invoke {@link #send(Message)}.
    * 
    * @param dst
    *           Destination address for message. If null, message will be sent to all current group
    *           members
    * @param obj
    *           A serializable object. Will be marshalled into the byte buffer of the Message. If it
    *           is <em>not</em> serializable, an exception will be thrown
    * @throws Exception
    *            exception thrown if message sending was not successful
    */
    public void send(Address dst, Object obj) throws Exception {
        ch.send(dst, obj);
    }

   /**
    * Sends a message. See {@link #send(Address,byte[],int,int)} for details
    * 
    * @param dst
    *           destination address for message. If null, message will be sent to all current group
    *           members
    * @param buf
    *           buffer message payload
    * @throws Exception
    *            exception thrown if message sending was not successful
    */
    public void send(Address dst, byte[] buf) throws Exception {
        ch.send(dst, buf);
    }

   /**
    * Sends a message to a destination.
    * 
    * @param dst
    *           The destination address. If null, the message will be sent to all cluster nodes (=
    *           group members)
    * @param buf
    *           The buffer to be sent
    * @param offset
    *           The offset into the buffer
    * @param length
    *           The length of the data to be sent. Has to be <= buf.length - offset. This will send
    *           <code>length</code> bytes starting at <code>offset</code>
    * @throws Exception
    *            If send() failed
    */
    public void send(Address dst, byte[] buf, int offset, int length) throws Exception {
        ch.send(dst, buf, offset, length);
    }


   /**
    * Enables access to event mechanism of a channel and is normally not used by clients directly.
    * 
    * @param evt sends an Event to a specific protocol layer and receives a response.
    * @return a response from a particular protocol layer targeted by Event parameter
    */
    public Object down(Event evt) {
        if(evt.type() == 1) // MSG
            return ch.down((Message)evt.getArg());
        return ch.down(evt);
    }


   /**
    * Gets the current view. The view may only be available after a successful
    * <code>connect()</code>. The result of calling this method on an unconnected channel is
    * implementation defined (may return null). Calling this method on a closed channel returns a
    * null view.
    * 
    * @return The current view.
    */
    public View getView() {return ch.getView();}


   /**
    * Returns the channel's own address. The result of calling this method on an unconnected channel
    * is implementation defined (may return null). Calling this method on a closed channel returns
    * null. Addresses can be used as destination in the <code>send()</code> operation.
    * 
    * @return The channel's address (opaque)
    */
    public Address getAddress() {return ch.getAddress();}

   /**
    * Returns the logical name of this channel if set.
    * 
    * @return The logical name or null (if not set)
    */
    public String getName() {return ch.getName();}

   /**
    * Returns the logical name of a given member. The lookup is from the local cache of logical
    * address / logical name mappings and no remote communication is performed.
    *
    * @param member
    * @return The logical name for <code>member</code>
    */
    public String getName(Address member) {return member != null? NameCache.get(member) : null;}


   /**
    * Sets the logical name for the channel. The name will stay associated with this channel for the
    * channel's lifetime (until close() is called). This method should be called <em>before</em>
    * calling connect().
    * 
    * @param name
    */
    public void setName(String name) {ch.setName(name);}

    /** Names a channel, same as {@link #setName(String)} */
    public Channel name(String name) {ch.name(name); return this;}


   /**
    * Returns the cluster name of the group of which the channel is a member. This is the object
    * that was the argument to <code>connect()</code>. Calling this method on a closed channel
    * returns <code>null</code>.
    * 
    * @return The cluster name
    */
    public String getClusterName() {return ch.getClusterName();}


    public String getProperties() {
        return ch.getProtocolStack() != null? ch.getProtocolStack().printProtocolSpec(true) : null;
    }

   /**
    * Sets this channel event handler to be a recipient off all events . These will not be received
    * by the channel (except connect/disconnect, state retrieval and the like). This can be used by
    * building blocks on top of a channel; thus the channel is used as a pass-through medium, and
    * the building blocks take over some of the channel's tasks. However, tasks such as connection
    * management and state transfer is still handled by the channel.
    * 
    * @param up_handler handler to handle channel events
    */
    public void setUpHandler(UpHandler up_handler) {
        ch.setUpHandler(up_handler);
    }

    /**
    * Returns UpHandler installed for this channel
    * 
    * @return the installed UpHandler implementation
    */
    public UpHandler getUpHandler() {
        return ch.getUpHandler();
    }

   /**
    * Adds a ChannelListener instance that will be notified when a channel event such as connect,
    * disconnect or close occurs.
    * 
    * @param listener to be notified
    */
    public synchronized void addChannelListener(ChannelListener listener) {
        ch.addChannelListener(listener);
    }

    /**
    * Removes a ChannelListener previously installed
    * 
    * @param listener to be removed
    */
    public synchronized void removeChannelListener(ChannelListener listener) {
        ch.removeChannelListener(listener);
    }

    /** Clears all installed ChannelListener instances */
    public synchronized void clearChannelListeners() {
        ch.clearChannelListeners();
    }

   /**
    * Sets the receiver for this channel. Receiver will in turn handle all messages, view changes,
    * implement state transfer logic and so on.
    * 
    * @param r the receiver instance for this channel
    * @see Receiver
    * 
    * */
    public void setReceiver(Receiver r) {
        ch.setReceiver(r);
    }

   /**
    * Returns a receiver for this channel if it has been installed using
    * {@link Channel#setReceiver(Receiver)} , null otherwise
    * 
    * @return a receiver installed on this channel
    */
   public Receiver getReceiver() {
        return ch.getReceiver();
    }

   /**
    * When set to true, all messages sent by a member A will be discarded by A.
    * 
    * @param flag
    */
    public void setDiscardOwnMessages(boolean flag) {ch.setDiscardOwnMessages(flag);}
    
    /**
     * Returns true if this channel will discard its own messages, false otherwise
     *
     * @return
     */
    public boolean getDiscardOwnMessages() {return ch.getDiscardOwnMessages();}

    public boolean flushSupported() {return ch.flushSupported();}

   /**
    * Performs the flush of the cluster but only for the specified flush participants.
    * <p/>
    * All pending messages are flushed out but only for the flush participants. The remaining
    * members in the cluster are not included in the flush. The list of flush participants should be
    * a proper subset of the current view.
    * <p/>
    * If this flush is not automatically resumed it is an obligation of the application to invoke
    * the matching {@link #stopFlush(List)} method with the same list of members used in
    * {@link #startFlush(List, boolean)}.
    *
    * @param automatic_resume
    *           if true call {@link #stopFlush()} after the flush
    * @see #startFlush(boolean)
    * @see Util#startFlush(Channel, List, int, long, long)
    */
    public void startFlush(List<Address> flushParticipants, boolean automatic_resume) throws Exception {
        ch.startFlush(flushParticipants, automatic_resume);
    }

   /**
    * Performs the flush of the cluster, ie. all pending application messages are flushed out of the cluster and
    * all members ack their reception. After this call returns, no member will be allowed to send any
    * messages until {@link #stopFlush()} is called.
    * <p/>
    * In the case of flush collisions (another member attempts flush at roughly the same time) start flush will
    * fail by throwing an Exception. Applications can re-attempt flushing after certain back-off period.
    * <p/>
    * JGroups provides a helper random sleep time backoff algorithm for flush using Util class.
    *
    * @param automatic_resume
    *           if true call {@link #stopFlush()} after the flush
    *
    * @see Util#startFlush(Channel, List, int, long, long)
    */
    public void startFlush(boolean automatic_resume) throws Exception {
        ch.startFlush(automatic_resume);
    }
    
   /**
    * Stops the current flush of the cluster. Cluster members are unblocked and allowed to send new
    * and pending messages.
    *
    * @see Channel#startFlush(boolean)
    * @see Channel#startFlush(List, boolean)
    */
   public void stopFlush() {
       ch.stopFlush();
   }
    
   /**
    * Stops the current flush of the cluster for the specified flush participants. Flush
    * participants are unblocked and allowed to send new and pending messages.
    * <p>
    *
    * It is an obligation of the application to invoke the matching
    * {@link #startFlush(List, boolean)} method with the same list of members prior to invocation of
    * this method.
    *
    * @param flushParticipants
    *           the flush participants
    */
   public void stopFlush(List<Address> flushParticipants) {
       ch.stopFlush(flushParticipants);
   }


   /**
    * Retrieves the full state from the target member.
    * <p>
    * State transfer is initiated by invoking getState on this channel. The state provider in turn
    * invokes {@link Receiver#getState(java.io.OutputStream)} callback and sends a state to
    * this node, the state receiver. After the state arrives to the state receiver
    * {@link Receiver#setState(java.io.InputStream)} callback is invoked to install the
    * state.
    * 
    * @param target
    *           The state provider. If null the coordinator is used by default
    * @param timeout
    *           The number of milliseconds to wait for the operation to complete successfully. 0
    *           waits until the state has been received
    * 
    * @see Receiver#getState(java.io.OutputStream)
    * @see Receiver#setState(java.io.InputStream)
    * 
    * @exception IllegalStateException
    *               The channel was closed or disconnected, or the flush (if present) failed
    * @exception StateTransferException
    *               raised if there was a problem during the state transfer
    */
    public void getState(Address target, long timeout) throws Exception {
        ch.getState(target, timeout);
    }



}
