package org.jgroups;

import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.conf.ConfiguratorFactory;
import org.jgroups.conf.ProtocolConfiguration;
import org.jgroups.conf.ProtocolStackConfigurator;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.TP;
import org.jgroups.stack.*;
import org.jgroups.util.UUID;
import org.jgroups.util.*;

import java.io.*;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.Callable;
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
 */
@MBean(description="JGroups channel")
public class JChannel implements Closeable {

    public enum State {
        OPEN,       // initial state, after channel has been created, or after a disconnect()
        CONNECTING, // when connect() is called
        CONNECTED,  // after successful connect()
        CLOSED      // after close() has been called
    }

    protected Receiver                              receiver;
    protected Address                               local_addr;
    protected String                                name;
    protected String                                cluster_name;
    protected View                                  view;
    protected volatile State                        state=State.OPEN;
    protected ProtocolStack                         prot_stack;
    protected UpHandler                             up_handler;   // when set, all events are passed to the UpHandler
    protected Set<ChannelListener>                  channel_listeners;
    protected final Log                             log=LogFactory.getLog(getClass());
    protected List<AddressGenerator>                address_generators;
    protected final Promise<StateTransferResult>    state_promise=new Promise<>();
    protected boolean                               state_transfer_supported; // true if state transfer prot is in the stack
    protected volatile boolean                      flush_supported; // true if FLUSH is present in the stack
    protected final DiagnosticsHandler.ProbeHandler probe_handler=new JChannelProbeHandler(this);
    protected long                                  sent_msgs, received_msgs, sent_bytes, received_bytes;

    @ManagedAttribute(description="Collect channel statistics",writable=true)
    protected boolean                               stats=true;

    @ManagedAttribute(description="Whether or not to discard messages sent by this channel",writable=true)
    protected boolean                               discard_own_messages;





    /**
     * Creates a JChannel without a protocol stack; used for programmatic creation of channel and protocol stack
     * @param create_protocol_stack If true, the default config is used. If false, no protocol stack is created
     */
    public JChannel(boolean create_protocol_stack) {
        if(create_protocol_stack) {
            try {
                init(ConfiguratorFactory.getStackConfigurator(Global.DEFAULT_PROTOCOL_STACK));
            }
            catch(Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /** Creates a {@code JChannel} with the default stack */
    public JChannel() throws Exception {
        this(Global.DEFAULT_PROTOCOL_STACK);
    }


    /**
     * Constructs a JChannel instance with the protocol stack configuration based upon the specified properties parameter.
     * @param props A file containing a JGroups XML configuration or a URL pointing to an XML configuration
     */
    public JChannel(String props) throws Exception {
        this(ConfiguratorFactory.getStackConfigurator(props));
    }

    /**
     * Creates a channel with a configuration based on an input stream.
     * @param input An input stream, pointing to a streamed configuration
     */
    public JChannel(InputStream input) throws Exception {
        this(ConfiguratorFactory.getStackConfigurator(input));
    }

    /**
     * Constructs a JChannel with the protocol stack configuration contained by the protocol stack configurator parameter.
     * <p>
     * All of the public constructors of this class eventually delegate to this method.
     * @param configurator A protocol stack configurator containing a JGroups protocol stack configuration.
     */
    public JChannel(ProtocolStackConfigurator configurator) throws Exception {
        init(configurator);
    }


    /**
     * Creates a channel from an array of protocols. Note that after a {@link org.jgroups.JChannel#close()}, the protocol
     * list <em>should not</em> be reused, ie. new JChannel(protocols) would reuse the same protocol list, and this
     * might lead to problems!
     * @param protocols The list of protocols, from bottom to top, ie. the first protocol in the list is the transport,
     *                  the last the top protocol
     */
    public JChannel(Protocol ... protocols) throws Exception {
        this(Arrays.asList(protocols));
    }



    /**
     * Creates a channel from a list of protocols. Note that after a {@link org.jgroups.JChannel#close()}, the protocol
     * list <em>should not</em> be reused, ie. new JChannel(protocols) would reuse the same protocol list, and this
     * might lead to problems !
     * @param protocols The list of protocols, from bottom to top, ie. the first protocol in the list is the transport,
     *                  the last the top protocol
     */
    public JChannel(List<Protocol> protocols) throws Exception {
        prot_stack=new ProtocolStack().setChannel(this);
        for(Protocol prot: protocols) {
            prot_stack.addProtocol(prot);
            prot.setProtocolStack(prot_stack);
        }
        prot_stack.init();

        StackType ip_version=Util.getIpStackType();
        TP transport=(TP)protocols.get(0);
        InetAddress resolved_addr=Configurator.getValueFromProtocol(transport, "bind_addr");
        if(resolved_addr != null)
            ip_version=resolved_addr instanceof Inet6Address? StackType.IPv6 : StackType.IPv4;
        else if(ip_version == StackType.Dual)
            ip_version=StackType.IPv4; // prefer IPv4 addresses

        // Substitute vars with defined system props (if any)
        List<Protocol> prots=prot_stack.getProtocols();
        Map<String,String> map=new HashMap<>();
        for(Protocol prot: prots)
            Configurator.resolveAndAssignFields(prot, map, ip_version);
    }


    /**
     * Creates a channel with the same configuration as the channel passed to this constructor. This is used by
     * testing code, and should not be used by clients!
     */
    public JChannel(JChannel ch) throws Exception {
        init(ch);
        discard_own_messages=ch.discard_own_messages;
    }


    public Receiver      getReceiver()                       {return receiver;}
    public JChannel      setReceiver(Receiver r)             {receiver=r; return this;}
    public JChannel      receiver(Receiver r)                {return setReceiver(r);}
    public Address       getAddress()                        {return address();}
    public Address       address()                           {return state == State.CLOSED ? null : local_addr;}
    public String        getName()                           {return name;}
    public String        name()                              {return name;}
    public JChannel      name(String name)                   {return setName(name);}
    public String        clusterName()                       {return getClusterName();}
    public View          getView()                           {return view();}
    public View          view()                              {return state == State.CONNECTED ? view : null;}
    public ProtocolStack getProtocolStack()                  {return prot_stack;}
    public ProtocolStack stack()                             {return prot_stack;}
    public UpHandler     getUpHandler()                      {return up_handler;}
    public JChannel      setUpHandler(UpHandler h)           {this.up_handler=h; return this;}
    public boolean       getStats()                          {return stats;}
    public boolean       stats()                             {return stats;}
    public JChannel      setStats(boolean stats)             {this.stats=stats; return this;}
    public JChannel      stats(boolean stats)                {this.stats=stats; return this;}
    public boolean       getDiscardOwnMessages()             {return discard_own_messages;}
    public JChannel      setDiscardOwnMessages(boolean flag) {discard_own_messages=flag; return this;}
    public boolean       flushSupported()                    {return flush_supported;}


    @ManagedAttribute(name="address")
    public String getAddressAsString() {return local_addr != null? local_addr.toString() : "n/a";}

    @ManagedAttribute(name="address_uuid")
    public String getAddressAsUUID() {return local_addr instanceof UUID? ((UUID)local_addr).toStringLong() : null;}

    /** Sets the logical name for the channel. The name will stay associated with this channel for the channel's lifetime
     * (until close() is called). This method must be called <em>before</em> calling connect() */
    @ManagedAttribute(writable=true, description="The logical name of this channel. Stays with the channel until " +
      "the channel is closed")
    public JChannel setName(String name) {
        if(name != null) {
            if(isConnected())
                throw new IllegalStateException("name cannot be set if channel is connected (should be done before)");
            this.name=name;
            if(local_addr != null)
                NameCache.add(local_addr, this.name);
        }
        return this;
    }

    @ManagedAttribute(description="Returns cluster name this channel is connected to")
    public String getClusterName() {return state == State.CONNECTED? cluster_name : null;}

    @ManagedAttribute(name="view")
    public String getViewAsString() {View v=getView(); return v != null ? v.toString() : "n/a";}

    @ManagedAttribute(description="The current state")
    public String getState()                               {return state.toString();}
    @ManagedAttribute public boolean isOpen()              {return state != State.CLOSED;}
    @ManagedAttribute public boolean isConnected()         {return state == State.CONNECTED;}
    @ManagedAttribute public boolean isConnecting()        {return state == State.CONNECTING;}
    @ManagedAttribute public boolean isClosed()            {return state == State.CLOSED;}
    @ManagedAttribute public long    getSentMessages()     {return sent_msgs;}
    @ManagedAttribute public long    getSentBytes()        {return sent_bytes;}
    @ManagedAttribute public long    getReceivedMessages() {return received_msgs;}
    @ManagedAttribute public long    getReceivedBytes()    {return received_bytes;}
    @ManagedAttribute public static  String getVersion()   {return Version.printDescription();}


    /** Adds a ChannelListener that will be notified when a connect, disconnect or close occurs */
    public synchronized JChannel addChannelListener(ChannelListener listener) {
        if(listener == null)
            return this;
        if(channel_listeners == null)
            channel_listeners=new CopyOnWriteArraySet<>();
        channel_listeners.add(listener);
        return this;
    }

    public synchronized JChannel removeChannelListener(ChannelListener listener) {
        if(channel_listeners != null && listener != null)
            channel_listeners.remove(listener);
        return this;
    }

    public synchronized JChannel clearChannelListeners() {
        if(channel_listeners != null)
            channel_listeners.clear();
        return this;
    }

    /**
     * Sets the new {@link AddressGenerator}. New addresses will be generated using the new generator. This
     * should <em>not</em> be done while a channel is connected, but before connecting.
     * @param address_generator
     * @since 2.12
     */
    public JChannel addAddressGenerator(AddressGenerator address_generator) {
        if(address_generator == null)
            return this;
        if(address_generators == null)
            address_generators=new ArrayList<>(3);
        address_generators.add(address_generator);
        return this;
    }

    public boolean removeAddressGenerator(AddressGenerator address_generator) {
        return address_generator != null && address_generators != null && address_generators.remove(address_generator);
    }


    /**
     * Returns the protocol stack configuration in string format. An example of this property is
     * <pre>"UDP:PING:FDALL:STABLE:NAKACK2:UNICAST3:FRAG2:GMS"</pre>
     */
    public String getProperties() {return prot_stack != null? prot_stack.printProtocolSpec(true) : null;}

    @ManagedOperation
    public JChannel resetStats() {sent_msgs=received_msgs=sent_bytes=received_bytes=0; return this;}

    /** Dumps all protocols in string format. If include_props is set, the attrs of each protocol are also printed */
    @ManagedOperation
    public String printProtocolSpec(boolean include_props) {
        ProtocolStack ps=getProtocolStack();
        return ps != null? ps.printProtocolSpec(include_props) : null;
    }

    /** Returns a map of statistics of the various protocols and of the channel itself */
    @ManagedOperation
    public Map<String,Map<String,Object>> dumpStats() {
        Map<String,Map<String,Object>> retval=prot_stack.dumpStats();
        retval.put("channel", dumpChannelStats());
        return retval;
    }

    public Map<String,Map<String,Object>> dumpStats(String protocol_name, List<String> attrs) {
        return prot_stack.dumpStats(protocol_name, attrs);
    }

    @ManagedOperation
    public Map<String,Map<String,Object>> dumpStats(String protocol_name) {
        return prot_stack.dumpStats(protocol_name, null);
    }

    protected Map<String,Object> dumpChannelStats() {
        Map<String,Object> retval=new HashMap<>();
        retval.put("sent_msgs",      sent_msgs);
        retval.put("sent_bytes",     sent_bytes);
        retval.put("received_msgs",  received_msgs);
        retval.put("received_bytes", received_bytes);
        return retval;
    }


    /**
     * Joins the cluster. The application is now able to receive messages from cluster members, views and to send
     * messages to (all or single) cluster members. This is a no-op if already connected.<p/>
     * All channels connecting to the same cluster name form a cluster; messages sent to the cluster will
     * be received by all cluster members.
     * @param cluster_name The name of the cluster to join
     * @exception Exception The protocol stack cannot be started
     * @exception IllegalStateException The channel is closed
     */
    @ManagedOperation(description="Connects the channel to a group")
    public synchronized JChannel connect(String cluster_name) throws Exception {
        return connect(cluster_name, true);
    }

    /** Connects the channel to a cluster. */
    @ManagedOperation(description="Connects the channel to a group")
    protected synchronized JChannel connect(String cluster_name, boolean useFlushIfPresent) throws Exception {
        if(!_preConnect(cluster_name))
            return this;
        Event connect_event=new Event(useFlushIfPresent? Event.CONNECT_USE_FLUSH : Event.CONNECT, cluster_name);
        _connect(connect_event);
        state=State.CONNECTED;
        notifyChannelConnected(this);
        return this;
    }

    /**
     * Joins the cluster and gets the state from a specified state provider.
     * <p/>
     * This method essentially invokes <code>connect<code> and <code>getState<code> methods successively.
     * If FLUSH protocol is in channel's stack definition only one flush is executed for both connecting and
     * fetching state rather than two flushes if we invoke <code>connect<code> and <code>getState<code> in succession.<p/>
     * If the channel is closed an exception will be thrown.
     * @param cluster_name  the cluster name to connect to. Cannot be null.
     * @param target the state provider. If null state will be fetched from coordinator, unless this channel is coordinator.
     * @param timeout the timeout for state transfer.
     * @exception Exception Connecting to the cluster or state transfer was not successful
     * @exception IllegalStateException The channel is closed and therefore cannot be used
     */
    public synchronized JChannel connect(String cluster_name, Address target, long timeout) throws Exception {
    	return connect(cluster_name, target, timeout, true);
    }

    
    /**
     * Joins the cluster and gets a state from a specified state provider.<p/>
     * This method invokes {@code connect()} and then {@code getState}.<p/>
     * If the FLUSH protocol is in the channel's stack definition, only one flush round is executed for both connecting and
     * fetching the state rather than two flushes if we invoke {@code connect} and {@code getState} in succession.<p/>
     * If the channel is closed a ChannelClosed exception will be thrown.
     * @param cluster_name  The cluster name to connect to. Cannot be null.
     * @param target The state provider. If null, the state will be fetched from the coordinator, unless this channel
     *               is the coordinator.
     * @param timeout The timeout for the state transfer.
     * @exception Exception The protocol stack cannot be started, or the JOIN failed
     * @exception IllegalStateException The channel is closed or disconnected
     * @exception StateTransferException State transfer was not successful
     *
     */
    public synchronized JChannel connect(String cluster_name, Address target, long timeout,
                                     boolean useFlushIfPresent) throws Exception {
        if(!_preConnect(cluster_name))
            return this;

        boolean canFetchState=false;
        try {
            Event connect_event=new Event(useFlushIfPresent? Event.CONNECT_WITH_STATE_TRANSFER_USE_FLUSH : Event.CONNECT_WITH_STATE_TRANSFER, cluster_name);
            _connect(connect_event);
            state=State.CONNECTED;
            notifyChannelConnected(this);
            canFetchState=view != null && view.size() > 1;
            if(canFetchState) // if I am not the only member in cluster then ...
                getState(target, timeout, false); // fetch state from target
        }
        finally {
            // stopFlush if we fetched the state or failed to connect...
            if((flushSupported() && useFlushIfPresent) && (canFetchState || state != State.CONNECTED) )
                stopFlush();
        }
        return this;
    }


    /**
     * Leaves the cluster (disconnects the channel if it is connected). If the channel is closed or disconnected, this
     * operation is ignored. The channel can then be used to join the same or a different cluster again.
     * @see #connect(String)
     */
    @ManagedOperation(description="Disconnects the channel if connected")
    public synchronized JChannel disconnect() {
        switch(state) {
            case OPEN: case CLOSED:
                break;
            case CONNECTING: case CONNECTED:
                if(cluster_name != null) {
                    try {
                        down(new Event(Event.DISCONNECT, local_addr));   // DISCONNECT is handled by each layer
                    }
                    catch(Throwable t) {
                        log.error(Util.getMessage("DisconnectFailure"), local_addr, t);
                    }
                }
                state=State.OPEN;
                stopStack(true, false);
                notifyChannelDisconnected(this);
                init(); // sets local_addr=null; changed March 18 2003 (bela) -- prevented successful rejoining
                break;
            default:
                throw new IllegalStateException("state " + state + " unknown");
        }
        return this;
    }


    /**
     * Destroys the channel and its associated resources (e.g. the protocol stack). After a channel has been closed,
     * invoking methods on it will throw a {@code ChannelClosed} exception (or results in a null operation).
     * It is a no-op if the channel is already closed.<p/>
     * If the channel is connected to a cluster, {@code disconnect()} will be called first.
     */
    @ManagedOperation(description="Disconnects and destroys the channel")
    public synchronized void close() {
        _close(true); // by default disconnect before closing channel and close mq
    }


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
    public JChannel send(Message msg) throws Exception {
        if(msg == null)
            throw new NullPointerException("msg is null");
        checkClosedOrNotConnected();
        down(msg);
        return this;
    }


    /**
     * Helper method to create a Message with given parameters and invoke {@link #send(Message)}.
     * @param dst destination address for the message. If null, the message will be sent to all cluster members
     * @param obj a serializable object. Will be marshalled into the byte buffer of the message. If it
     *           is <em>not</em> serializable, an exception will be thrown
     * @throws Exception exception thrown if message sending was not successful
     */
    public JChannel send(Address dst, Object obj) throws Exception {
        return send(new Message(dst, obj));
    }

    /**
     * Sends a message. See {@link #send(Address,byte[],int,int)} for details
     * @param dst destination address for the message. If null, the message will be sent to all cluster members
     * @param buf buffer message payload
     * @throws Exception exception thrown if the message sending was not successful
     */
    public JChannel send(Address dst, byte[] buf) throws Exception {
        return send(new Message(dst, buf));
    }

    /**
     * Sends a message to a destination.
     * * @param dst the destination address. If null, the message will be sent to all cluster nodes (= cluster members)
     * @param buf the buffer to be sent
     * @param offset the offset into the buffer
     * @param length the length of the data to be sent. Has to be <= buf.length - offset. This will send
     *           {@code length} bytes starting at {@code offset}
     * @throws Exception thrown if send() failed
     */
    public JChannel send(Address dst, byte[] buf, int offset, int length) throws Exception {
        return send(new Message(dst, buf, offset, length));
    }


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
    public JChannel getState(Address target, long timeout) throws Exception {
        return getState(target, timeout, true);
    }


    /** Retrieves state from the target member. See {@link #getState(Address,long)} for details */
    public JChannel getState(Address target, long timeout, boolean useFlushIfPresent) throws Exception {
    	Callable<Boolean> flusher =() -> Util.startFlush(JChannel.this);
		return getState(target, timeout, useFlushIfPresent?flusher:null);
	}


    /**
     * Performs the flush of the cluster, ie. all pending application messages are flushed out of the cluster and
     * all members ack their reception. After this call returns, no member will be allowed to send any
     * messages until {@link #stopFlush()} is called.<p/>
     * In the case of flush collisions (another member attempts flush at roughly the same time) start flush will
     * fail by throwing an Exception. Applications can re-attempt flushing after certain back-off period.<p/>
     * JGroups provides a helper random sleep time backoff algorithm for flush using Util class.
     * @param automatic_resume if true call {@link #stopFlush()} after the flush
     */
    public JChannel startFlush(boolean automatic_resume) throws Exception {
        if(!flushSupported())
            throw new IllegalStateException("Flush is not supported, add pbcast.FLUSH protocol to your configuration");
        try {
            down(new Event(Event.SUSPEND));
            return this;
        }
        catch (Exception e) {
            throw new Exception("Flush failed", e.getCause());
        }
        finally {
            if (automatic_resume)
                stopFlush();
        }
    }

    /**
     * Performs the flush of the cluster but only for the specified flush participants.<p/>
     * All pending messages are flushed out but only for the flush participants. The remaining members in the cluster
     * are not included in the flush. The list of flush participants should be a proper subset of the current view.<p/>
     * If this flush is not automatically resumed it is an obligation of the application to invoke the matching
     * {@link #stopFlush(List)} method with the same list of members used in {@link #startFlush(List, boolean)}.
     * @param automatic_resume if true call {@link #stopFlush()} after the flush
     */
    public JChannel startFlush(List<Address> flushParticipants, boolean automatic_resume) throws Exception {
        if (!flushSupported())
            throw new IllegalStateException("Flush is not supported, add pbcast.FLUSH protocol to your configuration");
        View v = getView();
        boolean validParticipants = v != null && v.getMembers().containsAll(flushParticipants);
        if (!validParticipants)
            throw new IllegalArgumentException("Current view " + v
                                                 + " does not contain all flush participants " + flushParticipants);
        try {
            down(new Event(Event.SUSPEND, flushParticipants));
            return this;
        }
        catch (Exception e) {
            throw new Exception("Flush failed", e.getCause());
        }
        finally {
            if (automatic_resume)
                stopFlush(flushParticipants);
        }
    }

    /** Stops the current flush round. Cluster members are unblocked and allowed to send new and pending messages */
    public JChannel stopFlush() {
        if(!flushSupported())
            throw new IllegalStateException("Flush is not supported, add pbcast.FLUSH protocol to your configuration");
        down(new Event(Event.RESUME));
        return this;
    }

    /**
     * Stops the current flush of the cluster for the specified flush participants. Flush participants are unblocked and
     * allowed to send new and pending messages.<p/>
     * It is an obligation of the application to invoke the matching {@link #startFlush(List, boolean)} method with the
     * same list of members prior to invocation of this method.
     * @param flushParticipants the flush participants
     */
    public JChannel stopFlush(List<Address> flushParticipants) {
        if(!flushSupported())
            throw new IllegalStateException("Flush is not supported, add pbcast.FLUSH protocol to your configuration");
        down(new Event(Event.RESUME, flushParticipants));
        return this;
    }


    /**
     * Sends an event down the protocol stack. Note that - contrary to {@link #send(Message)}, if the event is a message,
     * no checks are performed whether the channel is closed or disconnected. Note that this method is not typically
     * used by applications.
     * @param evt the message to send down, encapsulated in an event
     */
    public Object down(Event evt) {
        if(evt == null) return null;
        return prot_stack.down(evt);
    }

    public Object down(Message msg) {
        if(msg == null) return null;
        if(stats) {
            sent_msgs++;
            sent_bytes+=msg.getLength();
        }
        return prot_stack.down(msg);
    }


    /**
     * Callback method <BR>
     * Called by the ProtocolStack when a message is received.
     * @param evt the event carrying the message from the protocol stack
     */
    public Object up(Event evt) {
        switch(evt.getType()) {
            case Event.VIEW_CHANGE:
                View tmp=evt.getArg();
                if(tmp instanceof MergeView)
                    view=new View(tmp.getViewId(), tmp.getMembers());
                else
                    view=tmp;

                // Bela&Vladimir Oct 27th,2006 (JGroups 2.4): we need to set connected=true because a client can
                // call channel.getView() in viewAccepted() callback invoked on this thread (see Event.VIEW_CHANGE handling below)

                // not good: we are only connected when we returned from connect() - bela June 22 2007
                // Changed: when a channel gets a view of which it is a member then it should be
                // connected even if connect() hasn't returned yet ! (bela Noc 2010)
                if(state != State.CONNECTED)
                    state=State.CONNECTED;
                break;

            case Event.CONFIG:
                Map<String,Object> cfg=evt.getArg();
                if(cfg != null) {
                    if(cfg.containsKey("state_transfer")) {
                        state_transfer_supported=(Boolean)cfg.get("state_transfer");
                    }
                    if(cfg.containsKey("flush_supported")) {
                        flush_supported=(Boolean)cfg.get("flush_supported");
                    }
                }
                break;
                
            case Event.GET_STATE_OK:
                StateTransferResult result=evt.getArg();
                if(up_handler != null) {
                    try {
                        Object retval=up_handler.up(evt);
                        state_promise.setResult(result);
                        return retval;
                    }
                    catch(Throwable t) {
                        state_promise.setResult(new StateTransferResult(t));
                    }
                }

                if(receiver != null) {
                    try {
                        if(result.hasBuffer()) {
                            byte[] tmp_state=result.getBuffer();
                            ByteArrayInputStream input=new ByteArrayInputStream(tmp_state);
                            receiver.setState(input);
                        }
                        state_promise.setResult(result);
                    }
                    catch(Throwable t) {
                        state_promise.setResult(new StateTransferResult(t));
                    }
                }
                break;

            case Event.STATE_TRANSFER_INPUTSTREAM_CLOSED:
                state_promise.setResult(evt.getArg());
                break;

            case Event.STATE_TRANSFER_INPUTSTREAM:
                // Oct 13,2006 moved to down() when Event.STATE_TRANSFER_INPUTSTREAM_CLOSED is received
                // state_promise.setResult(is != null? Boolean.TRUE : Boolean.FALSE);

                if(up_handler != null)
                    return up_handler.up(evt);

                InputStream is=evt.getArg();
                if(is != null && receiver != null) {
                    try {
                        receiver.setState(is);
                    }
                    catch(Throwable t) {
                        throw new RuntimeException("failed calling setState() in state requester", t);
                    }
                }
                break;

            case Event.STATE_TRANSFER_OUTPUTSTREAM:
                if(receiver != null && evt.getArg() != null) {
                    try {
                        receiver.getState(evt.getArg());
                    }
                    catch(Exception e) {
                        throw new RuntimeException("failed calling getState() in state provider", e);
                    }
                }
                break;

            case Event.GET_LOCAL_ADDRESS:
                return local_addr;

            case Event.SET_LOCAL_ADDRESS:
                Address tmp_addr=evt.arg();
                if(tmp_addr != null) {
                    this.local_addr=tmp_addr;
                    if(name != null && !name.isEmpty())
                        NameCache.add(local_addr, name);
                }
                break;

            default:
                break;
        }


        // If UpHandler is installed, pass all events to it and return (UpHandler is e.g. a building block)
        if(up_handler != null)
            return up_handler.up(evt);

        if(receiver != null)
            return invokeCallback(evt.getType(), evt.getArg());
        return null;
    }

    public Object up(Message msg) {
        if(stats) {
            received_msgs++;
            received_bytes+=msg.getLength();
        }

        // discard local messages (sent by myself to me)
        if(discard_own_messages && local_addr != null && msg.getSrc() != null && local_addr.equals(msg.getSrc()))
            return null;

        // If UpHandler is installed, pass all events to it and return (UpHandler is e.g. a building block)
        if(up_handler != null)
            return up_handler.up(msg);

        if(receiver != null)
            receiver.receive(msg);
        return null;
    }


    /** Callback invoked by the protocol stack to deliver a message batch */
    public JChannel up(MessageBatch batch) {
        if(stats) {
            received_msgs+=batch.size();
            received_bytes+=batch.length();
        }

        // discard local messages (sent by myself to me)
        if(discard_own_messages && local_addr != null && batch.sender() != null && local_addr.equals(batch.sender()))
            return this;

        if(up_handler != null) {
            try {
                up_handler.up(batch);
            }
            catch(Throwable t) {
                log.error(Util.getMessage("UpHandlerFailure"), t);
            }
            return this;
        }
        if(receiver != null) {
            try {
                receiver.receive(batch);
            }
            catch(Throwable t) {
                log.error(Util.getMessage("ReceiverFailure"), t);
            }
        }
        return this;
    }



    @ManagedOperation
    public String toString(boolean details) {
        StringBuilder sb=new StringBuilder();
        sb.append("local_addr=").append(local_addr).append('\n').append("cluster_name=")
        		.append(cluster_name).append('\n').append("my_view=").append(view).append('\n')
                .append("state=").append(state).append('\n');
        if(details) {
            sb.append("discard_own_messages=").append(discard_own_messages).append('\n');
            sb.append("state_transfer_supported=").append(state_transfer_supported).append('\n');
            sb.append("props=").append(getProperties()).append('\n');
        }
        return sb.toString();
    }


    /* ----------------------------------- Private Methods ------------------------------------- */
    protected boolean _preConnect(String cluster_name) throws Exception {
        if(cluster_name == null)
            throw new IllegalArgumentException("cluster name cannot be null");
        if(state == State.CONNECTED) {
            log.trace("already connected to %s", this.cluster_name);
            return false;
        }
        checkClosed();
        setAddress();
        State old_state=state;
        state=State.CONNECTING;
        try {
            startStack(cluster_name);
        }
        catch(Exception ex) {
            state=old_state;
            throw ex;
        }
        return true;
    }

    protected JChannel _connect(Event evt) throws Exception {
        try {
            down(evt);
            return this;
        }
        catch(Exception ex) {
            cleanup();
            throw ex;
        }
    }

    protected void cleanup() {
        stopStack(true, false);
        state=State.OPEN;
        init();
    }

    protected JChannel getState(Address target, long timeout, Callable<Boolean> flushInvoker) throws Exception {
        checkClosedOrNotConnected();
        if(!state_transfer_supported)
            throw new IllegalStateException("fetching state will fail as state transfer is not supported. "
                                              + "Add one of the state transfer protocols to your configuration");

        if(target == null)
            target=determineCoordinator();
        if(Objects.equals(target, local_addr)) {
            log.trace(local_addr + ": cannot get state from myself (" + target + "): probably the first member");
            return this;
        }

        boolean initiateFlush=flushSupported() && flushInvoker != null;
        if(initiateFlush) {
            boolean successfulFlush=false;
            try {
                successfulFlush=flushInvoker.call();
            }
            catch(Throwable e) {
                successfulFlush=false; // http://jira.jboss.com/jira/browse/JGRP-759
            }
            if(!successfulFlush)
                throw new IllegalStateException("Node " + local_addr + " could not flush the cluster for state retrieval");
        }

        state_promise.reset();
        StateTransferInfo state_info=new StateTransferInfo(target, timeout);
        long start=System.currentTimeMillis();
        down(new Event(Event.GET_STATE, state_info));
        StateTransferResult result=state_promise.getResult(state_info.timeout);

        if(initiateFlush)
            stopFlush();

        if(result == null)
            throw new StateTransferException("timeout during state transfer (" + (System.currentTimeMillis() - start) + "ms)");
        if(result.hasException())
            throw new StateTransferException("state transfer failed", result.getException());
        return this;
    }



    protected Object invokeCallback(int type, Object arg) {
        switch(type) {
            case Event.VIEW_CHANGE:
                receiver.viewAccepted((View)arg);
                break;
            case Event.SUSPECT:
                // todo: change this in 4.1 to only accept collections
                Collection<Address> suspects=arg instanceof Address? Collections.singletonList((Address)arg)
                  : (Collection<Address>)arg;
                suspects.forEach(receiver::suspect);
                break;
            case Event.GET_APPLSTATE:
                byte[] tmp_state=null;
                if(receiver != null) {
                    ByteArrayOutputStream output=new ByteArrayOutputStream(1024);
                    try {
                        receiver.getState(output);
                        tmp_state=output.toByteArray();
                    }
                    catch(Exception e) {
                        throw new RuntimeException(local_addr + ": failed getting state from application", e);
                    }
                }
                return new StateTransferInfo(null, 0L, tmp_state);
            case Event.BLOCK:
                receiver.block();
                return true;
            case Event.UNBLOCK:
                receiver.unblock();
        }
        return null;
    }

    protected final JChannel init(ProtocolStackConfigurator configurator) throws Exception {
        List<ProtocolConfiguration> configs=configurator.getProtocolStack();
        // replace vars with system props
        configs.forEach(ProtocolConfiguration::substituteVariables);
        prot_stack=new ProtocolStack(this);
        prot_stack.setup(configs); // Setup protocol stack (creates protocol, calls init() on them)
        return this;
    }

    protected final JChannel init(JChannel ch) throws Exception {
        if(ch == null)
            throw new IllegalArgumentException("channel is null");
        prot_stack=new ProtocolStack(this);
        prot_stack.setup(ch.getProtocolStack()); // Setup protocol stack (creates protocol, calls init() on them)
        return this;
    }


    /** Initializes all variables. Used after close() or disconnect(), to be ready for new connect() */
    protected JChannel init() {
        if(local_addr != null)
            down(new Event(Event.REMOVE_ADDRESS, local_addr));
        local_addr=null;
        cluster_name=null;
        view=null;
        return this;
    }


    protected JChannel startStack(String cluster_name) throws Exception {
        checkClosed();

        this.cluster_name=cluster_name;
        prot_stack.startStack(); // calls start() in all protocols, from bottom to top

        /*create a temporary view, assume this channel is the only member and is the coordinator*/
        view=new View(local_addr, 0, Collections.singletonList(local_addr));  // create a dummy view
        TP transport=prot_stack.getTransport();
        transport.registerProbeHandler(probe_handler);
        return this;
    }

    /**
     * Generates and sets local_addr. Sends down a REMOVE_ADDRESS (if existing address was present) and
     * a SET_LOCAL_ADDRESS
     */
    protected JChannel setAddress() {
        Address old_addr=local_addr;
        local_addr=generateAddress();
        if(old_addr != null)
            down(new Event(Event.REMOVE_ADDRESS, old_addr));
        if(name == null || name.isEmpty()) // generate a logical name if not set
            name=Util.generateLocalName();
        if(name != null && !name.isEmpty())
            NameCache.add(local_addr, name);

        Event evt=new Event(Event.SET_LOCAL_ADDRESS, local_addr);
        down(evt);
        if(up_handler != null)
            up_handler.up(evt);
        return this;
    }

    protected Address generateAddress() {
        if(address_generators == null || address_generators.isEmpty())
            return UUID.randomUUID();
        if(address_generators.size() == 1)
            return address_generators.get(0).generateAddress();

        // at this point we have multiple AddressGenerators installed
        Address[] addrs=new Address[address_generators.size()];
        for(int i=0; i < addrs.length; i++)
            addrs[i]=address_generators.get(i).generateAddress();

        for(int i=0; i < addrs.length; i++) {
            if(!(addrs[i] instanceof ExtendedUUID)) {
                log.error("address generator %s does not subclass %s which is required if multiple address generators " +
                            "are installed, removing it", addrs[i].getClass().getSimpleName(), ExtendedUUID.class.getSimpleName());
                addrs[i]=null;
            }
        }
        ExtendedUUID uuid=null;
        for(int i=0; i < addrs.length; i++) { // we only have ExtendedUUIDs in addrs
            if(addrs[i] != null) {
                if(uuid == null)
                    uuid=(ExtendedUUID)addrs[i];
                else
                    uuid.addContents((ExtendedUUID)addrs[i]);
            }
        }
        return uuid != null? uuid : UUID.randomUUID();
    }


    protected JChannel checkClosed() {
        if(state == State.CLOSED)
            throw new IllegalStateException("channel is closed");
        return this;
    }

    protected JChannel checkClosedOrNotConnected() {
        State tmp=state;
        if(tmp == State.CLOSED)
            throw new IllegalStateException("channel is closed");
        if(!(tmp == State.CONNECTING || tmp == State.CONNECTED))
            throw new IllegalStateException("channel is disconnected");
        return this;
    }


    protected JChannel _close(boolean disconnect) {
        Address old_addr=local_addr;
        if(state == State.CLOSED)
            return this;
        if(disconnect)
            disconnect();                     // leave group if connected
        stopStack(true, true);
        state=State.CLOSED;
        notifyChannelClosed(this);
        init(); // sets local_addr=null; changed March 18 2003 (bela) -- prevented successful rejoining
        if(old_addr != null)
            NameCache.remove(old_addr);
        return this;
    }

    protected JChannel stopStack(boolean stop, boolean destroy) {
        if(prot_stack != null) {
            try {
                if(stop)
                    prot_stack.stopStack(cluster_name);

                if(destroy)
                    prot_stack.destroy();
            }
            catch(Exception e) {
                log.error(Util.getMessage("StackDestroyFailure"), e);
            }

            TP transport=prot_stack.getTransport();
            if(transport != null)
                transport.unregisterProbeHandler(probe_handler);
        }
        return this;
    }

    protected Address determineCoordinator() {
        return view != null? view.getCoord() : null;
    }



    protected JChannel notifyChannelConnected(JChannel c) {
        return notifyListeners(l -> l.channelConnected(c), "channelConnected");
    }

    protected JChannel notifyChannelDisconnected(JChannel c) {
        return notifyListeners(l -> l.channelDisconnected(c), "channelDisconnected()");
    }

    protected JChannel notifyChannelClosed(JChannel c) {
        return notifyListeners(l -> l.channelClosed(c), "channelClosed()");
    }

    protected JChannel notifyListeners(Consumer<ChannelListener> func, String msg) {
        if(channel_listeners != null) {
            try {
                channel_listeners.forEach(func);
            }
            catch(Throwable t) {
                log.error(Util.getMessage("CallbackException"), msg, t);
            }
        }
        return this;
    }

    /* ------------------------------- End of Private Methods ---------------------------------- */
}
