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
import org.jgroups.util.*;
import org.jgroups.util.UUID;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.InputStream;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * A channel represents a group communication endpoint (like a socket). An application joins a cluster by connecting
 * the channel to a cluster name and leaves it by disconnecting. Messages sent over the channel are received by all
 * cluster members that are connected to the same cluster (that is, all members that have the same cluster name).
 * <p>
 * The state machine for a channel is as follows: a channel is created (<em>unconnected</em>). The
 * channel is connected to a cluster (<em>connected</em>). Messages can now be sent and received. The
 * channel is disconnected from the cluster (<em>unconnected</em>). The channel could now be connected
 * to a different cluster again. The channel is closed (<em>closed</em>).
 * <p>
 * Only a single sender is allowed to be connected to a channel at a time, but there can be more than one channel
 * in an application.
 * <p>
 * Messages can be sent to the cluster members using the <em>send</em> method and messages can be received by setting
 * a {@link Receiver} in {@link #setReceiver(Receiver)} and implementing the {@link Receiver#receive(Message)} callback.
 *
 * @author Bela Ban
 * @since  2.0
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
    protected final DiagnosticsHandler.ProbeHandler probe_handler=new JChannelProbeHandler(this);

    @ManagedAttribute(description="Collect channel statistics",writable=true)
    protected boolean                               stats=true;

    @ManagedAttribute(description="Whether or not to discard messages sent by this channel",writable=true)
    protected boolean                               discard_own_messages;

    // https://issues.redhat.com/browse/JGRP-2922
    protected final Lock                            lock=new ReentrantLock();





    /**
     * Creates a JChannel without a protocol stack; used for programmatic creation of channel and protocol stack
     * @param create_protocol_stack If true, the default config is used. If false, no protocol stack is created
     */
    public JChannel(boolean create_protocol_stack) {
        if(create_protocol_stack) {
            try {
                init(ConfiguratorFactory.getStackConfigurator(Global.DEFAULT_PROTOCOL_STACK));
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new JGroupsException(e);
            }
        }
    }

    /** Creates a {@code JChannel} with the default stack */
    public JChannel() {
        this(Global.DEFAULT_PROTOCOL_STACK);
    }


    /**
     * Constructs a JChannel instance with the protocol stack configuration based upon the specified properties parameter.
     * @param props A file containing a JGroups XML configuration or a URL pointing to an XML configuration
     */
    public JChannel(String props) {
        try {
            init(ConfiguratorFactory.getStackConfigurator(props));
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new JGroupsException(e);
        }
    }

    /**
     * Creates a channel with a configuration based on an input stream.
     * @param input An input stream, pointing to a streamed configuration. It is the caller's resposibility to close
     *              the input stream after the constructor returns
     */
    public JChannel(InputStream input) {
        try {
            init(ConfiguratorFactory.getStackConfigurator(input));
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new JGroupsException(e);
        }
    }

    /**
     * Constructs a JChannel with the protocol stack configuration contained by the protocol stack configurator parameter.
     * <p>
     * All the public constructors of this class eventually delegate to this method.
     * @param configurator A protocol stack configurator containing a JGroups protocol stack configuration.
     */
    public JChannel(ProtocolStackConfigurator configurator) {
        try {
            init(configurator);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new JGroupsException(e);
        }
    }


    /**
     * Creates a channel from an array of protocols. Note that after a {@link org.jgroups.JChannel#close()}, the protocol
     * list <em>should not</em> be reused, ie. new JChannel(protocols) would reuse the same protocol list, and this
     * might lead to problems!
     * @param protocols The list of protocols, from bottom to top, ie. the first protocol in the list is the transport,
     *                  the last the top protocol
     */
    public JChannel(Protocol ... protocols) {
        this(Arrays.asList(protocols));
    }



    /**
     * Creates a channel from a list of protocols. Note that after a {@link org.jgroups.JChannel#close()}, the protocol
     * list <em>should not</em> be reused, ie. new JChannel(protocols) would reuse the same protocol list, and this
     * might lead to problems !
     * @param protocols The list of protocols, from bottom to top, ie. the first protocol in the list is the transport,
     *                  the last the top protocol
     */
    public JChannel(List<Protocol> protocols) {
        try {
            prot_stack=new ProtocolStack().setChannel(this);
            for(Protocol prot: protocols) {
                if(prot != null) {
                    prot_stack.addProtocol(prot);
                    prot.setProtocolStack(prot_stack);
                }
            }
            prot_stack.init();
            prot_stack.getTransport().getDiagnosticsHandler().setEnabled(false);
            StackType ip_version=Util.getIpStackType();
            TP transport=(TP)protocols.get(0);
            InetAddress resolved_addr=Configurator.getValueFromObject(transport, "bind_addr");
            if(resolved_addr != null)
                ip_version=resolved_addr instanceof Inet6Address? StackType.IPv6 : StackType.IPv4;
            else if(ip_version == StackType.Dual)
                ip_version=StackType.IPv4; // prefer IPv4 addresses
    
            // Substitute vars with defined system props (if any)
            List<Protocol> prots=prot_stack.getProtocols();
            Map<String,String> map=new HashMap<>();
            for(Protocol prot: prots)
                Configurator.resolveAndAssignFields(prot, map, ip_version);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new JGroupsException(e);
        }
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


    @ManagedAttribute(name="address")
    public String getAddressAsString() {return local_addr != null? local_addr.toString() : "n/a";}

    @ManagedAttribute(name="address_uuid")
    public String getAddressAsUUID() {return local_addr instanceof UUID? ((UUID)local_addr).toStringLong() : null;}

    /** Sets the logical name for the channel. The name will stay associated with this channel for the channel's lifetime
     * (until close() is called). This method must be called <em>before</em> calling connect() */
    @ManagedAttribute(writable=true, description="The logical name of this channel. Stays with the channel until " +
      "the channel is closed")
    public JChannel setName(String name) {
        if(name == null || isConnected())
            return this;
        this.name=name;
        if(local_addr != null)
            NameCache.add(local_addr, this.name);
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
    @ManagedAttribute public static  String getVersion()   {return Version.printDescription();}


    /** Adds a ChannelListener that will be notified when a connect, disconnect or close occurs */
    public JChannel addChannelListener(ChannelListener listener) {
        if(listener == null)
            return this;
        lock.lock();
        try {
            if(channel_listeners == null)
                channel_listeners=new CopyOnWriteArraySet<>();
            channel_listeners.add(listener);
            return this;
        }
        finally {
            lock.unlock();
        }
    }

    public JChannel removeChannelListener(ChannelListener listener) {
        if(listener == null)
            return this;
        lock.lock();
        try {
            if(channel_listeners != null)
                channel_listeners.remove(listener);
            return this;
        }
        finally {
            lock.unlock();
        }
    }

    public JChannel clearChannelListeners() {
        lock.lock();
        try {
            if(channel_listeners != null)
                channel_listeners.clear();
            return this;
        }
        finally {
            lock.unlock();
        }
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

    /** Dumps all protocols in string format. If include_props is set, the attrs of each protocol are also printed */
    @ManagedOperation
    public String printProtocolSpec(boolean include_props) {
        ProtocolStack ps=getProtocolStack();
        return ps != null? ps.printProtocolSpec(include_props) : null;
    }

    /** Returns a map of statistics of the various protocols and of the channel itself */
    @ManagedOperation
    public Map<String,Map<String,Object>> dumpStats() {
        return prot_stack.dumpStats();
    }

    public Map<String,Map<String,Object>> dumpStats(String protocol_name, List<String> attrs) {
        return prot_stack.dumpStats(protocol_name, attrs);
    }

    @ManagedOperation
    public Map<String,Map<String,Object>> dumpStats(String protocol_name) {
        return prot_stack.dumpStats(protocol_name, null);
    }



    /**
     * Joins the cluster. The application is now able to receive messages from cluster members, views and to send
     * messages to (all or single) cluster members. This is a no-op if already connected.<p>
     * All channels connecting to the same cluster name form a cluster; messages sent to the cluster will
     * be received by all cluster members.
     * @param cluster_name The name of the cluster to join
     * @exception JGroupsException The protocol stack cannot be started
     * @exception IllegalStateException The channel is closed
     */
    @ManagedOperation(description="Connects the channel to a group")
    public JChannel connect(String cluster_name) {
        lock.lock();
        try {
            if(!_preConnect(cluster_name))
                return this;
            Event connect_event=new Event(Event.CONNECT, cluster_name);
            _connect(connect_event);
            state=State.CONNECTED;
            notifyChannelConnected(this);
            return this;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new JGroupsException(e);
        } finally {
            lock.unlock();
        }
    }


    /**
     * Joins the cluster and gets the state from a specified state provider.
     * <p>
     * This method invokes {@code connect} and {@code getState} methods.
     * If the channel is closed an exception will be thrown.
     * @param cluster_name  the cluster name to connect to. Cannot be null.
     * @param target the state provider. If null, the state will be fetched from coordinator, unless this channel is coordinator.
     * @param timeout the timeout for state transfer.
     * @exception JGroupsException Connecting to the cluster was not successful
     * @exception StateTransferException State transfer was not successful
     * @exception IllegalStateException The channel is closed and therefore cannot be used
     */
    public JChannel connect(String cluster_name, Address target, long timeout) {
        lock.lock();
        try {
            if(!_preConnect(cluster_name))
                return this;
            Event connect_event=new Event(Event.CONNECT_WITH_STATE_TRANSFER, cluster_name);
            _connect(connect_event);
            state=State.CONNECTED;
            notifyChannelConnected(this);
            boolean canFetchState=view != null && view.size() > 1;
            if(canFetchState) // if I am not the only member in cluster then ...
                getState(target, timeout); // fetch state from target
            return this;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new JGroupsException(e);
        } finally {
            lock.unlock();
        }
    }

    
    /**
     * Leaves the cluster (disconnects the channel if it is connected). If the channel is closed or disconnected, this
     * operation is ignored. The channel can then be used to join the same or a different cluster again.
     * @see #connect(String)
     */
    @ManagedOperation(description="Disconnects the channel if connected")
    public JChannel disconnect() {
        lock.lock();
        try {
            switch(state) {
                case OPEN:
                case CLOSED:
                    break;
                case CONNECTING:
                case CONNECTED:
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
        finally {
            lock.unlock();
        }
    }


    /**
     * Destroys the channel and its associated resources (e.g. the protocol stack). After a channel has been closed,
     * invoking methods on it will throw a {@code ChannelClosed} exception (or results in a null operation).
     * It is a no-op if the channel is already closed.<p>
     * If the channel is connected to a cluster, {@code disconnect()} will be called first.
     */
    @ManagedOperation(description="Disconnects and destroys the channel")
    public void close() {
        lock.lock();
        try {
            _close(true); // by default disconnect before closing channel and close mq
        }
        finally {
            lock.unlock();
        }
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
    public JChannel send(Message msg) {
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
    public JChannel send(Address dst, Object obj) {
        Message msg=new ObjectMessage(dst, obj);
        return send(msg);
    }

    /**
     * Sends a message. See {@link #send(Address,byte[],int,int)} for details
     * @param dst destination address for the message. If null, the message will be sent to all cluster members
     * @param buf buffer message payload
     * @throws Exception exception thrown if the message sending was not successful
     */
    public JChannel send(Address dst, byte[] buf) {
        return send(new BytesMessage(dst, buf));
    }

    /**
     * Sends a message to a destination.
     * @param dst the destination address. If null, the message will be sent to all cluster nodes (= cluster members)
     * @param buf the buffer to be sent
     * @param offset the offset into the buffer
     * @param length the length of the data to be sent. Has to be smaller thanq buf.length - offset. This will send
     *           {@code length} bytes starting at {@code offset}
     * @throws Exception thrown if send() failed
     */
    public JChannel send(Address dst, byte[] buf, int offset, int length) {
        return send(new BytesMessage(dst, buf, offset, length));
    }


    /**
     * Retrieves the full state from the target member.<br/>
     * The state transfer is initiated by invoking getState() on this channel. The state provider in turn invokes the
     * {@link Receiver#getState(java.io.OutputStream)} callback and sends the state to this node, the state receiver.
     * After the state is received by the state receiver, the {@link Receiver#setState(java.io.InputStream)} callback
     * is invoked to install the state.
     * @param target the state provider. If null the coordinator is used by default
     * @param timeout the number of milliseconds to wait for the operation to complete successfully. 0
     *           waits forever, or until the state has been received
     * @see Receiver#getState(java.io.OutputStream)
     * @see Receiver#setState(java.io.InputStream)
     * @exception IllegalStateException the channel was closed or disconnected
     * @exception StateTransferException raised if there was a problem during the state transfer
     */
    public JChannel getState(Address target, long timeout) {
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
        state_promise.reset();
        StateTransferInfo state_info=new StateTransferInfo(target, timeout);
        long start=System.currentTimeMillis();
        down(new Event(Event.GET_STATE, state_info));
        StateTransferResult result=state_promise.getResult(state_info.timeout);
        if(result == null)
            throw new StateTransferException("timeout during state transfer (" + (System.currentTimeMillis() - start) + "ms)");
        if(result.hasException())
            throw new StateTransferException(result.getException());
        return this;
    }

    /**
     * Sends an event down the protocol stack. Note that - contrary to {@link #send(Message)}, if the event is a message,
     * no checks are performed whether the channel is closed or disconnected. Note that this method is not typically
     * used by applications.
     * @param evt the message to send down, encapsulated in an event
     */
    public Object down(Event evt) {
        return evt != null? prot_stack.down(evt) : null;
    }

    public Object down(Message msg) {
        return msg != null? prot_stack.down(msg) : null;
    }

    /**
     * Sends a message down asynchronously. The sending is executed in the transport's thread pool. If the pool is full
     * and the message is marked as {@link org.jgroups.Message.TransientFlag#DONT_BLOCK}, then it will be dropped,
     * otherwise it will be sent on the caller's thread.
     * @param msg The message to be sent
     * @param async Whether to send the message asynchronously
     * @return A CompletableFuture of the result (or exception)
     */
    public CompletableFuture<Object> down(Message msg, boolean async) {
        return msg != null? prot_stack.down(msg, async) : null;
    }

    /**
     * Callback method <BR>
     * Called by the ProtocolStack when a message is received.
     * @param evt the event carrying the message from the protocol stack
     */
    public Object up(Event evt) {
        switch(evt.getType()) {
            case Event.VIEW_CHANGE:
                view=evt.getArg();

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
                if(cfg != null && cfg.containsKey("state_transfer"))
                    state_transfer_supported=(Boolean)cfg.get("state_transfer");
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

    public String toString() {
        return isConnected()? String.format("%s (%s)", address(), cluster_name) : String.format("%s (%s)", name, state);
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

    protected JChannel _connect(Event evt) {
        try {
            down(evt);
            return this;
        }
        catch(Exception ex) {
            cleanup();
            throw ex;
        }
    }

    protected JChannel cleanup() {
        stopStack(true, false);
        state=State.OPEN;
        return init();
    }

    protected Object invokeCallback(int type, Object arg) {
        switch(type) {
            case Event.VIEW_CHANGE:
                receiver.viewAccepted((View)arg);
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
        }
        return null;
    }

    protected final JChannel init(ProtocolStackConfigurator configurator) throws Exception {
        List<ProtocolConfiguration> configs=configurator.getProtocolStack();
        // replace vars with system props
        configs.forEach(ProtocolConfiguration::substituteVariables);
        prot_stack=new ProtocolStack(this);
        prot_stack.setup(configs, configurator); // Setup protocol stack (creates protocol, calls init() on them)
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

    /** Generates local_addr. Sends down a REMOVE_ADDRESS (if existing address was present) and a SET_LOCAL_ADDRESS */
    protected JChannel setAddress() {
        if(name == null || name.isEmpty()) // generate a logical name if not set
            name=Util.generateLocalName();
        Address old_addr=local_addr;
        local_addr=generateAddress(name);
        if(name != null && !name.isEmpty()) {
            log.info("local_addr: %s, name: %s", local_addr, name);
            NameCache.add(local_addr, name);
        }
        if(old_addr != null)
            down(new Event(Event.REMOVE_ADDRESS, old_addr));

        for(Protocol p=prot_stack.getTopProtocol(); p != null; p=p.getDownProtocol())
            p.setAddress(local_addr);
        if(up_handler != null)
            up_handler.setLocalAddress(local_addr);
        return this;
    }

    protected Address generateAddress(String name) {
        if(address_generators == null || address_generators.isEmpty())
            return UUID.randomUUID();
        if(address_generators.size() == 1)
            return address_generators.get(0).generateAddress(name);

        // at this point we have multiple AddressGenerators installed
        Address[] addrs=new Address[address_generators.size()];
        for(int i=0; i < addrs.length; i++)
            addrs[i]=address_generators.get(i).generateAddress(name);

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
