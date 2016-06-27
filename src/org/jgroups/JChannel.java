package org.jgroups;

import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.blocks.MethodCall;
import org.jgroups.conf.ConfiguratorFactory;
import org.jgroups.conf.ProtocolConfiguration;
import org.jgroups.conf.ProtocolStackConfigurator;
import org.jgroups.jmx.ResourceDMBean;
import org.jgroups.protocols.TP;
import org.jgroups.stack.*;
import org.jgroups.util.*;
import org.jgroups.util.UUID;
import org.w3c.dom.Element;

import java.io.*;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;

/**
 * JChannel is a default implementation of a Channel abstraction.
 * <p/>
 * 
 * JChannel is instantiated using an appropriate form of a protocol stack description. Protocol
 * stack can be described using a file, URL or a stream containing XML stack description.
 * 
 * @author Bela Ban
 * @since 2.0
 */
@MBean(description="JGroups channel")
public class JChannel extends Channel {

    /** The default protocol stack used by the default constructor  */
    public static final String                      DEFAULT_PROTOCOL_STACK="udp.xml";

    /*the address of this JChannel instance*/
    protected Address                               local_addr;

    protected List<AddressGenerator>                address_generators;

    protected String                                name;

    /* the channel (also know as group) name */
    protected String                                cluster_name;

    /* the latest view of the group membership */
    protected View                                  my_view;

    /*the protocol stack, used to send and receive messages from the protocol stack*/
    protected ProtocolStack                         prot_stack;

    protected final Promise<StateTransferResult>    state_promise=new Promise<>();


    /** True if a state transfer protocol is available, false otherwise (set by CONFIG event from STATE_TRANSFER protocol) */
    protected boolean                               state_transfer_supported=false;

    /** True if a flush protocol is available, false otherwise (set by CONFIG event from FLUSH protocol) */
    protected volatile boolean                      flush_supported=false;


    protected final ConcurrentMap<String,Object>    config=Util.createConcurrentMap(16);

    /** Collect statistics */
    @ManagedAttribute(description="Collect channel statistics",writable=true)
    protected boolean                               stats=true;

    protected long                                  sent_msgs=0, received_msgs=0, sent_bytes=0, received_bytes=0;

    protected final DiagnosticsHandler.ProbeHandler probe_handler=new MyProbeHandler();



    /**
     * Creates a JChannel without a protocol stack; used for programmatic creation of channel and protocol stack
     *
     * @param create_protocol_stack If true, the default configuration will be used. If false, no protocol stack
     *        will be created
     */
    public JChannel(boolean create_protocol_stack) {
        if(create_protocol_stack) {
            try {
                init(ConfiguratorFactory.getStackConfigurator(DEFAULT_PROTOCOL_STACK));
            }
            catch(Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Constructs a <code>JChannel</code> instance with the protocol stack
     * specified by the <code>DEFAULT_PROTOCOL_STACK</code> member.
     * @throws Exception If problems occur during the initialization of the protocol stack.
     */
    public JChannel() throws Exception {
        this(DEFAULT_PROTOCOL_STACK);
    }

    /**
     * Constructs a JChannel instance with the protocol stack configuration contained by the specified file.
     * @param properties A file containing a JGroups XML protocol stack configuration.
     * @throws Exception If problems occur during the configuration or initialization of the protocol stack.
     */
    public JChannel(File properties) throws Exception {
        this(ConfiguratorFactory.getStackConfigurator(properties));
    }

    /**
     * Constructs a JChannel instance with the protocol stack configuration contained by the specified XML element.
     * @param properties An XML element containing a JGroups XML protocol stack configuration.
     * @throws Exception If problems occur during the configuration or initialization of the protocol stack.
     */
    public JChannel(Element properties) throws Exception {
        this(ConfiguratorFactory.getStackConfigurator(properties));
    }

    /**
     * Constructs a JChannel instance with the protocol stack configuration indicated by the specified URL.
     * @param properties A URL pointing to a JGroups XML protocol stack configuration.
     * @throws Exception If problems occur during the configuration or initialization of the protocol stack.
     */
    public JChannel(URL properties) throws Exception {
        this(ConfiguratorFactory.getStackConfigurator(properties));
    }

    /**
     * Constructs a JChannel instance with the protocol stack configuration based upon the specified properties parameter.
     * @param props A file containing a JGroups XML configuration, a URL pointing to an XML configuration, or an old
     *              style plain configuration string.
     * @throws Exception If problems occur during the configuration or initialization of the protocol stack.
     */
    public JChannel(String props) throws Exception {
        this(ConfiguratorFactory.getStackConfigurator(props));
    }

    /**
     * Creates a channel with a configuration based on an input stream.
     * @param input An input stream, pointing to a streamed configuration
     * @throws Exception
     */
    public JChannel(InputStream input) throws Exception {
        this(ConfiguratorFactory.getStackConfigurator(input));
    }

    /**
     * Constructs a JChannel with the protocol stack configuration contained by the protocol stack configurator parameter.
     * <p>
     * All of the public constructors of this class eventually delegate to this method.
     * @param configurator A protocol stack configurator containing a JGroups protocol stack configuration.
     * @throws Exception If problems occur during the initialization of the protocol stack.
     */
    public JChannel(ProtocolStackConfigurator configurator) throws Exception {
        init(configurator);
    }


    /**
     * Creates a channel from an array of protocols. Note that after a {@link org.jgroups.JChannel#close()}, the protocol
     * list <em>should not</em> be reused, ie. new JChannel(protocols) would reuse the same protocol list, and this
     * might lead to problems !
     * @param protocols The list of protocols, from bottom to top, ie. the first protocol in the list is the transport,
     *                  the last the top protocol
     * @throws Exception
     */
    public JChannel(Protocol ... protocols) throws Exception {
        this(Arrays.asList(protocols));
    }

    /**
     * Creates a channel from an array of protocols. Note that after a {@link org.jgroups.JChannel#close()}, the protocol
     * list <em>should not</em> be reused, ie. new JChannel(protocols) would reuse the same protocol list, and this
     * might lead to problems !
     * @param protocols The list of protocols, from bottom to top, ie. the first protocol in the list is the transport,
     *                  the last the top protocol
     * @throws Exception
     */
    public JChannel(Collection<Protocol> protocols) throws Exception {
        prot_stack=new ProtocolStack();
        setProtocolStack(prot_stack);
        for(Protocol prot: protocols) {
            prot_stack.addProtocol(prot);
            prot.setProtocolStack(prot_stack);
        }
        prot_stack.init();

        // Substitute vars with defined system props (if any)
        List<Protocol> prots=prot_stack.getProtocols();
        Map<String,String> map=new HashMap<>();
        for(Protocol prot: prots)
            Configurator.resolveAndAssignFields(prot, map);
    }


    /**
     * Creates a channel with the same configuration as the channel passed to this constructor. This is used by
     * testing code, and should not be used by clients !
     * @param ch
     * @throws Exception
     */
    public JChannel(JChannel ch) throws Exception {
        init(ch);
        discard_own_messages=ch.discard_own_messages;
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


    /**
     * Returns the protocol stack configuration in string format. An example of this property is<br/>
     * "UDP:PING:FD:STABLE:NAKACK:UNICAST:FRAG:FLUSH:GMS:VIEW_ENFORCER:STATE_TRANSFER:QUEUE"
     */
    public String getProperties() {return prot_stack != null? prot_stack.printProtocolSpec(true) : null;}

    public boolean statsEnabled() {return stats;}

    public void enableStats(boolean stats) {this.stats=stats;}

    @ManagedOperation
    public void resetStats()          {sent_msgs=received_msgs=sent_bytes=received_bytes=0;}

    @ManagedAttribute
    public long getSentMessages()     {return sent_msgs;}
    @ManagedAttribute
    public long getSentBytes()        {return sent_bytes;}
    @ManagedAttribute
    public long getReceivedMessages() {return received_msgs;}
    @ManagedAttribute
    public long getReceivedBytes()    {return received_bytes;}
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


    @ManagedOperation(description="Connects the channel to a group")
    public synchronized void connect(String cluster_name) throws Exception {
    	connect(cluster_name, true);
    }

    /**
     * Connects the channel to a group.
     * @see JChannel#connect(String)
     */
    @ManagedOperation(description="Connects the channel to a group")
    protected synchronized void connect(String cluster_name, boolean useFlushIfPresent) throws Exception {
        if(!_preConnect(cluster_name))
            return;

        if(cluster_name != null) { // only connect if we are not a unicast channel
            Event connect_event=useFlushIfPresent? new Event(Event.CONNECT_USE_FLUSH, cluster_name)
              : new Event(Event.CONNECT, cluster_name);
            _connect(connect_event);
        }
        state=State.CONNECTED;
        notifyChannelConnected(this);
    }

    public synchronized void connect(String cluster_name, Address target, long timeout) throws Exception {
    	connect(cluster_name, target, timeout, true);
    }

    
    /**
     * Connects this channel to a group and gets a state from a specified state provider.<p/>
     * This method invokes <code>connect()</code> and then <code>getState</code>.<p/>
     * If the FLUSH protocol is in the channel's stack definition, only one flush round is executed for both connecting and
     * fetching the state rather than two flushes if we invoke <code>connect</code> and <code>getState</code> in succession.
     * <p/>
     * If the channel is already connected, an error message will be printed to the error log.
     * If the channel is closed a ChannelClosed exception will be thrown.
     * @param cluster_name  The cluster name to connect to. Cannot be null.
     * @param target The state provider. If null, the state will be fetched from the coordinator, unless this channel
     *               is the coordinator.
     * @param timeout The timeout for the state transfer.
     * 
     * @exception Exception The protocol stack cannot be started, or the JOIN failed
     * @exception IllegalStateException The channel is closed or disconnected
     * @exception StateTransferException State transfer was not successful
     *
     */
    public synchronized void connect(String cluster_name, Address target, long timeout,
                                     boolean useFlushIfPresent) throws Exception {
        if(!_preConnect(cluster_name))
            return;

        if(cluster_name == null) { // only connect if we are not a unicast channel
            state=State.CONNECTED;
            return;
        }

        boolean canFetchState=false;
        try {
            Event connect_event=useFlushIfPresent? new Event(Event.CONNECT_WITH_STATE_TRANSFER_USE_FLUSH, cluster_name)
              : new Event(Event.CONNECT_WITH_STATE_TRANSFER, cluster_name);
            _connect(connect_event);
            state=State.CONNECTED;
            notifyChannelConnected(this);
            canFetchState=getView() != null && getView().size() > 1;

            // if I am not the only member in cluster then
            if(canFetchState)
                getState(target, timeout, false); // fetch state from target
        }
        finally {
            if(flushSupported() && useFlushIfPresent) {
                if(canFetchState || state != State.CONNECTED) // stopFlush if we fetched the state or failed to connect...
                    stopFlush();
            }
        }
    }


    @ManagedOperation(description="Disconnects the channel if connected")
    public synchronized void disconnect() {
        switch(state) {
            case OPEN:
            case CLOSED:
                return;
            case CONNECTING:
            case CONNECTED:
                if(cluster_name != null) {
                    // Send down a DISCONNECT event, which travels down to the GMS, where a response is returned
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
    }


    @ManagedOperation(description="Disconnects and destroys the channel")
    public synchronized void close() {
        _close(true); // by default disconnect before closing channel and close mq
    }





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
        Map<String,Long> retval=new HashMap<>();
        retval.put("sent_msgs",      sent_msgs);
        retval.put("sent_bytes",     sent_bytes);
        retval.put("received_msgs",  received_msgs);
        retval.put("received_bytes", received_bytes);
        return retval;
    }


    public void send(Message msg) throws Exception {
        checkClosedOrNotConnected();
        if(msg == null)
            throw new NullPointerException("msg is null");
        down(new Event(Event.MSG, msg));
    }


    public void send(Address dst, Object obj) throws Exception {
        send(new Message(dst, obj));
    }

    public void send(Address dst, byte[] buf) throws Exception {
        send(new Message(dst, buf));
    }

    public void send(Address dst, byte[] buf, int offset, int length) throws Exception {
        send(new Message(dst, buf, offset, length));
    }


    public View getView() {
        return state == State.CONNECTED ? my_view : null;
    }
    
    @ManagedAttribute(name="view")
    public String getViewAsString() {
        View v=getView();
        return v != null ? v.toString() : "n/a";
    }
    
    @ManagedAttribute
    public static String getVersion() {return Version.printDescription();}


    public Address getAddress() {return state == State.CLOSED ? null : local_addr;}

    @ManagedAttribute(name="address")
    public String getAddressAsString() {return local_addr != null? local_addr.toString() : "n/a";}

    @ManagedAttribute(name="address_uuid")
    public String getAddressAsUUID() {return local_addr instanceof UUID? ((UUID)local_addr).toStringLong() : null;}

    public String getName() {return name;}

    public String getName(Address member) {return member != null? UUID.get(member) : null;}

    @ManagedAttribute(writable=true, description="The logical name of this channel. Stays with the channel until " +
            "the channel is closed")
    public void setName(String name) {
        if(name != null) {
            if(isConnected())
                throw new IllegalStateException("name cannot be set if channel is connected (should be done before)");
            this.name=name;
            if(local_addr != null)
                UUID.add(local_addr, this.name);
        }
    }

    public JChannel name(String name) {setName(name); return this;}

    public JChannel receiver(Receiver r) {setReceiver(r); return this;}

    @ManagedAttribute(description="Returns cluster name this channel is connected to")
    public String getClusterName() {return state == State.CONNECTED? cluster_name : null;}

    /**
     * Returns the first {@link AddressGenerator} in the list, or null if none is set
     * @return
     * @since 2.12
     * @deprecated Doesn't make any sense as there's list of address generators, will be removed in 4.0
     */
    @Deprecated
    public AddressGenerator getAddressGenerator() {
        return (address_generators == null || address_generators.isEmpty())? null : address_generators.get(0);
    }

    /**
     * @deprecated Use {@link #addAddressGenerator(org.jgroups.stack.AddressGenerator)} instead
     */
    @Deprecated
    public void setAddressGenerator(AddressGenerator address_generator) {
        addAddressGenerator(address_generator);
    }

    /**
     * Sets the new {@link AddressGenerator}. New addresses will be generated using the new generator. This
     * should <em>not</em> be done while a channel is connected, but before connecting.
     * @param address_generator
     * @since 2.12
     */
    public void addAddressGenerator(AddressGenerator address_generator) {
        if(address_generator == null)
            return;
        if(address_generators == null)
            address_generators=new ArrayList<>(3);
        address_generators.add(address_generator);
    }

    public boolean removeAddressGenerator(AddressGenerator address_generator) {
        return address_generator != null && address_generators != null && address_generators.remove(address_generator);
    }


    public void getState(Address target, long timeout) throws Exception {
        getState(target, timeout, true);
    }


    /**
     * Retrieves state from the target member. See {@link #getState(Address,long)} for details.
     */
    public void getState(Address target, long timeout, boolean useFlushIfPresent) throws Exception {
    	Callable<Boolean> flusher = new Callable<Boolean>() {
			public Boolean call() throws Exception {
				return Util.startFlush(JChannel.this);
			}
		};
		getState(target, timeout, useFlushIfPresent?flusher:null);
	}

    protected boolean _preConnect(String cluster_name) throws Exception {
        if(state == State.CONNECTED) {
            if(log.isTraceEnabled()) log.trace("already connected to " + this.cluster_name);
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

    protected void _connect(Event connect_event) throws Exception {
        try {
            down(connect_event);
        }
        catch(Throwable t) {
            stopStack(true, false);
            state=State.OPEN;
            init();
            throw new Exception("connecting to channel \"" + connect_event.getArg() + "\" failed", t);
        }
    }


    protected void getState(Address target, long timeout, Callable<Boolean> flushInvoker) throws Exception {
        checkClosedOrNotConnected();
        if(!state_transfer_supported)
            throw new IllegalStateException("fetching state will fail as state transfer is not supported. "
                                              + "Add one of the state transfer protocols to your configuration");

        if(target == null)
            target=determineCoordinator();
        if(target != null && local_addr != null && target.equals(local_addr)) {
            log.trace(local_addr + ": cannot get state from myself (" + target + "): probably the first member");
            return;
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
    }


    /**
     * Callback method <BR>
     * Called by the ProtocolStack when a message is received.
     * @param evt the event carrying the message from the protocol stack
     */
    public Object up(Event evt) {
        switch(evt.getType()) {

            case Event.MSG:
                Message msg=(Message)evt.getArg();
                if(stats) {
                    received_msgs++;
                    received_bytes+=msg.getLength();
                }

                // discard local messages (sent by myself to me)
                if(discard_own_messages && local_addr != null && msg.getSrc() != null && local_addr.equals(msg.getSrc()))
                    return null;
                break;

            case Event.VIEW_CHANGE:
                View tmp=(View)evt.getArg();
                if(tmp instanceof MergeView)
                    my_view=new View(tmp.getViewId(), tmp.getMembers());
                else
                    my_view=tmp;

                // Bela&Vladimir Oct 27th,2006 (JGroups 2.4): we need to set connected=true because a client can
                // call channel.getView() in viewAccepted() callback invoked on this thread (see Event.VIEW_CHANGE handling below)

                // not good: we are only connected when we returned from connect() - bela June 22 2007
                // Changed: when a channel gets a view of which it is a member then it should be
                // connected even if connect() hasn't returned yet ! (bela Noc 2010)
                if(state != State.CONNECTED)
                    state=State.CONNECTED;
                break;

            case Event.CONFIG:
                Map<String,Object> cfg=(Map<String,Object>)evt.getArg();
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
                StateTransferResult result=(StateTransferResult)evt.getArg();
                if(up_handler != null) {
                    try {
                        Object retval=up_handler.up(evt);
                        state_promise.setResult(new StateTransferResult());
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
                state_promise.setResult((StateTransferResult)evt.getArg());
                break;

            case Event.STATE_TRANSFER_INPUTSTREAM:
                // Oct 13,2006 moved to down() when Event.STATE_TRANSFER_INPUTSTREAM_CLOSED is received
                // state_promise.setResult(is != null? Boolean.TRUE : Boolean.FALSE);

                if(up_handler != null)
                    return up_handler.up(evt);

                InputStream is=(InputStream)evt.getArg();
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
                        receiver.getState((OutputStream)evt.getArg());
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


    /** Callback invoked by the protocol stack to deliver a message batch */
    public void up(MessageBatch batch) {
        if(stats) {
            received_msgs+=batch.size();
            received_bytes+=batch.length();
        }

        // discard local messages (sent by myself to me)
        if(discard_own_messages && local_addr != null && batch.sender() != null && local_addr.equals(batch.sender()))
            return;

        for(Message msg: batch) {
            if(up_handler != null) {
                try {
                    up_handler.up(new Event(Event.MSG, msg));
                }
                catch(Throwable t) {
                    log.error(Util.getMessage("UpHandlerFailure"), t);
                }
            }
            else if(receiver != null) {
                try {
                    receiver.receive(msg);
                }
                catch(Throwable t) {
                    log.error(Util.getMessage("ReceiverFailure"), t);
                }
            }
        }
    }



    /**
     * Sends an event down the protocol stack. Note that - contrary to {@link #send(Message)}, if the event is a message,
     * no checks are performed whether the channel is closed or disconnected.
     * @param evt the message to send down, encapsulated in an event
     */
    public Object down(Event evt) {
        if(evt == null) return null;
        if(stats && evt.getType() == Event.MSG) {
            sent_msgs++;
            sent_bytes+=((Message)evt.getArg()).getLength();
        }
        return prot_stack.down(evt);
    }




    @ManagedOperation
    public String toString(boolean details) {
        StringBuilder sb=new StringBuilder();
        sb.append("local_addr=").append(local_addr).append('\n').append("cluster_name=")
        		.append(cluster_name).append('\n').append("my_view=").append(my_view).append('\n')
                .append("state=").append(state).append('\n');
        if(details) {
            sb.append("discard_own_messages=").append(discard_own_messages).append('\n');
            sb.append("state_transfer_supported=").append(state_transfer_supported).append('\n');
            sb.append("props=").append(getProperties()).append('\n');
        }

        return sb.toString();
    }


    /* ----------------------------------- Private Methods ------------------------------------- */
    protected Object invokeCallback(int type, Object arg) {
        switch(type) {
            case Event.MSG:
                receiver.receive((Message)arg);
                break;
            case Event.VIEW_CHANGE:
                receiver.viewAccepted((View)arg);
                break;
            case Event.SUSPECT:
                receiver.suspect((Address)arg);
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

    protected final void init(ProtocolStackConfigurator configurator) throws Exception {
        List<ProtocolConfiguration> configs=configurator.getProtocolStack();
        for(ProtocolConfiguration config: configs)
            config.substituteVariables();  // replace vars with system props

        prot_stack=new ProtocolStack(this);
        prot_stack.setup(configs); // Setup protocol stack (creates protocol, calls init() on them)
    }

    protected final void init(JChannel ch) throws Exception {
        if(ch == null)
            throw new IllegalArgumentException("channel is null");
        prot_stack=new ProtocolStack(this);
        prot_stack.setup(ch.getProtocolStack()); // Setup protocol stack (creates protocol, calls init() on them)
    }


    /**
     * Initializes all variables. Used after <tt>close()</tt> or <tt>disconnect()</tt>,
     * to be ready for new <tt>connect()</tt>
     */
    protected void init() {
        if(local_addr != null)
            down(new Event(Event.REMOVE_ADDRESS, local_addr));
        local_addr=null;
        cluster_name=null;
        my_view=null;
    }


    protected void startStack(String cluster_name) throws Exception {
        /*make sure the channel is not closed*/
        checkClosed();

        /*make sure we have a valid channel name*/
        if(cluster_name == null)
            log.debug("cluster_name is null, assuming unicast channel");
        else
            this.cluster_name=cluster_name;

        if(socket_factory != null)
            prot_stack.getTopProtocol().setSocketFactory(socket_factory);

        prot_stack.startStack(cluster_name, local_addr); // calls start() in all protocols, from top to bottom

        /*create a temporary view, assume this channel is the only member and is the coordinator*/
        List<Address> t=new ArrayList<>(1);
        t.add(local_addr);
        my_view=new View(local_addr, 0, t);  // create a dummy view

        TP transport=prot_stack.getTransport();
        transport.registerProbeHandler(probe_handler);
    }

    /**
     * Generates new UUID and sets local address. Sends down a REMOVE_ADDRESS (if existing address was present) and
     * a SET_LOCAL_ADDRESS
     */
    protected void setAddress() {
        Address old_addr=local_addr;
        local_addr=generateAddress();
        if(old_addr != null)
            down(new Event(Event.REMOVE_ADDRESS, old_addr));
        if(name == null || name.isEmpty()) // generate a logical name if not set
            name=Util.generateLocalName();
        if(name != null && !name.isEmpty())
            UUID.add(local_addr, name);

        Event evt=new Event(Event.SET_LOCAL_ADDRESS, local_addr);
        down(evt);
        if(up_handler != null)
            up_handler.up(evt);
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


    /**
     * health check<BR>
     * throws a ChannelClosed exception if the channel is closed
     */
    protected void checkClosed() {
        if(state == State.CLOSED)
            throw new IllegalStateException("channel is closed");
    }


    protected void checkClosedOrNotConnected() {
        if(state == State.CLOSED)
            throw new IllegalStateException("channel is closed");

        if(!(state == State.CONNECTING || state == State.CONNECTED))
            throw new IllegalStateException("channel is disconnected");
    }




    /**
     * Disconnects and closes the channel. This method does the following things
     * <ol>
     * <li>Calls <code>this.disconnect</code> if the disconnect parameter is true
     * <li>Calls <code>ProtocolStack.stop</code> on the protocol stack
     * <li>Calls <code>ProtocolStack.destroy</code> on the protocol stack
     * <li>Sets the channel closed and channel connected flags to true and false
     * <li>Notifies any channel listener of the channel close operation
     * </ol>
     */
    protected void _close(boolean disconnect) {
        Address old_addr=local_addr;
        if(state == State.CLOSED)
            return;

        if(disconnect)
            disconnect();                     // leave group if connected
        stopStack(true, true);
        state=State.CLOSED;
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
                log.error(Util.getMessage("StackDestroyFailure"), e);
            }

            TP transport=prot_stack.getTransport();
            if(transport != null)
                transport.unregisterProbeHandler(probe_handler);
        }
    }



    public boolean flushSupported() {
        return flush_supported;
    }

    public void startFlush(boolean automatic_resume) throws Exception {
        if(!flushSupported())
            throw new IllegalStateException("Flush is not supported, add pbcast.FLUSH protocol to your configuration");
        
        try {
            down(new Event(Event.SUSPEND));            
        } catch (Exception e) {
            throw new Exception("Flush failed", e.getCause());
        } finally {
            if (automatic_resume)
                stopFlush();
        }
    }

    public void startFlush(List<Address> flushParticipants, boolean automatic_resume) throws Exception {
        if (!flushSupported())
            throw new IllegalStateException("Flush is not supported, add pbcast.FLUSH protocol to your configuration");
        View v = getView();
        boolean validParticipants = v != null && v.getMembers().containsAll(flushParticipants);
        if (!validParticipants)
            throw new IllegalArgumentException("Current view " + v
                        + " does not contain all flush participants " + flushParticipants);
        try {
            down(new Event(Event.SUSPEND, flushParticipants));
        } catch (Exception e) {
            throw new Exception("Flush failed", e.getCause());
        } finally {
            if (automatic_resume)
                stopFlush(flushParticipants);
        }          
    }

    public void stopFlush() {
        if(!flushSupported())
            throw new IllegalStateException("Flush is not supported, add pbcast.FLUSH protocol to your configuration");
        down(new Event(Event.RESUME));
    }

    public void stopFlush(List<Address> flushParticipants) {
        if(!flushSupported())
            throw new IllegalStateException("Flush is not supported, add pbcast.FLUSH protocol to your configuration");
        down(new Event(Event.RESUME, flushParticipants));
    }
    

    Address determineCoordinator() {
        List<Address> mbrs=my_view != null? my_view.getMembers() : null;
        if(mbrs == null)
            return null;
        if(!mbrs.isEmpty())
            return mbrs.iterator().next();
        return null;
    }

    protected TimeScheduler getTimer() {
        if(prot_stack != null) {
            TP transport=prot_stack.getTransport();
            if(transport != null)
                return transport.getTimer();
        }
        return null;
    }

    /* ------------------------------- End of Private Methods ---------------------------------- */

    class MyProbeHandler implements DiagnosticsHandler.ProbeHandler {

        public Map<String, String> handleProbe(String... keys) {
            Map<String, String> map=new TreeMap<>();
            for(String key: keys) {
                if(key.startsWith("jmx")) {
                    handleJmx(map, key);
                    continue;
                }
                if(key.startsWith("reset-stats")) {
                    resetAllStats();
                    continue;
                }
                if(key.startsWith("invoke") || key.startsWith("op")) {
                    int index=key.indexOf("=");
                    if(index != -1) {
                        try {
                            handleOperation(map, key.substring(index+1));
                        }
                        catch(Throwable throwable) {
                            log.error(Util.getMessage("OperationInvocationFailure"), key.substring(index+1), throwable);
                        }
                    }
                }
            }
            return map;
        }

        public String[] supportedKeys() {
            return new String[]{"reset-stats", "jmx", "op=<operation>[<args>]"};
        }

        protected void resetAllStats() {
            List<Protocol> prots=getProtocolStack().getProtocols();
            for(Protocol prot: prots)
                prot.resetStatistics();
            resetStats();
        }

        protected void handleJmx(Map<String, String> map, String input) {
            Map<String, Object> tmp_stats;
            int index=input.indexOf("=");
            if(index > -1) {
                List<String> list=null;
                String protocol_name=input.substring(index +1);
                index=protocol_name.indexOf(".");
                if(index > -1) {
                    String rest=protocol_name;
                    protocol_name=protocol_name.substring(0, index);
                    String attrs=rest.substring(index +1); // e.g. "num_sent,msgs,num_received_msgs"
                    list=Util.parseStringList(attrs, ",");

                    // check if there are any attribute-sets in the list
                    for(Iterator<String> it=list.iterator(); it.hasNext();) {
                        String tmp=it.next();
                        index=tmp.indexOf("=");
                        if(index != -1) {
                            String attrname=tmp.substring(0, index);
                            String attrvalue=tmp.substring(index+1);
                            Protocol prot=prot_stack.findProtocol(protocol_name);
                            Field field=prot != null? Util.getField(prot.getClass(), attrname) : null;
                            if(field != null) {
                                Object value=MethodCall.convert(attrvalue,field.getType());
                                  if(value != null)
                                      prot.setValue(attrname, value);
                            }
                            else {
                                // try to find a setter for X, e.g. x(type-of-x) or setX(type-of-x)
                                ResourceDMBean.Accessor setter=ResourceDMBean.findSetter(prot, attrname);  // Util.getSetter(prot.getClass(), attrname);
                                if(setter != null) {
                                    try {
                                        Class<?> type=setter instanceof ResourceDMBean.FieldAccessor?
                                          ((ResourceDMBean.FieldAccessor)setter).getField().getType() :
                                          setter instanceof ResourceDMBean.MethodAccessor?
                                            ((ResourceDMBean.MethodAccessor)setter).getMethod().getParameterTypes()[0].getClass() : null;
                                        Object converted_value=MethodCall.convert(attrvalue, type);
                                        setter.invoke(converted_value);
                                    }
                                    catch(Exception e) {
                                        log.error("unable to invoke %s() on %s: %s", setter, protocol_name, e);
                                    }
                                }
                                else
                                    log.warn(Util.getMessage("FieldNotFound"), attrname, protocol_name);
                            }

                            it.remove();
                        }
                    }
                }
                tmp_stats=dumpStats(protocol_name, list);
                if(tmp_stats != null) {
                    for(Map.Entry<String,Object> entry : tmp_stats.entrySet()) {
                        Map<String,Object> tmp_map=(Map<String,Object>)entry.getValue();
                        String key=entry.getKey();
                        map.put(key, tmp_map != null? tmp_map.toString() : null);
                    }
                }
            }
            else {
                tmp_stats=dumpStats();
                if(tmp_stats != null) {
                    for(Map.Entry<String,Object> entry : tmp_stats.entrySet()) {
                        Map<String,Object> tmp_map=(Map<String,Object>)entry.getValue();
                        String key=entry.getKey();
                        map.put(key, tmp_map != null? tmp_map.toString() : null);
                    }
                }
            }
        }

        /**
         * Invokes an operation and puts the return value into map
         * @param map
         * @param operation Protocol.OperationName[args], e.g. STABLE.foo[arg1 arg2 arg3]
         */
        protected void handleOperation(Map<String, String> map, String operation) throws Exception {
            int index=operation.indexOf(".");
            if(index == -1)
                throw new IllegalArgumentException("operation " + operation + " is missing the protocol name");
            String prot_name=operation.substring(0, index);
            Protocol prot=prot_stack.findProtocol(prot_name);
            if(prot == null)
                return; // less drastic than throwing an exception...


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
            if(method == null) {
                log.warn(Util.getMessage("MethodNotFound"), local_addr, prot.getClass().getSimpleName(), method_name);
                return;
            }
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
