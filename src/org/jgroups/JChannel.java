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
import org.jgroups.stack.*;
import org.jgroups.util.*;
import org.jgroups.util.UUID;
import org.w3c.dom.Element;

import java.io.*;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.DatagramSocket;
import java.net.ServerSocket;
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
    public static final String DEFAULT_PROTOCOL_STACK="udp.xml";

    /*the address of this JChannel instance*/
    protected Address local_addr=null;

    protected AddressGenerator address_generator=null;

    protected String name=null;

    /*the channel (also know as group) name*/
    private String cluster_name=null;

    /*the latest view of the group membership*/
    private View my_view=null;

    /*the protocol stack, used to send and receive messages from the protocol stack*/
    private ProtocolStack prot_stack=null;

    private final Promise<StateTransferResult> state_promise=new Promise<StateTransferResult>();

    /** channel connected flag */
    protected volatile boolean connected=false;

    /** channel closed flag*/
    protected volatile boolean closed=false;      // close() has been called, channel is unusable

    /** True if a state transfer protocol is available, false otherwise */
    private boolean state_transfer_supported=false; // set by CONFIG event from STATE_TRANSFER protocol

    /** True if a flush protocol is available, false otherwise */
    private volatile boolean flush_supported=false; // set by CONFIG event from FLUSH protocol


    protected final ConcurrentMap<String,Object> config=Util.createConcurrentMap(16);

    protected final Log log=LogFactory.getLog(JChannel.class);

    /** Collect statistics */
    @ManagedAttribute(description="Collect channel statistics",writable=true)
    protected boolean stats=true;

    protected long sent_msgs=0, received_msgs=0, sent_bytes=0, received_bytes=0;

    private final DiagnosticsHandler.ProbeHandler probe_handler=new MyProbeHandler();



    /**
     * Creates a JChannel without a protocol stack; used for programmatic creation of channel and protocol stack
     * @param create_protocol_stack If true, the default configuration will be used. If false, no protocol stack
     *        will be created
     * @param create_protocol_stack Creates the default stack if true, or no stack if false
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


    @ManagedOperation(description="Connects the channel to a group")
    public synchronized void connect(String cluster_name) throws Exception {
    	connect(cluster_name,true);
    }

    /**
     * Connects the channel to a group.
     * @see JChannel#connect(String)
     */
    @ManagedOperation(description="Connects the channel to a group")
    protected synchronized void connect(String cluster_name, boolean useFlushIfPresent) throws Exception {
        if(connected) {
            if(log.isTraceEnabled())
                log.trace("already connected to " + cluster_name);
            return;
        }

        setAddress();
        startStack(cluster_name);

        if(cluster_name != null) { // only connect if we are not a unicast channel

            Event connect_event;
            if(useFlushIfPresent) {
                connect_event=new Event(Event.CONNECT_USE_FLUSH, cluster_name);
            }
            else {
                connect_event=new Event(Event.CONNECT, cluster_name);
            }

            // waits forever until connected (or channel is closed)
            Object res=down(connect_event);
            if(res != null && res instanceof Exception) {
                // the JOIN was rejected by the coordinator
                stopStack(true, false);
                init();
                throw new Exception("connect() failed", (Throwable)res);
            }
        }
        connected=true;
        notifyChannelConnected(this);
    }

    public synchronized void connect(String cluster_name, Address target, long timeout) throws Exception {
    	connect(cluster_name, target, timeout,true);
    }

    
    /**
     * Connects this channel to a group and gets a state from a specified state provider.<p/>
     * This method invokes <code>connect()<code> and then <code>getState<code>.<p/>
     * If the FLUSH protocol is in the channel's stack definition, only one flush round is executed for both connecting and
     * fetching the state rather than two flushes if we invoke <code>connect<code> and <code>getState<code> in succession.
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

        if(connected) {
            if(log.isTraceEnabled()) log.trace("already connected to " + this.cluster_name);
            return;
        }

        setAddress();
        startStack(cluster_name);

        boolean canFetchState=false;

        if(cluster_name == null) // only connect if we are not a unicast channel
            return;

        try {
            Event connect_event=useFlushIfPresent? new Event(Event.CONNECT_WITH_STATE_TRANSFER_USE_FLUSH, cluster_name)
                                                 : new Event(Event.CONNECT_WITH_STATE_TRANSFER, cluster_name);

            Object res=down(connect_event); // waits forever until connected (or channel is closed)
            if(res instanceof Exception) {
                stopStack(true, false);
                init();
                throw new Exception("connect() failed", (Throwable)res);
            }

            connected=true;
            notifyChannelConnected(this);
            canFetchState=getView() != null && getView().size() > 1;

            // if I am not the only member in cluster then
            if(canFetchState)
                getState(target, timeout, false); // fetch state from target
        }
        finally {
            if(flushSupported() && useFlushIfPresent) {
                if(canFetchState || !connected) // stopFlush if we fetched the state or failed to connect...
                    stopFlush();
            }
        }
    }


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


    @ManagedOperation(description="Disconnects and destroys the channel")
    public synchronized void close() {
        _close(true); // by default disconnect before closing channel and close mq
    }




    @ManagedAttribute public boolean isOpen() {
        return !closed;
    }


    @ManagedAttribute public boolean isConnected() {
        return connected;
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
        Map<String,Long> retval=new HashMap<String,Long>();
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
        if(stats) {
            sent_msgs++;
            sent_bytes+=msg.getLength();
        }

        down(new Event(Event.MSG, msg));
    }


    public void send(Address dst, Object obj) throws Exception {
        send(new Message(dst, null, obj));
    }

    public void send(Address dst, byte[] buf) throws Exception {
        send(new Message(dst, null, buf));
    }

    public void send(Address dst, byte[] buf, int offset, int length) throws Exception {
        send(new Message(dst, null, buf, offset, length));
    }


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


    public Address getAddress() {
        return closed ? null : local_addr;
    }

    @ManagedAttribute(name="Address")
    public String getAddressAsString() {
        return local_addr != null? local_addr.toString() : "n/a";
    }

    @ManagedAttribute(name="Address (UUID)")
    public String getAddressAsUUID() {
        return local_addr instanceof UUID? ((UUID)local_addr).toStringLong() : null;
    }

    public String getName() {
        return name;
    }

    public String getName(Address member) {
        return member != null? UUID.get(member) : null;
    }

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

    @ManagedAttribute(description="Returns cluster name this channel is connected to")
    public String getClusterName() {
        return closed ? null : !connected ? null : cluster_name;
    }

    /**
     * Returns the current {@link AddressGenerator}, or null if none is set
     * @return
     * @since 2.12
     */
    public AddressGenerator getAddressGenerator() {
        return address_generator;
    }

    /**
     * Sets the new {@link AddressGenerator}. New addresses will be generated using the new generator. This
     * should <em>not</em> be done while a channel is connected, but before connecting.
     * @param address_generator
     * @since 2.12
     */
    public void setAddressGenerator(AddressGenerator address_generator) {
        this.address_generator=address_generator;
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
    

    protected void getState(Address target, long timeout, Callable<Boolean> flushInvoker) throws Exception {
        checkClosedOrNotConnected();
        if(!state_transfer_supported)
            throw new IllegalStateException("fetching state will fail as state transfer is not supported. "
                                              + "Add one of the state transfer protocols to your configuration");

        if(target == null)
            target=determineCoordinator();
        if(target != null && local_addr != null && target.equals(local_addr)) {
            if(log.isTraceEnabled())
                log.trace("cannot get state from myself (" + target + "): probably the first member");
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
        down(new Event(Event.GET_STATE, state_info));
        StateTransferResult result=state_promise.getResult(state_info.timeout);

        if(initiateFlush)
            stopFlush();

        if(result != null && result.hasException())
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
                    my_view=new View(tmp.getVid(), tmp.getMembers());
                else
                    my_view=tmp;

                // Bela&Vladimir Oct 27th,2006 (JGroups 2.4): we need to set connected=true because a client can
                // call channel.getView() in viewAccepted() callback invoked on this thread (see Event.VIEW_CHANGE handling below)

                // not good: we are only connected when we returned from connect() - bela June 22 2007
                // Changed: when a channel gets a view of which it is a member then it should be
                // connected even if connect() hasn't returned yet ! (bela Noc 2010)
                if(connected == false)
                    connected=true;
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

                byte[] state=result.getBuffer();
                if(receiver != null) {
                    try {
                        ByteArrayInputStream input=new ByteArrayInputStream(state);
                        receiver.setState(input);
                        state_promise.setResult(new StateTransferResult());
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


    /**
     * Sends a message through the protocol stack if the stack is available
     * @param evt the message to send down, encapsulated in an event
     */
    public Object down(Event evt) {
        if(evt == null) return null;
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

        synchronized(Channel.class) {
            prot_stack=new ProtocolStack(this);
            prot_stack.setup(configs); // Setup protocol stack (creates protocol, calls init() on them)
        }
    }

    protected final void init(JChannel ch) throws Exception {
        if(ch == null)
            throw new IllegalArgumentException("channel is null");
        synchronized(JChannel.class) {
            prot_stack=new ProtocolStack(this);
            prot_stack.setup(ch.getProtocolStack()); // Setup protocol stack (creates protocol, calls init() on them)
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


    private void startStack(String cluster_name) throws Exception {
        /*make sure the channel is not closed*/
        checkClosed();

        /*make sure we have a valid channel name*/
        if(cluster_name == null) {
            if(log.isDebugEnabled()) log.debug("cluster_name is null, assuming unicast channel");
        }
        else
            this.cluster_name=cluster_name;

        if(socket_factory != null)
            prot_stack.getTopProtocol().setSocketFactory(socket_factory);

        prot_stack.startStack(cluster_name, local_addr); // calls start() in all protocols, from top to bottom

        /*create a temporary view, assume this channel is the only member and is the coordinator*/
        List<Address> t=new ArrayList<Address>(1);
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
        local_addr=address_generator != null? address_generator.generateAddress() : UUID.randomUUID();
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
    protected void checkClosed() {
        if(closed)
            throw new IllegalStateException("channel is closed");
    }


    protected void checkClosedOrNotConnected() {
        if(closed)
            throw new IllegalStateException("channel is closed");
        if(!connected)
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
        if(closed)
            return;

        if(disconnect)
            disconnect();                     // leave group if connected
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

    private TimeScheduler getTimer() {
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
            Map<String, String> map=new HashMap<String, String>(2);
            for(String key: keys) {
                if(key.startsWith("jmx")) {
                    handleJmx(map, key);
                    continue;
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
            PhysicalAddress physical_addr=(PhysicalAddress)down(new Event(Event.GET_PHYSICAL_ADDRESS, local_addr));
            if(physical_addr != null)
                map.put("physical_addr", physical_addr.toString());
            map.put("cluster", getClusterName());
            return map;
        }

        public String[] supportedKeys() {
            return new String[]{"jmx", "invoke=<operation>[<args>]", "\nop=<operation>[<args>]", "socks"};
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
                            Field field=Util.getField(prot.getClass(), attrname);
                            if(field != null) {
                                Object value=MethodCall.convert(attrvalue, field.getType());
                                  if(value != null)
                                      prot.setValue(attrname, value);
                            }
                            else {
                                if(log.isWarnEnabled())
                                    log.warn("Field \"" + attrname + "\" not found in protocol " + protocol_name);
                            }
                            it.remove();
                        }
                    }
                }
                tmp_stats=dumpStats(protocol_name, list);
            }
            else
                tmp_stats=dumpStats();

            map.put("jmx", tmp_stats != null? Util.mapToString(tmp_stats) : "null");
        }

        /**
         * Invokes an operation and puts the return value into map
         * @param map
         * @param operation Protocol.OperationName[args], e.g. STABLE.foo[arg1 arg2 arg3]
         */
        private void handleOperation(Map<String, String> map, String operation) throws Exception {
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
    }

  
}
