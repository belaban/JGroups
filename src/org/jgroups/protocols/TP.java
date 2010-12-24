package org.jgroups.protocols;


import org.jgroups.*;
import org.jgroups.annotations.*;
import org.jgroups.blocks.LazyRemovalCache;
import org.jgroups.conf.PropertyConverters;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.stack.Protocol;
import org.jgroups.util.*;
import org.jgroups.util.ThreadFactory;
import org.jgroups.util.UUID;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.*;
import java.text.NumberFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Generic transport - specific implementations should extend this abstract class.
 * Features which are provided to the subclasses include
 * <ul>
 * <li>version checking
 * <li>marshalling and unmarshalling
 * <li>message bundling (handling single messages, and message lists)
 * <li>incoming packet handler
 * <li>loopback
 * </ul>
 * A subclass has to override
 * <ul>
 * <li>{@link #sendMulticast(byte[], int, int)}
 * <li>{@link #sendUnicast(org.jgroups.PhysicalAddress, byte[], int, int)}
 * <li>{@link #init()}
 * <li>{@link #start()}: subclasses <em>must</em> call super.start() <em>after</em> they initialize themselves
 * (e.g., created their sockets).
 * <li>{@link #stop()}: subclasses <em>must</em> call super.stop() after they deinitialized themselves
 * <li>{@link #destroy()}
 * </ul>
 * The create() or start() method has to create a local address.<br>
 * The {@link #receive(Address, byte[], int, int)} method must
 * be called by subclasses when a unicast or multicast message has been received.
 * @author Bela Ban
 */
@MBean(description="Transport protocol")
@DeprecatedProperty(names={"bind_to_all_interfaces", "use_incoming_packet_handler", "use_outgoing_packet_handler",
        "use_concurrent_stack", "prevent_port_reuse", "persistent_ports", "pm_expiry_time", "persistent_ports_file",
        "start_port", "end_port", "use_local_host", "marshaller_pool_size", "num_timer_threads", "timer.num_threads"})
public abstract class TP extends Protocol {

    protected static final byte LIST=1; // we have a list of messages rather than a single message when set
    protected static final byte MULTICAST=2; // message is a multicast (versus a unicast) message when set
    protected static final byte OOB=4; // message has OOB flag set (Message.OOB)

    protected static final boolean can_bind_to_mcast_addr; // are we running on Linux ?

    protected static NumberFormat f;

    static {
        can_bind_to_mcast_addr=Util.checkForLinux() || Util.checkForSolaris() || Util.checkForHp();
        f=NumberFormat.getNumberInstance();
        f.setGroupingUsed(false);
        f.setMaximumFractionDigits(2);
    }

    /* ------------------------------------------ JMX and Properties  ------------------------------------------ */



    @Property(name="bind_addr",
              description="The bind address which should be used by this transport. The following special values " +
                      "are also recognized: GLOBAL, SITE_LOCAL, LINK_LOCAL and NON_LOOPBACK",
              defaultValueIPv4=Global.NON_LOOPBACK_ADDRESS, defaultValueIPv6=Global.NON_LOOPBACK_ADDRESS,
              systemProperty={Global.BIND_ADDR, Global.BIND_ADDR_OLD},writable=false)
    protected InetAddress bind_addr=null;

    @Property(name="bind_interface", converter=PropertyConverters.BindInterface.class,
    		description="The interface (NIC) which should be used by this transport", dependsUpon="bind_addr",
            exposeAsManagedAttribute=false)
    protected String bind_interface_str=null;
    
    @Property(description="If true, the transport should use all available interfaces to receive multicast messages")
    protected boolean receive_on_all_interfaces=false;

    /**
     * List<NetworkInterface> of interfaces to receive multicasts on. The multicast receive socket will listen
     * on all of these interfaces. This is a comma-separated list of IP addresses or interface names. E.g.
     * "192.168.5.1,eth1,127.0.0.1". Duplicates are discarded; we only bind to
     * an interface once. If this property is set, it overrides receive_on_all_interfaces.
     */
    @Property(converter=PropertyConverters.NetworkInterfaceList.class,
              description="Comma delimited list of interfaces (IP addresses or interface names) to receive multicasts on")
    protected List<NetworkInterface> receive_interfaces=null;

    @Property(description="Max number of elements in the logical address cache before eviction starts")
    protected int logical_addr_cache_max_size=20;

    @Property(description="Time (in ms) after which entries in the logical address cache marked as removable are removed")
    protected long logical_addr_cache_expiration=120000;


    /** The port to which the transport binds. 0 means to bind to any (ephemeral) port */
    @Property(description="The port to which the transport binds. Default of 0 binds to any (ephemeral) port",writable=false)
    protected int bind_port=0;

    @Property(description="The range of valid ports, from bind_port to end_port. Infinite if 0")
    protected int port_range=50; // 27-6-2003 bgooren, Only try one port by default

  
    /**
     * If true, messages sent to self are treated specially: unicast messages are looped back immediately,
     * multicast messages get a local copy first and - when the real copy arrives - it will be discarded. Useful for
     * Window media (non)sense
     */
    @Property(description="Messages to self are looped back immediately if true")
    protected boolean loopback=true;

    /**
     * Discard packets with a different version. Usually minor version differences are okay. Setting this property
     * to true means that we expect the exact same version on all incoming packets
     */
    @Property(description="Discard packets with a different version if true. Default is false")
    protected boolean discard_incompatible_packets=false;


    @Property(description="Thread naming pattern for threads in this channel. Default is cl")
    protected String thread_naming_pattern="cl";

    @Property(name="oob_thread_pool.enabled",description="Switch for enabling thread pool for OOB messages. " +
            "Default=true",writable=false)
    protected boolean oob_thread_pool_enabled=true;

    protected int oob_thread_pool_min_threads=2;

    protected int oob_thread_pool_max_threads=10;

    protected long oob_thread_pool_keep_alive_time=30000;

    @Property(name="oob_thread_pool.queue_enabled", description="Use queue to enqueue incoming OOB messages")
    protected boolean oob_thread_pool_queue_enabled=true;

    @Property(name="oob_thread_pool.queue_max_size",description="Maximum queue size for incoming OOB messages. Default is 500")
    protected int oob_thread_pool_queue_max_size=500;

    @Property(name="oob_thread_pool.rejection_policy",
              description="Thread rejection policy. Possible values are Abort, Discard, DiscardOldest and Run. Default is Discard")
    String oob_thread_pool_rejection_policy="discard";

    protected int thread_pool_min_threads=2;

    protected int thread_pool_max_threads=10;

    protected long thread_pool_keep_alive_time=30000;

    @Property(name="thread_pool.enabled",description="Switch for enabling thread pool for regular messages. Default true")
    protected boolean thread_pool_enabled=true;

    @Property(name="thread_pool.queue_enabled", description="Use queue to enqueue incoming regular messages. Default is true")
    protected boolean thread_pool_queue_enabled=true;


    @Property(name="thread_pool.queue_max_size", description="Maximum queue size for incoming OOB messages. Default is 500")
    protected int thread_pool_queue_max_size=500;

    @Property(name="thread_pool.rejection_policy",
              description="Thread rejection policy. Possible values are Abort, Discard, DiscardOldest and Run. Default is Discard")
    protected String thread_pool_rejection_policy="Discard";

    @Property(description="Type of timer to be used. Valid values are \"old\" (DefaultTimeScheduler, used up to 2.10), " +
            "\"new\" (TimeScheduler2) and \"wheel\". Note that this property might disappear " +
            "in future releases, if one of the 3 timers is chosen as default timer")
    protected String timer_type="new";

    protected int timer_min_threads=4;

    protected int timer_max_threads=10;

    protected long timer_keep_alive_time=5000;

    @Property(name="timer.queue_max_size", description="Max number of elements on a timer queue")
    protected int timer_queue_max_size=500;

    // hashed timing wheel specific props
    @Property(name="timer.wheel_size",
              description="Number of ticks in the HashedTimingWheel timer. Only applicable if timer_type is \"wheel\"")
    protected int wheel_size=200;

    @Property(name="timer.tick_time",
              description="Tick duration in the HashedTimingWheel timer. Only applicable if timer_type is \"wheel\"")
    protected long tick_time=50L;

    @Property(description="Enable bundling of smaller messages into bigger ones. Default is true")
    protected boolean enable_bundling=true;

    /** Enable bundling for unicast messages. Ignored if enable_bundling is off */
    @Property(description="Enable bundling of smaller messages into bigger ones for unicast messages. Default is false")
    protected boolean enable_unicast_bundling=false;

    @Property(description="Switch to enable diagnostic probing. Default is true")
    protected boolean enable_diagnostics=true;

    @Property(description="Address for diagnostic probing. Default is 224.0.75.75", 
    		defaultValueIPv4="224.0.75.75",defaultValueIPv6="ff0e::0:75:75")
    protected InetAddress diagnostics_addr=null;

    @Property(description="Port for diagnostic probing. Default is 7500")
    protected int diagnostics_port=7500;

    @Property(description="If assigned enable this transport to be a singleton (shared) transport")
    protected String singleton_name=null;

    /** Whether or not warnings about messages from different groups are logged - private flag, not for common use */
    @Property(description="whether or not warnings about messages from different groups are logged")
    protected boolean log_discard_msgs=true;




    /**
     * Maximum number of bytes for messages to be queued until they are sent.
     * This value needs to be smaller than the largest datagram packet size in case of UDP
     */
    protected int max_bundle_size=64000;

    /**
     * Max number of milliseconds until queued messages are sent. Messages are sent when max_bundle_size
     * or max_bundle_timeout has been exceeded (whichever occurs faster)
     */
    protected long max_bundle_timeout=20;

    @Property(description="The type of bundler used. Has to be \"old\" (default) or \"new\"")
    protected String bundler_type="new";

    @Experimental
    @Property(description="The max number of elements in a bundler if the bundler supports size limitations")
    protected int bundler_capacity=20000;


    @Property(name="max_bundle_size", description="Maximum number of bytes for messages to be queued until they are sent")
    public void setMaxBundleSize(int size) {
        if(size <= 0) {
            throw new IllegalArgumentException("max_bundle_size (" + size + ") is <= 0");
        }
        max_bundle_size=size;
    }

    public long getMaxBundleTimeout() {return max_bundle_timeout;}
    

    @Property(name="max_bundle_timeout", description="Max number of milliseconds until queued messages are sent")
    public void setMaxBundleTimeout(long timeout) {
        if(timeout <= 0) {
            throw new IllegalArgumentException("max_bundle_timeout of " + timeout + " is invalid");
        }
        max_bundle_timeout=timeout;
    }

    public int getMaxBundleSize() {return max_bundle_size;}

    @ManagedAttribute public int getBundlerBufferSize() {
        if(bundler instanceof TransferQueueBundler)
            return ((TransferQueueBundler)bundler).getBufferSize();
        return 0;
    }

    @Property(name="oob_thread_pool.keep_alive_time", description="Timeout in ms to remove idle threads from the OOB pool")
    public void setOOBThreadPoolKeepAliveTime(long time) {
        oob_thread_pool_keep_alive_time=time;
        if(oob_thread_pool instanceof ThreadPoolExecutor)
            ((ThreadPoolExecutor)oob_thread_pool).setKeepAliveTime(time, TimeUnit.MILLISECONDS);
    }

    public long getOOBThreadPoolKeepAliveTime() {return oob_thread_pool_keep_alive_time;}


    @Property(name="oob_thread_pool.min_threads",description="Minimum thread pool size for the OOB thread pool")
    public void setOOBThreadPoolMinThreads(int size) {
        oob_thread_pool_min_threads=size;
        if(oob_thread_pool instanceof ThreadPoolExecutor)
            ((ThreadPoolExecutor)oob_thread_pool).setCorePoolSize(size);
    }

    public int getOOBThreadPoolMinThreads() {return oob_thread_pool_min_threads;}

    @Property(name="oob_thread_pool.max_threads",description="Max thread pool size for the OOB thread pool")
    public void setOOBThreadPoolMaxThreads(int size) {
        oob_thread_pool_max_threads=size;
        if(oob_thread_pool instanceof ThreadPoolExecutor)
            ((ThreadPoolExecutor)oob_thread_pool).setMaximumPoolSize(size);
    }

    public int getOOBThreadPoolMaxThreads() {return oob_thread_pool_max_threads;}


    @Property(name="thread_pool.min_threads",description="Minimum thread pool size for the regular thread pool")
    public void setThreadPoolMinThreads(int size) {
        thread_pool_min_threads=size;
        if(thread_pool instanceof ThreadPoolExecutor)
            ((ThreadPoolExecutor)thread_pool).setCorePoolSize(size);
    }

    public int getThreadPoolMinThreads() {return thread_pool_min_threads;}


    @Property(name="thread_pool.max_threads",description="Maximum thread pool size for the regular thread pool")
    public void setThreadPoolMaxThreads(int size) {
        thread_pool_max_threads=size;
        if(thread_pool instanceof ThreadPoolExecutor)
            ((ThreadPoolExecutor)thread_pool).setMaximumPoolSize(size);
    }

    public int getThreadPoolMaxThreads() {return thread_pool_max_threads;}


    @Property(name="thread_pool.keep_alive_time",description="Timeout in milliseconds to remove idle thread from regular pool")
    public void setThreadPoolKeepAliveTime(long time) {
        thread_pool_keep_alive_time=time;
        if(thread_pool instanceof ThreadPoolExecutor)
            ((ThreadPoolExecutor)thread_pool).setKeepAliveTime(time, TimeUnit.MILLISECONDS);
    }

    public long getThreadPoolKeepAliveTime() {return thread_pool_keep_alive_time;}



    @Property(name="timer.min_threads",description="Minimum thread pool size for the timer thread pool")
    public void setTimerMinThreads(int size) {
        timer_min_threads=size;
        if(timer != null)
            timer.setMinThreads(size);
    }

    public int getTimerMinThreads() {return timer_min_threads;}


    @Property(name="timer.max_threads",description="Max thread pool size for the timer thread pool")
    public void setTimerMaxThreads(int size) {
        timer_max_threads=size;
        if(timer != null)
            timer.setMaxThreads(size);
    }

    public int getTimerMaxThreads() {return timer_max_threads;}


    @Property(name="timer.keep_alive_time", description="Timeout in ms to remove idle threads from the timer pool")
    public void setTimerKeepAliveTime(long time) {
        timer_keep_alive_time=time;
        if(timer != null)
            timer.setKeepAliveTime(time);
    }

    public long getTimerKeepAliveTime() {return timer_keep_alive_time;}

    @ManagedAttribute
    public int getTimerQueueSize() {
        if(timer instanceof TimeScheduler2)
            return ((TimeScheduler2)timer).getQueueSize();
        return 0;
    }

    /* --------------------------------------------- JMX  ---------------------------------------------- */


    @ManagedAttribute(description="Number of messages sent")
    protected long num_msgs_sent=0;
    @ManagedAttribute(description="Number of messages received")
    protected long num_msgs_received=0;

    @ManagedAttribute(description="Number of bytes sent")
    protected long num_bytes_sent=0;

    @ManagedAttribute(description="Number of bytes received")
    protected long num_bytes_received=0;

    /** The name of the group to which this member is connected. With a shared transport, the channel name is
     * in TP.ProtocolAdapter (cluster_name), and this field is not used */
    @ManagedAttribute(description="Channel (cluster) name")
    protected String channel_name=null;

    @ManagedAttribute(description="Number of OOB messages received")
    protected long num_oob_msgs_received=0;

    @ManagedAttribute(description="Number of regular messages received")
    protected long num_incoming_msgs_received=0;

    @ManagedAttribute(description="Class of the timer implementation")
    public String getTimerClass() {
        return timer != null? timer.getClass().getSimpleName() : "null";
    }

    /* --------------------------------------------- Fields ------------------------------------------------------ */



    /** The address (host and port) of this member. Null by default when a shared transport is used */
    protected Address local_addr=null;

    /** The members of this group (updated when a member joins or leaves). With a shared transport,
     * members contains *all* members from all channels sitting on the shared transport */
    protected final Set<Address> members=new CopyOnWriteArraySet<Address>();

    protected ThreadGroup pool_thread_group=new ThreadGroup(Util.getGlobalThreadGroup(), "Thread Pools");

    /** Keeps track of connects and disconnects, in order to start and stop threads */
    protected int connect_count=0;

    //http://jira.jboss.org/jira/browse/JGRP-849
    protected final ReentrantLock connectLock = new ReentrantLock();
    

    // ================================== OOB thread pool ========================
    protected Executor oob_thread_pool;

    /** Factory which is used by oob_thread_pool */
    protected ThreadFactory oob_thread_factory=null;

    /** Used if oob_thread_pool is a ThreadPoolExecutor and oob_thread_pool_queue_enabled is true */
    protected BlockingQueue<Runnable> oob_thread_pool_queue=null;


    // ================================== Regular thread pool ======================

    /** The thread pool which handles unmarshalling, version checks and dispatching of regular messages */
    protected Executor thread_pool;

    /** Factory which is used by oob_thread_pool */
    protected ThreadFactory default_thread_factory=null;

    /** Used if thread_pool is a ThreadPoolExecutor and thread_pool_queue_enabled is true */
    protected BlockingQueue<Runnable> thread_pool_queue=null;

    // ================================== Timer thread pool  =========================
    protected TimeScheduler timer=null;

    protected ThreadFactory timer_thread_factory;

    // ================================ Default thread factory ========================
    /** Used by all threads created by JGroups outside of the thread pools */
    protected ThreadFactory global_thread_factory=null;

    // ================================= Default SocketFactory ========================
    protected SocketFactory socket_factory=new DefaultSocketFactory();

    protected Bundler bundler=null;

    protected DiagnosticsHandler diag_handler=null;
    protected final List<ProbeHandler> preregistered_probe_handlers=new LinkedList<ProbeHandler>();

    /**
     * If singleton_name is enabled, this map is used to de-multiplex incoming messages according to their cluster
     * names (attached to the message by the transport anyway). The values are the next protocols above the
     * transports.
     */
    protected final ConcurrentMap<String,Protocol> up_prots=Util.createConcurrentMap(16, 0.75f, 16);

    /** The header including the cluster name, sent with each message. Not used with a shared transport (instead
     * TP.ProtocolAdapter attaches the header to the message */
    protected TpHeader header;


    /**
     * Cache which maintains mappings between logical and physical addresses. When sending a message to a logical
     * address,  we look up the physical address from logical_addr_cache and send the message to the physical address
     * <br/>
     * The keys are logical addresses, the values physical addresses
     */
    protected LazyRemovalCache<Address,PhysicalAddress> logical_addr_cache=null;

    Future<?> logical_addr_cache_reaper=null;

    protected static final LazyRemovalCache.Printable<Address,PhysicalAddress> print_function=new LazyRemovalCache.Printable<Address,PhysicalAddress>() {
        public java.lang.String print(final Address logical_addr, final PhysicalAddress physical_addr) {
            StringBuilder sb=new StringBuilder();
            String tmp_logical_name=UUID.get(logical_addr);
            if(tmp_logical_name != null)
                sb.append(tmp_logical_name).append(": ");
            sb.append(((UUID)logical_addr).toStringLong()).append(": ").append(physical_addr).append("\n");
            return sb.toString();
        }
    };

    /** Cache keeping track of WHO_HAS requests for physical addresses (given a logical address) and expiring
     * them after 5000ms */
    protected AgeOutCache<Address> who_has_cache=null;


    /**
     * Creates the TP protocol, and initializes the state variables, does
     * however not start any sockets or threads.
     */
    protected TP() {
    }

    /** Whether or not hardware multicasting is supported */
    public abstract boolean supportsMulticasting();

    public String toString() {
        if(!isSingleton())
            return local_addr != null? name + "(local address: " + local_addr + ')' : name;
        else
            return name + " (singleton=" + singleton_name + ")";
    }

    public void resetStats() {
        num_msgs_sent=num_msgs_received=num_bytes_sent=num_bytes_received=0;
        num_oob_msgs_received=num_incoming_msgs_received=0;
    }

    public void registerProbeHandler(ProbeHandler handler) {
        if(diag_handler != null)
            diag_handler.registerProbeHandler(handler);
        else
            preregistered_probe_handlers.add(handler);
    }

    public void unregisterProbeHandler(ProbeHandler handler) {
        if(diag_handler != null)
            diag_handler.unregisterProbeHandler(handler);
    }

    public ThreadGroup getPoolThreadGroup() {
        return pool_thread_group;
    }

    public void setThreadPoolQueueEnabled(boolean flag) {thread_pool_queue_enabled=flag;}


    public Executor getDefaultThreadPool() {
        return thread_pool;
    }

    public void setDefaultThreadPool(Executor thread_pool) {
        if(this.thread_pool != null)
            shutdownThreadPool(this.thread_pool);
        this.thread_pool=thread_pool;
    }

    public ThreadFactory getDefaultThreadPoolThreadFactory() {
        return default_thread_factory;
    }

    public void setDefaultThreadPoolThreadFactory(ThreadFactory factory) {
        default_thread_factory=factory;
        if(thread_pool instanceof ThreadPoolExecutor)
            ((ThreadPoolExecutor)thread_pool).setThreadFactory(factory);
    }

    public Executor getOOBThreadPool() {
        return oob_thread_pool;
    }

    public void setOOBThreadPool(Executor oob_thread_pool) {
        if(this.oob_thread_pool != null) {
            shutdownThreadPool(this.oob_thread_pool);
        }
        this.oob_thread_pool=oob_thread_pool;
    }

    public ThreadFactory getOOBThreadPoolThreadFactory() {
        return oob_thread_factory;
    }

    public void setOOBThreadPoolThreadFactory(ThreadFactory factory) {
        oob_thread_factory=factory;
        if(oob_thread_pool instanceof ThreadPoolExecutor)
            ((ThreadPoolExecutor)oob_thread_pool).setThreadFactory(factory);
    }

    public ThreadFactory getTimerThreadFactory() {
        return timer_thread_factory;
    }

    public void setTimerThreadFactory(ThreadFactory factory) {
        timer_thread_factory=factory;
        timer.setThreadFactory(factory);
    }

    public TimeScheduler getTimer() {return timer;}

    public ThreadFactory getThreadFactory() {
        return global_thread_factory;
    }

    public void setThreadFactory(ThreadFactory factory) {
        global_thread_factory=factory;
    }

    public SocketFactory getSocketFactory() {
        return socket_factory;
    }

    public void setSocketFactory(SocketFactory factory) {
        if(factory != null)
            socket_factory=factory;
    }

    /**
     * Names the current thread. Valid values are "pcl":
     * p: include the previous (original) name, e.g. "Incoming thread-1", "UDP ucast receiver"
     * c: include the cluster name, e.g. "MyCluster"
     * l: include the local address of the current member, e.g. "192.168.5.1:5678"
     */
    public String getThreadNamingPattern() {return thread_naming_pattern;}


    public long getNumMessagesSent()     {return num_msgs_sent;}
    public long getNumMessagesReceived() {return num_msgs_received;}
    public long getNumBytesSent()        {return num_bytes_sent;}
    public long getNumBytesReceived()    {return num_bytes_received;}
    public String getBindAddress() {return bind_addr != null? bind_addr.toString() : "null";}
    public void setBindAddress(String bind_addr) throws UnknownHostException {
        this.bind_addr=InetAddress.getByName(bind_addr);
    }
    public InetAddress getBindAddressAsInetAddress() {return bind_addr;}
    public int getBindPort() {return bind_port;}
    public void setBindPort(int port) {this.bind_port=port;}
    /** @deprecated Use {@link #isReceiveOnAllInterfaces()} instead */
    public boolean getBindToAllInterfaces() {return receive_on_all_interfaces;}
    public void setBindToAllInterfaces(boolean flag) {this.receive_on_all_interfaces=flag;}

    public boolean isReceiveOnAllInterfaces() {return receive_on_all_interfaces;}
    public List<NetworkInterface> getReceiveInterfaces() {return receive_interfaces;}
    /** @deprecated This property was removed in 2.7*/
    public static boolean isSendOnAllInterfaces() {return false;}
    /** @deprecated This property was removed in 2.7*/
    public static List<NetworkInterface> getSendInterfaces() {return null;}
    public boolean isDiscardIncompatiblePackets() {return discard_incompatible_packets;}
    public void setDiscardIncompatiblePackets(boolean flag) {discard_incompatible_packets=flag;}
    public boolean isEnableBundling() {return enable_bundling;}
    public void setEnableBundling(boolean flag) {enable_bundling=flag;}
    public boolean isEnableUnicastBundling() {return enable_unicast_bundling;}
    public void setEnableUnicastBundling(boolean enable_unicast_bundling) {this.enable_unicast_bundling=enable_unicast_bundling;}
    public void setPortRange(int range) {this.port_range=range;}
    public int getPortRange() {return port_range ;}

    /** @deprecated the concurrent stack is used by default */
    @Deprecated
    public void setUseConcurrentStack(boolean flag) {}

    public boolean isOOBThreadPoolEnabled() { return oob_thread_pool_enabled; }

    public boolean isDefaulThreadPoolEnabled() { return thread_pool_enabled; }

    public boolean isLoopback() {return loopback;}
    public void setLoopback(boolean b) {loopback=b;}

    /** @deprecated With the concurrent stack being the default, this property is ignored */
    public static boolean isUseIncomingPacketHandler() {return false;}

    public ConcurrentMap<String,Protocol> getUpProtocols() {return up_prots;}

    
    @ManagedAttribute(description="Current number of threads in the OOB thread pool")
    public int getOOBPoolSize() {
        return oob_thread_pool instanceof ThreadPoolExecutor? ((ThreadPoolExecutor)oob_thread_pool).getPoolSize() : 0;
    }

    public long getOOBMessages() {
        return num_oob_msgs_received;
    }

    @ManagedAttribute(description="Number of messages in the OOB thread pool's queue")
    public int getOOBQueueSize() {
        return oob_thread_pool_queue.size();
    }

    public int getOOBMaxQueueSize() {
        return oob_thread_pool_queue_max_size;
    }


    public void setOOBRejectionPolicy(String rejection_policy) {
        RejectedExecutionHandler handler=parseRejectionPolicy(rejection_policy);
        if(oob_thread_pool instanceof ThreadPoolExecutor)
            ((ThreadPoolExecutor)oob_thread_pool).setRejectedExecutionHandler(new ShutdownRejectedExecutionHandler(handler));
    }


    @ManagedAttribute(description="Current number of threads in the default thread pool")
    public int getRegularPoolSize() {
        return thread_pool instanceof ThreadPoolExecutor? ((ThreadPoolExecutor)thread_pool).getPoolSize() : 0;
    }

    public long getRegularMessages() {
        return num_incoming_msgs_received;
    }

    @ManagedAttribute(description="Number of messages in the default thread pool's queue")
    public int getRegularQueueSize() {
        return thread_pool_queue.size();
    }

    public int getRegularMaxQueueSize() {
        return thread_pool_queue_max_size;
    }

    @ManagedAttribute(name="TimerTasks",description="Number of timer tasks queued up for execution")
    public int getNumTimerTasks() {
        return timer != null? timer.size() : -1;
    }

    @ManagedOperation
    public String dumpTimerTasks() {
        return timer.dumpTimerTasks();
    }

    @ManagedAttribute(description="Number of threads currently in the pool")
    public int getTimerThreads() {
        return timer.getCurrentThreads();
    }

    public void setRegularRejectionPolicy(String rejection_policy) {
        RejectedExecutionHandler handler=parseRejectionPolicy(rejection_policy);
        if(thread_pool instanceof ThreadPoolExecutor)
            ((ThreadPoolExecutor)thread_pool).setRejectedExecutionHandler(new ShutdownRejectedExecutionHandler(handler));
    }

    public void setLogDiscardMessages(boolean flag) {
        log_discard_msgs=flag;
    }

    public boolean getLogDiscardMessages() {
        return log_discard_msgs;
    }

    @ManagedOperation(description="Dumps the contents of the logical address cache")
    public String printLogicalAddressCache() {
        return logical_addr_cache.printCache(print_function);
    }

    @ManagedOperation(description="Evicts elements in the logical address cache which have expired")
    public void evictLogicalAddressCache(boolean force) {
        logical_addr_cache.removeMarkedElements(force); 
    }

    /**
     * Send to all members in the group. UDP would use an IP multicast message, whereas TCP would send N
     * messages, one for each member
     * @param data The data to be sent. This is not a copy, so don't modify it
     * @param offset
     * @param length
     * @throws Exception
     */
    public abstract void sendMulticast(byte[] data, int offset, int length) throws Exception;

    /**
     * Send a unicast to 1 member. Note that the destination address is a *physical*, not a logical address
     * @param dest Must be a non-null unicast address
     * @param data The data to be sent. This is not a copy, so don't modify it
     * @param offset
     * @param length
     * @throws Exception
     */
    public abstract void sendUnicast(PhysicalAddress dest, byte[] data, int offset, int length) throws Exception;

    public abstract String getInfo();

    /* ------------------------------------------------------------------------------- */



    /*------------------------------ Protocol interface ------------------------------ */


    public void init() throws Exception {
        super.init();

        // Create the default thread factory
        global_thread_factory=new DefaultThreadFactory(Util.getGlobalThreadGroup(), "", false);

        // Create the timer and the associated thread factory - depends on singleton_name
        // timer_thread_factory=new DefaultThreadFactory(Util.getGlobalThreadGroup(), "Timer", true, true);
        timer_thread_factory=new LazyThreadFactory(Util.getGlobalThreadGroup(), "Timer", true, true);
        if(isSingleton()) {
            timer_thread_factory.setIncludeClusterName(false);
        }

        default_thread_factory=new DefaultThreadFactory(pool_thread_group, "Incoming", false, true);

        oob_thread_factory=new DefaultThreadFactory(pool_thread_group, "OOB", false, true);

        // local_addr is null when shared transport, channel_name is not used
        setInAllThreadFactories(channel_name, local_addr, thread_naming_pattern);

        if(timer_type.equalsIgnoreCase("old")) {
            if(timer_min_threads < 2) {
                log.warn("timer.min_threads should not be less than 2 for timer_type=\"old\"; setting value to 2 (from " +
                        timer_min_threads + ")");
                timer_min_threads=2;
            }
            timer=new DefaultTimeScheduler(timer_thread_factory, timer_min_threads);
        }
        else if(timer_type.equalsIgnoreCase("new")) {
            timer=new TimeScheduler2(timer_thread_factory, timer_min_threads, timer_max_threads, timer_keep_alive_time,
                                     timer_queue_max_size);
        }
        else if(timer_type.equalsIgnoreCase("wheel")) {
            timer=new HashedTimingWheel(timer_thread_factory, timer_min_threads, timer_max_threads, timer_keep_alive_time,
                                        timer_queue_max_size, wheel_size, tick_time);
        }
        else {
            throw new Exception("timer_type has to be either \"old\", \"new\" or \"wheel\"");
        }

        who_has_cache=new AgeOutCache<Address>(timer, 5000L);

        verifyRejectionPolicy(oob_thread_pool_rejection_policy);
        verifyRejectionPolicy(thread_pool_rejection_policy);

        // ========================================== OOB thread pool ==============================

        if(oob_thread_pool_enabled) {
            if(oob_thread_pool_queue_enabled)
                oob_thread_pool_queue=new LinkedBlockingQueue<Runnable>(oob_thread_pool_queue_max_size);
            else
                oob_thread_pool_queue=new SynchronousQueue<Runnable>();
            oob_thread_pool=createThreadPool(oob_thread_pool_min_threads, oob_thread_pool_max_threads, oob_thread_pool_keep_alive_time,
                                             oob_thread_pool_rejection_policy, oob_thread_pool_queue, oob_thread_factory);
        }
        else { // otherwise use the caller's thread to unmarshal the byte buffer into a message
            oob_thread_pool=new DirectExecutor();
        }

        // ====================================== Regular thread pool ===========================

        if(thread_pool_enabled) {
            if(thread_pool_queue_enabled)
                thread_pool_queue=new LinkedBlockingQueue<Runnable>(thread_pool_queue_max_size);
            else
                thread_pool_queue=new SynchronousQueue<Runnable>();
            thread_pool=createThreadPool(thread_pool_min_threads, thread_pool_max_threads, thread_pool_keep_alive_time,
                                         thread_pool_rejection_policy, thread_pool_queue, default_thread_factory);
        }
        else { // otherwise use the caller's thread to unmarshal the byte buffer into a message
            thread_pool=new DirectExecutor();
        }

        if(bind_addr != null) {
            Map<String, Object> m=new HashMap<String, Object>(1);
            m.put("bind_addr", bind_addr);
            up(new Event(Event.CONFIG, m));
        }

        logical_addr_cache=new LazyRemovalCache<Address,PhysicalAddress>(logical_addr_cache_max_size, logical_addr_cache_expiration);
        
        if(logical_addr_cache_reaper == null || logical_addr_cache_reaper.isDone()) {
            if(logical_addr_cache_expiration <= 0)
                throw new IllegalArgumentException("logical_addr_cache_expiration has to be > 0");
            logical_addr_cache_reaper=timer.scheduleWithFixedDelay(new Runnable() {
                public void run() {
                    logical_addr_cache.removeMarkedElements();
                }

                public String toString() {
                    return "TP.LogicalAddressCacheReaper (interval=" + logical_addr_cache_expiration + " ms)";
                }
            }, logical_addr_cache_expiration, logical_addr_cache_expiration, TimeUnit.MILLISECONDS);
        }
    }


    public void destroy() {
        super.destroy();

        if(logical_addr_cache_reaper != null) {
            logical_addr_cache_reaper.cancel(false);
            logical_addr_cache_reaper=null;
        }

        if(timer != null) {
            timer.stop();
        }

        // 3. Stop the thread pools
        if(oob_thread_pool instanceof ThreadPoolExecutor) {
            shutdownThreadPool(oob_thread_pool);
        }

        if(thread_pool instanceof ThreadPoolExecutor) {
            shutdownThreadPool(thread_pool);
        }
    }

    /**
     * Creates the unicast and multicast sockets and starts the unicast and multicast receiver threads
     */
    public void start() throws Exception {
        fetchLocalAddresses();

        if(timer == null)
            throw new Exception("timer is null");

        if(enable_diagnostics) {
            diag_handler=new DiagnosticsHandler();
            diag_handler.start();
            for(ProbeHandler handler: preregistered_probe_handlers)
                diag_handler.registerProbeHandler(handler);
            preregistered_probe_handlers.clear();
        }

        if(enable_bundling) {
            if(bundler_type.equals("new")) 
                bundler=new TransferQueueBundler(bundler_capacity);
            else if(bundler_type.equals("old"))
                bundler=new DefaultBundler();
            else
                log.warn("bundler_type \"" + bundler_type + "\" not known; using default bundler");
            if(bundler == null)
                bundler=new DefaultBundler();

            bundler.start();
        }

        // local_addr is null when shared transport
        setInAllThreadFactories(channel_name, local_addr, thread_naming_pattern);
    }


    public void stop() {
        if(diag_handler != null) {
            diag_handler.stop();
            diag_handler=null;
        }
        preregistered_probe_handlers.clear();
        if(bundler != null)
            bundler.stop();
    }



    protected void handleConnect() throws Exception {
        connect_count++;
    }

    protected void handleDisconnect() {
        connect_count=Math.max(0, connect_count -1);
    }

    public String getSingletonName() {
        return singleton_name;
    }

      public boolean isSingleton(){
          return singleton_name != null;
      }


    /**
     * handle the UP event.
     * @param evt - the event being send from the stack
     */
    public Object up(Event evt) {
        if(isSingleton()) {
            passToAllUpProtocols(evt);
            return null;
        }
        else
            return up_prot.up(evt);
    }

    /**
     * Caller by the layer above this layer. Usually we just put this Message
     * into the send queue and let one or more worker threads handle it. A worker thread
     * then removes the Message from the send queue, performs a conversion and adds the
     * modified Message to the send queue of the layer below it, by calling down()).
     */
    public Object down(Event evt) {
        if(evt.getType() != Event.MSG) {  // unless it is a message handle it and respond
            return handleDownEvent(evt);
        }

        Message msg=(Message)evt.getArg();
        if(header != null) {
            // added patch by Roland Kurmann (March 20 2003)
            // msg.putHeader(this.id, new TpHeader(channel_name));
            msg.putHeaderIfAbsent(this.id, header);
        }

        if(!isSingleton())
            setSourceAddress(msg); // very important !! listToBuffer() will fail with a null src address !!
        if(log.isTraceEnabled()) {
            log.trace("sending msg to " + msg.getDest() + ", src=" + msg.getSrc() + ", headers are " + msg.printHeaders());
        }

        // Don't send if destination is local address. Instead, switch dst and src and send it up the stack.
        // If multicast message, loopback a copy directly to us (but still multicast). Once we receive this,
        // we will discard our own multicast message
        Address dest=msg.getDest();
        if(dest instanceof PhysicalAddress) {
            // We can modify the message because it won't get retransmitted. The only time we have a physical address
            // as dest is when TCPPING sends the initial discovery requests to initial_hosts: this is below UNICAST,
            // so no retransmission
            msg.setDest(null);
        }

        final boolean multicast=dest == null || dest.isMulticastAddress();
        if(loopback && (multicast || (dest.equals(msg.getSrc()) && dest.equals(local_addr)))) {

            // we *have* to make a copy, or else up_prot.up() might remove headers from msg which will then *not*
            // be available for marshalling further down (when sending the message)
            final Message copy=msg.copy();
            if(log.isTraceEnabled()) log.trace(new StringBuilder("looping back message ").append(copy));
            // up_prot.up(new Event(Event.MSG, copy));

            // changed to fix http://jira.jboss.com/jira/browse/JGRP-506
            Executor pool=msg.isFlagSet(Message.OOB)? oob_thread_pool : thread_pool;
            pool.execute(new Runnable() {
                public void run() {
                    passMessageUp(copy, false, multicast, false);
                }
            });

            if(!multicast)
                return null;
        }

        try {
            send(msg, dest, multicast);
        }
        catch(InterruptedIOException iex) {
            ;
        }
        catch(InterruptedException interruptedEx) {
            Thread.currentThread().interrupt(); // let someone else handle the interrupt
        }
        catch(Throwable e) {
            if(log.isErrorEnabled()) {
                log.error("failed sending message to " + (dest == null? "cluster" : dest) +
                            " (" + msg.size() + " bytes): " + e + ", cause: " + e.getCause());
            }
        }
        return null;
    }



    /*--------------------------- End of Protocol interface -------------------------- */


    /* ------------------------------ Private Methods -------------------------------- */



    /**
     * If the sender is null, set our own address. We cannot just go ahead and set the address
     * anyway, as we might be sending a message on behalf of someone else ! E.g. in case of
     * retransmission, when the original sender has crashed, or in a FLUSH protocol when we
     * have to return all unstable messages with the FLUSH_OK response.
     */
    protected void setSourceAddress(Message msg) {
        if(msg.getSrc() == null && local_addr != null) // should already be set by TP.ProtocolAdapter in shared transport case !
            msg.setSrc(local_addr);
    }


    protected void passMessageUp(Message msg, boolean perform_cluster_name_matching, boolean multicast, boolean discard_own_mcast) {
        TpHeader hdr=(TpHeader)msg.getHeader(this.id);
        if(hdr == null) {
            if(log.isErrorEnabled())
                log.error(new StringBuilder("message does not have a transport header, msg is ").append(msg).
                        append(", headers are ").append(msg.printHeaders()).append(", will be discarded").toString());
            return;
        }

        if(log.isTraceEnabled())
            log.trace(new StringBuilder("received ").append(msg).append(", headers are ").append(msg.printHeaders()));

        String ch_name=hdr.channel_name;

        final Protocol tmp_prot=isSingleton()? up_prots.get(ch_name) : up_prot;
        if(tmp_prot != null) {
            boolean is_protocol_adapter=tmp_prot instanceof ProtocolAdapter;
            // Discard if message's cluster name is not the same as our cluster name
            if(!is_protocol_adapter && perform_cluster_name_matching && channel_name != null && !channel_name.equals(ch_name)) {
                if(log.isWarnEnabled() && log_discard_msgs)
                    log.warn(new StringBuilder("discarded message from different cluster \"").append(ch_name).
                            append("\" (our cluster is \"").append(channel_name).append("\"). Sender was ").append(msg.getSrc()).toString());
                return;
            }

            if(loopback && multicast && discard_own_mcast) {
                Address local=is_protocol_adapter? ((ProtocolAdapter)tmp_prot).getAddress() : local_addr;
                if(local != null && local.equals(msg.getSrc()))
                    return;
            }
            tmp_prot.up(new Event(Event.MSG, msg));
        }
    }




    /**
     * Subclasses must call this method when a unicast or multicast message has been received.
     * Declared final so subclasses cannot override this method.
     *
     * @param sender
     * @param data
     * @param offset
     * @param length
     */
    protected void receive(Address sender, byte[] data, int offset, int length) {
        if(data == null) return;

        try {
            // determine whether OOB or not by looking at first byte of 'data'
            byte oob_flag=data[Global.SHORT_SIZE]; // we need to skip the first 2 bytes (version)

            if((oob_flag & OOB) == OOB) {
                num_oob_msgs_received++;
                dispatchToThreadPool(oob_thread_pool, sender, data, offset, length);
            }
            else {
                num_incoming_msgs_received++;
                dispatchToThreadPool(thread_pool, sender, data, offset, length);
            }
        }
        catch(Throwable t) {
            if(log.isErrorEnabled())
                log.error(new StringBuilder("failed handling data from ").append(sender).toString(), t);
        }
    }



    protected void dispatchToThreadPool(Executor pool, Address sender, byte[] data, int offset, int length) {
        if(pool instanceof DirectExecutor) {
            // we don't make a copy of the buffer if we execute on this thread
            pool.execute(new IncomingPacket(sender, data, offset, length));
        }
        else {
            byte[] tmp=new byte[length];
            System.arraycopy(data, offset, tmp, 0, length);
            pool.execute(new IncomingPacket(sender, tmp, 0, length));
        }
    }




    /** Serializes and sends a message. This method is not reentrant */
    protected void send(Message msg, Address dest, boolean multicast) throws Exception {

        // bundle only regular messages; send OOB messages directly
        if(enable_bundling && !(msg.isFlagSet(Message.OOB) || msg.isFlagSet(Message.DONT_BUNDLE))) {
            if(!enable_unicast_bundling && !multicast) {
                ; // don't bundle unicast msgs if enable_unicast_bundling is off (http://jira.jboss.com/jira/browse/JGRP-429)
            }
            else {
                bundler.send(msg);
                return;
            }
        }

        // we can create between 300'000 - 400'000 output streams and do the marshalling per second,
        // so this is not a bottleneck !
        ExposedByteArrayOutputStream out_stream=new ExposedByteArrayOutputStream((int)(msg.size() + 50));
        ExposedDataOutputStream dos=new ExposedDataOutputStream(out_stream);
        writeMessage(msg, dos, multicast);
        Buffer buf=new Buffer(out_stream.getRawBuffer(), 0, out_stream.size());
        doSend(buf, dest, multicast);
        // we don't need to close() or flush() any of the 2 streams above, as these ops are no-ops
    }


    protected void doSend(Buffer buf, Address dest, boolean multicast) throws Exception {
        if(stats) {
            num_msgs_sent++;
            num_bytes_sent+=buf.getLength();
        }
        if(multicast) {
            sendMulticast(buf.getBuf(), buf.getOffset(), buf.getLength());
        }
        else {
            sendToSingleMember(dest, buf.getBuf(), buf.getOffset(), buf.getLength());
        }
    }


    protected void sendToSingleMember(Address dest, byte[] buf, int offset, int length) throws Exception {
        PhysicalAddress physical_dest=dest instanceof PhysicalAddress? (PhysicalAddress)dest : getPhysicalAddressFromCache(dest);
        if(physical_dest == null) {
            if(!who_has_cache.contains(dest)) {
                who_has_cache.add(dest);
                if(log.isWarnEnabled())
                    log.warn(local_addr+  ": no physical address for " + dest + ", dropping message");
                up(new Event(Event.GET_PHYSICAL_ADDRESS, dest));
            }
            return;
        }
        sendUnicast(physical_dest, buf, offset, length);
    }


    protected void sendToAllPhysicalAddresses(byte[] buf, int offset, int length) throws Exception {
        Set<PhysicalAddress> dests=new HashSet<PhysicalAddress>(logical_addr_cache.values());
        for(PhysicalAddress dest: dests) {
            try {
                sendUnicast(dest, buf, offset, length);
            }
            catch(Throwable t) {
                if(log.isErrorEnabled())
                    log.error("failure sending message to " + dest + ": " + t);
            }
        }
    }

    /**
     * This method needs to be synchronized on out_stream when it is called
     * @param msg
     * @return
     * @throws java.io.IOException
     */
    protected static void writeMessage(Message msg, DataOutputStream dos, boolean multicast) throws Exception {
        byte flags=0;
        dos.writeShort(Version.version); // write the version
        if(multicast)
            flags+=MULTICAST;
        if(msg.isFlagSet(Message.OOB))
            flags+=OOB;
        dos.writeByte(flags);
        msg.writeTo(dos);
    }

    protected static Message readMessage(DataInputStream instream) throws Exception {
        Message msg=new Message(false); // don't create headers, readFrom() will do this
        msg.readFrom(instream);
        return msg;
    }




    /**
     * Write a lits of messages with the same destination and *mostly* the same src addresses. The message list is
     * marshalled as follows:
     * <pre>
     * List: * | version | flags | dest | src | [Message*] |
     *
     * Message:  | presence | leading | flags | [src] | length | [buffer] | size | [Headers*] |
     *
     * </pre>
     * @param dest
     * @param src
     * @param msgs
     * @param dos
     * @param multicast
     * @throws Exception
     */
    protected static void writeMessageList(Address dest, Address src,
                                         List<Message> msgs, DataOutputStream dos, boolean multicast) throws Exception {
        dos.writeShort(Version.version);

        byte flags=LIST;
        if(multicast)
            flags+=MULTICAST;

        dos.writeByte(flags);

        Util.writeAddress(dest, dos);

        Util.writeAddress(src, dos);

        if(msgs != null) {
            for(Message msg: msgs) {
                dos.writeBoolean(true);
                msg.writeToNoAddrs(src, dos);
            }
        }

        dos.writeBoolean(false); // terminating presence - no more messages will follow
    }



    protected static List<Message> readMessageList(DataInputStream in) throws Exception {
        List<Message> list=new LinkedList<Message>();
        Address dest=Util.readAddress(in);
        Address src=Util.readAddress(in);

        while(in.readBoolean()) {
            Message msg=new Message(false);
            msg.readFrom(in);
            msg.setDest(dest);
            if(msg.getSrc() == null)
                msg.setSrc(src);
            list.add(msg);
        }
        return list;
    }


    @SuppressWarnings("unchecked")
    protected Object handleDownEvent(Event evt) {
        switch(evt.getType()) {

            case Event.TMP_VIEW:
            case Event.VIEW_CHANGE:
                synchronized(members) {
                    View view=(View)evt.getArg();
                    members.clear();

                    if(!isSingleton()) {
                        Vector<Address> tmpvec=view.getMembers();
                        members.addAll(tmpvec);
                    }
                    else {
                        // add all members from all clusters
                        for(Protocol prot: up_prots.values()) {
                            if(prot instanceof ProtocolAdapter) {
                                ProtocolAdapter ad=(ProtocolAdapter)prot;
                                Set<Address> tmp=ad.getMembers();
                                members.addAll(tmp);
                            }
                        }
                    }

                    // fix for https://jira.jboss.org/jira/browse/JGRP-918
                    logical_addr_cache.retainAll(members);
                    UUID.retainAll(members);
                }

                break;

            case Event.CONNECT:
            case Event.CONNECT_WITH_STATE_TRANSFER:
            case Event.CONNECT_USE_FLUSH:
            case Event.CONNECT_WITH_STATE_TRANSFER_USE_FLUSH:
                channel_name=(String)evt.getArg();
                header=new TpHeader(channel_name);

                // local_addr is null when shared transport
                setInAllThreadFactories(channel_name, local_addr, thread_naming_pattern);
                setThreadNames();
                connectLock.lock();
                try {
                    handleConnect();
                }
                catch(Exception e) {
                    throw new RuntimeException(e);
                }
                finally {
                    connectLock.unlock();
                }
                return null;

            case Event.DISCONNECT:
                unsetThreadNames();
                connectLock.lock();
                try {
                    handleDisconnect();
                }
                finally {
                    connectLock.unlock();
                }
                break;

            case Event.GET_PHYSICAL_ADDRESS:
                return getPhysicalAddressFromCache((UUID)evt.getArg());

            case Event.GET_LOGICAL_PHYSICAL_MAPPINGS:
                return logical_addr_cache.contents();

            case Event.SET_PHYSICAL_ADDRESS:
                Tuple<UUID,PhysicalAddress> tuple=(Tuple<UUID,PhysicalAddress>)evt.getArg();
                addPhysicalAddressToCache(tuple.getVal1(), tuple.getVal2());
                break;

            case Event.REMOVE_ADDRESS:
                removeLogicalAddressFromCache((UUID)evt.getArg());
                break;

            case Event.SET_LOCAL_ADDRESS:
                if(!isSingleton())
                    local_addr=(Address)evt.getArg();
                registerLocalAddress((Address)evt.getArg());
                break;
        }
        return null;
    }

    /**
     * Associates the address with the physical address fetched from the cache
     * @param addr
     * @return true if registered successfully, otherwise false (e.g. physical addr could not be fetched)
     */
    protected void registerLocalAddress(Address addr) {
        PhysicalAddress physical_addr=getPhysicalAddress();
        if(physical_addr != null && addr != null)
            addPhysicalAddressToCache(addr, physical_addr);
    }

    /**
     * Grabs the local address (or addresses in the shared transport case) and registers them with the physical address
     * in the transport's cache
     */
    protected void fetchLocalAddresses() {
        if(!isSingleton()) {
            if(local_addr != null) {
                registerLocalAddress(local_addr);
            }
            else {
                Address addr=(Address)up_prot.up(new Event(Event.GET_LOCAL_ADDRESS));
                local_addr=addr;
                registerLocalAddress(addr);
            }
        }
        else {
            for(Protocol prot: up_prots.values()) {
                Address addr=(Address)prot.up(new Event(Event.GET_LOCAL_ADDRESS));
                registerLocalAddress(addr);
            }
        }
    }


    protected void setThreadNames() {
        if(diag_handler != null)
            global_thread_factory.renameThread(DiagnosticsHandler.THREAD_NAME, diag_handler.getThread());
        if(bundler instanceof TransferQueueBundler) {
            global_thread_factory.renameThread(TransferQueueBundler.THREAD_NAME,
                                               ((TransferQueueBundler)bundler).getThread());
        }
    }


    protected void unsetThreadNames() {
        if(diag_handler != null && diag_handler.getThread() != null)
            diag_handler.getThread().setName(DiagnosticsHandler.THREAD_NAME);
        if(bundler instanceof TransferQueueBundler) {
            Thread thread=((TransferQueueBundler)bundler).getThread();
            if(thread != null)
                global_thread_factory.renameThread(TransferQueueBundler.THREAD_NAME, thread);
        }
    }

    protected void setInAllThreadFactories(String cluster_name, Address local_address, String pattern) {
        ThreadFactory[] factories= {timer_thread_factory,
                                    default_thread_factory,
                                    oob_thread_factory,
                                    global_thread_factory };

        boolean is_shared_transport=isSingleton();

        for(ThreadFactory factory:factories) {
            if(pattern != null) {
                factory.setPattern(pattern);
                if(is_shared_transport)
                    factory.setIncludeClusterName(false);
            }
            if(cluster_name != null && !is_shared_transport) // only set cluster name if we don't have a shared transport
                factory.setClusterName(cluster_name);
            if(local_address != null)
                factory.setAddress(local_address.toString());
        }
    }



    protected static ExecutorService createThreadPool(int min_threads, int max_threads, long keep_alive_time, String rejection_policy,
                                                      BlockingQueue<Runnable> queue, final ThreadFactory factory) {

        ThreadPoolExecutor pool=new ThreadManagerThreadPoolExecutor(min_threads, max_threads, keep_alive_time, TimeUnit.MILLISECONDS, queue);
        pool.setThreadFactory(factory);
        RejectedExecutionHandler handler=parseRejectionPolicy(rejection_policy);
        pool.setRejectedExecutionHandler(new ShutdownRejectedExecutionHandler(handler));
        return pool;
    }


    protected static void shutdownThreadPool(Executor thread_pool) {
        if(thread_pool instanceof ExecutorService) {
            ExecutorService service=(ExecutorService)thread_pool;
            service.shutdownNow();
            try {
                service.awaitTermination(Global.THREADPOOL_SHUTDOWN_WAIT_TIME, TimeUnit.MILLISECONDS);
            }
            catch(InterruptedException e) {
            }
        }
    }

    protected void verifyRejectionPolicy(String str) throws Exception{
        if(!(str.equalsIgnoreCase("run") || str.equalsIgnoreCase("abort")|| str.equalsIgnoreCase("discard")|| str.equalsIgnoreCase("discardoldest"))) {
            log.error("rejection policy of " + str + " is unknown");
            throw new Exception("Unknown rejection policy " + str);
        }
    }

    protected static RejectedExecutionHandler parseRejectionPolicy(String rejection_policy) {
        if(rejection_policy == null)
            throw new IllegalArgumentException("rejection policy is null");
        if(rejection_policy.equalsIgnoreCase("abort"))
            return new ThreadPoolExecutor.AbortPolicy();
        if(rejection_policy.equalsIgnoreCase("discard"))
            return new ThreadPoolExecutor.DiscardPolicy();
        if(rejection_policy.equalsIgnoreCase("discardoldest"))
            return new ThreadPoolExecutor.DiscardOldestPolicy();
        if(rejection_policy.equalsIgnoreCase("run"))
            return new ThreadPoolExecutor.CallerRunsPolicy();
        throw new IllegalArgumentException("rejection policy \"" + rejection_policy + "\" not known");
    }


    protected void passToAllUpProtocols(Event evt) {
        for(Protocol prot: up_prots.values()) {
            try {
                prot.up(evt);
            }
            catch(Exception e) {
                if(log.isErrorEnabled())
                    log.error("failed passing up event " + evt, e);
            }
        }
    }



    protected void addPhysicalAddressToCache(Address logical_addr, PhysicalAddress physical_addr) {
        if(logical_addr != null && physical_addr != null)
            logical_addr_cache.add(logical_addr, physical_addr);
    }

    protected PhysicalAddress getPhysicalAddressFromCache(Address logical_addr) {
        return logical_addr != null? logical_addr_cache.get(logical_addr) : null;
    }

    protected void removeLogicalAddressFromCache(Address logical_addr) {
        if(logical_addr != null)
            logical_addr_cache.remove(logical_addr);
    }

    @ManagedOperation
    public void clearLogicalAddressCache() {
        logical_addr_cache.clear(true);
    }


    protected abstract PhysicalAddress getPhysicalAddress();

    /* ----------------------------- End of Private Methods ---------------------------------------- */



    /* ----------------------------- Inner Classes ---------------------------------------- */

    class IncomingPacket implements Runnable {
        final Address   sender;
        final byte[]    buf;
        final int       offset, length;

        IncomingPacket(Address sender, byte[] buf, int offset, int length) {
            this.sender=sender;
            this.buf=buf;
            this.offset=offset;
            this.length=length;
        }


        /** Code copied from handleIncomingPacket */
        public void run() {
            short                        version;
            byte                         flags;
            ExposedByteArrayInputStream  in_stream;
            DataInputStream              dis=null;

            try {
                in_stream=new ExposedByteArrayInputStream(buf, offset, length);
                dis=new DataInputStream(in_stream);
                try {
                    version=dis.readShort();
                }
                catch(IOException ex) {
                    if(discard_incompatible_packets)
                        return;
                    throw ex;
                }
                if(Version.isBinaryCompatible(version) == false) {
                    if(log.isWarnEnabled()) {
                        StringBuilder sb=new StringBuilder();
                        sb.append("packet from ").append(sender).append(" has different version (").append(Version.print(version));
                        sb.append(") from ours (").append(Version.printVersion()).append("). ");
                        if(discard_incompatible_packets)
                            sb.append("Packet is discarded");
                        else
                            sb.append("This may cause problems");
                        log.warn(sb.toString());
                    }
                    if(discard_incompatible_packets)
                        return;
                }

                flags=dis.readByte();
                boolean is_message_list=(flags & LIST) == LIST;
                boolean multicast=(flags & MULTICAST) == MULTICAST;

                if(is_message_list) { // used if message bundling is enabled
                    List<Message> msgs=readMessageList(dis);
                    for(Message msg: msgs) {
                        if(msg.isFlagSet(Message.OOB)) {
                            log.warn("bundled message should not be marked as OOB");
                        }
                        handleMyMessage(msg, multicast);
                    }
                }
                else {
                    Message msg=readMessage(dis);
                    handleMyMessage(msg, multicast);
                }
            }
            catch(Throwable t) {
                if(log.isErrorEnabled())
                    log.error("failed handling incoming message", t);
            }
            finally {
                Util.close(dis);
            }
        }


        private void handleMyMessage(Message msg, boolean multicast) {
            if(stats) {
                num_msgs_received++;
                num_bytes_received+=msg.getLength();
            }
            passMessageUp(msg, true, multicast, true);
        }
    }



    protected interface Bundler {
        void start();
        void stop();
        void send(Message msg) throws Exception;
    }



    private class DefaultBundler implements Bundler {
    	static final int 		   		   MIN_NUMBER_OF_BUNDLING_TASKS=2;
        /** Keys are destinations, values are lists of Messages */
        final Map<SingletonAddress,List<Message>>  msgs=new HashMap<SingletonAddress,List<Message>>(36);
        @GuardedBy("lock")
        long                               count=0;    // current number of bytes accumulated
        int                                num_msgs=0;
        @GuardedBy("lock")
        int                                num_bundling_tasks=0;
        long                               last_bundle_time;
        final ReentrantLock                lock=new ReentrantLock();
        final Log                          log=LogFactory.getLog(getClass());

        public void start() {
        }

        public void stop() {
        }

        public void send(Message msg) throws Exception {
            long length=msg.size();
            boolean do_schedule=false;
            checkLength(length);

            lock.lock();
            try {
                if(count + length >= max_bundle_size) {
                    sendBundledMessages(msgs);
                }
                addMessage(msg);
                count+=length;
                if(num_bundling_tasks < MIN_NUMBER_OF_BUNDLING_TASKS) {
                    num_bundling_tasks++;
                    do_schedule=true;
                }
            }
            finally {
                lock.unlock();
            }

            if(do_schedule)
                timer.schedule(new BundlingTimer(), max_bundle_timeout, TimeUnit.MILLISECONDS);
        }

        /** Run with lock acquired */
        private void addMessage(Message msg) {
            Address dst=msg.getDest();
            String cluster_name;

            if(!isSingleton())
                cluster_name=TP.this.channel_name;
            else {
                TpHeader hdr=(TpHeader)msg.getHeader(id);
                cluster_name=hdr.channel_name;
            }

            SingletonAddress dest=new SingletonAddress(cluster_name, dst);

            if(msgs.isEmpty())
                last_bundle_time=System.currentTimeMillis();
            List<Message> tmp=msgs.get(dest);
            if(tmp == null) {
                tmp=new LinkedList<Message>();
                msgs.put(dest, tmp);
            }
            tmp.add(msg);
            num_msgs++;
        }


        /**
         * Sends all messages from the map, all messages for the same destination are bundled into 1 message.
         * This method may be called by timer and bundler concurrently
         * @param msgs
         */
        private void sendBundledMessages(final Map<SingletonAddress,List<Message>> msgs) {
            if(log.isTraceEnabled()) {
                long stop=System.currentTimeMillis();
                double percentage=100.0 / max_bundle_size * count;
                StringBuilder sb=new StringBuilder("sending ").append(num_msgs).append(" msgs (");
                num_msgs=0;
                sb.append(count).append(" bytes (" + f.format(percentage) + "% of max_bundle_size)");
                if(last_bundle_time > 0) {
                    sb.append(", collected in ").append(stop-last_bundle_time).append("ms) ");
                }
                sb.append(" to ").append(msgs.size()).append(" destination(s)");
                if(msgs.size() > 1) sb.append(" (dests=").append(msgs.keySet()).append(")");
                log.trace(sb);
            }

            ExposedByteArrayOutputStream bundler_out_stream=new ExposedByteArrayOutputStream((int)(count + 50));
            ExposedDataOutputStream bundler_dos=new ExposedDataOutputStream(bundler_out_stream);

            for(Map.Entry<SingletonAddress,List<Message>> entry: msgs.entrySet()) {
                List<Message> list=entry.getValue();
                if(list.isEmpty())
                    continue;
                SingletonAddress dst=entry.getKey();
                Address dest=dst.getAddress();
                Address src_addr=list.get(0).getSrc();

                boolean multicast=dest == null || dest.isMulticastAddress();
                try {
                    bundler_out_stream.reset();
                    bundler_dos.reset();
                    writeMessageList(dest, src_addr, list, bundler_dos, multicast); // flushes output stream when done
                    Buffer buffer=new Buffer(bundler_out_stream.getRawBuffer(), 0, bundler_out_stream.size());
                    doSend(buffer, dest, multicast);
                }
                catch(Throwable e) {
                    if(log.isErrorEnabled()) log.error("exception sending bundled msgs", e);
                }
            }
            msgs.clear();
            count=0;
        }



        private void checkLength(long len) throws Exception {
            if(len > max_bundle_size)
                throw new Exception("message size (" + len + ") is greater than max bundling size (" + max_bundle_size +
                        "). Set the fragmentation/bundle size in FRAG and TP correctly");
        }


        private class BundlingTimer implements Runnable {

            public void run() {
                lock.lock();
                try {
                    if(!msgs.isEmpty()) {
                        try {
                            sendBundledMessages(msgs);
                        }
                        catch(Exception e) {
                            log.error("failed sending bundled messages", e);
                        }
                    }
                }
                finally {
                    num_bundling_tasks--;
                    lock.unlock();
                }
            }

            public String toString() {
                return getClass().getSimpleName();
            }
        }
    }



    private class TransferQueueBundler implements Bundler, Runnable {
        final int                          threshold;
        final BlockingQueue<Message>       buffer;
        volatile Thread                    bundler_thread;
        final Log                          log=LogFactory.getLog(getClass());

        /** Keys are destinations, values are lists of Messages */
        final Map<SingletonAddress,List<Message>>  msgs=new HashMap<SingletonAddress,List<Message>>(36);
        long                               count=0;    // current number of bytes accumulated
        int                                num_msgs=0;
        long                               next_bundle_time;
        volatile boolean                   running=true;
        public static final String         THREAD_NAME="TransferQueueBundler";



        public TransferQueueBundler(int capacity) {
            if(capacity <=0) throw new IllegalArgumentException("Bundler capacity cannot be " + capacity);
            buffer=new LinkedBlockingQueue<Message>(capacity);
            threshold=(int)(capacity * .9); // 90% of capacity
        }

        public void start() {
            if(bundler_thread == null || !bundler_thread.isAlive()) {
                bundler_thread=getThreadFactory().newThread(this, THREAD_NAME);
                running=true;
                bundler_thread.start();
            }
        }

        public Thread getThread() {return bundler_thread;}

        public void stop() {
            running=false;
            if(bundler_thread != null)
                bundler_thread.interrupt();
        }

        public void send(Message msg) throws Exception {
            long length=msg.size();
            checkLength(length);
            buffer.put(msg);
        }

        public int getBufferSize() {
            return buffer.size();
        }
        


        public void run() {
            next_bundle_time=System.currentTimeMillis() + max_bundle_timeout;
            while(running) {
                Message msg=null;
                long sleep_time=next_bundle_time - System.currentTimeMillis();

                try {
                    if(count == 0)
                        msg=buffer.take();
                    else
                        msg=buffer.poll(sleep_time, TimeUnit.MILLISECONDS);

                    long size=msg != null? msg.size() : 0;
                    boolean send_msgs=(msg != null && count + size >= max_bundle_size) ||
                            buffer.size() >= threshold ||
                            System.currentTimeMillis() >= next_bundle_time;

                    if(send_msgs) {
                        next_bundle_time=System.currentTimeMillis() + max_bundle_timeout;
                        try {
                            if(!msgs.isEmpty()) {
                                sendBundledMessages(msgs);
                                msgs.clear();
                            }
                            count=0;
                        }
                        catch(Exception e) {
                            log.error("failed sending bundled messages: " + e.getMessage());
                        }
                    }

                    if(msg != null) {
                        count+=size;
                        addMessage(msg);
                    }
                }
                catch(Throwable t) {
                }
            }
        }


        private void checkLength(long len) throws Exception {
            if(len > max_bundle_size)
                throw new Exception("message size (" + len + ") is greater than max bundling size (" + max_bundle_size +
                        "). Set the fragmentation/bundle size in FRAG and TP correctly");
        }


        private void addMessage(Message msg) {
            Address dst=msg.getDest();
            String cluster_name;

            if(!isSingleton())
                cluster_name=TP.this.channel_name;
            else {
                TpHeader hdr=(TpHeader)msg.getHeader(id);
                cluster_name=hdr.channel_name;
            }

            SingletonAddress dest=new SingletonAddress(cluster_name, dst);
            
            List<Message> tmp=msgs.get(dest);
            if(tmp == null) {
                tmp=new LinkedList<Message>();
                msgs.put(dest, tmp);
            }
            tmp.add(msg);
            num_msgs++;
        }



        /**
         * Sends all messages from the map, all messages for the same destination are bundled into 1 message.
         * This method may be called by timer and bundler concurrently
         * @param msgs
         */
        private void sendBundledMessages(final Map<SingletonAddress,List<Message>> msgs) {
            boolean   multicast;

            if(log.isTraceEnabled()) {
                double percentage=100.0 / max_bundle_size * count;
                StringBuilder sb=new StringBuilder("sending ").append(num_msgs).append(" msgs (");
                sb.append(count).append(" bytes (" + f.format(percentage) + "% of max_bundle_size)");
                sb.append(" to ").append(msgs.size()).append(" destination(s)");
                if(msgs.size() > 1) sb.append(" (dests=").append(msgs.keySet()).append(")");
                log.trace(sb);
                num_msgs=0;
            }

            ExposedByteArrayOutputStream bundler_out_stream=new ExposedByteArrayOutputStream((int)(count + 50));
            ExposedDataOutputStream bundler_dos=new ExposedDataOutputStream(bundler_out_stream);

            for(Map.Entry<SingletonAddress,List<Message>> entry: msgs.entrySet()) {
                List<Message> list=entry.getValue();
                if(list.isEmpty())
                    continue;

                SingletonAddress dst=entry.getKey();
                Address dest=dst.getAddress();
                Address src_addr=list.get(0).getSrc();

                multicast=dest == null || dest.isMulticastAddress();
                try {
                    bundler_out_stream.reset();
                    bundler_dos.reset();
                    writeMessageList(dest, src_addr, list, bundler_dos, multicast); // flushes output stream when done
                    Buffer buf=new Buffer(bundler_out_stream.getRawBuffer(), 0, bundler_out_stream.size());
                    doSend(buf, dest, multicast);
                }
                catch(Throwable e) {
                    if(log.isErrorEnabled()) log.error("exception sending bundled msgs: " + e + ":, cause: " + e.getCause());
                }
            }
        }


    }




    public interface ProbeHandler {
        /**
         * Handles a probe. For each key that is handled, the key and its result should be in the returned map.
         * @param keys
         * @return Map<String,String>. A map of keys and values. A null return value is permissible.
         */
        Map<String,String> handleProbe(String... keys);

        /** Returns a list of supported keys */
        String[] supportedKeys();
    }

//    /**
//     * Maps UUIDs to physical addresses
//     */
//    public interface AddressMapper {
//        /**
//         * Given a UUID, pick one physical address from a list. If the UUID is null, the message needs to be sent to
//         * the entire cluster. In UDP, for example, we would pick a multicast address
//         * @param uuid The UUID. Null for a cluster wide destination
//         * @param physical_addrs A list of physical addresses
//         * @return an address from the list
//         */
//        Address pick(UUID uuid, List<Address> physical_addrs);
//    }


    private class DiagnosticsHandler implements Runnable {
    	public static final String THREAD_NAME = "DiagnosticsHandler";
        private Thread thread=null;
        private MulticastSocket diag_sock=null;
        private final Set<ProbeHandler> handlers=new HashSet<ProbeHandler>();

        DiagnosticsHandler() {
        }

        Thread getThread(){
        	return thread;
        }

        void registerProbeHandler(ProbeHandler handler) {
            if(handler != null)
                handlers.add(handler);
        }

        void unregisterProbeHandler(ProbeHandler handler) {
            if(handler != null)
                handlers.remove(handler);
        }

        void start() throws IOException {

            registerProbeHandler(new ProbeHandler() {

                public Map<String, String> handleProbe(String... keys) {
                    Map<String,String> retval=new HashMap<String,String>(2);
                    for(String key: keys) {
                        if(key.equals("dump")) {
                            retval.put("dump", Util.dumpThreads());
                            continue;
                        }
                        if(key.equals("uuids")) {
                            retval.put("uuids", printLogicalAddressCache());
                            if(!isSingleton() && !retval.containsKey("local_addr"))
                                retval.put("local_addr", local_addr != null? local_addr.toString() : null);
                            continue;
                        }
                        if(key.equals("keys")) {
                            StringBuilder sb=new StringBuilder();
                            for(ProbeHandler handler: handlers) {
                                String[] tmp=handler.supportedKeys();
                                if(tmp != null && tmp.length > 0) {
                                    for(String s: tmp)
                                        sb.append(s).append(" ");
                                }
                            }
                            retval.put("keys", sb.toString());
                        }
                        if(key.equals("info")) {
                            if(singleton_name != null && singleton_name.length() > 0)
                                retval.put("singleton_name", singleton_name);

                        }
                    }
                    return retval;
                }

                public String[] supportedKeys() {
                    return new String[]{"dump", "keys", "uuids", "info"};
                }
            });

            // https://jira.jboss.org/jira/browse/JGRP-777 - this doesn't work on MacOS, and we don't have
            // cross talking on Windows anyway, so we just do it for Linux. (How about Solaris ?)
            if(can_bind_to_mcast_addr)
                diag_sock=Util.createMulticastSocket(getSocketFactory(),
                                                     Global.TP_DIAG_MCAST_SOCK, diagnostics_addr, diagnostics_port, log);
            else
                diag_sock=getSocketFactory().createMulticastSocket(Global.TP_DIAG_MCAST_SOCK, diagnostics_port);
            
            List<NetworkInterface> interfaces=Util.getAllAvailableInterfaces();
            bindToInterfaces(interfaces, diag_sock);

            if(thread == null || !thread.isAlive()) {
                thread=global_thread_factory.newThread(this, THREAD_NAME);
                thread.setDaemon(true);
                thread.start();
            }
        }

        void stop() {
            if(diag_sock != null)
                getSocketFactory().close(diag_sock);
            handlers.clear();
            if(thread != null){
                try{
                    thread.join(Global.THREAD_SHUTDOWN_WAIT_TIME);
                }
                catch(InterruptedException e){
                    Thread.currentThread().interrupt(); // set interrupt flag
                }
            }
        }

        public void run() {
            byte[] buf=new byte[1500]; // requests are small (responses might be bigger)
            DatagramPacket packet;
            while(!diag_sock.isClosed() && Thread.currentThread().equals(thread)) {
                packet=new DatagramPacket(buf, 0, buf.length);
                try {
                    diag_sock.receive(packet);
                    handleDiagnosticProbe(packet.getSocketAddress(), diag_sock,
                                          new String(packet.getData(), packet.getOffset(), packet.getLength()));
                }
                catch(IOException socket_ex) {
                }
                catch(Throwable e) {
                    if(log.isErrorEnabled())
                        log.error("failure handling diagnostics request", e);
                }
            }
        }

        private void handleDiagnosticProbe(SocketAddress sender, DatagramSocket sock, String request) {
            StringTokenizer tok=new StringTokenizer(request);
            List<String> list=new ArrayList<String>(10);

            while(tok.hasMoreTokens()) {
                String req=tok.nextToken().trim();
                if(req.length() > 0)
                    list.add(req);
            }

            String[] tokens=new String[list.size()];
            for(int i=0; i < list.size(); i++)
                tokens[i]=list.get(i);


            for(ProbeHandler handler: handlers) {
                Map<String, String> map=handler.handleProbe(tokens);
                if(map == null || map.isEmpty())
                    continue;
                if(!map.containsKey("cluster"))
                    map.put("cluster", channel_name != null? channel_name : "n/a");
                StringBuilder info=new StringBuilder();
                for(Map.Entry<String,String> entry: map.entrySet()) {
                    info.append(entry.getKey()).append("=").append(entry.getValue()).append("\r\n");
                }

                byte[] diag_rsp=info.toString().getBytes();
                if(log.isDebugEnabled())
                    log.debug("sending diag response to " + sender);
                try {
                    sendResponse(sock, sender, diag_rsp);
                }
                catch(Throwable t) {
                    if(log.isErrorEnabled())
                        log.error("failed sending diag rsp to " + sender, t);
                }
            }
        }

        private void sendResponse(DatagramSocket sock, SocketAddress sender, byte[] buf) throws IOException {
            DatagramPacket p=new DatagramPacket(buf, 0, buf.length, sender);
            sock.send(p);
        }

        private void bindToInterfaces(List<NetworkInterface> interfaces, MulticastSocket s) {
            SocketAddress group_addr=new InetSocketAddress(diagnostics_addr, diagnostics_port);
            for(Iterator<NetworkInterface> it=interfaces.iterator(); it.hasNext();) {
                NetworkInterface i=it.next();
                try {
                    if (i.getInetAddresses().hasMoreElements()) { // fix for VM crash - suggested by JJalenak@netopia.com
                        s.joinGroup(group_addr, i);
                        if(log.isTraceEnabled())
                            log.trace("joined " + group_addr + " on " + i.getName());
                    }
                }
                catch(IOException e) {
                    log.warn("failed to join " + group_addr + " on " + i.getName() + ": " + e);
                }
            }
        }
    }

    /**
     * Used when the transport is shared (singleton_name is not null). Maintains the cluster name, local address and
     * view
     */
    public static class ProtocolAdapter extends Protocol implements ProbeHandler {
        String                  cluster_name;
        final short             transport_id;
        TpHeader                header;
        final Set<Address>      members=new CopyOnWriteArraySet<Address>();
        final ThreadFactory     factory;
        protected SocketFactory socket_factory=new DefaultSocketFactory();
        Address                 local_addr;

        // TODO [JGRP-1194] - Revisit implementation of TUNNEL and shared transport
        static final ThreadLocal<ProtocolAdapter> thread_local=new ThreadLocal<ProtocolAdapter>();

        public ProtocolAdapter(String cluster_name, Address local_addr, short transport_id, Protocol up, Protocol down, String pattern) {
            this.cluster_name=cluster_name;
            this.local_addr=local_addr;
            this.transport_id=transport_id;
            this.up_prot=up;
            this.down_prot=down;
            this.header=new TpHeader(cluster_name);
            this.factory=new DefaultThreadFactory(Util.getGlobalThreadGroup(), "", false);
            factory.setPattern(pattern);
            if(local_addr != null)
                factory.setAddress(local_addr.toString());
            if(cluster_name != null)
                factory.setClusterName(cluster_name);
        }

        @ManagedAttribute(description="Name of the cluster to which this adapter proxies")
        public String getClusterName() {
            return cluster_name;
        }


        public Address getAddress() {
            return local_addr;
        }

        @ManagedAttribute(name="Address", description="local address")
        public String getAddressAsString() {
            return local_addr != null? local_addr.toString() : null;
        }

        @ManagedAttribute(name="AddressUUID", description="local address")
        public String getAddressAsUUID() {
            return (local_addr instanceof UUID)? ((UUID)local_addr).toStringLong() : null;
        }

        @ManagedAttribute(description="ID of the transport")
        public short getTransportName() {
            return transport_id;
        }

        public Set<Address> getMembers() {
            return Collections.unmodifiableSet(members);
        }

        public ThreadFactory getThreadFactory() {
            return factory;
        }

        public SocketFactory getSocketFactory() {
            return socket_factory;
        }

        public void setSocketFactory(SocketFactory factory) {
            if(factory != null)
                socket_factory=factory;
        }

        public void start() throws Exception {
            TP tp=getTransport();
            if(tp != null)
                tp.registerProbeHandler(this);
        }

        public void stop() {
            TP tp=getTransport();
            if(tp != null)
                tp.unregisterProbeHandler(this);
        }

        public Object down(Event evt) {
            switch(evt.getType()) {
                case Event.MSG:
                    Message msg=(Message)evt.getArg();
                    msg.putHeader(transport_id, header);
                    if(msg.getSrc() == null)
                        msg.setSrc(local_addr);
                    break;
                case Event.VIEW_CHANGE:
                    View view=(View)evt.getArg();
                    Vector<Address> tmp=view.getMembers();
                    members.clear();
                    members.addAll(tmp);
                    break;
                case Event.DISCONNECT:
                    // TODO [JGRP-1194] - Revisit implementation of TUNNEL and shared transport
                    thread_local.set(this);
                    break;
                case Event.CONNECT:
                case Event.CONNECT_WITH_STATE_TRANSFER:
                case Event.CONNECT_USE_FLUSH:
                case Event.CONNECT_WITH_STATE_TRANSFER_USE_FLUSH:  
                    // TODO [JGRP-1194] - Revisit implementation of TUNNEL and shared transport
                    thread_local.set(this);
                    cluster_name=(String)evt.getArg();
                    factory.setClusterName(cluster_name);
                    this.header=new TpHeader(cluster_name);
                    break;
                case Event.SET_LOCAL_ADDRESS:
                    Address addr=(Address)evt.getArg();
                    if(addr != null) {
                        local_addr=addr;
                        factory.setAddress(addr.toString()); // used for thread naming                        
                    }
                    break;
            }
            return down_prot.down(evt);
        }

        public String getName() {
            return "TP.ProtocolAdapter";
        }

        public String toString() {
            return cluster_name + " (" + transport_id + ")";
        }

        public Map<String, String> handleProbe(String... keys) {
            HashMap<String, String> retval=new HashMap<String, String>();
            retval.put("cluster", cluster_name);
            retval.put("local_addr", local_addr != null? local_addr.toString() : null);
            retval.put("local_addr (UUID)", local_addr instanceof UUID? ((UUID)local_addr).toStringLong() : null);
            retval.put("transport_id", Short.toString(transport_id));
            return retval;
        }

        public String[] supportedKeys() {
            return null;
        }
    }
}
