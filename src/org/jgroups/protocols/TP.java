package org.jgroups.protocols;


import org.jgroups.*;
import org.jgroups.annotations.*;
import org.jgroups.blocks.LazyRemovalCache;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.conf.PropertyConverters;
import org.jgroups.jmx.AdditionalJmxObjects;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.stack.*;
import org.jgroups.util.ThreadFactory;
import org.jgroups.util.UUID;
import org.jgroups.util.*;

import java.io.DataInput;
import java.io.InterruptedIOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
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
public abstract class TP extends Protocol implements DiagnosticsHandler.ProbeHandler, AdditionalJmxObjects {

    public static final    byte    LIST=1; // we have a list of messages rather than a single message when set
    public static final    byte    MULTICAST=2; // message is a multicast (versus a unicast) message when set
    public static final    int     MSG_OVERHEAD=Global.SHORT_SIZE + Global.BYTE_SIZE; // version + flags
    protected static final long    MIN_WAIT_BETWEEN_DISCOVERIES=TimeUnit.NANOSECONDS.convert(10, TimeUnit.SECONDS);  // ns


    /* ------------------------------------------ JMX and Properties  ------------------------------------------ */
    @LocalAddress
    @Property(name="bind_addr",
              description="The bind address which should be used by this transport. The following special values " +
                      "are also recognized: GLOBAL, SITE_LOCAL, LINK_LOCAL, NON_LOOPBACK, match-interface, match-host, match-address",
              defaultValueIPv4=Global.NON_LOOPBACK_ADDRESS, defaultValueIPv6=Global.NON_LOOPBACK_ADDRESS,
              systemProperty={Global.BIND_ADDR},writable=false)
    protected InetAddress bind_addr;

    @Property(description="Use IP addresses (IpAddressUUID) instead of UUIDs as addresses. This is currently not " +
      "compatible with RELAY2: disable if RELAY2 is used.")
    protected boolean use_ip_addrs;

    @Property(description="Use \"external_addr\" if you have hosts on different networks, behind " +
      "firewalls. On each firewall, set up a port forwarding rule (sometimes called \"virtual server\") to " +
      "the local IP (e.g. 192.168.1.100) of the host then on each host, set \"external_addr\" TCP transport " +
      "parameter to the external (public IP) address of the firewall.",
              systemProperty=Global.EXTERNAL_ADDR,writable=false)
    protected InetAddress external_addr;


    @Property(description="Used to map the internal port (bind_port) to an external port. Only used if > 0",
              systemProperty=Global.EXTERNAL_PORT,writable=false)
    protected int external_port;

    @ManagedAttribute(description="tracing is enabled or disabled for the given log",writable=true)
    protected boolean is_trace=log.isTraceEnabled();


    @Property(description="If true, the transport should use all available interfaces to receive multicast messages")
    protected boolean receive_on_all_interfaces;

    /**
     * List<NetworkInterface> of interfaces to receive multicasts on. The multicast receive socket will listen
     * on all of these interfaces. This is a comma-separated list of IP addresses or interface names. E.g.
     * "192.168.5.1,eth1,127.0.0.1". Duplicates are discarded; we only bind to
     * an interface once. If this property is set, it overrides receive_on_all_interfaces.
     */
    @Property(converter=PropertyConverters.NetworkInterfaceList.class,
              description="Comma delimited list of interfaces (IP addresses or interface names) to receive multicasts on")
    protected List<NetworkInterface> receive_interfaces;

    @Property(description="Max number of elements in the logical address cache before eviction starts")
    protected int logical_addr_cache_max_size=2000;

    @Property(description="Time (in ms) after which entries in the logical address cache marked as removable " +
      "can be removed. 0 never removes any entries (not recommended)")
    protected long logical_addr_cache_expiration=360000;

    @Property(description="Interval (in ms) at which the reaper task scans logical_addr_cache and removes entries " +
      "marked as removable. 0 disables reaping.")
    protected long logical_addr_cache_reaper_interval=60000;

    /** The port to which the transport binds. 0 means to bind to any (ephemeral) port. See also {@link #port_range} */
    @Property(description="The port to which the transport binds. Default of 0 binds to any (ephemeral) port." +
      " See also port_range",systemProperty={Global.BIND_PORT},writable=false)
    protected int bind_port;

    @Property(description="The range of valid ports: [bind_port .. bind_port+port_range ]. " +
      "0 only binds to bind_port and fails if taken")
    protected int port_range=10; // 27-6-2003 bgooren, Only try one port by default

    @Property(description="Whether or not to make a copy of a message before looping it back up. Don't use this; might " +
      "get removed without warning")
    protected boolean loopback_copy;

    @Property(description="Loop back the message on a separate thread or use the current thread. Don't use this; " +
      "might get removed without warning")
    protected boolean loopback_separate_thread=true;

    @Property(description="The fully qualified name of a class implementing MessageProcessingPolicy")
    protected String message_processing_policy;


    @Property(name="message_processing_policy.max_buffer_size",
      description="Max number of messages buffered for consumption of the delivery thread in MaxOneThreadPerSender. 0 creates an unbounded buffer")
    protected int msg_processing_max_buffer_size=5000;

    @Property(description="Thread naming pattern for threads in this channel. Valid values are \"pcl\": " +
      "\"p\": includes the thread name, e.g. \"Incoming thread-1\", \"UDP ucast receiver\", " +
      "\"c\": includes the cluster name, e.g. \"MyCluster\", " +
      "\"l\": includes the local address of the current member, e.g. \"192.168.5.1:5678\"")
    protected String thread_naming_pattern="cl";

    @Property(name="thread_pool.use_fork_join_pool",description="If enabled, a ForkJoinPool will be used rather than a ThreadPoolExecutor")
    protected boolean use_fork_join_pool;

    @Property(name="thread_pool.use_common_fork_join_pool",
      description="If true, the common fork-join pool will be used; otherwise a custom ForkJoinPool will be created")
    protected boolean use_common_fork_join_pool;

    @Property(name="thread_pool.enabled",description="Enable or disable the thread pool")
    protected boolean thread_pool_enabled=true;

    @Property(name="thread_pool.min_threads",description="Minimum thread pool size for the thread pool")
    protected int thread_pool_min_threads=0;

    @Property(name="thread_pool.max_threads",description="Maximum thread pool size for the thread pool")
    protected int thread_pool_max_threads=100;

    @Property(name="thread_pool.keep_alive_time",description="Timeout in milliseconds to remove idle threads from pool")
    protected long thread_pool_keep_alive_time=30000;


    @Property(description="Interval (in ms) at which the time service updates its timestamp. 0 disables the time service")
    protected long time_service_interval=500;

    @Property(description="Switch to enable diagnostic probing. Default is true")
    protected boolean enable_diagnostics=true;

    @Property(description="Use a multicast socket to listen for probe requests (ignored if enable_diagnostics is false)")
    protected boolean diag_enable_udp=true;

    @Property(description="Use a TCP socket to listen for probe requests (ignored if enable_diagnostics is false)")
    protected boolean diag_enable_tcp;

    @Property(description="Multicast address for diagnostic probing. Used when diag_enable_udp is true",
              defaultValueIPv4="224.0.75.75",defaultValueIPv6="ff0e::0:75:75")
    protected InetAddress diagnostics_addr;

    @Property(description="Bind address for diagnostic probing. Used when diag_enable_tcp is true")
    protected InetAddress diagnostics_bind_addr;

    @Property(converter=PropertyConverters.NetworkInterfaceList.class,
              description="Comma delimited list of interfaces (IP addresses or interface names) that the " +
                "diagnostics multicast socket should bind to")
    protected List<NetworkInterface> diagnostics_bind_interfaces;

    @Property(description="Port for diagnostic probing. Default is 7500")
    protected int diagnostics_port=7500;

    @Property(description="The number of ports to be probed for an available port (TCP)")
    protected int diagnostics_port_range=50;

    @Property(description="TTL of the diagnostics multicast socket")
    protected int diagnostics_ttl=8;
    
    @Property(description="Authorization passcode for diagnostics. If specified every probe query will be authorized")
    protected String diagnostics_passcode;

    /** Whether or not warnings about messages from different groups are logged - private flag, not for common use */
    @Property(description="whether or not warnings about messages from different groups are logged")
    protected boolean log_discard_msgs=true;

    @Property(description="whether or not warnings about messages from members with a different version are discarded")
    protected boolean log_discard_msgs_version=true;

    @Property(description="Timeout (in ms) to determine how long to wait until a request to fetch the physical address " +
      "for a given logical address will be sent again. Subsequent requests for the same physical address will therefore " +
      "be spaced at least who_has_cache_timeout ms apart")
    protected long who_has_cache_timeout=2000;

    @Property(description="Time during which identical warnings about messages from a member with a different version " +
      "will be suppressed. 0 disables this (every warning will be logged). Setting the log level to ERROR also " +
      "disables this.")
    protected long suppress_time_different_version_warnings=60000;

    @Property(description="Time during which identical warnings about messages from a member from a different cluster " +
      "will be suppressed. 0 disables this (every warning will be logged). Setting the log level to ERROR also " +
      "disables this.")
    protected long suppress_time_different_cluster_warnings=60000;


    /**
     * Maximum number of bytes for messages to be queued until they are sent.
     * This value needs to be smaller than the largest datagram packet size in case of UDP
     */
    @Property(name="max_bundle_size", description="Maximum number of bytes for messages to be queued until they are sent")
    protected int max_bundle_size=64000;

    @Property(description="The type of bundler used (\"ring-buffer\", \"transfer-queue\" (default), \"sender-sends\" or " +
      "\"no-bundler\") or the fully qualified classname of a Bundler implementation")
    protected String bundler_type="transfer-queue";

    @Property(description="The max number of elements in a bundler if the bundler supports size limitations")
    protected int bundler_capacity=16384;

    @Property(description="Number of spins before a real lock is acquired")
    protected int bundler_num_spins=5;

    @Property(description="The wait strategy for a RingBuffer")
    protected String bundler_wait_strategy="park";

    @ManagedAttribute(description="Fully qualified classname of bundler")
    public String getBundlerClass() {
        return bundler != null? bundler.getClass().getName() : "null";
    }

    public <T extends TP> T setMaxBundleSize(int size) {
        if(size <= 0)
            throw new IllegalArgumentException("max_bundle_size (" + size + ") is <= 0");
        max_bundle_size=size;
        return (T)this;
    }
    public final int getMaxBundleSize()            {return max_bundle_size;}
    public int getBundlerCapacity()                {return bundler_capacity;}
    public int getMessageProcessingMaxBufferSize() {return msg_processing_max_buffer_size;}

    @ManagedAttribute public int getBundlerBufferSize() {
        if(bundler instanceof TransferQueueBundler)
            return ((TransferQueueBundler)bundler).getBufferSize();
        return bundler != null? bundler.size() : 0;
    }

    @ManagedAttribute(description="The wait strategy for a RingBuffer")
    public String bundlerWaitStrategy() {
        return bundler instanceof RingBufferBundler? ((RingBufferBundler)bundler).waitStrategy() : bundler_wait_strategy;
    }

    @ManagedAttribute(description="Sets the wait strategy in the RingBufferBundler. Allowed values are \"spin\", " +
      "\"yield\", \"park\", \"spin-park\" and \"spin-yield\" or a fully qualified classname")
    public <T extends TP> T bundlerWaitStrategy(String strategy) {
        if(bundler instanceof RingBufferBundler) {
            ((RingBufferBundler)bundler).waitStrategy(strategy);
            this.bundler_wait_strategy=strategy;
        }
        else
            this.bundler_wait_strategy=strategy;
        return (T)this;
    }

    @ManagedAttribute(description="Number of spins before a real lock is acquired")
    public int bundlerNumSpins() {
        return bundler instanceof RingBufferBundler? ((RingBufferBundler)bundler).numSpins() : bundler_num_spins;
    }

    @ManagedAttribute(description="Sets the number of times a thread spins until a real lock is acquired")
    public <T extends TP> T bundlerNumSpins(int spins) {
        this.bundler_num_spins=spins;
        if(bundler instanceof RingBufferBundler)
            ((RingBufferBundler)bundler).numSpins(spins);
        return (T)this;
    }

    @ManagedAttribute(description="Is the logical_addr_cache reaper task running")
    public boolean isLogicalAddressCacheReaperRunning() {
        return logical_addr_cache_reaper != null && !logical_addr_cache_reaper.isDone();
    }

    @ManagedAttribute(description="Returns the average batch size of received batches")
    public String getAvgBatchSize() {
        return avg_batch_size.toString();
    }

    public AverageMinMax avgBatchSize() {return avg_batch_size;}


    public <T extends TP> T setThreadPoolMinThreads(int size) {
        thread_pool_min_threads=size;
        if(thread_pool instanceof ThreadPoolExecutor)
            ((ThreadPoolExecutor)thread_pool).setCorePoolSize(size);
        return (T)this;
    }

    public int getThreadPoolMinThreads() {return thread_pool_min_threads;}


    public <T extends TP> T setThreadPoolMaxThreads(int size) {
        thread_pool_max_threads=size;
        if(thread_pool instanceof ThreadPoolExecutor)
            ((ThreadPoolExecutor)thread_pool).setMaximumPoolSize(size);
        return (T)this;
    }

    public int getThreadPoolMaxThreads() {return thread_pool_max_threads;}


    public <T extends TP> T setThreadPoolKeepAliveTime(long time) {
        thread_pool_keep_alive_time=time;
        if(thread_pool instanceof ThreadPoolExecutor)
            ((ThreadPoolExecutor)thread_pool).setKeepAliveTime(time, TimeUnit.MILLISECONDS);
        return (T)this;
    }

    public long getThreadPoolKeepAliveTime() {return thread_pool_keep_alive_time;}

    public Object[] getJmxObjects() {
        return new Object[]{msg_stats, msg_processing_policy, bundler};
    }

    @Property(name="level", description="Sets the level")
    public <T extends Protocol> T setLevel(String level) {
        T retval=super.setLevel(level);
        is_trace=log.isTraceEnabled();
        return retval;
    }

    @ManagedOperation(description="Changes the message processing policy. The fully qualified name of a class " +
      "implementing MessageProcessingPolicy needs to be given")
    public void setMessageProcessingPolicy(String policy) {
        if(policy == null)
            return;
        if(policy.startsWith("submit")) {
            msg_processing_policy=new SubmitToThreadPool();
            msg_processing_policy.init(this);
            return;
        }
        else if(policy.startsWith("max")) {
            msg_processing_policy=new MaxOneThreadPerSender();
            msg_processing_policy.init(this);
            return;
        }
        try {
            Class<MessageProcessingPolicy> clazz=Util.loadClass(policy, getClass());
            msg_processing_policy=clazz.getDeclaredConstructor().newInstance();
            message_processing_policy=policy;
            msg_processing_policy.init(this);
        }
        catch(Exception e) {
            log.error("failed setting message_processing_policy", e);
        }
    }

    /* --------------------------------------------- JMX  ---------------------------------------------- */

    protected final MsgStats msg_stats=new MsgStats();




    /** The name of the group to which this member is connected. With a shared transport, the channel name is
     * in TP.ProtocolAdapter (cluster_name), and this field is not used */
    @ManagedAttribute(description="Channel (cluster) name")
    protected AsciiString cluster_name;

    @ManagedAttribute(description="If enabled, the timer will run non-blocking tasks on its own (runner) thread, and " +
      "not submit them to the thread pool. Otherwise, all tasks are submitted to the thread pool. This attribute is " +
      "experimental and may be removed without notice.")
    protected boolean timer_handle_non_blocking_tasks=true;

    @ManagedAttribute(description="Class of the timer implementation")
    public String getTimerClass() {
        return timer != null? timer.getClass().getSimpleName() : "null";
    }

    @ManagedAttribute(description="Name of the cluster to which this transport is connected")
    public String getClusterName() {
        return cluster_name != null? cluster_name.toString() : null;
    }

    public AsciiString getClusterNameAscii() {return cluster_name;}

    @ManagedAttribute(description="Number of messages from members in a different cluster")
    public int getDifferentClusterMessages() {
        return suppress_log_different_cluster != null? suppress_log_different_cluster.getCache().size() : 0;
    }

    @ManagedAttribute(description="Number of messages from members with a different JGroups version")
    public int getDifferentVersionMessages() {
        return suppress_log_different_version != null? suppress_log_different_version.getCache().size() : 0;
    }

    @ManagedOperation(description="Clears the cache for messages from different clusters")
    public <T extends TP> T clearDifferentClusterCache() {
        if(suppress_log_different_cluster != null)
            suppress_log_different_cluster.getCache().clear();
        return (T)this;
    }

    @ManagedOperation(description="Clears the cache for messages from members with different versions")
    public <T extends TP> T clearDifferentVersionCache() {
        if(suppress_log_different_version != null)
            suppress_log_different_version.getCache().clear();
        return (T)this;
    }


    @ManagedAttribute(description="Type of logger used")
    public static String loggerType() {return LogFactory.loggerType();}
    /* --------------------------------------------- Fields ------------------------------------------------------ */

    @ManagedOperation(description="If enabled, the timer will run non-blocking tasks on its own (runner) thread, and " +
      "not submit them to the thread pool. Otherwise, all tasks are submitted to the thread pool. This attribute is " +
      "experimental and may be removed without notice.")
    public <T extends TP> T enableBlockingTimerTasks(boolean flag) {
        if(flag != this.timer_handle_non_blocking_tasks) {
            this.timer_handle_non_blocking_tasks=flag;
            timer.setNonBlockingTaskHandling(flag);
        }
        return (T)this;
    }


    /** The address (host and port) of this member */
    protected Address         local_addr;
    protected PhysicalAddress local_physical_addr;
    protected volatile        View view;

    /** The members of this group (updated when a member joins or leaves). With a shared transport,
     * members contains *all* members from all channels sitting on the shared transport */
    protected final Set<Address> members=new CopyOnWriteArraySet<>();


    //http://jira.jboss.org/jira/browse/JGRP-849
    protected final ReentrantLock connectLock = new ReentrantLock();
    

    // ================================== Thread pool ======================

    /** The thread pool which handles unmarshalling, version checks and dispatching of messages */
    protected Executor                thread_pool;

    /** Factory which is used by the thread pool */
    protected ThreadFactory           thread_factory;

    protected ThreadFactory           internal_thread_factory;

    protected Executor                internal_pool; // only created if thread_pool is enabled, to handle internal msgs

    // ================================== Timer thread pool  =========================
    protected TimeScheduler           timer;

    protected TimeService             time_service;


    // ================================= Default SocketFactory ========================
    protected SocketFactory           socket_factory=new DefaultSocketFactory();

    protected Bundler                 bundler;

    protected MessageProcessingPolicy msg_processing_policy=new MaxOneThreadPerSender();

    protected DiagnosticsHandler      diag_handler;
    protected final List<DiagnosticsHandler.ProbeHandler> preregistered_probe_handlers=new LinkedList<>();


    /** The header including the cluster name, sent with each message. Not used with a shared transport (instead
     * TP.ProtocolAdapter attaches the header to the message */
    protected TpHeader                header;


    /**
     * Cache which maintains mappings between logical and physical addresses. When sending a message to a logical
     * address,  we look up the physical address from logical_addr_cache and send the message to the physical address<br/>
     * The keys are logical addresses, the values physical addresses
     */
    protected LazyRemovalCache<Address,PhysicalAddress> logical_addr_cache;

    // last time (in ns) we sent a discovery request
    protected long last_discovery_request;

    protected Future<?> logical_addr_cache_reaper;

    protected final AverageMinMax avg_batch_size=new AverageMinMax();

    protected static final LazyRemovalCache.Printable<Address,LazyRemovalCache.Entry<PhysicalAddress>> print_function
      =(logical_addr, entry) -> {
        StringBuilder sb=new StringBuilder();
        String tmp_logical_name=NameCache.get(logical_addr);
        if(tmp_logical_name != null)
            sb.append(tmp_logical_name).append(": ");
        if(logical_addr instanceof UUID)
            sb.append(((UUID)logical_addr).toStringLong()).append(": ");
        sb.append(entry.toString(val -> val instanceof PhysicalAddress? val.printIpAddress() : val.toString()));
        sb.append("\n");
        return sb.toString();
    };

    /** Cache keeping track of WHO_HAS requests for physical addresses (given a logical address) and expiring
     * them after who_has_cache_timeout ms */
    protected ExpiryCache<Address>   who_has_cache;

    /** Log to suppress identical warnings for messages from members with different (incompatible) versions */
    protected SuppressLog<Address>   suppress_log_different_version;

    /** Log to suppress identical warnings for messages from members in different clusters */
    protected SuppressLog<Address>   suppress_log_different_cluster;

    




    /**
     * Creates the TP protocol, and initializes the state variables, does
     * however not start any sockets or threads.
     */
    protected TP() {
    }

    public MsgStats getMessageStats() {return msg_stats;}

    /** Whether or not hardware multicasting is supported */
    public abstract boolean supportsMulticasting();

    public boolean isMulticastCapable() {return supportsMulticasting();}

    public String toString() {
        return local_addr != null? getName() + "(local address: " + local_addr + ')' : getName();
    }

    @ManagedAttribute(description="The address of the channel")
    public String  getLocalAddress() {return local_addr != null? local_addr.toString() : null;}
    public Address localAddress()    {return local_addr;}
    public View    view()            {return view;}

    @ManagedAttribute(description="The physical address of the channel")
    public String getLocalPhysicalAddress() {return local_physical_addr != null? local_physical_addr.printIpAddress() : null;}



    public void resetStats() {
        msg_stats.reset();
        avg_batch_size.clear();
        msg_processing_policy.reset();
    }

    public <T extends TP> T registerProbeHandler(DiagnosticsHandler.ProbeHandler handler) {
        if(diag_handler != null)
            diag_handler.registerProbeHandler(handler);
        else {
            synchronized(preregistered_probe_handlers) {
                preregistered_probe_handlers.add(handler);
            }
        }
        return (T)this;
    }

    public <T extends TP> T unregisterProbeHandler(DiagnosticsHandler.ProbeHandler handler) {
        if(diag_handler != null)
            diag_handler.unregisterProbeHandler(handler);
        return (T)this;
    }

    public DiagnosticsHandler getDiagnosticsHandler() {return diag_handler;}

    /**
     * Sets a {@link DiagnosticsHandler}. Should be set before the stack is started
     * @param handler
     */
    public <T extends TP> T setDiagnosticsHandler(DiagnosticsHandler handler) throws Exception {
        if(handler != null) {
            if(diag_handler != null)
                diag_handler.stop();
            diag_handler=handler;
            if(diag_handler != null)
                diag_handler.start();
        }
        return (T)this;
    }

    public Bundler getBundler() {return bundler;}

    /** Installs a bundler. Needs to be done before the channel is connected */
    public <T extends TP> T setBundler(Bundler bundler) {
        if(bundler != null)
            this.bundler=bundler;
        return (T)this;
    }


    public Executor getThreadPool() {
        return thread_pool;
    }

    public <T extends TP> T setThreadPool(Executor thread_pool) {
        if(this.thread_pool != null)
            shutdownThreadPool(this.thread_pool);
        this.thread_pool=thread_pool;
        if(timer instanceof TimeScheduler3)
            ((TimeScheduler3)timer).setThreadPool(thread_pool);
        return (T)this;
    }

    public ThreadFactory getThreadPoolThreadFactory() {
        return thread_factory;
    }

    public <T extends TP> T setThreadPoolThreadFactory(ThreadFactory factory) {
        thread_factory=factory;
        if(thread_pool instanceof ThreadPoolExecutor)
            ((ThreadPoolExecutor)thread_pool).setThreadFactory(factory);
        return (T)this;
    }

    public Executor getInternalThreadPool() {return internal_pool;}

    public <T extends TP> T setInternalThreadPool(Executor thread_pool) {
        if(this.internal_pool != null)
            shutdownThreadPool(this.internal_pool);
        this.internal_pool=thread_pool;
        return (T)this;
    }

    public ThreadFactory getInternalThreadPoolThreadFactory() {return internal_thread_factory;}

    public <T extends TP> T setInternalThreadPoolThreadFactory(ThreadFactory factory) {
        internal_thread_factory=factory;
        if(internal_pool instanceof ThreadPoolExecutor)
            ((ThreadPoolExecutor)internal_pool).setThreadFactory(factory);
        return (T)this;
    }

    public TimeScheduler getTimer() {return timer;}

    /**
     * Sets a new timer. This should be done before the transport is initialized; be very careful, as replacing a
     * running timer with tasks in it can wreak havoc !
     * @param timer
     */
    public <T extends TP> T setTimer(TimeScheduler timer) {this.timer=timer; return (T)this;}

    public TimeService getTimeService() {return time_service;}

    public <T extends TP> T setTimeService(TimeService ts) {
        if(ts == null)
            return (T)this;
        if(time_service != null)
            time_service.stop();
        time_service=ts;
        time_service.start();
        return (T)this;
    }

    public ThreadFactory getThreadFactory() {
        return thread_factory;
    }

    public <T extends TP> T setThreadFactory(ThreadFactory factory) {thread_factory=factory; return (T)this;}

    public SocketFactory getSocketFactory() {
        return socket_factory;
    }

    @Override public void setSocketFactory(SocketFactory factory) {
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


    public long                   getNumMessagesSent()              {return msg_stats.getNumMsgsSent();}
    public <T extends TP> T       incrBatchesSent(int delta)        {if(stats) msg_stats.incrNumBatchesSent(delta); return (T)this;}
    public <T extends TP> T       incrNumSingleMsgsSent(int d)      {if(stats) msg_stats.incrNumSingleMsgsSent(d); return (T)this;}
    public InetAddress            getBindAddress()                  {return bind_addr;}
    public <T extends TP> T       setBindAddress(InetAddress a)     {this.bind_addr=a; return (T)this;}
    public int                    getBindPort()                     {return bind_port;}
    public <T extends TP> T       setBindPort(int port)             {this.bind_port=port; return (T)this;}
    public <T extends TP> T       setBindToAllInterfaces(boolean f) {this.receive_on_all_interfaces=f; return (T)this;}
    public boolean                isReceiveOnAllInterfaces()        {return receive_on_all_interfaces;}
    public List<NetworkInterface> getReceiveInterfaces()            {return receive_interfaces;}
    public <T extends TP> T       setPortRange(int range)           {this.port_range=range; return (T)this;}
    public int                    getPortRange()                    {return port_range;}



    @ManagedAttribute(description="Current number of threads in the thread pool")
    public int getThreadPoolSize() {
        if(thread_pool instanceof ThreadPoolExecutor)
            return ((ThreadPoolExecutor)thread_pool).getPoolSize();
        if(thread_pool instanceof ForkJoinPool)
            return ((ForkJoinPool)thread_pool).getPoolSize();
        return 0;
    }


    @ManagedAttribute(description="Current number of active threads in the thread pool")
    public int getThreadPoolSizeActive() {
        if(thread_pool instanceof ThreadPoolExecutor)
            return ((ThreadPoolExecutor)thread_pool).getActiveCount();
        if(thread_pool instanceof ForkJoinPool)
            return ((ForkJoinPool)thread_pool).getRunningThreadCount();
        return 0;
    }

    @ManagedAttribute(description="Largest number of threads in the thread pool")
    public int getThreadPoolSizeLargest() {
        if(thread_pool instanceof ThreadPoolExecutor)
            return ((ThreadPoolExecutor)thread_pool).getLargestPoolSize();
        return 0;
    }

    @ManagedAttribute(description="Current number of threads in the internal thread pool")
    public int getInternalThreadPoolSize() {
        if(internal_pool instanceof ThreadPoolExecutor)
            return ((ThreadPoolExecutor)internal_pool).getPoolSize();
        return 0;
    }

    @ManagedAttribute(description="Largest number of threads in the internal thread pool")
    public int getInternalThreadPoolSizeLargest() {
        if(internal_pool instanceof ThreadPoolExecutor)
            return ((ThreadPoolExecutor)internal_pool).getLargestPoolSize();
        return 0;
    }

    @ManagedAttribute(name="timer_tasks",description="Number of timer tasks queued up for execution")
    public int getNumTimerTasks() {return timer.size();}

    @ManagedOperation public String dumpTimerTasks() {return timer.dumpTimerTasks();}

    @ManagedOperation(description="Purges cancelled tasks from the timer queue")
    public void removeCancelledTimerTasks() {timer.removeCancelledTasks();}

    @ManagedAttribute(description="Number of threads currently in the pool")
    public int getTimerThreads() {return timer.getCurrentThreads();}

    @ManagedAttribute(description="Returns the number of live threads in the JVM")
    public static int getNumThreads() {return ManagementFactory.getThreadMXBean().getThreadCount();}

    @ManagedAttribute(description="Whether the diagnostics handler is running or not")
    public boolean isDiagnosticsRunning() {return diag_handler != null && diag_handler.isRunning();}

    public <T extends TP> T setLogDiscardMessages(boolean flag)     {log_discard_msgs=flag; return (T)this;}
    public boolean          getLogDiscardMessages()                 {return log_discard_msgs;}
    public <T extends TP> T setLogDiscardMessagesVersion(boolean f) {log_discard_msgs_version=f; return (T)this;}
    public boolean          getLogDiscardMessagesVersion()          {return log_discard_msgs_version;}
    public boolean          getUseIpAddresses()                     {return use_ip_addrs;}
    public boolean          isDiagnosticsEnabled()                  {return enable_diagnostics;}
    public <T extends TP> T setDiagnosticsEnabled(boolean f)        {enable_diagnostics=f; return (T)this;}
    public boolean          isDiagUdEnabled()                       {return diag_handler != null && diag_handler.udpEnabled();}
    public <T extends TP> T diagEnableUdp(boolean f)                {diag_enable_udp=f;
                                                                       if(diag_handler != null) diag_handler.enableUdp(f); return (T)this;}
    public boolean          diagTcpEnabled()                        {return diag_enable_tcp;}
    public <T extends TP> T diagEnableTcp(boolean f)                {diag_enable_tcp=f;
                                                                       if(diag_handler != null) diag_handler.enableTcp(f); return (T)this;}

    @ManagedOperation(description="Dumps the contents of the logical address cache")
    public String printLogicalAddressCache() {
        return logical_addr_cache.size() + " elements:\n" + logical_addr_cache.printCache(print_function);
    }

    @ManagedOperation(description="Prints the contents of the who-has cache")
    public String printWhoHasCache() {return who_has_cache.toString();}

    @ManagedOperation(description="Evicts elements in the logical address cache which have expired")
    public void evictLogicalAddressCache() {
        evictLogicalAddressCache(false);
    }

    public void evictLogicalAddressCache(boolean force) {
        logical_addr_cache.removeMarkedElements(force);
        fetchLocalAddresses();
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
        this.id=ClassConfigurator.getProtocolId(TP.class);

        if(thread_factory == null)
            //thread_factory=new DefaultThreadFactory("jgroups", false, true);
          thread_factory=new LazyThreadFactory("jgroups", false, true);

        if(internal_thread_factory == null)
            internal_thread_factory=new LazyThreadFactory("jgroups-int", false, true);
        
        // local_addr is null when shared transport, channel_name is not used
        setInAllThreadFactories(cluster_name != null? cluster_name.toString() : null, local_addr, thread_naming_pattern);

        if(enable_diagnostics && diag_handler == null)
            diag_handler=createDiagnosticsHandler();

        who_has_cache=new ExpiryCache<>(who_has_cache_timeout);

        if(suppress_time_different_version_warnings > 0)
            suppress_log_different_version=new SuppressLog<>(log, "VersionMismatch", "SuppressMsg");
        if(suppress_time_different_cluster_warnings > 0)
            suppress_log_different_cluster=new SuppressLog<>(log, "MsgDroppedDiffCluster", "SuppressMsg");


        // ====================================== Thread pool ===========================
        if(use_common_fork_join_pool)
            use_fork_join_pool=true;
        if(use_fork_join_pool)
            thread_pool_max_threads=Runtime.getRuntime().availableProcessors();
        if(thread_pool == null || (thread_pool instanceof ExecutorService && ((ExecutorService)thread_pool).isShutdown())) {
            if(thread_pool_enabled) {
                int num_cores=Runtime.getRuntime().availableProcessors();
                int max_internal_size=Math.max(4, num_cores);
                log.debug("thread pool min/max/keep-alive: %d/%d/%d use_fork_join=%b, internal pool: %d/%d/%d (%d cores available)",
                          thread_pool_min_threads, thread_pool_max_threads, thread_pool_keep_alive_time, use_fork_join_pool,
                          0, max_internal_size, 30000, num_cores);
                thread_pool=createThreadPool(thread_pool_min_threads, thread_pool_max_threads, thread_pool_keep_alive_time,
                                             "abort", new SynchronousQueue<>(), thread_factory, log, use_fork_join_pool, use_common_fork_join_pool);
                internal_pool=createThreadPool(0, max_internal_size, 30000, "abort",
                                               new SynchronousQueue<>(), internal_thread_factory, log, false, false);
            }
            else // otherwise use the caller's thread to unmarshal the byte buffer into a message
                thread_pool=new DirectExecutor();
        }


        // ========================================== Timer ==============================
        if(timer == null) {
            timer=new TimeScheduler3(thread_pool, thread_factory, false); // don't start the timer thread yet (JGRP-2332)
            timer.setNonBlockingTaskHandling(timer_handle_non_blocking_tasks);
        }

        if(time_service_interval > 0)
            time_service=new TimeService(timer, time_service_interval);

        Map<String, Object> m=new HashMap<>(2);
        if(bind_addr != null)
            m.put("bind_addr", bind_addr);
        if(external_addr != null)
            m.put("external_addr", external_addr);
        if(external_port > 0)
            m.put("external_port", external_port);
        if(!m.isEmpty())
            up(new Event(Event.CONFIG, m));

        logical_addr_cache=new LazyRemovalCache<>(logical_addr_cache_max_size, logical_addr_cache_expiration);
        
        if(logical_addr_cache_reaper_interval > 0 && (logical_addr_cache_reaper == null || logical_addr_cache_reaper.isDone())) {
            logical_addr_cache_reaper=timer.scheduleWithFixedDelay(new Runnable() {
                public void run() {
                    evictLogicalAddressCache();
                }

                public String toString() {
                    return TP.this.getClass().getSimpleName() + ": LogicalAddressCacheReaper (interval=" + logical_addr_cache_reaper_interval + " ms)";
                }
            }, logical_addr_cache_reaper_interval, logical_addr_cache_reaper_interval, TimeUnit.MILLISECONDS, false);
        }

        if(message_processing_policy != null)
            setMessageProcessingPolicy(message_processing_policy);
        else
            msg_processing_policy.init(this);
    }


    /** Creates the unicast and multicast sockets and starts the unicast and multicast receiver threads */
    public void start() throws Exception {
        timer.start();
        if(time_service != null)
            time_service.start();
        if(use_ip_addrs) {
            PhysicalAddress tmp=getPhysicalAddress();
            if(tmp instanceof IpAddress) {
                local_addr=new IpAddressUUID(((IpAddress)tmp).getIpAddress(), ((IpAddress)tmp).getPort());
                stack.getTopProtocol().down(new Event(Event.SET_LOCAL_ADDRESS, local_addr));
                stack.getTopProtocol().up(new Event(Event.SET_LOCAL_ADDRESS, local_addr));
            }
        }
        fetchLocalAddresses();
        startDiagnostics();
        if(bundler == null) {
            bundler=createBundler(bundler_type);
            bundler.init(this);
            bundler.start();
        }
        // local_addr is null when shared transport
        setInAllThreadFactories(cluster_name != null? cluster_name.toString() : null, local_addr, thread_naming_pattern);
    }

    public void stop() {
        stopDiagnostics();
        if(bundler != null) {
            bundler.stop();
            bundler=null;
        }
        if(msg_processing_policy != null)
            msg_processing_policy.destroy();

        if(time_service != null)
            time_service.stop();
        timer.stop();
    }

    public void destroy() {
        super.destroy();

        if(logical_addr_cache_reaper != null) {
            logical_addr_cache_reaper.cancel(false);
            logical_addr_cache_reaper=null;
        }

        // Stop the thread pool
        if(thread_pool instanceof ExecutorService)
            shutdownThreadPool(thread_pool);

        if(internal_pool instanceof ExecutorService)
            shutdownThreadPool(internal_pool);
    }


    @ManagedAttribute(description="Returns stats about the current bundler")
    public String bundlerStats() {
        Map<String,Object> tmp=bundler != null? bundler.getStats() : null;
        return tmp != null? tmp.toString() : "n/a";
    }

    @ManagedOperation(description="Resets stats of the current bundler")
    public void bundlerStatsReset() {bundler.resetStats();}

    @ManagedOperation(description="Creates and sets a new bundler. Type has to be either a bundler_type or the fully " +
      "qualified classname of a Bundler impl. Stops the current bundler (if running)")
    public <T extends TP> T bundler(String type) {
        Bundler new_bundler=createBundler(type);
        String old_bundler_class=null;
        if(bundler != null) {
            bundler.stop();
            old_bundler_class=bundler.getClass().getName();
        }
        new_bundler.init(this);
        new_bundler.start();
        bundler=new_bundler;
        bundler_type=type;
        if(old_bundler_class != null)
            log.debug("%s: replaced bundler %s with %s", local_addr, old_bundler_class, bundler.getClass().getName());
        return (T)this;
    }




    @ManagedOperation(description="Enables diagnostics and starts DiagnosticsHandler (if not running)")
    public void enableDiagnostics() {
        enable_diagnostics=true;
        try {
            startDiagnostics();
        }
        catch(Exception e) {
            log.error(Util.getMessage("FailedStartingDiagnostics"), e);
        }
    }

    @ManagedOperation(description="Disables diagnostics and stops DiagnosticsHandler (if running)")
    public void disableDiagnostics() {
        enable_diagnostics=false;
        stopDiagnostics();
    }

    protected void startDiagnostics() throws Exception {
        if(enable_diagnostics) {
            if(diag_handler == null)
                diag_handler=createDiagnosticsHandler();
            diag_handler.registerProbeHandler(this);
            diag_handler.start();
            synchronized(preregistered_probe_handlers) {
                for(DiagnosticsHandler.ProbeHandler handler : preregistered_probe_handlers)
                    diag_handler.registerProbeHandler(handler);
            }
        }
        synchronized(preregistered_probe_handlers) {
            preregistered_probe_handlers.clear(); // https://issues.jboss.org/browse/JGRP-1834
        }
    }

    protected void stopDiagnostics() {
        if(diag_handler != null) {
            diag_handler.unregisterProbeHandler(this);
            diag_handler.stop();
        }
        synchronized(preregistered_probe_handlers) {
            preregistered_probe_handlers.clear();
        }
    }


    public Map<String, String> handleProbe(String... keys) {
        Map<String,String> retval=new HashMap<>(keys != null? keys.length : 2);
        if(keys == null)
            return retval;

        for(String key: keys) {
            switch(key) {
                case "dump":
                    retval.put(key, Util.dumpThreads());
                    break;
                case "uuids":
                    retval.put(key, printLogicalAddressCache());
                    if(!retval.containsKey("local_addr"))
                        retval.put("local_addr", local_addr != null? local_addr.toString() : null);
                    break;
                case "keys":
                    StringBuilder sb=new StringBuilder();
                    if(diag_handler != null) {
                        for(DiagnosticsHandler.ProbeHandler handler : diag_handler.getProbeHandlers()) {
                            String[] tmp=handler.supportedKeys();
                            if(tmp != null && tmp.length > 0) {
                                for(String s : tmp)
                                    sb.append(s).append(" ");
                            }
                        }
                    }
                    retval.put(key, sb.toString());
                    break;
                case "member-addrs":
                    Set<PhysicalAddress> physical_addrs=logical_addr_cache.nonRemovedValues();
                    String list=Util.print(physical_addrs);
                    retval.put(key, list);
                break;
            }
        }
        return retval;
    }

    public String[] supportedKeys() {
        return new String[]{"dump", "keys", "uuids", "member-addrs"};
    }


    protected void handleConnect() throws Exception {
    }

    protected void handleDisconnect() {
    }


    public Object down(Event evt) {
        switch(evt.getType()) {

            case Event.TMP_VIEW:
            case Event.VIEW_CHANGE:
                Collection<Address> old_members;
                synchronized(members) {
                    View v=evt.getArg();
                    this.view=v;
                    old_members=new ArrayList<>(members);
                    members.clear();
                    members.addAll(v.getMembers());

                    // fix for https://jira.jboss.org/jira/browse/JGRP-918
                    logical_addr_cache.retainAll(members);
                    fetchLocalAddresses();

                    List<Address> left_mbrs=Util.leftMembers(old_members,members);
                    if(left_mbrs != null && !left_mbrs.isEmpty())
                        NameCache.removeAll(left_mbrs);

                    if(suppress_log_different_version != null)
                        suppress_log_different_version.removeExpired(suppress_time_different_version_warnings);
                    if(suppress_log_different_cluster != null)
                        suppress_log_different_cluster.removeExpired(suppress_time_different_cluster_warnings);
                }
                who_has_cache.removeExpiredElements();
                if(bundler != null)
                    bundler.viewChange(evt.getArg());
                if(msg_processing_policy instanceof MaxOneThreadPerSender)
                    ((MaxOneThreadPerSender)msg_processing_policy).viewChange(view.getMembers());
                break;

            case Event.CONNECT:
            case Event.CONNECT_WITH_STATE_TRANSFER:
            case Event.CONNECT_USE_FLUSH:
            case Event.CONNECT_WITH_STATE_TRANSFER_USE_FLUSH:
                cluster_name=new AsciiString((String)evt.getArg());
                header=new TpHeader(cluster_name);

                // local_addr is null when shared transport
                setInAllThreadFactories(cluster_name != null? cluster_name.toString() : null, local_addr, thread_naming_pattern);
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
                Address addr=evt.getArg();
                PhysicalAddress physical_addr=getPhysicalAddressFromCache(addr);
                if(physical_addr != null)
                    return physical_addr;
                if(Objects.equals(addr, local_addr)) {
                    physical_addr=getPhysicalAddress();
                    if(physical_addr != null)
                        addPhysicalAddressToCache(addr, physical_addr);
                }
                return physical_addr;

            case Event.GET_PHYSICAL_ADDRESSES:
                return getAllPhysicalAddressesFromCache();

            case Event.GET_LOGICAL_PHYSICAL_MAPPINGS:
                Object arg=evt.getArg();
                boolean skip_removed_values=arg instanceof Boolean && (Boolean)arg;
                return logical_addr_cache.contents(skip_removed_values);

            case Event.ADD_PHYSICAL_ADDRESS:
                Tuple<Address,PhysicalAddress> tuple=evt.getArg();
                return addPhysicalAddressToCache(tuple.getVal1(), tuple.getVal2());

            case Event.REMOVE_ADDRESS:
                removeLogicalAddressFromCache(evt.getArg());
                break;

            case Event.SET_LOCAL_ADDRESS:
                local_addr=evt.getArg();
                registerLocalAddress(evt.getArg());
                break;
        }
        return null;
    }

    /** A message needs to be sent to a single member or all members */
    public Object down(Message msg) {
        if(header != null)
            msg.putHeader(this.id, header); // added patch by Roland Kurmann (March 20 2003)

        setSourceAddress(msg); // very important !! listToBuffer() will fail with a null src address !!

        Address dest=msg.getDest(), sender=msg.getSrc();
        if(is_trace)
            log.trace("%s: sending msg to %s, src=%s, headers are %s", local_addr, dest, sender, msg.printHeaders());

        // Don't send if dest is local address. Instead, send it up the stack. If multicast message, loop back directly
        // to us (but still multicast). Once we receive this, we discard our own multicast message
        boolean multicast=dest == null, do_send=multicast || !dest.equals(sender),
          loop_back=(multicast || dest.equals(sender)) && !msg.isTransientFlagSet(Message.TransientFlag.DONT_LOOPBACK);

        if(dest instanceof PhysicalAddress && dest.equals(local_physical_addr)) {
            loop_back=true;
            do_send=false;
        }

        if(loopback_separate_thread) {
            if(loop_back)
                loopback(msg, multicast);
            if(do_send)
                _send(msg, dest);
        }
        else {
            if(do_send)
                _send(msg, dest);
            if(loop_back)
                loopback(msg, multicast);
        }
        return null;
    }



    /*--------------------------- End of Protocol interface -------------------------- */


    /* ------------------------------ Private Methods -------------------------------- */

  protected DiagnosticsHandler createDiagnosticsHandler() {
      return new DiagnosticsHandler(diagnostics_addr, diagnostics_port, diagnostics_bind_interfaces,
                                    diagnostics_ttl, log, getSocketFactory(), getThreadFactory(), diagnostics_passcode)
        .transport(this).setDiagnosticsBindAddress(diagnostics_bind_addr)
        .enableUdp(diag_enable_udp).enableTcp(diag_enable_tcp)
        .setDiagnosticsPortRange(diagnostics_port_range);
  }

    protected Bundler createBundler(String type) {
        if(type == null)
            throw new IllegalArgumentException("bundler type has to be non-null");

        switch(type) {
            case "transfer-queue":
            case "tq":
                return new TransferQueueBundler(bundler_capacity);
            case "simplified-transfer-queue":
            case "stq":
                return new SimplifiedTransferQueueBundler(bundler_capacity);
            case "sender-sends":
            case "ss":
                return new SenderSendsBundler();
            case "ring-buffer":
            case "rb":
                return new RingBufferBundler(bundler_capacity).numSpins(bundler_num_spins).waitStrategy(bundler_wait_strategy);
            case "ring-buffer-lockless":
            case "rbl":
                return new RingBufferBundlerLockless(bundler_capacity);
            case "ring-buffer-lockless2":
            case "rbl2":
                return new RingBufferBundlerLockless2(bundler_capacity);
            case "no-bundler":
            case "nb":
                return new NoBundler();
            case "async-no-bundler":
            case "anb":
                return new AsyncNoBundler();
            case "ab":
            case "alternating-bundler":
                return new AlternatingBundler();
            case "rqb": case "rq":
            case "remove-queue-bundler": case "remove-queue":
                return new RemoveQueueBundler();
        }

        try {
            Class<Bundler> clazz=Util.loadClass(type, getClass());
            return clazz.getDeclaredConstructor().newInstance();
        }
        catch(Throwable t) {
            log.warn("failed creating instance of bundler %s: %s", type, t);
        }
        return new TransferQueueBundler(bundler_capacity);
    }

    protected void loopback(Message msg, final boolean multicast) {
        final Message copy=loopback_copy? msg.copy() : msg;
        if(is_trace)
            log.trace("%s: looping back message %s, headers are %s", local_addr, copy, copy.printHeaders());

        if(!loopback_separate_thread) {
            passMessageUp(copy, null, false, multicast, false);
            return;
        }

        // changed to fix http://jira.jboss.com/jira/browse/JGRP-506
        boolean internal=msg.isFlagSet(Message.Flag.INTERNAL);
        boolean oob=msg.isFlagSet(Message.Flag.OOB);
        // submitToThreadPool(() -> passMessageUp(copy, null, false, multicast, false), internal);
        msg_processing_policy.loopback(msg, oob, internal);
    }

    protected void _send(Message msg, Address dest) {
        try {
            send(msg);
        }
        catch(InterruptedIOException iex) {
        }
        catch(InterruptedException interruptedEx) {
            Thread.currentThread().interrupt(); // let someone else handle the interrupt
        }
        catch(Throwable e) {
            log.trace(Util.getMessage("SendFailure"),
                      local_addr, (dest == null? "cluster" : dest), msg.size(), e.toString(), msg.printHeaders());
        }
    }



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


    public void passMessageUp(Message msg, byte[] cluster_name, boolean perform_cluster_name_matching,
                              boolean multicast, boolean discard_own_mcast) {
        if(is_trace)
            log.trace("%s: received %s, headers are %s", local_addr, msg, msg.printHeaders());

        if(up_prot == null)
            return;

        if(multicast && discard_own_mcast && local_addr != null && local_addr.equals(msg.getSrc()))
            return;

        // Discard if message's cluster name is not the same as our cluster name
        if(perform_cluster_name_matching && this.cluster_name != null && !this.cluster_name.equals(cluster_name)) {
            if(log_discard_msgs && log.isWarnEnabled()) {
                Address sender=msg.getSrc();
                if(suppress_log_different_cluster != null)
                    suppress_log_different_cluster.log(SuppressLog.Level.warn, sender,
                                                       suppress_time_different_cluster_warnings,
                                                       new AsciiString(cluster_name),this.cluster_name, sender);
                else
                    log.warn(Util.getMessage("MsgDroppedDiffCluster"), new AsciiString(cluster_name),this.cluster_name, sender);
            }
            return;
        }
        up_prot.up(msg);
    }


    public void passBatchUp(MessageBatch batch, boolean perform_cluster_name_matching, boolean discard_own_mcast) {
        if(is_trace)
            log.trace("%s: received message batch of %d messages from %s", local_addr, batch.size(), batch.sender());
        if(up_prot == null)
            return;

        // Discard if message's cluster name is not the same as our cluster name
        if(perform_cluster_name_matching && cluster_name != null && !cluster_name.equals(batch.clusterName())) {
            if(log_discard_msgs && log.isWarnEnabled()) {
                Address sender=batch.sender();
                if(suppress_log_different_cluster != null)
                    suppress_log_different_cluster.log(SuppressLog.Level.warn, sender,
                                                       suppress_time_different_cluster_warnings,
                                                       batch.clusterName(),cluster_name, sender);
                else
                    log.warn(Util.getMessage("BatchDroppedDiffCluster"), batch.clusterName(),cluster_name, sender);
            }
            return;
        }

        if(batch.multicast() && discard_own_mcast && local_addr != null && local_addr.equals(batch.sender()))
            return;
        up_prot.up(batch);
    }


    /**
     * Subclasses must call this method when a unicast or multicast message has been received.
     */
    public void receive(Address sender, byte[] data, int offset, int length) {
        if(data == null) return;

        // drop message from self; it has already been looped back up (https://issues.jboss.org/browse/JGRP-1765)
        if(Objects.equals(local_physical_addr, sender))
            return;

        // the length of a message needs to be at least 3 bytes: version (2) and flags (1) // JGRP-2210
        if(length < Global.SHORT_SIZE + Global.BYTE_SIZE)
            return;

        short version=Bits.readShort(data, offset);
        if(!versionMatch(version, sender))
            return;
        offset+=Global.SHORT_SIZE;
        byte flags=data[offset];
        offset+=Global.BYTE_SIZE;

        boolean is_message_list=(flags & LIST) == LIST, multicast=(flags & MULTICAST) == MULTICAST;
        ByteArrayDataInputStream in=new ByteArrayDataInputStream(data, offset, length);
        if(is_message_list) // used if message bundling is enabled
            handleMessageBatch(in, multicast);
        else
            handleSingleMessage(in, multicast);
    }

    public void receive(Address sender, DataInput in) throws Exception {
        if(in == null) return;

        // drop message from self; it has already been looped back up (https://issues.jboss.org/browse/JGRP-1765)
        if(Objects.equals(local_physical_addr, sender))
            return;

        short version=in.readShort();
        if(!versionMatch(version, sender))
            return;
        byte flags=in.readByte();

        boolean is_message_list=(flags & LIST) == LIST, multicast=(flags & MULTICAST) == MULTICAST;
        if(is_message_list) // used if message bundling is enabled
            handleMessageBatch(in, multicast);
        else
            handleSingleMessage(in, multicast);
    }


    protected void handleMessageBatch(DataInput in, boolean multicast) {
        try {
            final MessageBatch[] batches=Util.readMessageBatch(in, multicast);
            final MessageBatch batch=batches[0], oob_batch=batches[1], internal_batch_oob=batches[2], internal_batch=batches[3];

            processBatch(oob_batch,          true,  false);
            processBatch(batch,              false, false);
            processBatch(internal_batch_oob, true,  true);
            processBatch(internal_batch,     false, true);
        }
        catch(Throwable t) {
            log.error(String.format(Util.getMessage("IncomingMsgFailure"), local_addr), t);
        }
    }


    protected void handleSingleMessage(DataInput in, boolean multicast) {
        try {
            Message msg=new Message(false); // don't create headers, readFrom() will do this
            msg.readFrom(in);

            if(!multicast && unicastDestMismatch(msg.getDest()))
                return;

            boolean oob=msg.isFlagSet(Message.Flag.OOB), internal=msg.isFlagSet(Message.Flag.INTERNAL);
            msg_processing_policy.process(msg, oob, internal);
        }
        catch(Throwable t) {
            log.error(String.format(Util.getMessage("IncomingMsgFailure"), local_addr), t);
        }
    }

    protected void processBatch(MessageBatch batch, boolean oob, boolean internal) {
        try {
            if(batch != null && !batch.isEmpty())
                msg_processing_policy.process(batch, oob, internal);
        }
        catch(Throwable t) {
            log.error("processing batch failed", t);
        }
    }

    public boolean unicastDestMismatch(Address dest) {
        return dest != null && !(Objects.equals(dest, local_addr) || Objects.equals(dest, local_physical_addr));
    }


    public boolean submitToThreadPool(Runnable task, boolean spawn_thread_on_rejection) {
        return submitToThreadPool(thread_pool, task, spawn_thread_on_rejection, true);
    }

    public boolean submitToThreadPool(Executor pool, Runnable task, boolean spawn_thread_on_rejection, boolean forward_to_internal_pool) {
        try {
            pool.execute(task);
        }
        catch(RejectedExecutionException ex) {
            if(!spawn_thread_on_rejection) {
                msg_stats.incrNumRejectedMsgs(1);
                return false;
            }

            if(forward_to_internal_pool && internal_pool != null)
                return submitToThreadPool(internal_pool, task, true, false);
            else {
                msg_stats.incrNumThreadsSpawned(1);
                return runInNewThread(task);
            }
        }
        catch(Throwable t) {
            log.error("failure submitting task to thread pool", t);
            msg_stats.incrNumRejectedMsgs(1);
            return false;
        }
        return true;
    }


    protected boolean runInNewThread(Runnable task) {
        try {
            Thread thread=thread_factory != null? thread_factory.newThread(task, "jgroups-temp-thread")
              : new Thread(task, "jgroups-temp-thread");
            thread.start();
            return true;
        }
        catch(Throwable t) {
            log.error("failed spawning new thread", t);
            return false;
        }
    }


    protected boolean versionMatch(short version, Address sender) {
        boolean match=Version.isBinaryCompatible(version);
        if(!match && log_discard_msgs_version && log.isWarnEnabled()) {
            if(suppress_log_different_version != null)
                suppress_log_different_version.log(SuppressLog.Level.warn, sender,
                        suppress_time_different_version_warnings,
                        sender, Version.print(version), Version.printVersion());
            else
                log.warn(Util.getMessage("VersionMismatch"), sender, Version.print(version), Version.printVersion());
        }
        return match;
    }



    /** Serializes and sends a message. This method is not reentrant */
    protected void send(Message msg) throws Exception {
        // bundle all messages, even the ones tagged with DONT_BUNDLE: https://issues.jboss.org/browse/JGRP-1737
        // remove the ones tagged as OOB|DONT_BUNDLE at the receiver and pass them up individually (in separate threads)
        Bundler tmp_bundler=bundler;
        if(tmp_bundler != null)
            tmp_bundler.send(msg);
    }


    public void doSend(byte[] buf, int offset, int length, Address dest) throws Exception {
        if(stats) {
            msg_stats.incrNumMsgsSent(1);
            msg_stats.incrNumBytesSent(length);
        }
        if(dest == null)
            sendMulticast(buf, offset, length);
        else
            sendToSingleMember(dest, buf, offset, length);
    }


    protected void sendToSingleMember(final Address dest, byte[] buf, int offset, int length) throws Exception {
        if(dest instanceof PhysicalAddress) {
            sendUnicast((PhysicalAddress)dest, buf, offset, length);
            return;
        }

        PhysicalAddress physical_dest;
        if((physical_dest=getPhysicalAddressFromCache(dest)) != null) {
            sendUnicast(physical_dest,buf,offset,length);
            return;
        }

        if(who_has_cache.addIfAbsentOrExpired(dest)) { // true if address was added
            // FIND_MBRS must return quickly
            Responses responses=fetchResponsesFromDiscoveryProtocol(Collections.singletonList(dest));
            try {
                for(PingData data : responses) {
                    if(data.getAddress() != null && data.getAddress().equals(dest)) {
                        if((physical_dest=data.getPhysicalAddr()) != null) {
                            sendUnicast(physical_dest, buf, offset, length);
                            return;
                        }
                    }
                }
                log.warn(Util.getMessage("PhysicalAddrMissing"), local_addr, dest);
            }
            finally {
                responses.done();
            }
        }
    }



    /** Fetches the physical addrs for mbrs and sends the msg to each physical address. Asks discovery for missing
     * members' physical addresses if needed */
    protected void sendToMembers(Collection<Address> mbrs, byte[] buf, int offset, int length) throws Exception {
        List<Address> missing=null;

        if(mbrs == null || mbrs.isEmpty())
            mbrs=logical_addr_cache.keySet();

        for(Address mbr: mbrs) {
            PhysicalAddress target=mbr instanceof PhysicalAddress? (PhysicalAddress)mbr : logical_addr_cache.get(mbr);
            if(target == null) {
                if(missing == null)
                    missing=new ArrayList<>(mbrs.size());
                missing.add(mbr);
                continue;
            }

            try {
                if(!Objects.equals(local_physical_addr, target))
                    sendUnicast(target, buf, offset, length);
            }
            catch(SocketException sock_ex) {
                log.debug(Util.getMessage("FailureSendingToPhysAddr"), local_addr, mbr, sock_ex);
            }
            catch(Throwable t) {
                log.error(Util.getMessage("FailureSendingToPhysAddr"), local_addr, mbr, t);
            }
        }
        if(missing != null)
            fetchPhysicalAddrs(missing);
    }


    protected void fetchPhysicalAddrs(List<Address> missing) {
        long current_time=0;
        boolean do_send=false;
        synchronized(this) {
            if(last_discovery_request == 0 ||
              (current_time=timestamp()) - last_discovery_request >= MIN_WAIT_BETWEEN_DISCOVERIES) {
                last_discovery_request=current_time == 0? timestamp() : current_time;
                do_send=true;
            }
        }
        if(do_send) {
            missing.removeAll(logical_addr_cache.keySet());
            if(!missing.isEmpty()) {  // FIND_MBRS either returns immediately or is processed in a separate thread
                Responses rsps=fetchResponsesFromDiscoveryProtocol(missing);
                rsps.done();
            }
        }
    }

    protected Responses fetchResponsesFromDiscoveryProtocol(List<Address> missing) {
        return (Responses)up_prot.up(new Event(Event.FIND_MBRS, missing));
    }


    protected long timestamp() {return time_service != null? time_service.timestamp() : System.nanoTime();}

    /**
     * Associates the address with the physical address fetched from the cache
     * @param addr
     */
    protected void registerLocalAddress(Address addr) {
        PhysicalAddress physical_addr=getPhysicalAddress();
        if(physical_addr == null)
            return;
        local_physical_addr=physical_addr;
        if(addr != null) {
            if(use_ip_addrs && local_addr instanceof IpAddressUUID)
                addPhysicalAddressToCache(addr, (PhysicalAddress)local_addr, true);
            else
                addPhysicalAddressToCache(addr, physical_addr, true);
        }
    }

    /**
     * Grabs the local address (or addresses in the shared transport case) and registers them with the physical address
     * in the transport's cache
     */
    protected void fetchLocalAddresses() {
        if(local_addr != null)
            registerLocalAddress(local_addr);
        else {
            Address addr=(Address)up_prot.up(new Event(Event.GET_LOCAL_ADDRESS));
            local_addr=addr;
            registerLocalAddress(addr);
        }
    }


    protected void setThreadNames() {
        if(diag_handler != null)
            diag_handler.setThreadNames();
        if(bundler instanceof TransferQueueBundler)
            thread_factory.renameThread(TransferQueueBundler.THREAD_NAME, ((TransferQueueBundler)bundler).getThread());
    }


    protected void unsetThreadNames() {
        if(diag_handler != null)
            diag_handler.unsetThreadNames();
        if(bundler instanceof TransferQueueBundler) {
            Thread thread=((TransferQueueBundler)bundler).getThread();
            if(thread != null)
                thread_factory.renameThread(TransferQueueBundler.THREAD_NAME, thread);
        }
        else if(bundler instanceof RingBufferBundler) {
            Thread thread=((RingBufferBundler)bundler).getThread();
            if(thread != null)
                thread_factory.renameThread(RingBufferBundler.THREAD_NAME, thread);
        }
    }

    protected void setInAllThreadFactories(String cluster_name, Address local_address, String pattern) {
        ThreadFactory[] factories= {thread_factory,internal_thread_factory};

        for(ThreadFactory factory: factories) {
            if(pattern != null)
                factory.setPattern(pattern);
            if(cluster_name != null) // if we have a shared transport, use singleton_name as cluster_name
                factory.setClusterName(cluster_name);
            if(local_address != null)
                factory.setAddress(local_address.toString());
        }
    }



    protected static ExecutorService createThreadPool(int min_threads, int max_threads, long keep_alive_time, String rejection_policy,
                                                      BlockingQueue<Runnable> queue, final ThreadFactory factory, Log log,
                                                      boolean use_fork_join_pool, boolean use_common_fork_join_pool) {
        if(use_fork_join_pool) {
            if(use_common_fork_join_pool)
                return ForkJoinPool.commonPool();

            int num_cores=Runtime.getRuntime().availableProcessors();
            if(max_threads > num_cores)
                log.warn("max_threads (%d) is higher than available cores (%d)", max_threads, num_cores);
            return new ForkJoinPool(max_threads, ForkJoinPool.defaultForkJoinWorkerThreadFactory, null, true);
        }


        ThreadPoolExecutor pool=new ThreadPoolExecutor(min_threads, max_threads, keep_alive_time, TimeUnit.MILLISECONDS, queue, factory);
        RejectedExecutionHandler handler=Util.parseRejectionPolicy(rejection_policy);
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


    protected boolean addPhysicalAddressToCache(Address logical_addr, PhysicalAddress physical_addr) {
        return addPhysicalAddressToCache(logical_addr, physical_addr, true);
    }

    protected boolean addPhysicalAddressToCache(Address logical_addr, PhysicalAddress physical_addr, boolean overwrite) {
        return logical_addr != null && physical_addr != null &&
          overwrite? logical_addr_cache.add(logical_addr, physical_addr) : logical_addr_cache.addIfAbsent(logical_addr, physical_addr);
    }

    protected PhysicalAddress getPhysicalAddressFromCache(Address logical_addr) {
        return logical_addr != null? logical_addr_cache.get(logical_addr) : null;
    }

    protected Collection<PhysicalAddress> getAllPhysicalAddressesFromCache() {
        return logical_addr_cache.nonRemovedValues();
    }

    protected void removeLogicalAddressFromCache(Address logical_addr) {
        if(logical_addr != null) {
            logical_addr_cache.remove(logical_addr);
            fetchLocalAddresses();
        }
    }

    /** Clears the cache. <em>Do not use, this is only for unit testing !</em> */
    @ManagedOperation(description="Clears the logical address cache; only used for testing")
    public void clearLogicalAddressCache() {
        logical_addr_cache.clear(true);
        fetchLocalAddresses();
    }


    protected abstract PhysicalAddress getPhysicalAddress();

    /* ----------------------------- End of Private Methods ---------------------------------------- */


}
