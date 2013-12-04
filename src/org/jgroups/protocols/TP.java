package org.jgroups.protocols;


import org.jgroups.*;
import org.jgroups.annotations.*;
import org.jgroups.blocks.LazyRemovalCache;
import org.jgroups.conf.PropertyConverters;
import org.jgroups.logging.LogFactory;
import org.jgroups.stack.DiagnosticsHandler;
import org.jgroups.stack.Protocol;
import org.jgroups.util.*;
import org.jgroups.util.ThreadFactory;
import org.jgroups.util.UUID;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InterruptedIOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.NetworkInterface;
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
public abstract class TP extends Protocol {

    protected static final byte LIST=1; // we have a list of messages rather than a single message when set
    protected static final byte MULTICAST=2; // message is a multicast (versus a unicast) message when set
    protected static final int  MSG_OFFSET=Global.SHORT_SIZE + Global.BYTE_SIZE*2; // offset for flags for single msgs

    protected static final boolean can_bind_to_mcast_addr; // are we running on Linux ?

    protected static NumberFormat f;

    static {
        can_bind_to_mcast_addr=(Util.checkForLinux() && !Util.checkForAndroid())
          || Util.checkForSolaris()
          || Util.checkForHp();
        f=NumberFormat.getNumberInstance();
        f.setGroupingUsed(false);
        f.setMaximumFractionDigits(2);
    }

    /* ------------------------------------------ JMX and Properties  ------------------------------------------ */


    @LocalAddress
    @Property(name="bind_addr",
              description="The bind address which should be used by this transport. The following special values " +
                      "are also recognized: GLOBAL, SITE_LOCAL, LINK_LOCAL, NON_LOOPBACK, match-interface, match-host, match-address",
              defaultValueIPv4=Global.NON_LOOPBACK_ADDRESS, defaultValueIPv6=Global.NON_LOOPBACK_ADDRESS,
              systemProperty={Global.BIND_ADDR},writable=false)
    protected InetAddress bind_addr=null;

    @Property(description="Use \"external_addr\" if you have hosts on different networks, behind " +
      "firewalls. On each firewall, set up a port forwarding rule (sometimes called \"virtual server\") to " +
      "the local IP (e.g. 192.168.1.100) of the host then on each host, set \"external_addr\" TCP transport " +
      "parameter to the external (public IP) address of the firewall.",
              systemProperty=Global.EXTERNAL_ADDR,writable=false)
    protected InetAddress external_addr=null;

    @Property(description="Used to map the internal port (bind_port) to an external port. Only used if > 0",
              systemProperty=Global.EXTERNAL_PORT,writable=false)
    protected int external_port=0;

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
    protected int logical_addr_cache_max_size=500;

    @Property(description="Time (in ms) after which entries in the logical address cache marked as removable are removed")
    protected long logical_addr_cache_expiration=120000;


    /** The port to which the transport binds. 0 means to bind to any (ephemeral) port */
    @Property(description="The port to which the transport binds. Default of 0 binds to any (ephemeral) port",writable=false)
    protected int bind_port=0;

    @Property(description="The range of valid ports, from bind_port to end_port. 0 only binds to bind_port and fails if taken")
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
    @Deprecated
    @Property(description="Discard packets with a different version if true",
              deprecatedMessage="incompatible packets are discarded anyway",writable=false)
    protected boolean discard_incompatible_packets=true;


    @Property(description="Thread naming pattern for threads in this channel. Valid values are \"pcl\": " +
      "\"p\": includes the thread name, e.g. \"Incoming thread-1\", \"UDP ucast receiver\", " +
      "\"c\": includes the cluster name, e.g. \"MyCluster\", " +
      "\"l\": includes the local address of the current member, e.g. \"192.168.5.1:5678\"")
    protected String thread_naming_pattern="cl";

    @Property(name="oob_thread_pool.enabled",description="Switch for enabling thread pool for OOB messages. " +
            "Default=true",writable=false)
    protected boolean oob_thread_pool_enabled=true;

    protected int oob_thread_pool_min_threads=2;

    protected int oob_thread_pool_max_threads=10;

    protected long oob_thread_pool_keep_alive_time=30000;

    @Property(name="oob_thread_pool.queue_enabled", description="Use queue to enqueue incoming OOB messages")
    protected boolean oob_thread_pool_queue_enabled=true;

    @Property(name="oob_thread_pool.queue_max_size",description="Maximum queue size for incoming OOB messages")
    protected int oob_thread_pool_queue_max_size=500;

    @Property(name="oob_thread_pool.rejection_policy",
              description="Thread rejection policy. Possible values are Abort, Discard, DiscardOldest and Run")
    protected String oob_thread_pool_rejection_policy="discard";

    protected int thread_pool_min_threads=2;

    protected int thread_pool_max_threads=10;

    protected long thread_pool_keep_alive_time=30000;

    @Property(name="thread_pool.enabled",description="Switch for enabling thread pool for regular messages")
    protected boolean thread_pool_enabled=true;

    @Property(name="thread_pool.queue_enabled", description="Queue to enqueue incoming regular messages")
    protected boolean thread_pool_queue_enabled=true;


    @Property(name="thread_pool.queue_max_size", description="Maximum queue size for incoming OOB messages")
    protected int thread_pool_queue_max_size=500;

    @Property(name="thread_pool.rejection_policy",
              description="Thread rejection policy. Possible values are Abort, Discard, DiscardOldest and Run")
    protected String thread_pool_rejection_policy="Discard";


    @Property(name="internal_thread_pool.enabled",description="Switch for enabling thread pool for internal messages",
              writable=false)
    protected boolean internal_thread_pool_enabled=true;

    @Property(name="internal_thread_pool.min_threads",description="Minimum thread pool size for the internal thread pool")
    protected int internal_thread_pool_min_threads=2;

    @Property(name="internal_thread_pool.max_threads",description="Maximum thread pool size for the internal thread pool")
    protected int internal_thread_pool_max_threads=4;

    @Property(name="internal_thread_pool.keep_alive_time", description="Timeout in ms to remove idle threads from the internal pool")
    protected long internal_thread_pool_keep_alive_time=30000;

    @Property(name="internal_thread_pool.queue_enabled", description="Queue to enqueue incoming internal messages")
    protected boolean internal_thread_pool_queue_enabled=true;

    @Property(name="internal_thread_pool.queue_max_size",description="Maximum queue size for incoming internal messages")
    protected int internal_thread_pool_queue_max_size=500;

    @Property(name="internal_thread_pool.rejection_policy",
              description="Thread rejection policy. Possible values are Abort, Discard, DiscardOldest and Run")
    protected String internal_thread_pool_rejection_policy="discard";



    @Property(description="Type of timer to be used. Valid values are \"old\" (DefaultTimeScheduler, used up to 2.10), " +
      "\"new\" or \"new2\" (TimeScheduler2), \"new3\" (TimeScheduler3) and \"wheel\". Note that this property " +
      "might disappear in future releases, if one of the 3 timers is chosen as default timer")
    protected String timer_type="new3";

    protected int timer_min_threads=4;

    protected int timer_max_threads=10;

    protected long timer_keep_alive_time=5000;

    @Property(name="timer.queue_max_size", description="Max number of elements on a timer queue")
    protected int timer_queue_max_size=500;

    @Property(name="timer.rejection_policy",description="Timer rejection policy. Possible values are Abort, Discard, DiscardOldest and Run")
    protected String timer_rejection_policy="abort"; // abort will spawn a new thread if the timer thread pool is full

    // hashed timing wheel specific props
    @Property(name="timer.wheel_size",
              description="Number of ticks in the HashedTimingWheel timer. Only applicable if timer_type is \"wheel\"")
    protected int wheel_size=200;

    @Property(name="timer.tick_time",
              description="Tick duration in the HashedTimingWheel timer. Only applicable if timer_type is \"wheel\"")
    protected long tick_time=50L;

    @Property(description="Enable bundling of smaller messages into bigger ones. Default is true",
              deprecatedMessage="will be ignored as bundling is on by default")
    @Deprecated
    protected boolean enable_bundling=true;

    /** Enable bundling for unicast messages. Ignored if enable_bundling is off */
    @Property(description="Enable bundling of smaller messages into bigger ones for unicast messages. Default is true",
              deprecatedMessage="will be ignored")
    @Deprecated
    protected boolean enable_unicast_bundling=true;


    @Property(description="Allows the transport to pass received message batches up as MessagesBatch instances " +
      "(up(MessageBatch)), rather than individual messages. This flag will be removed in a future version " +
      "when batching has been implemented by all protocols")
    protected boolean enable_batching=true;

    @Property(description="Switch to enable diagnostic probing. Default is true")
    protected boolean enable_diagnostics=true;

    @Property(description="Address for diagnostic probing. Default is 224.0.75.75", 
    		defaultValueIPv4="224.0.75.75",defaultValueIPv6="ff0e::0:75:75")
    protected InetAddress diagnostics_addr=null;

    @Property(converter=PropertyConverters.NetworkInterfaceList.class,
              description="Comma delimited list of interfaces (IP addresses or interface names) that the " +
                "diagnostics multicast socket should bind to")
    protected List<NetworkInterface> diagnostics_bind_interfaces=null;

    @Property(description="Port for diagnostic probing. Default is 7500")
    protected int diagnostics_port=7500;

    @Property(description="TTL of the diagnostics multicast socket")
    protected int diagnostics_ttl=8;
    
    @Property(description="Authorization passcode for diagnostics. If specified every probe query will be authorized")
    protected String diagnostics_passcode;

    @Property(description="If assigned enable this transport to be a singleton (shared) transport")
    protected String singleton_name=null;

    /** Whether or not warnings about messages from different groups are logged - private flag, not for common use */
    @Property(description="whether or not warnings about messages from different groups are logged")
    protected boolean log_discard_msgs=true;

    @Property(description="whether or not warnings about messages from members with a different version are discarded")
    protected boolean log_discard_msgs_version=true;

    @Property(description="Timeout (in ms) to determine how long to wait until a request to fetch the physical address " +
      "for a given logical address will be sent again. Subsequent requests for the same physical address will therefore " +
      "be spaced at least who_has_cache_timeout ms apart")
    protected long who_has_cache_timeout=2000;

    @Property(description="Max number of attempts to fetch a physical address (when not in the cache) before giving up")
    protected int physical_addr_max_fetch_attempts=3;

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
    protected int max_bundle_size=64000;

    /**
     * Max number of milliseconds until queued messages are sent. Messages are sent when max_bundle_size
     * or max_bundle_timeout has been exceeded (whichever occurs faster)
     */
    protected long max_bundle_timeout=20;

    @Property(description="The type of bundler used. Has to be \"old\" or \"new\" (default)")
    protected String bundler_type="new";

    @Property(description="The max number of elements in a bundler if the bundler supports size limitations")
    protected int bundler_capacity=20000;


    @Property(name="max_bundle_size", description="Maximum number of bytes for messages to be queued until they are sent")
    public void setMaxBundleSize(int size) {
        if(size <= 0)
            throw new IllegalArgumentException("max_bundle_size (" + size + ") is <= 0");
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

    public void setOOBThreadPoolQueueEnabled(boolean flag) {this.oob_thread_pool_queue_enabled=flag;}



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

    @ManagedAttribute(description="Number of messages rejected by the thread pool")
    protected int num_rejected_msgs=0;

    /** The name of the group to which this member is connected. With a shared transport, the channel name is
     * in TP.ProtocolAdapter (cluster_name), and this field is not used */
    @ManagedAttribute(description="Channel (cluster) name")
    protected String channel_name=null;

    @ManagedAttribute(description="Number of OOB messages received")
    protected long num_oob_msgs_received;

    @ManagedAttribute(description="Number of regular messages received")
    protected long num_incoming_msgs_received;

    @ManagedAttribute(description="Number of internal messages received")
    protected long num_internal_msgs_received;

    @ManagedAttribute(description="Class of the timer implementation")
    public String getTimerClass() {
        return timer != null? timer.getClass().getSimpleName() : "null";
    }

    @ManagedAttribute(description="Number of messages from members in a different cluster")
    public int getDifferentClusterMessages() {
        return suppress_log_different_cluster != null? suppress_log_different_cluster.getCache().size() : 0;
    }

    @ManagedAttribute(description="Number of messages from members with a different JGroups version")
    public int getDifferentVersionMessages() {
        return suppress_log_different_version != null? suppress_log_different_version.getCache().size() : 0;
    }

    @ManagedOperation(description="Clears the cache for messages from different clusters")
    public void clearDifferentClusterCache() {
        if(suppress_log_different_cluster != null)
            suppress_log_different_cluster.getCache().clear();
    }

    @ManagedOperation(description="Clears the cache for messages from members with different versions")
    public void clearDifferentVersionCache() {
        if(suppress_log_different_version != null)
            suppress_log_different_version.getCache().clear();
    }


    @ManagedAttribute(description="Type of logger used")
    public static String loggerType() {return LogFactory.loggerType();}
    /* --------------------------------------------- Fields ------------------------------------------------------ */



    /** The address (host and port) of this member. Null by default when a shared transport is used */
    protected Address local_addr=null;

    /** The members of this group (updated when a member joins or leaves). With a shared transport,
     * members contains *all* members from all channels sitting on the shared transport */
    protected final Set<Address> members=new CopyOnWriteArraySet<Address>();


    /** Keeps track of connects and disconnects, in order to start and stop threads */
    protected int connect_count=0;

    //http://jira.jboss.org/jira/browse/JGRP-849
    protected final ReentrantLock connectLock = new ReentrantLock();
    

    // ================================== OOB thread pool ========================
    protected Executor oob_thread_pool;

    /** Factory which is used by oob_thread_pool */
    protected ThreadFactory oob_thread_factory;

    /** Used if oob_thread_pool is a ThreadPoolExecutor and oob_thread_pool_queue_enabled is true */
    protected BlockingQueue<Runnable> oob_thread_pool_queue;


    // ================================== Regular thread pool ======================

    /** The thread pool which handles unmarshalling, version checks and dispatching of regular messages */
    protected Executor thread_pool;

    /** Factory which is used by oob_thread_pool */
    protected ThreadFactory default_thread_factory;

    /** Used if thread_pool is a ThreadPoolExecutor and thread_pool_queue_enabled is true */
    protected BlockingQueue<Runnable> thread_pool_queue;

    // ================================== Internal thread pool ======================

    /** The thread pool which handles JGroups internal messages (Flag.INTERNAL) */
    protected Executor                internal_thread_pool;

    /** Factory which is used by internal_thread_pool */
    protected ThreadFactory           internal_thread_factory;

    /** Used if thread_pool is a ThreadPoolExecutor and thread_pool_queue_enabled is true */
    protected BlockingQueue<Runnable> internal_thread_pool_queue;

    // ================================== Timer thread pool  =========================
    protected TimeScheduler timer;

    protected ThreadFactory timer_thread_factory;

    // ================================ Default thread factory ========================
    /** Used by all threads created by JGroups outside of the thread pools */
    protected ThreadFactory global_thread_factory=null;

    // ================================= Default SocketFactory ========================
    protected SocketFactory socket_factory=new DefaultSocketFactory();

    protected Bundler bundler;

    protected DiagnosticsHandler diag_handler=null;
    protected final List<DiagnosticsHandler.ProbeHandler> preregistered_probe_handlers=new LinkedList<DiagnosticsHandler.ProbeHandler>();

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
    protected LazyRemovalCache<Address,PhysicalAddress> logical_addr_cache;

    // last time we sent a discovery request
    protected long last_discovery_request=0;

    Future<?> logical_addr_cache_reaper=null;

    protected static final LazyRemovalCache.Printable<Address,PhysicalAddress> print_function=new LazyRemovalCache.Printable<Address,PhysicalAddress>() {
        public java.lang.String print(final Address logical_addr, final PhysicalAddress physical_addr) {
            StringBuilder sb=new StringBuilder();
            String tmp_logical_name=UUID.get(logical_addr);
            if(tmp_logical_name != null)
                sb.append(tmp_logical_name).append(": ");
            if(logical_addr instanceof UUID)
                sb.append(((UUID)logical_addr).toStringLong());
            else
                sb.append(logical_addr);
            sb.append(": ").append(physical_addr).append("\n");
            return sb.toString();
        }
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

    /** Whether or not hardware multicasting is supported */
    public abstract boolean supportsMulticasting();

    public boolean isMulticastCapable() {return supportsMulticasting();}

    public String toString() {
        if(!isSingleton())
            return local_addr != null? name + "(local address: " + local_addr + ')' : name;
        else
            return name + " (singleton=" + singleton_name + ")";
    }

    public void resetStats() {
        num_msgs_sent=num_msgs_received=num_bytes_sent=num_bytes_received=0;
        num_oob_msgs_received=num_incoming_msgs_received=num_internal_msgs_received=0;
    }

    public void registerProbeHandler(DiagnosticsHandler.ProbeHandler handler) {
        if(diag_handler != null)
            diag_handler.registerProbeHandler(handler);
        else
            preregistered_probe_handlers.add(handler);
    }

    public void unregisterProbeHandler(DiagnosticsHandler.ProbeHandler handler) {
        if(diag_handler != null)
            diag_handler.unregisterProbeHandler(handler);
    }

    /**
     * Sets a {@link DiagnosticsHandler}. Should be set before the stack is started
     * @param handler
     */
    public void setDiagnosticsHandler(DiagnosticsHandler handler) {
        if(handler != null) {
            if(diag_handler != null)
                diag_handler.stop();
            diag_handler=handler;
        }
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

    public ThreadFactory getInternalThreadPoolThreadFactory() {
        return internal_thread_factory;
    }

    public void setInternalThreadPoolThreadFactory(ThreadFactory factory) {
        internal_thread_factory=factory;
        if(internal_thread_pool instanceof ThreadPoolExecutor)
            ((ThreadPoolExecutor)internal_thread_pool).setThreadFactory(factory);
    }

    public ThreadFactory getTimerThreadFactory() {
        return timer_thread_factory;
    }

    public void setTimerThreadFactory(ThreadFactory factory) {
        timer_thread_factory=factory;
        timer.setThreadFactory(factory);
    }

    public TimeScheduler getTimer() {return timer;}

    /**
     * Sets a new timer. This should be done before the transport is initialized; be very careful, as replacing a
     * running timer with tasks in it can wreak havoc !
     * @param timer
     */
    public void setTimer(TimeScheduler timer) {
        this.timer=timer;
    }

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

    public InetAddress getBindAddress()               {return bind_addr;}
    public void setBindAddress(InetAddress bind_addr) {this.bind_addr=bind_addr;}
    public int getBindPort()                          {return bind_port;}
    public void setBindPort(int port)                 {this.bind_port=port;}
    public void setBindToAllInterfaces(boolean flag)  {this.receive_on_all_interfaces=flag;}

    public boolean isReceiveOnAllInterfaces() {return receive_on_all_interfaces;}
    public List<NetworkInterface> getReceiveInterfaces() {return receive_interfaces;}
    public static boolean isDiscardIncompatiblePackets() {return true;}
    public static void setDiscardIncompatiblePackets(boolean flag) {}
    @Deprecated public static boolean isEnableBundling() {return true;}
    @Deprecated public void setEnableBundling(boolean flag) {}
    @Deprecated public static boolean isEnableUnicastBundling() {return true;}
    @Deprecated public void setEnableUnicastBundling(boolean enable_unicast_bundling) {}
    public void setPortRange(int range) {this.port_range=range;}
    public int getPortRange() {return port_range ;}

    public boolean isOOBThreadPoolEnabled() { return oob_thread_pool_enabled; }

    public boolean isDefaulThreadPoolEnabled() { return thread_pool_enabled; }

    public boolean isLoopback() {return loopback;}
    public void setLoopback(boolean b) {loopback=b;}

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
        return oob_thread_pool_queue != null? oob_thread_pool_queue.size() : 0;
    }

    public int getOOBMaxQueueSize() {
        return oob_thread_pool_queue_max_size;
    }


    public void setOOBRejectionPolicy(String rejection_policy) {
        RejectedExecutionHandler handler=Util.parseRejectionPolicy(rejection_policy);
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
        return thread_pool_queue != null? thread_pool_queue.size() : 0;
    }

    public int getRegularMaxQueueSize() {
        return thread_pool_queue_max_size;
    }


    @ManagedAttribute(description="Current number of threads in the internal thread pool")
    public int getInternalPoolSize() {
        return internal_thread_pool instanceof ThreadPoolExecutor? ((ThreadPoolExecutor)internal_thread_pool).getPoolSize() : 0;
    }

    public long getInternalMessages() {
        return num_internal_msgs_received;
    }

    @ManagedAttribute(description="Number of messages in the internal thread pool's queue")
    public int getInternalQueueSize() {
        return internal_thread_pool_queue != null? internal_thread_pool_queue.size() : 0;
    }

    public int getInternalMaxQueueSize() {
        return internal_thread_pool_queue_max_size;
    }


    @ManagedAttribute(name="timer_tasks",description="Number of timer tasks queued up for execution")
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

    @ManagedAttribute(description="Returns the number of live threads in the JVM")
    public static int getNumThreads() {
        return ManagementFactory.getThreadMXBean().getThreadCount();
    }

    @ManagedAttribute(description="Whether the diagnostics handler is running or not")
    public boolean isDiagnosticsHandlerRunning() {return diag_handler != null && diag_handler.isRunning();}

    public void setRegularRejectionPolicy(String rejection_policy) {
        RejectedExecutionHandler handler=Util.parseRejectionPolicy(rejection_policy);
        if(thread_pool instanceof ThreadPoolExecutor)
            ((ThreadPoolExecutor)thread_pool).setRejectedExecutionHandler(new ShutdownRejectedExecutionHandler(handler));
    }

    public void    setLogDiscardMessages(boolean flag)        {log_discard_msgs=flag;}
    public boolean getLogDiscardMessages()                    {return log_discard_msgs;}
    public void    setLogDiscardMessagesVersion(boolean flag) {log_discard_msgs_version=flag;}
    public boolean getLogDiscardMessagesVersion()             {return log_discard_msgs_version;}


    @ManagedOperation(description="Dumps the contents of the logical address cache")
    public String printLogicalAddressCache() {
        return logical_addr_cache.printCache(print_function);
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
        super.init();

        if(physical_addr_max_fetch_attempts < 1)
            throw new IllegalArgumentException("Property \"physical_addr_max_fetch_attempts\" cannot be less than 1");

        // Create the default thread factory
        if(global_thread_factory == null)
            global_thread_factory=new DefaultThreadFactory("", false);

        // Create the timer and the associated thread factory - depends on singleton_name
        if(timer_thread_factory == null)
            timer_thread_factory=new LazyThreadFactory("Timer", true, true);
        if(isSingleton())
            timer_thread_factory.setIncludeClusterName(false);

        if(default_thread_factory == null)
            default_thread_factory=new DefaultThreadFactory("Incoming", false, true);
        
        if(oob_thread_factory == null)
            oob_thread_factory=new DefaultThreadFactory("OOB", false, true);

        if(internal_thread_factory == null)
            internal_thread_factory=new DefaultThreadFactory("INT", false, true);

        // local_addr is null when shared transport, channel_name is not used
        setInAllThreadFactories(channel_name, local_addr, thread_naming_pattern);

        if(timer == null) {
            if(timer_type.equalsIgnoreCase("old")) {
                if(timer_min_threads < 2) {
                    log.warn(Util.getMessage("TimerMinThreads"), timer_min_threads);
                    timer_min_threads=2;
                }
                timer=new DefaultTimeScheduler(timer_thread_factory, timer_min_threads);
            }
            else if(timer_type.equalsIgnoreCase("new") || timer_type.equalsIgnoreCase("new2")) {
                timer=new TimeScheduler2(timer_thread_factory, timer_min_threads, timer_max_threads, timer_keep_alive_time,
                                         timer_queue_max_size, timer_rejection_policy);
            }
            else if(timer_type.equalsIgnoreCase("new3")) {
                timer=new TimeScheduler3(timer_thread_factory, timer_min_threads, timer_max_threads, timer_keep_alive_time,
                                         timer_queue_max_size, timer_rejection_policy);
            }
            else if(timer_type.equalsIgnoreCase("wheel")) {
                timer=new HashedTimingWheel(timer_thread_factory, timer_min_threads, timer_max_threads, timer_keep_alive_time,
                                            timer_queue_max_size, wheel_size, tick_time);
            }
            else {
                throw new Exception("timer_type has to be either \"old\", \"new\", \"new2\", \"new3\" or \"wheel\"");
            }
        }

        who_has_cache=new ExpiryCache<Address>(who_has_cache_timeout);

        if(suppress_time_different_version_warnings > 0)
            suppress_log_different_version=new SuppressLog<Address>(log, "VersionMismatch", "SuppressMsg");
        if(suppress_time_different_cluster_warnings > 0)
            suppress_log_different_cluster=new SuppressLog<Address>(log, "MsgDroppedDiffCluster", "SuppressMsg");

        // ========================================== OOB thread pool ==============================

        if(oob_thread_pool == null
          || (oob_thread_pool instanceof ThreadPoolExecutor && ((ThreadPoolExecutor)oob_thread_pool).isShutdown())) {
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
        }

        // ====================================== Regular thread pool ===========================

        if(thread_pool == null
          || (thread_pool instanceof ThreadPoolExecutor && ((ThreadPoolExecutor)thread_pool).isShutdown())) {
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
        }


        // ========================================== Internal thread pool ==============================

        if(internal_thread_pool == null
          || (internal_thread_pool instanceof ThreadPoolExecutor && ((ThreadPoolExecutor)internal_thread_pool).isShutdown())) {
            if(internal_thread_pool_enabled) {
                if(internal_thread_pool_queue_enabled)
                    internal_thread_pool_queue=new LinkedBlockingQueue<Runnable>(internal_thread_pool_queue_max_size);
                else
                    internal_thread_pool_queue=new SynchronousQueue<Runnable>();
                internal_thread_pool=createThreadPool(internal_thread_pool_min_threads, internal_thread_pool_max_threads, internal_thread_pool_keep_alive_time,
                                                 internal_thread_pool_rejection_policy, internal_thread_pool_queue, internal_thread_factory);
            }
            // if the internal thread pool is disabled, we won't create it (not even a DirectExecutor)
        }


        Map<String, Object> m=new HashMap<String, Object>(2);
        if(bind_addr != null)
            m.put("bind_addr", bind_addr);
        if(external_addr != null)
            m.put("external_addr", external_addr);
        if(external_port > 0)
            m.put("external_port", external_port);
        if(!m.isEmpty())
            up(new Event(Event.CONFIG, m));

        logical_addr_cache=new LazyRemovalCache<Address,PhysicalAddress>(logical_addr_cache_max_size, logical_addr_cache_expiration);
        
        if(logical_addr_cache_reaper == null || logical_addr_cache_reaper.isDone()) {
            if(logical_addr_cache_expiration <= 0)
                throw new IllegalArgumentException("logical_addr_cache_expiration has to be > 0");
            logical_addr_cache_reaper=timer.scheduleWithFixedDelay(new Runnable() {
                public void run() {
                    evictLogicalAddressCache();
                }

                public String toString() {
                    return TP.this.getClass().getSimpleName() + ": LogicalAddressCacheReaper (interval=" + logical_addr_cache_expiration + " ms)";
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

        if(timer != null)
            timer.stop();

        // 3. Stop the thread pools
        if(oob_thread_pool instanceof ThreadPoolExecutor)
            shutdownThreadPool(oob_thread_pool);

        if(thread_pool instanceof ThreadPoolExecutor)
            shutdownThreadPool(thread_pool);

        if(internal_thread_pool instanceof ThreadPoolExecutor)
            shutdownThreadPool(internal_thread_pool);
    }

    /**
     * Creates the unicast and multicast sockets and starts the unicast and multicast receiver threads
     */
    public void start() throws Exception {
        fetchLocalAddresses();

        if(timer == null)
            throw new Exception("timer is null");

        if(enable_diagnostics) {
            boolean diag_handler_created=diag_handler == null;
            if(diag_handler == null)
                diag_handler=new DiagnosticsHandler(diagnostics_addr, diagnostics_port, diagnostics_bind_interfaces,
                                                    diagnostics_ttl, log, getSocketFactory(), getThreadFactory(), diagnostics_passcode);

            diag_handler.registerProbeHandler(new DiagnosticsHandler.ProbeHandler() {
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
                            for(DiagnosticsHandler.ProbeHandler handler: diag_handler.getProbeHandlers()) {
                                String[] tmp=handler.supportedKeys();
                                if(tmp != null && tmp.length > 0) {
                                    for(String s: tmp)
                                        sb.append(s).append(" ");
                                }
                            }
                            retval.put("keys", sb.toString());
                        }
                        if(key.equals("info")) {
                            if(singleton_name != null && !singleton_name.isEmpty())
                                retval.put("singleton_name", singleton_name);

                        }
                        if(key.equals("addrs")) {
                            Set<PhysicalAddress> physical_addrs=logical_addr_cache.nonRemovedValues();
                            String list=Util.print(physical_addrs);
                            retval.put("addrs", list);
                        }
                        if(key.startsWith("cluster")) {
                            String cluster_name_pattern=key.substring("cluster".length()+1).trim();
                            if(!isSingleton()) {
                                if(cluster_name_pattern != null && !Util.patternMatch(cluster_name_pattern, channel_name))
                                    throw new IllegalArgumentException("Request dropped as cluster name " + channel_name +
                                                                         "does not match cluster name pattern " + cluster_name_pattern);
                            }
                            else {
                                // not optimal, this matches *any* of the shared clusters. would be better to return only
                                // responses for matching clusters
                                if(up_prots != null) {
                                    boolean match=false;
                                    List<String> cluster_names=new ArrayList<String>();
                                    for(Protocol prot: up_prots.values())
                                        if(prot instanceof ProtocolAdapter)
                                            cluster_names.add(((ProtocolAdapter)prot).getClusterName());
                                    for(String cluster_name: cluster_names) {
                                        if(Util.patternMatch(cluster_name_pattern, cluster_name)) {
                                            match=true;
                                            break;
                                        }
                                    }

                                    if(!match)
                                        throw new IllegalArgumentException("Request dropped as cluster names " + cluster_names +
                                                                             " do not match cluster name pattern " + cluster_name_pattern);
                                }
                            }
                        }
                    }
                    return retval;
                }

                public String[] supportedKeys() {
                    return new String[]{"dump", "keys", "uuids", "info", "addrs", "cluster"};
                }
            });
            if(diag_handler_created)
                diag_handler.start();
            
            for(DiagnosticsHandler.ProbeHandler handler: preregistered_probe_handlers)
                diag_handler.registerProbeHandler(handler);
            preregistered_probe_handlers.clear();
        }

        if(bundler_type.startsWith("new")) {
            if(bundler_type.endsWith("new2"))
                log.warn(Util.getMessage("OldBundlerType"), "new2", "TransferQueueBundler (new)");
            bundler=new TransferQueueBundler(bundler_capacity);
        }
        else if(bundler_type.startsWith("old")) {
            if(bundler_type.endsWith("old2"))
                log.warn(Util.getMessage("OldBundlerType"), "old2", "DefaultBundler (old)");
            bundler=new DefaultBundler();
        }
        else
            log.warn(Util.getMessage("UnknownBundler"), bundler_type);
        if(bundler == null)
            bundler=new TransferQueueBundler(bundler_capacity);
        bundler.start();

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
        if(evt.getType() != Event.MSG)  // unless it is a message handle it and respond
            return handleDownEvent(evt);

        Message msg=(Message)evt.getArg();
        if(header != null)
            msg.putHeaderIfAbsent(this.id, header); // added patch by Roland Kurmann (March 20 2003)

        if(!isSingleton())
            setSourceAddress(msg); // very important !! listToBuffer() will fail with a null src address !!
        if(log.isTraceEnabled())
            log.trace("%s: sending msg to %s, src=%s, headers are %s", local_addr, msg.getDest(), msg.getSrc(), msg.printHeaders());


        Address dest=msg.getDest();
        if(dest instanceof PhysicalAddress) {
            // We can modify the message because it won't get retransmitted. The only time we have a physical address
            // as dest is when TCPPING sends the initial discovery requests to initial_hosts: this is below UNICAST,
            // so no retransmission
            msg.dest(null).setFlag(Message.Flag.DONT_BUNDLE);
        }

        // Don't send if destination is local address. Instead, switch dst and src and send it up the stack.
        // If multicast message, loopback a copy directly to us (but still multicast). Once we receive this,
        // we will discard our own multicast message
        final boolean multicast=dest == null;
        if(loopback && (multicast || dest.equals(msg.getSrc()))) {

            // we *have* to make a copy, or else up_prot.up() might remove headers from msg which will then *not*
            // be available for marshalling further down (when sending the message)
            final Message copy=msg.copy();
            if(log.isTraceEnabled()) log.trace("%s: looping back message %s", local_addr, copy);

            TpHeader hdr=(TpHeader)msg.getHeader(this.id); // added by the code above *or* by ProtocolAdapter.down()
            final String cluster_name=hdr.channel_name;

            // changed to fix http://jira.jboss.com/jira/browse/JGRP-506
            boolean internal=msg.isFlagSet(Message.Flag.INTERNAL);
            Executor pool=internal && internal_thread_pool != null? internal_thread_pool
              : internal || msg.isFlagSet(Message.Flag.OOB)? oob_thread_pool : thread_pool;
            pool.execute(new Runnable() {
                public void run() {
                    passMessageUp(copy, cluster_name, false, multicast, false);
                }
            });

            if(!multicast)
                return null;
        }

        try {
            send(msg, dest, multicast);
        }
        catch(InterruptedIOException iex) {
        }
        catch(InterruptedException interruptedEx) {
            Thread.currentThread().interrupt(); // let someone else handle the interrupt
        }
        catch(Throwable e) {
            log.error(Util.getMessage("SendFailure"),
                      local_addr, (dest == null? "cluster" : dest), msg.size(), e.toString(), msg.printHeaders());
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


    protected void passMessageUp(Message msg, String cluster_name, boolean perform_cluster_name_matching,
                                 boolean multicast, boolean discard_own_mcast) {
        if(log.isTraceEnabled())
            log.trace("%s: received %s, headers are %s", local_addr, msg, msg.printHeaders());

        final Protocol tmp_prot=isSingleton()? up_prots.get(cluster_name) : up_prot;
        if(tmp_prot == null)
            return;
        boolean is_protocol_adapter=tmp_prot instanceof ProtocolAdapter;
        // Discard if message's cluster name is not the same as our cluster name
        if(!is_protocol_adapter && perform_cluster_name_matching && channel_name != null && !channel_name.equals(cluster_name)) {
            if(log_discard_msgs && log.isWarnEnabled()) {
                Address sender=msg.getSrc();
                if(suppress_log_different_cluster != null)
                    suppress_log_different_cluster.log(SuppressLog.Level.warn, sender,
                                                       suppress_time_different_cluster_warnings,
                                                       cluster_name, channel_name, sender);
                else
                    log.warn(Util.getMessage("MsgDroppedDiffCluster"), cluster_name, channel_name, sender);
            }
            return;
        }

        if(loopback && multicast && discard_own_mcast) {
            Address local=is_protocol_adapter? ((ProtocolAdapter)tmp_prot).getAddress() : local_addr;
            if(local != null && local.equals(msg.getSrc()))
                return;
        }
        tmp_prot.up(new Event(Event.MSG, msg));
    }


    protected void passBatchUp(MessageBatch batch, boolean perform_cluster_name_matching, boolean discard_own_mcast) {
        if(log.isTraceEnabled())
            log.trace("%s: received message batch of %d messages from %s", local_addr, batch.size(), batch.sender());

        String ch_name=batch.clusterName();
        final Protocol tmp_prot=isSingleton()? up_prots.get(ch_name) : up_prot;
        if(tmp_prot == null)
            return;

        boolean is_protocol_adapter=tmp_prot instanceof ProtocolAdapter;
        // Discard if message's cluster name is not the same as our cluster name
        if(!is_protocol_adapter && perform_cluster_name_matching && channel_name != null && !channel_name.equals(ch_name)) {
            if(log_discard_msgs && log.isWarnEnabled()) {
                Address sender=batch.sender();
                if(suppress_log_different_cluster != null)
                    suppress_log_different_cluster.log(SuppressLog.Level.warn, sender,
                                                       suppress_time_different_cluster_warnings,
                                                       ch_name, channel_name, sender);
                else
                    log.warn(Util.getMessage("BatchDroppedDiffCluster"), ch_name, channel_name, sender);
            }
            return;
        }

        if(loopback && batch.multicast() && discard_own_mcast) {
            Address local=is_protocol_adapter? ((ProtocolAdapter)tmp_prot).getAddress() : local_addr;
            if(local != null && local.equals(batch.sender()))
                return;
        }
        tmp_prot.up(batch);
    }


    /**
     * Subclasses must call this method when a unicast or multicast message has been received.
     */
    public void receive(Address sender, byte[] data, int offset, int length) {
        if(data == null) return;

        byte flags=data[Global.SHORT_SIZE];
        boolean is_message_list=(flags & LIST) == LIST;

        if(is_message_list) // used if message bundling is enabled
            handleMessageBatch(sender, data, offset, length);
        else
            handleSingleMessage(sender, data, offset, length);
    }


    protected void handleMessageBatch(Address sender, byte[] data, int offset, int length) {
        DataInputStream dis=null;
        try {
            ExposedByteArrayInputStream in_stream=new ExposedByteArrayInputStream(data, offset, length);
            dis=new DataInputStream(in_stream);
            short version=dis.readShort();
            if(!versionMatch(version, sender))
                return;

            byte flags=dis.readByte();
            final boolean multicast=(flags & MULTICAST) == MULTICAST;

            final MessageBatch[] batches=readMessageBatch(dis, multicast);
            final MessageBatch batch=batches[0], oob_batch=batches[1], internal_batch_oob=batches[2], internal_batch=batches[3];

            if(oob_batch != null) {
                num_oob_msgs_received+=oob_batch.size();
                oob_thread_pool.execute(new BatchHandler(oob_batch));
            }
            if(batch != null) {
                num_incoming_msgs_received+=batch.size();
                thread_pool.execute(new BatchHandler(batch));
            }
            if(internal_batch_oob != null) {
                num_oob_msgs_received+=internal_batch_oob.size();
                Executor pool=internal_thread_pool != null? internal_thread_pool : oob_thread_pool;
                pool.execute(new BatchHandler(internal_batch_oob));
            }
            if(internal_batch != null) {
                num_internal_msgs_received+=internal_batch.size();
                Executor pool=internal_thread_pool != null? internal_thread_pool : oob_thread_pool;
                pool.execute(new BatchHandler(internal_batch));
            }
        }
        catch(RejectedExecutionException rejected) {
            num_rejected_msgs++;
        }
        catch(Throwable t) {
            log.error(Util.getMessage("IncomingMsgFailure"), local_addr, t);
        }
        finally {
            Util.close(dis);
        }
    }

    protected void handleSingleMessage(Address sender, byte[] data, int offset, int length) {
        // the message flags are at indexes 4-5
        short   msg_flags=Util.makeShort(data, offset + MSG_OFFSET);
        boolean internal=(msg_flags & Message.Flag.INTERNAL.value()) == Message.Flag.INTERNAL.value();
        boolean oob=(msg_flags & Message.Flag.OOB.value()) == Message.Flag.OOB.value();

        if(oob)
            num_oob_msgs_received++;
        else if(internal)
            num_internal_msgs_received++;
        else
            num_incoming_msgs_received++;

        Executor pool=internal && internal_thread_pool != null? internal_thread_pool
          : internal || oob? oob_thread_pool : thread_pool;

        try {
            if(pool instanceof DirectExecutor)
                pool.execute(new MyHandler(sender, data, offset, length)); // we don't make a copy if we execute on this thread
            else {
                byte[] tmp=new byte[length];
                System.arraycopy(data, offset, tmp, 0, length);
                pool.execute(new MyHandler(sender, tmp, 0, tmp.length));
            }
        }
        catch(RejectedExecutionException ex) {
            num_rejected_msgs++;
        }
    }

    protected boolean versionMatch(short version, Address sender) {
        boolean match=Version.isBinaryCompatible(version);
        if(!match) {
            if(log_discard_msgs_version && log.isWarnEnabled()) {
                if(suppress_log_different_version != null)
                    suppress_log_different_version.log(SuppressLog.Level.warn, sender,
                                                       suppress_time_different_version_warnings,
                                                       sender, Version.print(version), Version.printVersion());
                else
                    log.warn(Util.getMessage("VersionMismatch"), sender, Version.print(version), Version.printVersion());
            }
        }
        return match;
    }



    protected class MyHandler implements Runnable {
        protected final Address sender;
        protected final byte[]  data;
        protected final int     offset;
        protected final int     length;

        protected MyHandler(Address sender, byte[] data, int offset, int length) {
            this.sender=sender;
            this.data=data;
            this.offset=offset;
            this.length=length;
        }

        public void run() {
            DataInputStream dis=null;
            try {
                ExposedByteArrayInputStream in_stream=new ExposedByteArrayInputStream(data, offset, length);
                dis=new DataInputStream(in_stream);
                short version=dis.readShort();
                if(!versionMatch(version, sender))
                    return;

                byte flags=dis.readByte();
                final boolean multicast=(flags & MULTICAST) == MULTICAST;
                Message msg=readMessage(dis);

                if(!multicast) {
                    Address dest=msg.getDest(), target=local_addr;
                    if(dest != null && target != null && !dest.equals(target)) {
                        log.warn(Util.getMessage("IncorrectDest"), local_addr, dest);
                        return;
                    }
                }

                if(stats) {
                    num_msgs_received++;
                    num_bytes_received+=length;
                }

                TpHeader hdr=(TpHeader)msg.getHeader(id);
                String cluster_name=hdr.channel_name;
                passMessageUp(msg, cluster_name, true, multicast, true);
            }
            catch(Throwable t) {
                log.error(Util.getMessage("IncomingMsgFailure"), local_addr, t);
            }
            finally {
                Util.close(dis); // only nulls the buffer
            }
        }
    }


    protected class BatchHandler implements Runnable {
        protected final MessageBatch batch;
        protected final long         created; // timestamp (ns) when created

        public BatchHandler(final MessageBatch batch) {
            this.batch=batch;
            this.created=stats? System.nanoTime() : 0;
        }

        public void run() {
            if(stats) {
                num_msgs_received+=batch.size();
                num_bytes_received+=batch.length();
            }

            if(!batch.multicast()) {
                Address dest=batch.dest(), target=local_addr;
                if(dest != null && target != null && !dest.equals(target)) {
                    log.warn(Util.getMessage("IncorrectDest"), local_addr, dest);
                    return;
                }
            }

            if(enable_batching) {
                passBatchUp(batch, true, true);
                return;
            }

            for(Message msg: batch) {
                try {
                    passMessageUp(msg, batch.clusterName(), true, batch.multicast(), true);
                }
                catch(Throwable t) {
                    log.error(Util.getMessage("PassUpFailure"), t);
                }
            }
        }
    }


    /** Serializes and sends a message. This method is not reentrant */
    protected void send(Message msg, Address dest, boolean multicast) throws Exception {
        // bundle all messages except when tagged with DONT_BUNDLE
        if(!msg.isFlagSet(Message.Flag.DONT_BUNDLE)) {
            bundler.send(msg);
            return;
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
        if(multicast)
            sendMulticast(buf.getBuf(), buf.getOffset(), buf.getLength());
        else
            sendToSingleMember(dest, buf.getBuf(), buf.getOffset(), buf.getLength());
    }


    protected void sendToSingleMember(Address dest, byte[] buf, int offset, int length) throws Exception {
        if(dest instanceof PhysicalAddress) {
            sendUnicast((PhysicalAddress)dest, buf, offset, length);
            return;
        }

        PhysicalAddress physical_dest=null;
        int cnt=1;
        long sleep_time=20;
        while((physical_dest=getPhysicalAddressFromCache(dest)) == null && cnt++ <= physical_addr_max_fetch_attempts) {
            if(who_has_cache.addIfAbsentOrExpired(dest)) { // true if address was added
                Util.sleepRandom(1, 500); // to prevent a discovery flood in large clusters (by staggering requests)
                if((physical_dest=getPhysicalAddressFromCache(dest)) != null)
                    break;
                up(new Event(Event.GET_PHYSICAL_ADDRESS, dest));
            }
            Util.sleep(sleep_time);
            sleep_time=Math.min(1000, sleep_time *2);
        }

        if(physical_dest != null)
            sendUnicast(physical_dest, buf, offset, length);
        else if(log.isWarnEnabled())
            log.warn(Util.getMessage("PhysicalAddrMissing"), local_addr, dest);
    }


    protected void sendToAllPhysicalAddresses(byte[] buf, int offset, int length) throws Exception {
        if(!logical_addr_cache.containsKeys(members)) {
            long current_time=0;
            synchronized(this) {
                if(last_discovery_request == 0 || (current_time=System.currentTimeMillis()) - last_discovery_request >= 10000) {
                    last_discovery_request=current_time == 0? System.currentTimeMillis() : current_time;
                    log.warn(Util.getMessage("NotAllPhysAddrsFound"), local_addr);
                    up(new Event(Event.FIND_INITIAL_MBRS));
                }
            }
        }

        for(LazyRemovalCache.Entry<PhysicalAddress> entry: logical_addr_cache.valuesIterator()) {
            try {
                if(!entry.isRemovable())
                    sendUnicast(entry.getVal(), buf, offset, length);
            }
            catch(Throwable t) {
                log.error(Util.getMessage("FailureSendingToPhysAddr"), local_addr, entry.getVal(), t);
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
        dos.writeByte(flags);
        msg.writeTo(dos);
    }

    public static Message readMessage(DataInputStream instream) throws Exception {
        Message msg=new Message(false); // don't create headers, readFrom() will do this
        msg.readFrom(instream);
        return msg;
    }




    /**
     * Write a list of messages with the *same* destination and src addresses. The message list is
     * marshalled as follows (see doc/design/MarshallingFormat.txt for details):
     * <pre>
     * List: * | version | flags | dest | src | cluster-name | [Message*] |
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
    public static void writeMessageList(Address dest, Address src, String cluster_name,
                                        List<Message> msgs, DataOutputStream dos, boolean multicast, short transport_id) throws Exception {
        dos.writeShort(Version.version);

        byte flags=LIST;
        if(multicast)
            flags+=MULTICAST;

        dos.writeByte(flags);

        Util.writeAddress(dest, dos);

        Util.writeAddress(src, dos);

        Util.writeString(cluster_name, dos);

        // Number of messages (0 == no messages)
        dos.writeInt(msgs != null? msgs.size() : 0);

        if(msgs != null)
            for(Message msg: msgs)
                msg.writeToNoAddrs(src, dos, transport_id); // exclude the transport header
    }



    public static List<Message> readMessageList(DataInputStream in, short transport_id) throws Exception {
        List<Message> list=new LinkedList<Message>();
        Address dest=Util.readAddress(in);
        Address src=Util.readAddress(in);
        String cluster_name=Util.readString(in); // not used here
        TpHeader transport_header=new TpHeader(cluster_name);

        int len=in.readInt();

        for(int i=0; i < len; i++) {
            Message msg=new Message(false);
            msg.readFrom(in);
            msg.setDest(dest);
            if(msg.getSrc() == null)
                msg.setSrc(src);

            // Now add a TpHeader back on, was not marshalled. Every message references the *same* TpHeader, saving memory !
            msg.putHeader(transport_id, transport_header);

            list.add(msg);
        }
        return list;
    }

    /**
     * Reads a list of messages into 4 MessageBatches:
     * <ol>
     *     <li>regular</li>
     *     <li>OOB</li>
     *     <li>INTERNAL-OOB (INTERNAL and OOB)</li>
     *     <li>INTERNAL (INTERNAL)</li>
     * </ol>
     * @param in
     * @return an array of 4 MessageBatches in the order above, the first batch is at index 0
     * @throws Exception
     */
    public static MessageBatch[] readMessageBatch(DataInputStream in, boolean multicast) throws Exception {
        MessageBatch[] batches=new MessageBatch[4]; // [0]: reg, [1]: OOB, [2]: internal-oob, [3]: internal
        Address dest=Util.readAddress(in);
        Address src=Util.readAddress(in);
        String  cluster_name=Util.readString(in);

        int len=in.readInt();
        for(int i=0; i < len; i++) {
            Message msg=new Message(false);
            msg.readFrom(in);
            msg.setDest(dest);
            if(msg.getSrc() == null)
                msg.setSrc(src);
            boolean oob=msg.isFlagSet(Message.Flag.OOB);
            boolean internal=msg.isFlagSet(Message.Flag.INTERNAL);
            int index=0;
            MessageBatch.Mode mode=MessageBatch.Mode.REG;

            if(oob && !internal) {
                index=1; mode=MessageBatch.Mode.OOB;
            }
            else if(oob && internal) {
                index=2; mode=MessageBatch.Mode.OOB;
            }
            else if(!oob && internal) {
                index=3; mode=MessageBatch.Mode.INTERNAL;
            }

            if(batches[index] == null)
                batches[index]=new MessageBatch(dest, src, cluster_name, multicast, mode, len);
            batches[index].add(msg);
        }
        return batches;
    }


    @SuppressWarnings("unchecked")
    protected Object handleDownEvent(Event evt) {
        switch(evt.getType()) {

            case Event.TMP_VIEW:
            case Event.VIEW_CHANGE:
                Collection<Address> old_members;
                synchronized(members) {
                    View view=(View)evt.getArg();
                    old_members=new ArrayList<Address>(members);
                    members.clear();

                    if(!isSingleton()) {
                        List<Address> tmpvec=view.getMembers();
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
                    fetchLocalAddresses();

                    List<Address> left_mbrs=Util.leftMembers(old_members,members);
                    if(left_mbrs != null && !left_mbrs.isEmpty())
                        UUID.removeAll(left_mbrs);

                    if(suppress_log_different_version != null)
                        suppress_log_different_version.removeExpired(suppress_time_different_version_warnings);
                    if(suppress_log_different_cluster != null)
                        suppress_log_different_cluster.removeExpired(suppress_time_different_cluster_warnings);
                }
                who_has_cache.removeExpiredElements();
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
                Address addr=(Address)evt.getArg();
                PhysicalAddress physical_addr=getPhysicalAddressFromCache(addr);
                if(physical_addr != null)
                    return physical_addr;
                if(addr != null && local_addr != null && addr.equals(local_addr)) {
                    physical_addr=getPhysicalAddress();
                    if(physical_addr != null)
                        addPhysicalAddressToCache(addr, physical_addr);
                }
                return physical_addr;

            case Event.GET_PHYSICAL_ADDRESSES:
                return getAllPhysicalAddressesFromCache();

            case Event.GET_LOGICAL_PHYSICAL_MAPPINGS:
                return logical_addr_cache.contents();

            case Event.SET_PHYSICAL_ADDRESS:
                Tuple<Address,PhysicalAddress> tuple=(Tuple<Address,PhysicalAddress>)evt.getArg();
                addPhysicalAddressToCache(tuple.getVal1(), tuple.getVal2());
                break;

            case Event.REMOVE_ADDRESS:
                removeLogicalAddressFromCache((Address)evt.getArg());
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
        ThreadFactory[] factories= {timer_thread_factory, default_thread_factory, oob_thread_factory,
          internal_thread_factory, global_thread_factory };

        boolean is_shared_transport=isSingleton();

        for(ThreadFactory factory: factories) {
            if(pattern != null && !is_shared_transport) {
                factory.setPattern(pattern);
            }
            if(cluster_name != null) { // if we have a shared transport, use singleton_name as cluster_name
                factory.setClusterName(is_shared_transport? singleton_name : cluster_name);
            }
            if(local_address != null)
                factory.setAddress(local_address.toString());
        }
    }



    protected static ExecutorService createThreadPool(int min_threads, int max_threads, long keep_alive_time, String rejection_policy,
                                                      BlockingQueue<Runnable> queue, final ThreadFactory factory) {

        ThreadPoolExecutor pool=new ThreadPoolExecutor(min_threads, max_threads, keep_alive_time, TimeUnit.MILLISECONDS, queue);
        pool.setThreadFactory(factory);
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



    protected void passToAllUpProtocols(Event evt) {
        for(Protocol prot: up_prots.values()) {
            try {
                prot.up(evt);
            }
            catch(Exception e) {
                log.error(Util.getMessage("PassUpFailureEvent"), local_addr, evt, e);
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
    public void clearLogicalAddressCache() {
        logical_addr_cache.clear(true);
        fetchLocalAddresses();
    }


    protected abstract PhysicalAddress getPhysicalAddress();

    /* ----------------------------- End of Private Methods ---------------------------------------- */



    /* ----------------------------- Inner Classes ---------------------------------------- */




    protected interface Bundler {
        void start();
        void stop();
        void send(Message msg) throws Exception;
    }



    /**
     * The sender's thread adds a message to the hashmap and - if the accumulated size has been exceeded - sends all
     * bundled messages. The cost of sending the bundled messages is therefore distributed over different threads;
     * whoever happens to send a message exceeding the max size gets to send the accumulated messages. We also use a
     * number of timer tasks to send bundled messages after a certain time has elapsed. This is necessary e.g. when a
     * message is added that doesn't exceed the max size, but then no further messages are added, so elapsed time
     * will trigger the sending, not exceeding of the max size.
     */
    protected class DefaultBundler implements Bundler {
    	static final int 		   		           MIN_NUMBER_OF_BUNDLING_TASKS=2;
        /** Keys are destinations, values are lists of Messages */
        final Map<SingletonAddress,List<Message>>  msgs=new HashMap<SingletonAddress,List<Message>>(36);
        @GuardedBy("lock")
        long                                       count=0;    // current number of bytes accumulated
        int                                        num_msgs=0;
        @GuardedBy("lock")
        int                                        num_bundling_tasks=0;
        long                                       last_bundle_time; // in nanoseconds
        final ReentrantLock                        lock=new ReentrantLock();

        public void start() {}
        public void stop()  {}

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
            String cluster_name;

            if(!isSingleton())
                cluster_name=TP.this.channel_name;
            else {
                TpHeader hdr=(TpHeader)msg.getHeader(id);
                cluster_name=hdr.channel_name;
            }

            SingletonAddress dest=new SingletonAddress(cluster_name, msg.getDest());

            if(msgs.isEmpty())
                last_bundle_time=System.nanoTime();
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
                double percentage=100.0 / max_bundle_size * count;
                StringBuilder sb=new StringBuilder(local_addr + ": sending ").append(num_msgs).append(" msgs (");
                num_msgs=0;
                sb.append(count).append(" bytes (").append(f.format(percentage)).append("% of max_bundle_size)");
                if(last_bundle_time > 0) {
                    long diff=(System.nanoTime() - last_bundle_time) / 1000000;
                    sb.append(", collected in ").append(diff).append("ms) ");
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
                String cluster_name=dst.getClusterName();
                Address src_addr=list.get(0).getSrc();

                boolean multicast=dest == null;
                if(list.size() == 1) {
                    Message msg=null;
                    try {
                        bundler_out_stream.reset();
                        bundler_dos.reset();
                        msg=list.get(0);
                        writeMessage(msg, bundler_dos, multicast);
                        Buffer buf=new Buffer(bundler_out_stream.getRawBuffer(), 0, bundler_out_stream.size());
                        doSend(buf, dest, multicast);
                    }
                    catch(Throwable e) {
                        log.error(Util.getMessage("SendFailure"),
                                  local_addr, (dest == null? "cluster" : dest), msg.size(), e.toString(), msg.printHeaders());
                    }
                }
                else {
                    try {
                        bundler_out_stream.reset();
                        bundler_dos.reset();
                        writeMessageList(dest, src_addr, cluster_name, list, bundler_dos, multicast, id); // flushes output stream when done
                        Buffer buf=new Buffer(bundler_out_stream.getRawBuffer(), 0, bundler_out_stream.size());
                        doSend(buf, dest, multicast);
                    }
                    catch(Throwable e) {
                        log.error(Util.getMessage("FailureSendingMsgBundle"), local_addr, e);
                    }
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
                            log.error(Util.getMessage("FailureSendingMsgBundle"), local_addr, e);
                        }
                    }
                }
                finally {
                    num_bundling_tasks--;
                    lock.unlock();
                }
            }

            public String toString() {
                return TP.this.getClass() + ": BundlingTimer";
            }
        }
    }




    /**
     * This bundler adds all (unicast or multicast) messages to a queue until max size has been exceeded, but does send
     * messages immediately when no other messages are available. https://issues.jboss.org/browse/JGRP-1540
     */
    protected class TransferQueueBundler implements Bundler, Runnable {
        final int                                  threshold;
        final BlockingQueue<Message>               buffer;
        volatile Thread                            bundler_thread;

        /** Keys are destinations, values are lists of Messages */
        final Map<SingletonAddress,List<Message>>  msgs=new HashMap<SingletonAddress,List<Message>>(36);

        final ExposedByteArrayOutputStream         bundler_out_stream=new ExposedByteArrayOutputStream(1024);
        final ExposedDataOutputStream              bundler_dos=new ExposedDataOutputStream(bundler_out_stream);
        long                                       count;    // current number of bytes accumulated
        int                                        num_msgs;
        volatile boolean                           running=true;
        public static final String                 THREAD_NAME="TransferQueueBundler";



        protected TransferQueueBundler(int capacity) {
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

        public Thread getThread()     {return bundler_thread;}
        public int    getBufferSize() {return buffer.size();}


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

        public void run() {
            while(running) {
                Message msg=null;
                try {
                    if(count == 0) {
                        msg=buffer.take();
                        if(msg == null)
                            continue;
                        long size=msg.size();
                        if(count + size >= max_bundle_size || buffer.size() >= threshold) {
                            sendMessages();
                        }
                        addMessage(msg);
                        count+=size;
                    }
                    while(null != (msg=buffer.poll())) {
                        long size=msg.size();
                        if(count + size >= max_bundle_size || buffer.size() >= threshold) {
                            sendMessages();
                        }
                        addMessage(msg);
                        count+=size;
                    }
                    if(count > 0)
                        sendMessages();
                }
                catch(Throwable t) {
                }
            }
        }


        protected void sendMessages() {
            sendBundledMessages(msgs);
            msgs.clear();
            count=0;
        }

        protected void checkLength(long len) throws Exception {
            if(len > max_bundle_size)
                throw new Exception("message size (" + len + ") is greater than max bundling size (" + max_bundle_size +
                        "). Set the fragmentation/bundle size in FRAG and TP correctly");
        }


        protected void addMessage(Message msg) {
            String cluster_name;

            if(!isSingleton())
                cluster_name=TP.this.channel_name;
            else {
                TpHeader hdr=(TpHeader)msg.getHeader(id);
                cluster_name=hdr.channel_name;
            }

            SingletonAddress dest=new SingletonAddress(cluster_name, msg.getDest());

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
        protected void sendBundledMessages(final Map<SingletonAddress,List<Message>> msgs) {
            if(log.isTraceEnabled()) {
                double percentage=100.0 / max_bundle_size * count;
                StringBuilder sb=new StringBuilder(local_addr + ": sending ").append(num_msgs).append(" msgs (");
                sb.append(count).append(" bytes (" + f.format(percentage) + "% of max_bundle_size)");
                sb.append(" to ").append(msgs.size()).append(" destination(s)");
                if(msgs.size() > 1) sb.append(" (dests=").append(msgs.keySet()).append(")");
                log.trace(sb);
                num_msgs=0;
            }


            for(Map.Entry<SingletonAddress,List<Message>> entry: msgs.entrySet()) {
                List<Message> list=entry.getValue();
                if(list.isEmpty())
                    continue;

                SingletonAddress dst=entry.getKey();
                Address dest=dst.getAddress();
                String cluster_name=dst.getClusterName();
                Address src_addr=list.get(0).getSrc();

                boolean multicast=dest == null;
                if(list.size() == 1) {
                    Message msg=null;
                    try {
                        bundler_out_stream.reset();
                        bundler_dos.reset();
                        msg=list.get(0);
                        writeMessage(msg, bundler_dos, multicast);
                        Buffer buf=new Buffer(bundler_out_stream.getRawBuffer(), 0, bundler_out_stream.size());
                        doSend(buf, dest, multicast);
                    }
                    catch(Throwable e) {
                        log.error(Util.getMessage("SendFailure"),
                                  local_addr, (dest == null? "cluster" : dest), msg.size(), e.toString(), msg.printHeaders());
                    }
                }
                else {
                    try {
                        bundler_out_stream.reset();
                        bundler_dos.reset();
                        writeMessageList(dest, src_addr, cluster_name, list, bundler_dos, multicast, id); // flushes output stream when done
                        Buffer buf=new Buffer(bundler_out_stream.getRawBuffer(), 0, bundler_out_stream.size());
                        doSend(buf, dest, multicast);
                    }
                    catch(Throwable e) {
                        log.error(Util.getMessage("FailureSendingMsgBundle"), local_addr, e);
                    }
                }
            }
        }
    }





    /**
     * Used when the transport is shared (singleton_name is not null). Maintains the cluster name, local address and
     * view
     */
    public static class ProtocolAdapter extends Protocol implements DiagnosticsHandler.ProbeHandler {
        String                  cluster_name;
        final short             transport_id;
        TpHeader                header;
        final Set<Address>      members=new CopyOnWriteArraySet<Address>();
        final ThreadFactory     factory;
        protected SocketFactory socket_factory=new DefaultSocketFactory();
        Address                 local_addr;


        public ProtocolAdapter(String cluster_name, Address local_addr, short transport_id, Protocol up, Protocol down, String pattern) {
            this.cluster_name=cluster_name;
            this.local_addr=local_addr;
            this.transport_id=transport_id;
            this.up_prot=up;
            this.down_prot=down;
            this.header=new TpHeader(cluster_name);
            this.factory=new DefaultThreadFactory("", false);
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

        @ManagedAttribute(name="address", description="local address")
        public String getAddressAsString() {
            return local_addr != null? local_addr.toString() : null;
        }

        @ManagedAttribute(name="address_uuid", description="local address")
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
                    List<Address> tmp=view.getMembers();
                    members.clear();
                    members.addAll(tmp);
                    break;
                case Event.CONNECT:
                case Event.CONNECT_WITH_STATE_TRANSFER:
                case Event.CONNECT_USE_FLUSH:
                case Event.CONNECT_WITH_STATE_TRANSFER_USE_FLUSH:  
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

        public Object up(Event evt) {
            if(evt.getType() == Event.MSG) {
                Message msg=(Message)evt.getArg();
                Address dest=msg.getDest();
                if(dest != null && local_addr != null && !dest.equals(local_addr)) {
                    log.warn(Util.getMessage("IncorrectDest"), local_addr, dest);
                    return null;
                }
            }
            return up_prot.up(evt);
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
