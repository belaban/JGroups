package org.jgroups.protocols;


import org.jgroups.*;
import org.jgroups.annotations.*;
import org.jgroups.blocks.LazyRemovalCache;
import org.jgroups.conf.AttributeType;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.conf.PropertyConverters;
import org.jgroups.logging.LogFactory;
import org.jgroups.stack.DiagnosticsHandler;
import org.jgroups.stack.MessageProcessingPolicy;
import org.jgroups.stack.Protocol;
import org.jgroups.util.UUID;
import org.jgroups.util.*;

import java.io.DataInput;
import java.io.InterruptedIOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
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
public abstract class TP extends Protocol implements DiagnosticsHandler.ProbeHandler {
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
      "can be removed. 0 never removes any entries (not recommended)",type=AttributeType.TIME)
    protected long logical_addr_cache_expiration=360000;

    @Property(description="Interval (in ms) at which the reaper task scans logical_addr_cache and removes entries " +
      "marked as removable. 0 disables reaping.",type=AttributeType.TIME)
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
    protected String  message_processing_policy;

    @Property(description="The fully qualified name of a class implementing LocalTransport")
    protected String  local_transport_class;

    @Property(description="If true, create virtual threads (Loom, if available), otherwise create native threads")
    protected boolean use_virtual_threads;

    @Property(description="Thread naming pattern for threads in this channel. Valid values are \"pcl\": " +
      "\"p\": includes the thread name, e.g. \"Incoming thread-1\", \"UDP ucast receiver\", " +
      "\"c\": includes the cluster name, e.g. \"MyCluster\", " +
      "\"l\": includes the local address of the current member, e.g. \"192.168.5.1:5678\"")
    protected String thread_naming_pattern="cl";


    @Property(description="Interval (in ms) at which the time service updates its timestamp. 0 disables the time service",
      type=AttributeType.TIME)
    protected long time_service_interval=500;


    /** Whether or not warnings about messages from different groups are logged - private flag, not for common use */
    @Property(description="whether or not warnings about messages from different groups are logged")
    protected boolean log_discard_msgs=true;

    @Property(description="whether or not warnings about messages from members with a different version are discarded")
    protected boolean log_discard_msgs_version=true;

    @Property(description="Timeout (in ms) to determine how long to wait until a request to fetch the physical address " +
      "for a given logical address will be sent again. Subsequent requests for the same physical address will therefore " +
      "be spaced at least who_has_cache_timeout ms apart",type=AttributeType.TIME)
    protected long who_has_cache_timeout=2000;

    @Property(description="Time during which identical warnings about messages from a member with a different version " +
      "will be suppressed. 0 disables this (every warning will be logged). Setting the log level to ERROR also " +
      "disables this.",type=AttributeType.TIME)
    protected long suppress_time_different_version_warnings=60000;

    @Property(description="Time during which identical warnings about messages from a member from a different cluster " +
      "will be suppressed. 0 disables this (every warning will be logged). Setting the log level to ERROR also " +
      "disables this.",type=AttributeType.TIME)
    protected long suppress_time_different_cluster_warnings=60000;

    @Property(description="The fully qualified name of a MessageFactory implementation",exposeAsManagedAttribute=false)
    protected String msg_factory_class;

    protected MessageFactory msg_factory=new DefaultMessageFactory();

    @Property(description="The type of bundler used (\"ring-buffer\", \"transfer-queue\" (default), \"sender-sends\" or " +
      "\"no-bundler\") or the fully qualified classname of a Bundler implementation")
    protected String bundler_type="transfer-queue";

    @ManagedAttribute(description="Fully qualified classname of bundler")
    public String getBundlerClass() {
        return bundler != null? bundler.getClass().getName() : "null";
    }

    public MessageFactory   getMessageFactory()                 {return msg_factory;}
    public <T extends TP> T setMessageFactory(MessageFactory m) {msg_factory=m; return (T)this;}

    public InetAddress getBindAddr() {return bind_addr;}
    public <T extends TP> T setBindAddr(InetAddress b) {this.bind_addr=b; return (T)this;}

    public InetAddress getExternalAddr() {return external_addr;}
    public <T extends TP> T setExternalAddr(InetAddress e) {this.external_addr=e; return (T)this;}

    public int getExternalPort() {return external_port;}
    public <T extends TP> T setExternalPort(int e) {this.external_port=e; return (T)this;}

    public boolean isTrace() {return is_trace;}
    public <T extends TP> T isTrace(boolean i) {this.is_trace=i; return (T)this;}

    public boolean receiveOnAllInterfaces() {return receive_on_all_interfaces;}
    public <T extends TP> T receiveOnAllInterfaces(boolean r) {this.receive_on_all_interfaces=r; return (T)this;}

    public int getLogicalAddrCacheMaxSize() {return logical_addr_cache_max_size;}
    public <T extends TP> T setLogicalAddrCacheMaxSize(int l) {this.logical_addr_cache_max_size=l; return (T)this;}

    public long getLogicalAddrCacheExpiration() {return logical_addr_cache_expiration;}
    public <T extends TP> T setLogicalAddrCacheExpiration(long l) {this.logical_addr_cache_expiration=l; return (T)this;}

    public long getLogicalAddrCacheReaperInterval() {return logical_addr_cache_reaper_interval;}
    public <T extends TP> T setLogicalAddrCacheReaperInterval(long l) {this.logical_addr_cache_reaper_interval=l; return (T)this;}

    public boolean loopbackCopy() {return loopback_copy;}
    public <T extends TP> T loopbackCopy(boolean l) {this.loopback_copy=l; return (T)this;}

    public boolean loopbackSeparateThread() {return loopback_separate_thread;}
    public <T extends TP> T loopbackSeparateThread(boolean l) {this.loopback_separate_thread=l; return (T)this;}

    public boolean          useVirtualThreads()          {return use_virtual_threads;}
    public <T extends TP> T useVirtualThreads(boolean b) {use_virtual_threads=b; return (T)this;}

    public long getTimeServiceInterval() {return time_service_interval;}
    public <T extends TP> T setTimeServiceInterval(long t) {this.time_service_interval=t; return (T)this;}

    public boolean logDiscardMsgs() {return log_discard_msgs;}
    public <T extends TP> T logDiscardMsgs(boolean l) {this.log_discard_msgs=l; return (T)this;}

    public boolean logDiscardMsgsVersion() {return log_discard_msgs_version;}
    public <T extends TP> T logDiscardMsgsVersion(boolean l) {this.log_discard_msgs_version=l; return (T)this;}

    public long getWhoHasCacheTimeout() {return who_has_cache_timeout;}
    public <T extends TP> T setWhoHasCacheTimeout(long w) {this.who_has_cache_timeout=w; return (T)this;}

    public long getSuppressTimeDifferentVersionWarnings() {return suppress_time_different_version_warnings;}
    public <T extends TP> T setSuppressTimeDifferentVersionWarnings(long s) {this.suppress_time_different_version_warnings=s; return (T)this;}

    public long getSuppressTimeDifferentClusterWarnings() {return suppress_time_different_cluster_warnings;}
    public <T extends TP> T setSuppressTimeDifferentClusterWarnings(long s) {this.suppress_time_different_cluster_warnings=s; return (T)this;}

    public String           getMsgFactoryClass()         {return msg_factory_class;}
    public <T extends TP> T setMsgFactoryClass(String m) {this.msg_factory_class=m; return (T)this;}

    public String           getBundlerType()         {return bundler_type;}
    public <T extends TP> T setBundlerType(String b) {this.bundler_type=b; return (T)this;}


    @ManagedAttribute
    public String getMessageFactoryClass() {
        return msg_factory != null? msg_factory.getClass().getName() : "n/a";
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
        msg_processing_policy=policy.startsWith("submit")? new SubmitToThreadPool() :
          policy.startsWith("max")? new MaxOneThreadPerSender() : null;
        try {
            if(msg_processing_policy == null) {
                Class<MessageProcessingPolicy> clazz=(Class<MessageProcessingPolicy>)Util.loadClass(policy, getClass());
                msg_processing_policy=clazz.getDeclaredConstructor().newInstance();
                message_processing_policy=policy;
            }
            msg_processing_policy.init(this);
        }
        catch(Exception e) {
            log.error("failed setting message_processing_policy", e);
        }
    }

    /* --------------------------------------------- JMX  ---------------------------------------------- */
    @Component(name="msg_stats")
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
    protected PhysicalAddress local_physical_addr;
    protected volatile View   view;

    /** The members of this group (updated when a member joins or leaves). With a shared transport,
     * members contains _all_ members from all channels sitting on the shared transport */
    protected final Set<Address> members=new CopyOnWriteArraySet<>();


    //http://jira.jboss.org/jira/browse/JGRP-849
    protected final ReentrantLock connectLock = new ReentrantLock();
    

    // ================================== Thread pool ======================

    /** The thread pool which handles unmarshalling, version checks and dispatching of messages */
    @Component(name="thread_pool")
    protected ThreadPool              thread_pool=new ThreadPool(this);

    /** Factory which is used by the thread pool */
    protected ThreadFactory           thread_factory;

    // ================================== Timer thread pool  =========================
    protected TimeScheduler           timer;

    protected TimeService             time_service;


    // ================================= Default SocketFactory ========================
    protected SocketFactory           socket_factory=new DefaultSocketFactory();

    @Component(name="bundler")
    protected Bundler                 bundler;

    @Component(name="msg_processing_policy")
    protected MessageProcessingPolicy msg_processing_policy=new MaxOneThreadPerSender();

    @Component(name="local_transport")
    protected LocalTransport          local_transport;

    @Component(name="diag")
    protected DiagnosticsHandler      diag_handler;

    /** The header including the cluster name, sent with each message */
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

    protected static final LazyRemovalCache.Printable<Address,LazyRemovalCache.Entry<PhysicalAddress>> print_function=
      (logical_addr, entry) -> {
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

    

    protected TP() {
    }

    public MsgStats getMessageStats() {return msg_stats;}

    /** Whether or not hardware multicasting is supported */
    public abstract boolean supportsMulticasting();

    public boolean isMulticastCapable() {return supportsMulticasting();}

    public LazyRemovalCache<Address,PhysicalAddress> getLogicalAddressCache() {return logical_addr_cache;}

    public String toString() {
        return local_addr != null? getName() + "(local address: " + local_addr + ')' : getName();
    }

    public <T extends Protocol> T setAddress(Address addr) {
        super.setAddress(addr);
        registerLocalAddress(addr);
        return (T)this;
    }

    public PhysicalAddress localPhysicalAddress() {return local_physical_addr;}
    public View            view()                 {return view;}

    @ManagedAttribute(description="The physical address of the channel")
    public String getLocalPhysicalAddress() {return local_physical_addr != null? local_physical_addr.printIpAddress() : null;}



    public void resetStats() {
        msg_stats.reset();
        avg_batch_size.clear();
        msg_processing_policy.reset();
        if(local_transport != null)
            local_transport.resetStats();
    }

    public <T extends TP> T registerProbeHandler(DiagnosticsHandler.ProbeHandler handler) {
        diag_handler.registerProbeHandler(handler);
        return (T)this;
    }

    public <T extends TP> T unregisterProbeHandler(DiagnosticsHandler.ProbeHandler handler) {
        diag_handler.unregisterProbeHandler(handler);
        return (T)this;
    }

    public DiagnosticsHandler getDiagnosticsHandler() {return diag_handler;}

    /**
     * Sets a {@link DiagnosticsHandler}. Should be set before the stack is started
     */
    public <T extends TP> T setDiagnosticsHandler(DiagnosticsHandler handler) throws Exception {
        if(handler != null) {
            diag_handler.stop();
            diag_handler=handler;
            diag_handler.start();
        }
        return (T)this;
    }

    public LocalTransport    getLocalTransport()                 {return local_transport;}
    public <T extends TP> T  setLocalTransport(LocalTransport l) {
        if(this.local_transport != null) {
            this.local_transport.resetStats();
            this.local_transport.stop();
            this.local_transport.destroy();
            this.local_transport_class=null;
        }
        this.local_transport=l;
        if(this.local_transport != null) {
            try {
                this.local_transport_class=l.getClass().toString();
                this.local_transport.init(this);
                this.local_transport.start();
            }
            catch(Exception ex) {
                log.error("failed setting new local transport", ex);
            }
        }
        return (T)this;
    }

    public <T extends TP> T setLocalTransport(String tp_class) throws Exception {
        if(tp_class == null || tp_class.trim().equals("null"))
            return setLocalTransport((LocalTransport)null);
        Class<?> cl=Util.loadClass(tp_class, getClass());
        LocalTransport ltp=(LocalTransport)cl.getDeclaredConstructor().newInstance();
        return setLocalTransport(ltp);
    }

    public Bundler           getBundler()             {return bundler;}

    /** Installs a bundler. Needs to be done before the channel is connected */
    public <T extends TP> T setBundler(Bundler bundler) {
        if(bundler != null)
            this.bundler=bundler;
        return (T)this;
    }


    public ThreadPool getThreadPool() {
        return thread_pool;
    }

    public <T extends TP> T setThreadPool(Executor thread_pool) {
        if(this.thread_pool != null)
            this.thread_pool.destroy();
        this.thread_pool.setThreadPool(thread_pool);
        return (T)this;
    }

    public ThreadFactory getThreadPoolThreadFactory() {
        return thread_factory;
    }

    public <T extends TP> T setThreadPoolThreadFactory(ThreadFactory factory) {
        thread_factory=factory;
        if(thread_pool != null)
            thread_pool.setThreadFactory(factory);
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


    public InetAddress            getBindAddress()                  {return bind_addr;}
    public <T extends TP> T       setBindAddress(InetAddress a)     {this.bind_addr=a; return (T)this;}
    public int                    getBindPort()                     {return bind_port;}
    public <T extends TP> T       setBindPort(int port)             {this.bind_port=port; return (T)this;}
    public <T extends TP> T       setBindToAllInterfaces(boolean f) {this.receive_on_all_interfaces=f; return (T)this;}
    public boolean                isReceiveOnAllInterfaces()        {return receive_on_all_interfaces;}
    public List<NetworkInterface> getReceiveInterfaces()            {return receive_interfaces;}
    public <T extends TP> T       setPortRange(int range)           {this.port_range=range; return (T)this;}
    public int                    getPortRange()                    {return port_range;}



    @ManagedAttribute(name="timer_tasks",description="Number of timer tasks queued up for execution")
    public int getNumTimerTasks() {return timer.size();}

    @ManagedOperation public String dumpTimerTasks() {return timer.dumpTimerTasks();}

    @ManagedOperation(description="Purges cancelled tasks from the timer queue")
    public void removeCancelledTimerTasks() {timer.removeCancelledTasks();}

    @ManagedAttribute(description="Number of threads currently in the pool")
    public int getTimerThreads() {return timer.getCurrentThreads();}

    @ManagedAttribute(description="Returns the number of live threads in the JVM")
    public static int getNumThreads() {return ManagementFactory.getThreadMXBean().getThreadCount();}

    public <T extends TP> T setLogDiscardMessages(boolean flag)     {log_discard_msgs=flag; return (T)this;}
    public boolean          getLogDiscardMessages()                 {return log_discard_msgs;}
    public <T extends TP> T setLogDiscardMessagesVersion(boolean f) {log_discard_msgs_version=f; return (T)this;}
    public boolean          getLogDiscardMessagesVersion()          {return log_discard_msgs_version;}


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

    public String defaultHeaders(boolean detailed) {
        int num_members=view != null? view.size() : 0;
        String fmt=detailed? "%s (ip=%s)\nview=%s\ncluster=%s\nversion=%s\n"
          : "%s [ip=%s, %d mbr(s), cluster=%s, version=%s]\n";
        return String.format(fmt, local_addr != null? local_addr.toString() : "n/a", local_physical_addr,
                             detailed? view : num_members, cluster_name, Version.description);
    }

    /**
     * Send a unicast to a member. Note that the destination address is a *physical*, not a logical address
     * @param dest Must be a non-null unicast address
     * @param data The data to be sent. This is not a copy, so don't modify it
     */
    public abstract void sendUnicast(PhysicalAddress dest, byte[] data, int offset, int length) throws Exception;

    public abstract String getInfo();

    /* ------------------------------------------------------------------------------- */



    /*------------------------------ Protocol interface ------------------------------ */


    public void init() throws Exception {
        this.id=ClassConfigurator.getProtocolId(TP.class);

        if(use_virtual_threads && !Util.fibersAvailable()) {
            log.warn("use_virtual_threads was set to false, as virtual threads are not available in this Java version");
            use_virtual_threads=false;
        }

        if(local_transport_class != null) {
            Class<?> cl=Util.loadClass(local_transport_class, getClass());
            local_transport=(LocalTransport)cl.getDeclaredConstructor().newInstance();
            local_transport.init(this);
        }

        if(thread_factory == null)
            thread_factory=new LazyThreadFactory("jgroups", false, true)
              .useFibers(use_virtual_threads).log(this.log);

        // local_addr is null when shared transport, channel_name is not used
        setInAllThreadFactories(cluster_name != null? cluster_name.toString() : null, local_addr, thread_naming_pattern);

        diag_handler=createDiagnosticsHandler();

        who_has_cache=new ExpiryCache<>(who_has_cache_timeout);

        if(suppress_time_different_version_warnings > 0)
            suppress_log_different_version=new SuppressLog<>(log, "VersionMismatch", "SuppressMsg");
        if(suppress_time_different_cluster_warnings > 0)
            suppress_log_different_cluster=new SuppressLog<>(log, "MsgDroppedDiffCluster", "SuppressMsg");

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

        if(msg_factory_class != null) {
            Class<MessageFactory> clazz=(Class<MessageFactory>)Util.loadClass(msg_factory_class, getClass());
            msg_factory=clazz.getDeclaredConstructor().newInstance();
        }

        bundler=createBundler(bundler_type);
        bundler.init(this);
    }


    /** Creates the unicast and multicast sockets and starts the unicast and multicast receiver threads */
    public void start() throws Exception {
        timer.start();
        if(time_service != null)
            time_service.start();
        fetchLocalAddresses();
        startDiagnostics();
        bundler.start();
        // local_addr is null when shared transport
        setInAllThreadFactories(cluster_name != null? cluster_name.toString() : null, local_addr, thread_naming_pattern);
    }

    public void stop() {
        stopDiagnostics();
        bundler.stop();
        if(msg_processing_policy != null)
            msg_processing_policy.destroy();

        if(time_service != null)
            time_service.stop();
        timer.stop();
    }

    public void destroy() {
        super.destroy();
        if(local_transport != null)
            local_transport.destroy();
        if(logical_addr_cache_reaper != null) {
            logical_addr_cache_reaper.cancel(false);
            logical_addr_cache_reaper=null;
        }
        if(thread_pool != null)
            thread_pool.destroy();
    }


    @ManagedOperation(description="Creates and sets a new bundler. Type has to be either a bundler_type or the fully " +
      "qualified classname of a Bundler impl. Stops the current bundler (if running)")
    public <T extends TP> T bundler(String type) throws Exception {
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
        diag_handler.setEnabled(true);
        try {
            startDiagnostics();
        }
        catch(Exception e) {
            log.error(Util.getMessage("FailedStartingDiagnostics"), e);
        }
    }

    @ManagedOperation(description="Disables diagnostics and stops DiagnosticsHandler (if running)")
    public void disableDiagnostics() {
        stopDiagnostics();
    }

    protected void startDiagnostics() throws Exception {
        if(diag_handler != null) {
            diag_handler.registerProbeHandler(this);
            diag_handler.start();
        }
    }

    protected void stopDiagnostics() {
        if(diag_handler != null) {
            diag_handler.unregisterProbeHandler(this);
            diag_handler.stop();
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
                    for(DiagnosticsHandler.ProbeHandler handler : diag_handler.getProbeHandlers()) {
                        String[] tmp=handler.supportedKeys();
                        if(tmp != null && tmp.length > 0) {
                            for(String s : tmp)
                                sb.append(s).append(" ");
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

                if(local_transport != null)
                    local_transport.viewChange(this.view);
                break;

            case Event.CONNECT:
            case Event.CONNECT_WITH_STATE_TRANSFER:
            case Event.CONNECT_USE_FLUSH:
            case Event.CONNECT_WITH_STATE_TRANSFER_USE_FLUSH:
                cluster_name=new AsciiString((String)evt.getArg());
                header=new TpHeader(cluster_name);
                setInAllThreadFactories(cluster_name != null? cluster_name.toString() : null, local_addr, thread_naming_pattern);
                setThreadNames();
                connectLock.lock();
                try {
                    if(local_transport != null)
                        local_transport.start();
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
                    if(local_transport != null)
                        local_transport.stop();
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
                local_addr=null;
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
            log.trace("%s: sending msg to %s, src=%s, size=%d, headers are %s", local_addr, dest, sender, msg.size(), msg.printHeaders());

        // Don't send if dest is local address. Instead, send it up the stack. If multicast message, loop back directly
        // to us (but still multicast). Once we receive this, we discard our own multicast message
        boolean multicast=dest == null, do_send=multicast || !dest.equals(sender),
          loop_back=(multicast || dest.equals(sender)) && !msg.isFlagSet(Message.TransientFlag.DONT_LOOPBACK);

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
      return new DiagnosticsHandler(log, socket_factory, thread_factory)
        .printHeaders(this::defaultHeaders).sameCluster(this::sameCluster);
  }

    protected Bundler createBundler(String type) throws Exception {
        if(type == null)
            throw new IllegalArgumentException("bundler type has to be non-null");

        switch(type) {
            case "transfer-queue":
            case "tq":
                return new TransferQueueBundler();
            case "simplified-transfer-queue":
            case "stq":
                return new SimplifiedTransferQueueBundler();
            case "sender-sends":
            case "ss":
                return new SenderSendsBundler();
            case "ring-buffer":
            case "rb":
                return new RingBufferBundler();
            case "ring-buffer-lockless":
            case "rbl":
                return new RingBufferBundlerLockless();
            case "ring-buffer-lockless2":
            case "rbl2":
                return new RingBufferBundlerLockless2();
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
        Class<Bundler> clazz=(Class<Bundler>)Util.loadClass(type, getClass());
        return clazz.getDeclaredConstructor().newInstance();
    }

    protected void loopback(Message msg, final boolean multicast) {
        final Message copy=loopback_copy? msg.copy(true, true) : msg;
        if(is_trace)
            log.trace("%s: looping back message %s, headers are %s", local_addr, copy, copy.printHeaders());

        if(!loopback_separate_thread) {
            passMessageUp(copy, null, false, multicast, false);
            return;
        }
        // changed to fix http://jira.jboss.com/jira/browse/JGRP-506
        msg_processing_policy.loopback(msg, msg.isFlagSet(Message.Flag.OOB));
    }

    protected void _send(Message msg, Address dest) {
        try {
            Bundler tmp_bundler=bundler;
            if(tmp_bundler != null)
                tmp_bundler.send(msg);
        }
        catch(InterruptedIOException ignored) {
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
        if(msg.getSrc() == null && local_addr != null)
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

    protected boolean sameCluster(String req) {
        if(!req.startsWith("cluster="))
            return true;
        String cluster_name_pattern=req.substring("cluster=".length()).trim();
        String cname=getClusterName();
        return cluster_name_pattern == null || Util.patternMatch(cluster_name_pattern, cname);
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
            handleMessageBatch(in, multicast, msg_factory);
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
            handleMessageBatch(in, multicast, msg_factory);
        else
            handleSingleMessage(in, multicast);
    }


    protected void handleMessageBatch(DataInput in, boolean multicast, MessageFactory factory) {
        try {
            final MessageBatch[] batches=Util.readMessageBatch(in, multicast, factory);
            final MessageBatch regular=batches[0], oob=batches[1];

            processBatch(oob,    true);
            processBatch(regular,false);
        }
        catch(Throwable t) {
            log.error(String.format(Util.getMessage("IncomingMsgFailure"), local_addr), t);
        }
    }


    protected void handleSingleMessage(DataInput in, boolean multicast) {
        try {
            short type=in.readShort();
            Message msg=msg_factory.create(type); // don't create headers, readFrom() will do this
            msg.readFrom(in);

            if(!multicast && unicastDestMismatch(msg.getDest()))
                return;

            boolean oob=msg.isFlagSet(Message.Flag.OOB);
            msg_processing_policy.process(msg, oob);
        }
        catch(Throwable t) {
            log.error(String.format(Util.getMessage("IncomingMsgFailure"), local_addr), t);
        }
    }

    protected void processBatch(MessageBatch batch, boolean oob) {
        try {
            if(batch != null && !batch.isEmpty() && !unicastDestMismatch(batch.getDest()))
                msg_processing_policy.process(batch, oob);
        }
        catch(Throwable t) {
            log.error("processing batch failed", t);
        }
    }

    public boolean unicastDestMismatch(Address dest) {
        return dest != null && !(Objects.equals(dest, local_addr) || Objects.equals(dest, local_physical_addr));
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



    public void doSend(byte[] buf, int offset, int length, Address dest) throws Exception {
        if(stats) {
            msg_stats.incrNumMsgsSent(1);
            msg_stats.incrNumBytesSent(length);
        }
        if(dest != null)
            sendTo(dest, buf, offset, length);
        else
            sendToAll(buf, offset, length);
    }


    protected void sendTo(final Address dest, byte[] buf, int offset, int length) throws Exception {
        if(local_transport != null && local_transport.isLocalMember(dest)) {
            try {
                local_transport.sendTo(dest, buf, offset, length);
                return;
            }
            catch(Exception ex) {
                log.warn("failed sending message to %s via local transport, sending message via regular transport: %s",
                         dest, ex);
            }
        }

        PhysicalAddress physical_dest=dest instanceof PhysicalAddress? (PhysicalAddress)dest : getPhysicalAddressFromCache(dest);
        if(physical_dest != null) {
            sendUnicast(physical_dest,buf,offset,length);
            return;
        }
        if(who_has_cache.addIfAbsentOrExpired(dest)) { // true if address was added
            // FIND_MBRS must return quickly
            Responses responses=fetchResponsesFromDiscoveryProtocol(Collections.singletonList(dest));
            try {
                for(PingData data: responses) {
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


    /** Fetches the physical addrs for all mbrs and sends the msg to each physical address. Asks discovery for missing
     * members' physical addresses if needed */
    protected void sendToAll(byte[] buf, int offset, int length) throws Exception {
        List<Address> missing=null;
        Set<Address>  mbrs=members;
        boolean       local_send_successful=true;

        if(mbrs == null || mbrs.isEmpty())
            mbrs=logical_addr_cache.keySet();

        if(local_transport != null) {
            try {
                local_transport.sendToAll(buf, offset, length);
            }
            catch(Exception ex) {
                log.warn("failed sending group message via local transport, sending it via regular transport", ex);
                local_send_successful=false;
            }
        }

        for(Address mbr: mbrs) {
            if(local_send_successful && local_transport != null && local_transport.isLocalMember(mbr))
                continue; // skip if local transport sent the message successfully

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
            catch(SocketException | SocketTimeoutException sock_ex) {
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
        if(addr != null)
            addPhysicalAddressToCache(addr, physical_addr, true);
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
        Thread thread=bundler instanceof TransferQueueBundler? ((TransferQueueBundler)bundler).getThread() :
          bundler instanceof RingBufferBundler? ((RingBufferBundler)bundler).getThread() : null;
        if(thread != null)
            thread_factory.renameThread(TransferQueueBundler.THREAD_NAME, thread);
    }

    protected void setInAllThreadFactories(String cluster_name, Address local_address, String pattern) {
        ThreadFactory[] factories= {thread_factory};

        for(ThreadFactory factory: factories) {
            if(pattern != null)
                factory.setPattern(pattern);
            if(cluster_name != null) // if we have a shared transport, use singleton_name as cluster_name
                factory.setClusterName(cluster_name);
            if(local_address != null)
                factory.setAddress(local_address.toString());
        }
    }


    public boolean addPhysicalAddressToCache(Address logical_addr, PhysicalAddress physical_addr) {
        return addPhysicalAddressToCache(logical_addr, physical_addr, true);
    }

    protected boolean addPhysicalAddressToCache(Address logical_addr, PhysicalAddress physical_addr, boolean overwrite) {
        return logical_addr != null && physical_addr != null &&
          overwrite? logical_addr_cache.add(logical_addr, physical_addr) : logical_addr_cache.addIfAbsent(logical_addr, physical_addr);
    }

    public PhysicalAddress getPhysicalAddressFromCache(Address logical_addr) {
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
