package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.*;
import org.jgroups.conf.AttributeType;
import org.jgroups.conf.PropertyConverters;
import org.jgroups.logging.LogFactory;
import org.jgroups.stack.DiagnosticsHandler;
import org.jgroups.stack.MessageProcessingPolicy;
import org.jgroups.stack.Protocol;
import org.jgroups.util.*;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;

import static org.jgroups.conf.AttributeType.SCALAR;

/**
 * Base class for {@link TP} which captures fields and accessors. Reduces the size of
 * TP (https://issues.redhat.com/browse/JGRP-2592).
 * @author Bela Ban
 * @since  5.5.4
 */
public class TPConfig extends Protocol {

    @LocalAddress
    @Property(name="bind_addr",
      description="The bind address which should be used by this transport. The following special values " +
        "are also recognized: GLOBAL, SITE_LOCAL, LINK_LOCAL, NON_LOOPBACK, match-interface, match-host, match-address",
      defaultValueIPv4=Global.NON_LOOPBACK_ADDRESS, defaultValueIPv6=Global.NON_LOOPBACK_ADDRESS,
      systemProperty={Global.BIND_ADDR},writable=false)
    protected InetAddress             bind_addr;

    /** The port to which the transport binds. 0 means to bind to any (ephemeral) port. See also {@link #port_range} */
    @Property(description="The port to which the transport binds. Default of 0 binds to any (ephemeral) port." +
      " See also port_range",systemProperty={Global.BIND_PORT},writable=false)
    protected int                     bind_port;

    @Property(description="The range of valid ports: [bind_port .. bind_port+port_range ]. " +
      "0 only binds to bind_port and fails if taken")
    protected int                     port_range=10; // 27-6-2003 bgooren, Only try one port by default

    @Property(description="Use \"external_addr\" if you have hosts on different networks, behind firewalls. On each " +
      "firewall, set up a port forwarding rule (sometimes called \"virtual server\") to the local IP " +
      "(e.g. 192.168.1.100) of the host then on each host, set \"external_addr\" TCP transport " +
      "parameter to the external (public IP) address of the firewall.",
      systemProperty=Global.EXTERNAL_ADDR,writable=false)
    protected InetAddress             external_addr;

    @Property(description="Used to map the internal port (bind_port) to an external port. Only used if > 0",
      systemProperty=Global.EXTERNAL_PORT,writable=false)
    protected int                     external_port;

    @Property(description="If true, the transport should use all available interfaces to receive multicast messages")
    protected boolean                 receive_on_all_interfaces;

    @Property(converter=PropertyConverters.NetworkInterfaceList.class,
      description="Comma delimited list of interfaces (IP addresses or interface names) to receive multicasts on")
    protected List<NetworkInterface>  receive_interfaces;

    @Property(description="Max number of elements in the logical address cache before eviction starts")
    protected int                     logical_addr_cache_max_size=2000;

    @Property(description="Time (in ms) after which entries in the logical address cache marked as removable and " +
      "can be removed. 0 never removes any entries (not recommended)",type=AttributeType.TIME)
    protected long                    logical_addr_cache_expiration=360000;

    @Property(description="Interval (in ms) at which the reaper task scans logical_addr_cache and removes entries " +
      "marked as removable. 0 disables reaping.",type=AttributeType.TIME)
    protected long                    logical_addr_cache_reaper_interval=60000;

    @Property(description="The fully qualified name of a class implementing MessageProcessingPolicy")
    protected String                  message_processing_policy="max";

    @Property(description="The fully qualified name of a class implementing LocalTransport")
    protected String                  local_transport_class;

    @Property(description="If true, create virtual threads, otherwise create native threads")
    protected boolean                 use_vthreads=true;

    @Property(description="Thread naming pattern for threads in this channel. Valid values are \"cl\": " +
      "\"c\": includes the cluster name, e.g. \"MyCluster\", " +
      "\"l\": includes the local address of the current member, e.g. \"A\"")
    protected String                  thread_naming_pattern="cl";

    @Property(description="Interval (in ms) at which the time service updates its timestamp. 0 disables the time service",
      type=AttributeType.TIME)
    protected long                    time_service_interval=500;

    /** Whether warnings about messages from different groups are logged - private flag, not for common use */
    @Property(description="whether or not warnings about messages from different groups are logged")
    protected boolean                 log_discard_msgs=true;

    @Property(description="whether or not warnings about messages from members with a different version are discarded")
    protected boolean                 log_discard_msgs_version=true;

    @Property(description="Timeout (in ms) to determine how long to wait until a request to fetch the physical address " +
      "for a given logical address will be sent again. Subsequent requests for the same physical address will therefore " +
      "be spaced at least who_has_cache_timeout ms apart",type=AttributeType.TIME)
    protected long                    who_has_cache_timeout=2000;

    @Property(description="Time during which identical warnings about messages from a member with a different version " +
      "will be suppressed. 0 disables this (every warning will be logged). Setting the log level to ERROR also " +
      "disables this.",type=AttributeType.TIME)
    protected long                    suppress_time_different_version_warnings=60000;

    @Property(description="Time during which identical warnings about messages from a member from a different cluster " +
      "will be suppressed. 0 disables this (every warning will be logged). Setting the log level to ERROR also " +
      "disables this.",type=AttributeType.TIME)
    protected long                    suppress_time_different_cluster_warnings=60000;

    @Property(description="The type of bundler used (\"ring-buffer\", \"transfer-queue\" (default), \"sender-sends\" or " +
      "\"no-bundler\") or the fully qualified classname of a Bundler implementation")
    protected String                  bundler_type="per-destination";

    @ManagedAttribute(description="If enabled, the timer will run non-blocking tasks on its own (runner) thread, and " +
      "not submit them to the thread pool. Otherwise, all tasks are submitted to the thread pool")
    protected boolean                 timer_handle_non_blocking_tasks=true;

    @ManagedAttribute(description="The cluster name")
    protected AsciiString             cluster_name;
    @ManagedAttribute(description="tracing is enabled or disabled for the given log",writable=true)
    protected boolean                 is_trace=log.isTraceEnabled();
    protected PhysicalAddress         local_physical_addr; // The address (host and port) of this member
    protected volatile View           view;
    protected final Set<Address>      members=new CopyOnWriteArraySet<>();
    protected final ReentrantLock     connectLock = new ReentrantLock(); // https://issues.redhat.com/browse/JGRP-849
    protected ThreadFactory           thread_factory; // Factory which is used by the thread pool
    protected TimeScheduler           timer;
    protected TimeService             time_service;
    protected SocketFactory           socket_factory=new DefaultSocketFactory();
    protected TpHeader                header; // The header including the cluster name, sent with each message

    /** Cache keeping track of WHO_HAS requests for physical addresses (given a logical address) and expiring
     * them after who_has_cache_timeout ms */
    protected ExpiryCache<Address>    who_has_cache;

    /** Log to suppress identical warnings for messages from members with different (incompatible) versions */
    protected SuppressLog<Address>    suppress_log_different_version;

    /** Log to suppress identical warnings for messages from members in different clusters */
    protected SuppressLog<Address>    suppress_log_different_cluster;

    // last time (in ns) we sent a discovery request
    protected long                    last_discovery_request;

    /**
     * Maintains mappings between logical and physical addresses. When sending a message to a logical address,  we look
     * up the physical address from logical_addr_cache and send the message to the physical address<br/>
     * The keys are logical addresses, the values physical addresses
     */
    protected LazyRemovalCache<Address,PhysicalAddress> logical_addr_cache;

    protected Future<?>               logical_addr_cache_reaper;

    @Component(name="msg_stats")
    protected MsgStats                msg_stats=new MsgStats();

    @Component(name="bundler")
    protected Bundler                 bundler;

    @Component(name="thread_pool")
    protected ThreadPool              thread_pool=new ThreadPool().log(this.log);

    @Component(name="async_executor")
    protected AsyncExecutor<Object>   async_executor=new AsyncExecutor<>(thread_pool);

    @Component(name="msg_processing_policy")
    protected MessageProcessingPolicy msg_processing_policy=new MaxOneThreadPerSender();

    @Component(name="local_transport")
    protected LocalTransport          local_transport;

    @Component(name="diag")
    protected DiagnosticsHandler      diag_handler;

    @Component
    protected RTT                     rtt=new RTT();


    public InetAddress      getBindAddr()                     {return bind_addr;}
    public <T extends TP> T setBindAddr(InetAddress b)        {this.bind_addr=b; return (T)this;}
    public InetAddress      getBindAddress()                  {return getBindAddr();}
    public <T extends TP> T setBindAddress(InetAddress a)     {return setBindAddr(a);}
    public int              getBindPort()                     {return bind_port;}
    public <T extends TP> T setBindPort(int port)             {this.bind_port=port; return (T)this;}
    public <T extends TP> T setPortRange(int range)           {this.port_range=range; return (T)this;}
    public int              getPortRange()                    {return port_range;}
    public InetAddress      getExternalAddr()                 {return external_addr;}
    public <T extends TP> T setExternalAddr(InetAddress e)    {this.external_addr=e; return (T)this;}
    public int              getExternalPort()                 {return external_port;}
    public <T extends TP> T setExternalPort(int e)            {this.external_port=e; return (T)this;}
    public boolean          receiveOnAllInterfaces()          {return receive_on_all_interfaces;}
    public <T extends TP> T receiveOnAllInterfaces(boolean r) {this.receive_on_all_interfaces=r; return (T)this;}
    public <T extends TP> T setBindToAllInterfaces(boolean f) {this.receive_on_all_interfaces=f; return (T)this;}
    public boolean          isReceiveOnAllInterfaces()        {return receive_on_all_interfaces;}
    public List<NetworkInterface> getReceiveInterfaces()      {return receive_interfaces;}
    public int              getLogicalAddrCacheMaxSize()      {return logical_addr_cache_max_size;}
    public <T extends TP> T setLogicalAddrCacheMaxSize(int l) {this.logical_addr_cache_max_size=l; return (T)this;}
    public long             getLogicalAddrCacheExpiration()   {return logical_addr_cache_expiration;}
    public <T extends TP> T setLogicalAddrCacheExpiration(long l) {this.logical_addr_cache_expiration=l; return (T)this;}
    public long             getLogicalAddrCacheReaperInterval() {return logical_addr_cache_reaper_interval;}
    public <T extends TP> T setLogicalAddrCacheReaperInterval(long l) {this.logical_addr_cache_reaper_interval=l; return (T)this;}
    @Property
    public <T extends TP> T useVirtualThreads(boolean f)      {use_vthreads=f; return (T)this;}
    public boolean          useVirtualThreads()               {return use_vthreads;}
    public String           getThreadNamingPattern()          {return thread_naming_pattern;}
    public <T extends TP> T threadNamingPattern(String p)     {thread_naming_pattern=p; return (T)this;}
    public long             getTimeServiceInterval()          {return time_service_interval;}
    public <T extends TP> T setTimeServiceInterval(long t)    {this.time_service_interval=t; return (T)this;}
    public boolean          logDiscardMsgs()                  {return log_discard_msgs;}
    public <T extends TP> T logDiscardMsgs(boolean l)         {this.log_discard_msgs=l; return (T)this;}
    public boolean          logDiscardMsgsVersion()           {return log_discard_msgs_version;}
    public <T extends TP> T logDiscardMsgsVersion(boolean l)  {this.log_discard_msgs_version=l; return (T)this;}
    public <T extends TP> T setLogDiscardMessages(boolean f)  {return logDiscardMsgs(f);}
    public boolean          getLogDiscardMessages()           {return logDiscardMsgs();}
    public <T extends TP> T setLogDiscardMessagesVersion(boolean f) {return logDiscardMsgsVersion(f);}
    public boolean          getLogDiscardMessagesVersion()    {return logDiscardMsgsVersion();}
    public long             getWhoHasCacheTimeout()           {return who_has_cache_timeout;}
    public <T extends TP> T setWhoHasCacheTimeout(long w)     {this.who_has_cache_timeout=w; return (T)this;}
    public long             getSuppressTimeDifferentVersionWarnings()       {return suppress_time_different_version_warnings;}
    public <T extends TP> T setSuppressTimeDifferentVersionWarnings(long s) {this.suppress_time_different_version_warnings=s; return (T)this;}
    public long             getSuppressTimeDifferentClusterWarnings()       {return suppress_time_different_cluster_warnings;}
    public <T extends TP> T setSuppressTimeDifferentClusterWarnings(long s) {this.suppress_time_different_cluster_warnings=s; return (T)this;}
    public String           getBundlerType()                  {return bundler_type;}
    public <T extends TP> T setBundlerType(String b)          {this.bundler_type=b; return (T)this;}
    public boolean          isTrace()                         {return is_trace;}
    public <T extends TP> T isTrace(boolean i)                {this.is_trace=i; return (T)this;}
    public MsgStats         getMessageStats()                 {return msg_stats;}
    public <T extends TP> T setMessageStats(MsgStats ms)      {this.msg_stats=ms; return (T)this;}
    @Override public void   enableStats(boolean flag)         {super.enableStats(flag); msg_stats.enable(flag);}
    public PhysicalAddress  localPhysicalAddress()            {return local_physical_addr;}
    @ManagedAttribute(description="The physical address of the channel")
    public String           getLocalPhysicalAddress()         {return local_physical_addr != null? local_physical_addr.printIpAddress() : null;}
    public View             view()                            {return view;}
    public ThreadPool       getThreadPool()                   {return thread_pool;}
    public AsyncExecutor<Object> getAsyncExecutor()           {return async_executor;}
    public <T extends TP> T setAsyncExecutor(AsyncExecutor<Object> e) {async_executor=e; return (T)this;}
    public ThreadFactory    getThreadFactory()                {return thread_factory;}
    public TimeScheduler    getTimer()                        {return timer;}
    @ManagedAttribute(name="timer_tasks",description="Number of timer tasks queued up for execution",type=SCALAR,gauge=true)
    public int              getNumTimerTasks()                {return timer.size();}
    @ManagedAttribute(description="Number of threads currently in the pool",type=SCALAR,gauge=true)
    public int              getTimerThreads()                 {return timer.getCurrentThreads();}
    @ManagedOperation
    public String           dumpTimerTasks()                  {return timer.dumpTimerTasks();}
    @ManagedAttribute(description="Class of the timer implementation")
    public String           getTimerClass()                   {return timer != null? timer.getClass().getSimpleName() : "null";}
    @ManagedOperation(description="Purges cancelled tasks from the timer queue")
    public void             removeCancelledTimerTasks()       {timer.removeCancelledTasks();}
    public TimeService      getTimeService()                  {return time_service;}
    public long             timestamp()                       {return time_service != null? time_service.timestamp() : System.nanoTime();}
    public SocketFactory    getSocketFactory()                {return socket_factory;}
    @Override public void   setSocketFactory(SocketFactory f) {if(f != null) socket_factory=f;}
    public Bundler          getBundler()                      {return bundler;}
    @ManagedAttribute(description="Fully qualified classname of bundler")
    public String           getBundlerClass()                 {return bundler != null? bundler.getClass().getName() : "null";}
    public String           clusterName()                     {return getClusterName();}
    @ManagedAttribute(description="Name of the cluster to which this transport is connected")
    public String           getClusterName()                  {return cluster_name != null? cluster_name.toString() : null;}
    public AsciiString      getClusterNameAscii()             {return cluster_name;}
    public MessageProcessingPolicy getMessageProcessingPolicy() {return msg_processing_policy;}
    public MessageProcessingPolicy msgProcessingPolicy()      {return msg_processing_policy;}
    public <T extends TP> T msgProcessingPolicy(MessageProcessingPolicy p) {this.msg_processing_policy=p; return (T)this;}
    public LocalTransport   getLocalTransport()               {return local_transport;}
    public DiagnosticsHandler getDiagnosticsHandler()         {return diag_handler;}
    public RTT              getRTT()                          {return rtt;}
    @ManagedOperation(description="Prints the contents of the who-has cache")
    public String           printWhoHasCache()                {return who_has_cache.toString();}
    public LazyRemovalCache<Address,PhysicalAddress> getLogicalAddressCache() {return logical_addr_cache;}
    @ManagedOperation(description="Dumps the contents of the logical address cache")
    public String           printLogicalAddressCache()        {return logical_addr_cache.printCache(print_function);}
    @ManagedAttribute(description="Returns the number of live threads in the JVM",type=SCALAR,gauge=true)
    public static int       getNumThreads()                   {return ManagementFactory.getThreadMXBean().getThreadCount();}


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

    public <T extends TP> T setThreadPool(Executor thread_pool) {
        this.thread_pool.destroy();
        this.thread_pool.setThreadPool(thread_pool);
        if(timer instanceof TimeScheduler3)
            ((TimeScheduler3)timer).setThreadPool(thread_pool);
        return (T)this;
    }

    public <T extends TP> T setThreadFactory(ThreadFactory factory) {
        thread_factory=factory;
        thread_pool.setThreadFactory(factory);
        return (T)this;
    }

    /**
     * Sets a new timer. This should be done before the transport is initialized; be very careful, as replacing a
     * running timer with tasks in it can wreak havoc !
     * @param timer
     */
    public <T extends TP> T setTimer(TimeScheduler timer)     {this.timer=timer; return (T)this;}

    public <T extends TP> T setTimeService(TimeService ts) {
        if(ts == null)
            return (T)this;
        if(time_service != null)
            time_service.stop();
        time_service=ts;
        time_service.start();
        return (T)this;
    }

    public <T extends TP> T setBundler(Bundler new_bundler) {
        String old_bundler_class=null;
        if(bundler != null) {
            bundler.stop();
            old_bundler_class=bundler.getClass().getName();
            new_bundler.init((TP)this);
            new_bundler.start();
        }
        bundler=new_bundler;
        bundler_type=bundler.getClass().getName();
        if(old_bundler_class != null)
            log.debug("%s: replaced bundler %s with %s", local_addr, old_bundler_class, bundler.getClass().getName());
        return (T)this;
    }

    @ManagedOperation(description="Creates and sets a new bundler. Type has to be either a bundler_type or the fully " +
      "qualified classname of a Bundler impl. Stops the current bundler (if running)")
    public <T extends TP> T bundler(String type) throws Exception {
        return setBundler(createBundler(type, getClass()));
    }

    public static Bundler createBundler(String type, Class<?> cl) throws Exception {
        if(type == null)
            throw new IllegalArgumentException("bundler type has to be non-null");

        switch(type) {
            case "transfer-queue":
            case "tq":
                return new TransferQueueBundler();
            case "no-bundler":
            case "nb":
                return new NoBundler();
            case "pd":
            case "pdb":
            case "per-destination":
                return new PerDestinationBundler();
        }
        Class<Bundler> clazz=(Class<Bundler>)Util.loadClass(type, cl);
        return clazz.getDeclaredConstructor().newInstance();
    }

    @ManagedOperation(description="Changes the message processing policy. The fully qualified name of a class " +
      "implementing MessageProcessingPolicy needs to be given")
    public <T extends TP> T setMessageProcessingPolicy(String policy) {
        if(policy == null)
            return (T)this;
        msg_processing_policy=null;
        message_processing_policy=null;
        if(policy.startsWith("submit"))
            msg_processing_policy=new SubmitToThreadPool();
        else if(policy.startsWith("max"))
            msg_processing_policy=new MaxOneThreadPerSender();
        else if(policy.startsWith("direct"))
            msg_processing_policy=new PassRegularMessagesUpDirectly();
        else if(policy.startsWith("all-direct"))
            msg_processing_policy=new PassAllMessagesUpDirectly();
        else if(policy.startsWith("unbatch"))
            msg_processing_policy=new UnbatchOOBBatches();
        try {
            if(msg_processing_policy == null) {
                Class<MessageProcessingPolicy> clazz=(Class<MessageProcessingPolicy>)Util.loadClass(policy, getClass());
                msg_processing_policy=clazz.getDeclaredConstructor().newInstance();
            }
            msg_processing_policy.init((TP)this);
        }
        catch(Exception e) {
            String f="message_processing_policy %s not found or cannot be created";
            throw new IllegalArgumentException(String.format(f, policy));
        }
        if(msg_processing_policy != null)
            message_processing_policy=policy;
        return (T)this;
    }

    public <T extends TP> T setLocalTransport(LocalTransport l) {
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
                this.local_transport.init((TP)this);
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

    /** Sets a {@link DiagnosticsHandler}. Should be set before the stack is started */
    public <T extends TP> T setDiagnosticsHandler(DiagnosticsHandler handler) throws Exception {
        if(handler != null && diag_handler != null) {
            diag_handler.stop();
            diag_handler=handler;
            diag_handler.start();
        }
        return (T)this;
    }

    public DiagnosticsHandler createDiagnosticsHandler() {
        return new DiagnosticsHandler(log, socket_factory, thread_factory)
          .printHeaders(this::defaultHeaders).sameCluster(this::sameCluster);
    }

    public <T extends TP> T registerProbeHandler(DiagnosticsHandler.ProbeHandler handler) {
        diag_handler.registerProbeHandler(handler);
        return (T)this;
    }

    public <T extends TP> T unregisterProbeHandler(DiagnosticsHandler.ProbeHandler handler) {
        diag_handler.unregisterProbeHandler(handler);
        return (T)this;
    }

    @Property(name="level", description="Sets the level")
    public <T extends Protocol> T setLevel(String level) {
        T retval=super.setLevel(level);
        is_trace=log.isTraceEnabled();
        return retval;
    }

    @ManagedAttribute(description="Type of logger used")
    public static String loggerType() {return LogFactory.loggerType();}

    @ManagedOperation(description="Enables or disables all stats in all protocols")
    public void enableAllStats(boolean flag) {
        for(Protocol p=this; p != null; p=p.getUpProtocol())
            p.enableStats(flag);
    }

    public void resetStats() {
        msg_stats.reset();
        msg_processing_policy.reset();
        if(local_transport != null)
            local_transport.resetStats();
        thread_pool.resetStats();
        async_executor.resetStats();
    }

    @ManagedAttribute(description="Is the logical_addr_cache reaper task running")
    public boolean isLogicalAddressCacheReaperRunning() {
        return logical_addr_cache_reaper != null && !logical_addr_cache_reaper.isDone();
    }

    @ManagedOperation(description="If enabled, the timer will run non-blocking tasks on its own (runner) thread, and " +
      "not submit them to the thread pool. Otherwise, all tasks are submitted to the thread pool")
    public <T extends TP> T enableBlockingTimerTasks(boolean flag) {
        this.timer_handle_non_blocking_tasks=flag;
        timer.setNonBlockingTaskHandling(flag);
        return (T)this;
    }

    @ManagedAttribute(description="Number of messages from members in a different cluster",type=SCALAR)
    public int getDifferentClusterMessages() {
        return suppress_log_different_cluster != null? suppress_log_different_cluster.getCache().size() : 0;
    }

    @ManagedAttribute(description="Number of messages from members with a different JGroups version",type=SCALAR)
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

    public String defaultHeaders(boolean detailed) {
        int num_members=view != null? view.size() : 0;
        String fmt=detailed? "%s (ip=%s)\nview=%s\ncluster=%s\nversion=%s %s\n"
          : "%s [ip=%s, %d mbr(s), cluster=%s, version=%s %s]\n";
        return String.format(fmt, local_addr != null? local_addr.toString() : "n/a", local_physical_addr,
                             detailed? view : num_members, cluster_name, Version.description,
                             Util.JAVA_VERSION.isEmpty()? "" : String.format("(java %s)", Util.JAVA_VERSION));
    }

    protected boolean sameCluster(String req) {
        if(!req.startsWith("cluster="))
            return true;
        String cluster_name_pattern=req.substring("cluster=".length()).trim();
        String cname=getClusterName();
        return cluster_name_pattern == null || Util.patternMatch(cluster_name_pattern, cname);
    }

    /** Don't remove! https://issues.redhat.com/browse/JGRP-2814 */
    @ManagedAttribute(type=SCALAR) @Deprecated
    public long             getNumRejectedMsgs()              {return thread_pool.numberOfRejectedMessages();}
    /** Don't remove! https://issues.redhat.com/browse/JGRP-2814 */
    @ManagedAttribute(type=SCALAR) @Deprecated
    public static long      getNumberOfThreadDumps()          {return ThreadPool.getNumberOfThreadDumps();}
    /** Don't remove! https://issues.redhat.com/browse/JGRP-2814 */
    @ManagedAttribute(type=SCALAR) @Deprecated
    public long             getNumUcastMsgsSent()             {return msg_stats.getNumUcastsSent();}
    /** Don't remove! https://issues.redhat.com/browse/JGRP-2814 */
    @ManagedAttribute(type=SCALAR) @Deprecated
    public long             getNumMcastMsgsSent()             {return msg_stats.getNumMcastsSent();}
    /** Don't remove! https://issues.redhat.com/browse/JGRP-2814 */
    @ManagedAttribute(type=SCALAR) @Deprecated
    public long             getNumUcastMsgsReceived()         {return msg_stats.getNumUcastsReceived();}
    /** Don't remove! https://issues.redhat.com/browse/JGRP-2814 */
    @ManagedAttribute(type=SCALAR) @Deprecated
    public long             getNumMcastMsgsReceived()         {return msg_stats.getNumMcastsReceived();}
}
