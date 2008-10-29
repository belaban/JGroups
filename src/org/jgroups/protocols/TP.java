package org.jgroups.protocols;


import org.jgroups.*;
import org.jgroups.annotations.*;
import org.jgroups.conf.PropertyConverters;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.Protocol;
import org.jgroups.util.*;
import org.jgroups.util.ThreadFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.*;
import java.text.NumberFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
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
 * <li>{@link #sendToAllMembers(byte[], int, int)}
 * <li>{@link #sendToSingleMember(org.jgroups.Address, byte[], int, int)}
 * <li>{@link #init()}
 * <li>{@link #start()}: subclasses <em>must</em> call super.start() <em>after</em> they initialize themselves
 * (e.g., created their sockets).
 * <li>{@link #stop()}: subclasses <em>must</em> call super.stop() after they deinitialized themselves
 * <li>{@link #destroy()}
 * </ul>
 * The create() or start() method has to create a local address.<br>
 * The {@link #receive(Address, Address, byte[], int, int)} method must
 * be called by subclasses when a unicast or multicast message has been received.
 * @author Bela Ban
 * @version $Id: TP.java,v 1.237 2008/10/29 15:13:34 belaban Exp $
 */
@MBean(description="Transport protocol")
@DeprecatedProperty(names={"bind_to_all_interfaces", "use_incoming_packet_handler", "use_outgoing_packet_handler",
        "use_concurrent_stack"})
public abstract class TP extends Protocol {
    
    private static final byte LIST=1; // we have a list of messages rather than a single message when set
    private static final byte MULTICAST=2; // message is a multicast (versus a unicast) message when set
    private static final byte OOB=4; // message has OOB flag set (Message.OOB)

    private static NumberFormat f;
    private static final int INITIAL_BUFSIZE=4095;

    static {
        f=NumberFormat.getNumberInstance();
        f.setGroupingUsed(false);
        f.setMaximumFractionDigits(2);
    }

    private ExposedByteArrayOutputStream out_stream=null;
    private ExposedDataOutputStream      dos=null;
    private final Lock                   out_stream_lock=new ReentrantLock();

    
    
    /* ------------------------------------------ JMX and Properties  ------------------------------------------ */
    
    
    
    @ManagedAttribute
    @Property(converter=PropertyConverters.BindAddress.class,
                      description="The interface (NIC) which should be used by this transport ")
    protected InetAddress bind_addr=null;

    @Property(description="Ignores all bind address parameters and  let's the OS return the local host address. Default is false")
    protected boolean use_local_host=false;

    @ManagedAttribute
    @Property(description=" If true, the transport should use all available interfaces to receive multicast messages. Default is false")
    protected boolean receive_on_all_interfaces=false;

    /**
     * List<NetworkInterface> of interfaces to receive multicasts on. The multicast receive socket will listen
     * on all of these interfaces. This is a comma-separated list of IP addresses or interface names. E.g.
     * "192.168.5.1,eth1,127.0.0.1". Duplicates are discarded; we only bind to
     * an interface once. If this property is set, it overrides receive_on_all_interfaces.
     */
    @ManagedAttribute
    @Property(converter=PropertyConverters.NetworkInterfaceList.class, 
                      description="Comma delimited list of interfaces (IP addresses or interface names) to receive multicasts on")
    protected List<NetworkInterface> receive_interfaces=null;



    /** The port to which the transport binds. 0 means to bind to any (ephemeral) port */
    @Property(name="start_port", deprecatedMessage="start_port is deprecated; use bind_port instead", 
                      description="The port to which the transport binds. Default of 0 binds to any (ephemeral) port")
    protected int bind_port=0;
    
    @Property(name="end_port", deprecatedMessage="end_port is deprecated; use port_range instead")
    protected int port_range=1; // 27-6-2003 bgooren, Only try one port by default

    @Property(description="TODO")
    protected boolean prevent_port_reuse=false;

    /**
     * If true, messages sent to self are treated specially: unicast messages are looped back immediately,
     * multicast messages get a local copy first and - when the real copy arrives - it will be discarded. Useful for
     * Window media (non)sense
     */
    @ManagedAttribute(description="", writable=true)
    @Property(description="Messages to self are looped back immediately if true. Default is false")
    protected boolean loopback=false;

    /**
     * Discard packets with a different version. Usually minor version differences are okay. Setting this property
     * to true means that we expect the exact same version on all incoming packets
     */
    @ManagedAttribute(description="Discard packets with a different version", writable=true)
    @Property(description="Discard packets with a different version if true. Default is false")
    protected boolean discard_incompatible_packets=false;


    @Property(description="Thread naming pattern for threads in this channel. Default is cl")
    protected String thread_naming_pattern="cl";

    @Property(name="oob_thread_pool.enabled",description="Switch for enabling thread pool for OOB messages. Default true")
    protected boolean oob_thread_pool_enabled=true;

    @ManagedAttribute(description="Minimum thread pool size for OOB messages. Default is 2")
    @Property(name="oob_thread_pool.min_threads",description="Minimum thread pool size for OOB messages. Default is 2")
    protected int oob_thread_pool_min_threads=2;
    
    @ManagedAttribute(description="Maximum thread pool size for OOB messages. Default is 10")
    @Property(name="oob_thread_pool.max_threads",description="Maximum thread pool size for OOB messages. Default is 10")
    protected int oob_thread_pool_max_threads=10;
      
    @ManagedAttribute(description="Timeout in milliseconds to remove idle thread from OOB pool. Default is 30000")
    @Property(name="oob_thread_pool.keep_alive_time",description="Timeout in milliseconds to remove idle thread from OOB pool. Default is 30000")
    protected long oob_thread_pool_keep_alive_time=30000;

    @ManagedAttribute(description="Use queue to enqueue incoming OOB messages. Default is true")
    @Property(name="oob_thread_pool.queue_enabled",
                      description="Use queue to enqueue incoming OOB messages. Default is true")
    protected boolean oob_thread_pool_queue_enabled=true;
  
    
    @ManagedAttribute(description="Maximum queue size for incoming OOB messages. Default is 500")
    @Property(name="oob_thread_pool.queue_max_size",description="Maximum queue size for incoming OOB messages. Default is 500")
    protected int oob_thread_pool_queue_max_size=500;
       
    @ManagedAttribute
    @Property(name="oob_thread_pool.rejection_policy",
                      description="Thread rejection policy. Possible values are Abort, Discard, DiscardOldest and Run. Default is Run")
    String oob_thread_pool_rejection_policy="Run";

    @ManagedAttribute(description="Minimum thread pool size for regular messages. Default is 2")
    @Property(name="thread_pool.min_threads",description="Minimum thread pool size for regular messages. Default is 2")
    protected int thread_pool_min_threads=2;

    @ManagedAttribute(description="Maximum thread pool size for regular messages. Default is 10")
    @Property(name="thread_pool.max_threads",description="Maximum thread pool size for regular messages. Default is 10")
    protected int thread_pool_max_threads=10;
   
    
    @ManagedAttribute(description="Timeout in milliseconds to remove idle thread from regular pool. Default is 30000")
    @Property(name="thread_pool.keep_alive_time",description="Timeout in milliseconds to remove idle thread from regular pool. Default is 30000")
    protected long thread_pool_keep_alive_time=30000;

    @ManagedAttribute(description="Switch for enabling thread pool for regular messages. Default true")
    @Property(name="thread_pool.enabled",description="Switch for enabling thread pool for regular messages. Default true")
    protected boolean thread_pool_enabled=true;
  
    @ManagedAttribute(description="Use queue to enqueue incoming regular messages")
    @Property(name="thread_pool.queue_enabled",
                      description="Use queue to enqueue incoming regular messages. Default is true")
    protected boolean thread_pool_queue_enabled=true;

    
    @ManagedAttribute(description="Maximum queue size for incoming OOB messages")
    @Property(name="thread_pool.queue_max_size",
                      description="Maximum queue size for incoming OOB messages. Default is 500")
    protected int thread_pool_queue_max_size=500;

    @ManagedAttribute
    @Property(name="thread_pool.rejection_policy",
                      description="Thread rejection policy. Possible values are Abort, Discard, DiscardOldest and Run Default is Run")
    protected String thread_pool_rejection_policy="Run";

    @ManagedAttribute(description="Number of threads to be used by the timer thread pool")
    @Property(name="timer.num_threads",description="Number of threads to be used by the timer thread pool. Default is 4")
    protected int num_timer_threads=4;
    
    @ManagedAttribute(description="Enable bundling of smaller messages into bigger ones", writable=true)
    @Property(description="Enable bundling of smaller messages into bigger ones. Default is true")
    protected boolean enable_bundling=true;

    /** Enable bundling for unicast messages. Ignored if enable_bundling is off */
    @Property(description="Enable bundling of smaller messages into bigger ones for unicast messages. Default is false")
    protected boolean enable_unicast_bundling=false;

    @Property(description="Switch to enable diagnostic probing. Default is true")
    protected boolean enable_diagnostics=true;
    
    @Property(description="Address for diagnostic probing. Default is 224.0.75.75")
    protected String diagnostics_addr="224.0.75.75";
    
    @Property(description="Port for diagnostic probing. Default is 7500")
    protected int diagnostics_port=7500;
  
    @Property(description="If assigned enable this transport to be a singleton (shared) transport")
    protected String singleton_name=null;

    @Property(description="Path to a file to store currently used ports on this machine.")
    protected String persistent_ports_file=null;
    
    @Property(name="ports_expiry_time",description="Timeout to expire ports used with PortManager. Default is 30000 msec")
    protected long pm_expiry_time=30000L;
    
    @Property(description="Switch to enable tracking of currently used ports on this machine. Default is false")
    protected boolean persistent_ports=false;
    
    
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
    
    
    

    /* --------------------------------------------- JMX  ---------------------------------------------- */

    
    
    
    @ManagedAttribute
    protected long num_msgs_sent=0;
    @ManagedAttribute
    protected long num_msgs_received=0;

    @ManagedAttribute
    protected long num_bytes_sent=0;

    @ManagedAttribute
    protected long num_bytes_received=0;

    /** The name of the group to which this member is connected */
    @ManagedAttribute
    protected String channel_name=null;

    /** whether or not warnings about messages from different groups are logged - private flag, not for common use */
    @ManagedAttribute(writable=true, description="whether or not warnings about messages from different groups are logged")
    private boolean log_discard_msgs=true;

    @ManagedAttribute
    protected long num_oob_msgs_received=0;

    @ManagedAttribute
    protected long num_incoming_msgs_received=0;
    
    

    /* --------------------------------------------- Fields ------------------------------------------------------ */

    
    
    /** The address (host and port) of this member */
    protected Address local_addr=null;

    /** The members of this group (updated when a member joins or leaves) */
    protected final HashSet<Address> members=new HashSet<Address>(11);

    protected View view=null;

    protected final ExposedByteArrayInputStream in_stream=new ExposedByteArrayInputStream(new byte[] { '0' });
    protected final DataInputStream dis=new DataInputStream(in_stream);


    protected ThreadGroup pool_thread_group=new ThreadGroup(Util.getGlobalThreadGroup(), "Thread Pools");

    /** Keeps track of connects and disconnects, in order to start and stop threads */
    protected int connect_count=0;
    
    //http://jira.jboss.org/jira/browse/JGRP-849
    protected final ReentrantLock connectLock = new ReentrantLock();

    /**
     * ================================== OOB thread pool ========================
     */   
    protected Executor oob_thread_pool;
    
    /** Factory which is used by oob_thread_pool */
    protected ThreadFactory oob_thread_factory=null;

    /** Used if oob_thread_pool is a ThreadPoolExecutor and oob_thread_pool_queue_enabled is true */
    protected BlockingQueue<Runnable> oob_thread_pool_queue=null;
   

    /**
     * ================================== Regular thread pool =======================
     */

    /** The thread pool which handles unmarshalling, version checks and dispatching of regular messages */
    protected Executor thread_pool;
    
    /** Factory which is used by oob_thread_pool */
    protected ThreadFactory default_thread_factory=null;

    /** Used if thread_pool is a ThreadPoolExecutor and thread_pool_queue_enabled is true */
    protected BlockingQueue<Runnable> thread_pool_queue=null;

    /**
     * ================================== Timer thread pool  =========================
     */
    protected TimeScheduler timer=null;

    protected ThreadFactory timer_thread_factory;

    /**
     * =================================Default thread factory ========================
     */
    /** Used by all threads created by JGroups outside of the thread pools */
    protected ThreadFactory global_thread_factory=null;

  
    /**
     * If set it will be added to <tt>local_addr</tt>. Used to implement for example transport independent addresses
     */
    protected byte[] additional_data=null;

    private Bundler bundler=null;

    private DiagnosticsHandler diag_handler=null;

    /**
     * If singleton_name is enabled, this map is used to de-multiplex incoming messages according to their cluster
     * names (attached to the message by the transport anyway). The values are the next protocols above the
     * transports.
     */
    private final ConcurrentMap<String,Protocol> up_prots=new ConcurrentHashMap<String,Protocol>();

    protected TpHeader header;

    protected final String name=getName();

    protected PortsManager pm=null;  
   



    /**
     * Creates the TP protocol, and initializes the state variables, does
     * however not start any sockets or threads.
     */
    protected TP() {
    }

    /**
     * debug only
     */
    public String toString() {
        return local_addr != null? name + "(local address: " + local_addr + ')' : name;
    }
    
    public void resetStats() {
        num_msgs_sent=num_msgs_received=num_bytes_sent=num_bytes_received=0;
        num_oob_msgs_received=num_incoming_msgs_received=0;
    }

    public void registerProbeHandler(ProbeHandler handler) {
        if(diag_handler != null)
            diag_handler.registerProbeHandler(handler);
    }

    public void unregisterProbeHandler(ProbeHandler handler) {
        if(diag_handler != null)
            diag_handler.unregisterProbeHandler(handler);
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

    /** @deprecated the concurrent stack is used by default */
    @Deprecated
    public void setUseConcurrentStack(boolean flag) {}


    @ManagedAttribute
    public String getLocalAddressAsString() {return local_addr != null? local_addr.toString() : "n/a";}
    
    @ManagedAttribute
    public boolean isOOBThreadPoolEnabled() { return oob_thread_pool_enabled; }

    @ManagedAttribute
    public boolean isDefaulThreadPoolEnabled() { return thread_pool_enabled; }     

    @ManagedAttribute(description="Maximum number of bytes for messages to be queued until they are sent")
    public int getMaxBundleSize() {return max_bundle_size;}

    @ManagedAttribute(description="Maximum number of bytes for messages to be queued until they are sent",writable=true)
    @Property(name="max_bundle_size")
    public void setMaxBundleSize(int size) {
        if(size <= 0) {
            throw new IllegalArgumentException("max_bundle_size (" + size + ") is <= 0");
        }
        max_bundle_size=size;
    }
    
    @ManagedAttribute(description="Max number of milliseconds until queued messages are sent")
    public long getMaxBundleTimeout() {return max_bundle_timeout;}
    
    @ManagedAttribute(description="Max number of milliseconds until queued messages are sent", writable=true)
    @Property(name="max_bundle_timeout")
    public void setMaxBundleTimeout(long timeout) {
        if(timeout <= 0) {
            throw new IllegalArgumentException("max_bundle_timeout of " + timeout + " is invalid");
        }
        max_bundle_timeout=timeout;
    }

    public Address getLocalAddress() {return local_addr;}
    public String getChannelName() {return channel_name;}
    public boolean isLoopback() {return loopback;}
    public void setLoopback(boolean b) {loopback=b;}

    /** @deprecated With the concurrent stack being the default, this property is ignored */
    public static boolean isUseIncomingPacketHandler() {return false;}

    public ConcurrentMap<String,Protocol> getUpProtocols() {
        return up_prots;
    }

    @ManagedAttribute
    public int getOOBMinPoolSize() {
        return oob_thread_pool instanceof ThreadPoolExecutor? ((ThreadPoolExecutor)oob_thread_pool).getCorePoolSize() : 0;
    }

    @ManagedAttribute
    public void setOOBMinPoolSize(int size) {
        if(oob_thread_pool instanceof ThreadPoolExecutor)
            ((ThreadPoolExecutor)oob_thread_pool).setCorePoolSize(size);
    }

    @ManagedAttribute
    public int getOOBMaxPoolSize() {
        return oob_thread_pool instanceof ThreadPoolExecutor? ((ThreadPoolExecutor)oob_thread_pool).getMaximumPoolSize() : 0;
    }

    @ManagedAttribute
    public void setOOBMaxPoolSize(int size) {
        if(oob_thread_pool instanceof ThreadPoolExecutor)
            ((ThreadPoolExecutor)oob_thread_pool).setMaximumPoolSize(size);
    }

    @ManagedAttribute
    public int getOOBPoolSize() {
        return oob_thread_pool instanceof ThreadPoolExecutor? ((ThreadPoolExecutor)oob_thread_pool).getPoolSize() : 0;
    }

    @ManagedAttribute
    public long getOOBKeepAliveTime() {
        return oob_thread_pool instanceof ThreadPoolExecutor? ((ThreadPoolExecutor)oob_thread_pool).getKeepAliveTime(TimeUnit.MILLISECONDS) : 0;
    }

    @ManagedAttribute
    public void setOOBKeepAliveTime(long time) {
        if(oob_thread_pool instanceof ThreadPoolExecutor)
            ((ThreadPoolExecutor)oob_thread_pool).setKeepAliveTime(time, TimeUnit.MILLISECONDS);
    }

    public long getOOBMessages() {
        return num_oob_msgs_received;
    }

    @ManagedAttribute
    public int getOOBQueueSize() {
        return oob_thread_pool_queue.size();
    }

    public int getOOBMaxQueueSize() {
        return oob_thread_pool_queue_max_size;
    }

    @ManagedAttribute
    public int getIncomingMinPoolSize() {
        return thread_pool instanceof ThreadPoolExecutor? ((ThreadPoolExecutor)thread_pool).getCorePoolSize() : 0;
    }

    @ManagedAttribute
    public void setIncomingMinPoolSize(int size) {
        if(thread_pool instanceof ThreadPoolExecutor)
            ((ThreadPoolExecutor)thread_pool).setCorePoolSize(size);
    }

    @ManagedAttribute
    public int getIncomingMaxPoolSize() {
        return thread_pool instanceof ThreadPoolExecutor? ((ThreadPoolExecutor)thread_pool).getMaximumPoolSize() : 0;
    }

    @ManagedAttribute
    public void setIncomingMaxPoolSize(int size) {
        if(thread_pool instanceof ThreadPoolExecutor)
            ((ThreadPoolExecutor)thread_pool).setMaximumPoolSize(size);
    }

    @ManagedAttribute
    public int getIncomingPoolSize() {
        return thread_pool instanceof ThreadPoolExecutor? ((ThreadPoolExecutor)thread_pool).getPoolSize() : 0;
    }

    @ManagedAttribute
    public long getIncomingKeepAliveTime() {
        return thread_pool instanceof ThreadPoolExecutor? ((ThreadPoolExecutor)thread_pool).getKeepAliveTime(TimeUnit.MILLISECONDS) : 0;
    }

    @ManagedAttribute
    public void setIncomingKeepAliveTime(long time) {
        if(thread_pool instanceof ThreadPoolExecutor)
            ((ThreadPoolExecutor)thread_pool).setKeepAliveTime(time, TimeUnit.MILLISECONDS);
    }

    public long getIncomingMessages() {
        return num_incoming_msgs_received;
    }

    @ManagedAttribute
    public int getIncomingQueueSize() {
        return thread_pool_queue.size();
    }

    public int getIncomingMaxQueueSize() {
        return thread_pool_queue_max_size;
    }

    public void setLogDiscardMessages(boolean flag) {
        log_discard_msgs=flag;
    }

    public boolean getLogDiscardMessages() {
        return log_discard_msgs;
    }


    /**
     * Send to all members in the group. UDP would use an IP multicast message, whereas TCP would send N
     * messages, one for each member
     * @param data The data to be sent. This is not a copy, so don't modify it
     * @param offset
     * @param length
     * @throws Exception
     */
    public abstract void sendToAllMembers(byte[] data, int offset, int length) throws Exception;

    /**
     * Send to all members in the group. UDP would use an IP multicast message, whereas TCP would send N
     * messages, one for each member
     * @param dest Must be a non-null unicast address
     * @param data The data to be sent. This is not a copy, so don't modify it
     * @param offset
     * @param length
     * @throws Exception
     */
    public abstract void sendToSingleMember(Address dest, byte[] data, int offset, int length) throws Exception;

    public abstract String getInfo();

    public abstract void postUnmarshalling(Message msg, Address dest, Address src, boolean multicast);

    public abstract void postUnmarshallingList(Message msg, Address dest, boolean multicast);

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

        setInAllThreadFactories(channel_name, local_addr, thread_naming_pattern);

        timer=new TimeScheduler(timer_thread_factory, num_timer_threads);

        verifyRejectionPolicy(oob_thread_pool_rejection_policy);
        verifyRejectionPolicy(thread_pool_rejection_policy);

        out_stream=new ExposedByteArrayOutputStream(INITIAL_BUFSIZE);
        dos=new ExposedDataOutputStream(out_stream);

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

        if(persistent_ports){
            pm = new PortsManager(pm_expiry_time,persistent_ports_file);
        }
        if(bind_addr != null) {
            Map<String, Object> m=new HashMap<String, Object>(1);
            m.put("bind_addr", bind_addr);
            up(new Event(Event.CONFIG, m));
        }
    }


    public void destroy() {
        super.destroy();
        if(timer != null) {
            try {
                timer.stop();
            }
            catch(InterruptedException e) {
                log.error("failed stopping the timer", e);
            }
        }

        // 3. Stop the thread pools
        if(oob_thread_pool instanceof ThreadPoolExecutor) {
            shutdownThreadPool(oob_thread_pool);
            oob_thread_pool=null;
        }

        if(thread_pool instanceof ThreadPoolExecutor) {
            shutdownThreadPool(thread_pool);
            thread_pool=null;
        }
    }

    /**
     * Creates the unicast and multicast sockets and starts the unicast and multicast receiver threads
     */
    public void start() throws Exception {
        if(timer == null)
            throw new Exception("timer is null");

        if(enable_diagnostics) {
            diag_handler=new DiagnosticsHandler();
            diag_handler.start();
        }

        if(enable_bundling) {
            bundler=new Bundler();
        }

        setInAllThreadFactories(channel_name, local_addr, thread_naming_pattern);
        sendUpLocalAddressEvent();
    }


    public void stop() {
        if(diag_handler != null) {
            diag_handler.stop();
            diag_handler=null;
        }
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
          return singleton_name != null && singleton_name.length() >0;
      }


    /**
     * handle the UP event.
     * @param evt - the event being send from the stack
     */
    public Object up(Event evt) {
        switch(evt.getType()) {
        case Event.CONFIG:
            if(isSingleton())
                passToAllUpProtocols(evt);
            else
                up_prot.up(evt);
            if(log.isDebugEnabled()) log.debug("received CONFIG event: " + evt.getArg());
            handleConfigEvent((Map<String,Object>)evt.getArg());
            return null;
        }
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
            // msg.putHeader(name, new TpHeader(channel_name));
            msg.putHeaderIfAbsent(name, header);
        }

        setSourceAddress(msg); // very important !! listToBuffer() will fail with a null src address !!
        if(log.isTraceEnabled()) {
            log.trace("sending msg to " + msg.getDest() + ", src=" + msg.getSrc() + ", headers are " + msg.printHeaders());
        }

        // Don't send if destination is local address. Instead, switch dst and src and put in up_queue.
        // If multicast message, loopback a copy directly to us (but still multicast). Once we receive this,
        // we will discard our own multicast message
        Address dest=msg.getDest();
        boolean multicast=dest == null || dest.isMulticastAddress();
        if(loopback && (multicast || dest.equals(local_addr))) {

            // we *have* to make a copy, or else up_prot.up() might remove headers from msg which will then *not*
            // be available for marshalling further down (when sending the message)
            final Message copy=msg.copy();
            if(log.isTraceEnabled()) log.trace(new StringBuilder("looping back message ").append(copy));
            // up_prot.up(new Event(Event.MSG, copy));

            // changed to fix http://jira.jboss.com/jira/browse/JGRP-506
            Executor pool=msg.isFlagSet(Message.OOB)? oob_thread_pool : thread_pool;
            pool.execute(new Runnable() {
                public void run() {
                    passMessageUp(copy, false);
                }
            });

            if(!multicast)
                return null;
        }

        try {
            send(msg, dest, multicast);
        }
        catch(InterruptedException interruptedEx) {
            Thread.currentThread().interrupt(); // let someone else handle the interrupt
        }
        catch(Throwable e) {
            if(log.isErrorEnabled()) {
                String dst=msg.getDest() == null? "null" : msg.getDest().toString();
                log.error("failed sending message to " + dst + " (" + msg.size() + " bytes)", e);
            }
        }
        return null;
    }



    /*--------------------------- End of Protocol interface -------------------------- */


    /* ------------------------------ Private Methods -------------------------------- */



    /**
     * If the sender is null, set our own address. We cannot just go ahead and set the address
     * anyway, as we might be sending a message on behalf of someone else ! E.gin case of
     * retransmission, when the original sender has crashed, or in a FLUSH protocol when we
     * have to return all unstable messages with the FLUSH_OK response.
     */
    private void setSourceAddress(Message msg) {
        if(msg.getSrc() == null)
            msg.setSrc(local_addr);
    }


    private void passMessageUp(Message msg, boolean perform_cluster_name_matching) {
        TpHeader hdr=(TpHeader)msg.getHeader(name); // replaced removeHeader() with getHeader()
        if(hdr == null) {
            if(channel_name == null) {
                Event evt=new Event(Event.MSG, msg);
                if(isSingleton()) {
                    passMessageToAll(evt);
                }
                else {
                    up_prot.up(evt);
                }
            }
            else {
                if(log.isErrorEnabled())
                    log.error(new StringBuilder("message does not have a transport header, msg is ").append(msg).
                            append(", headers are ").append(msg.printHeaders()).append(", will be discarded"));
            }
            return;
        }

        String ch_name=hdr.channel_name;
        if(isSingleton()) {
            Protocol tmp_prot=up_prots.get(ch_name);
            if(tmp_prot != null) {
                Event evt=new Event(Event.MSG, msg);
                if(log.isTraceEnabled()) {
                    StringBuilder sb=new StringBuilder("message is ").append(msg).append(", headers are ").append(msg.printHeaders());
                    log.trace(sb);
                }
                tmp_prot.up(evt);
            }
            else {
                // we discard messages for a group we don't have. If we had a scenario with channel C1 and A,B on it,
                // and channel C2 and only A on it (asymmetric setup), then C2 would always log warnings that B was
                // not found (Jan 25 2008 (bela))
                // if(log.isWarnEnabled())
                   // log.warn(new StringBuilder("discarded message from group \"").append(ch_name).
                     //       append("\" (our groups are ").append(up_prots.keySet()).append("). Sender was ").append(msg.getSrc()));
            }
        }
        else {
            // Discard if message's group name is not the same as our group name
            if(perform_cluster_name_matching && channel_name != null && !channel_name.equals(ch_name)) {
                if(log.isWarnEnabled() && log_discard_msgs)
                    log.warn(new StringBuilder("discarded message from different group \"").append(ch_name).
                            append("\" (our group is \"").append(channel_name).append("\"). Sender was ").append(msg.getSrc()));
            }
            else {
                Event evt=new Event(Event.MSG, msg);
                if(log.isTraceEnabled()) {
                    StringBuilder sb=new StringBuilder("message is ").append(msg).append(", headers are ").append(msg.printHeaders());
                    log.trace(sb);
                }
                up_prot.up(evt);
            }
        }
    }

    private void passMessageToAll(Event evt) {
        for(Protocol tmp_prot: up_prots.values()) {
            try {
                tmp_prot.up(evt);
            }
            catch(Exception ex) {
                if(log.isErrorEnabled())
                    log.error("failure passing message up: message is " + evt.getArg(), ex);
            }
        }
    }


    /**
     * Subclasses must call this method when a unicast or multicast message has been received.
     * Declared final so subclasses cannot override this method.
     *
     * @param dest
     * @param sender
     * @param data
     * @param offset
     * @param length
     */
    protected final void receive(Address dest, Address sender, byte[] data, int offset, int length) {
        if(data == null) return;

        if(log.isTraceEnabled()){
            boolean mcast=dest == null || dest.isMulticastAddress();
            StringBuilder sb=new StringBuilder("received (");
            sb.append(mcast? "mcast) " : "ucast) ").append(length).append(" bytes from ").append(sender);
            log.trace(sb);
        }

        try {
            // determine whether OOB or not by looking at first byte of 'data'
            byte oob_flag=data[Global.SHORT_SIZE]; // we need to skip the first 2 bytes (version)
            if((oob_flag & OOB) == OOB) {
                num_oob_msgs_received++;
                dispatchToThreadPool(oob_thread_pool, dest, sender, data, offset, length);
            }
            else {
                num_incoming_msgs_received++;
                dispatchToThreadPool(thread_pool, dest, sender, data, offset, length);
            }
        }
        catch(Throwable t) {
            if(log.isErrorEnabled())
                log.error(new StringBuilder("failed handling data from ").append(sender), t);
        }
    }



    private void dispatchToThreadPool(Executor pool, Address dest, Address sender, byte[] data, int offset, int length) {
        if(pool instanceof DirectExecutor) {
            // we don't make a copy of the buffer if we execute on this thread
            pool.execute(new IncomingPacket(dest, sender, data, offset, length));
        }
        else {
            byte[] tmp=new byte[length];
            System.arraycopy(data, offset, tmp, 0, length);
            pool.execute(new IncomingPacket(dest, sender, tmp, 0, length));
        }
    }




    /** Internal method to serialize and send a message. This method is not reentrant */
    private void send(Message msg, Address dest, boolean multicast) throws Exception {

        // bundle only regular messages; send OOB messages directly
        if(enable_bundling && !msg.isFlagSet(Message.OOB)) {
            if(!enable_unicast_bundling && !multicast) {
                ; // don't bundle unicast msgs if enable_unicast_bundling is off (http://jira.jboss.com/jira/browse/JGRP-429)
            }
            else {
                bundler.send(msg, dest);
                return;
            }
        }

        out_stream_lock.lock();
        try {
            out_stream.reset();
            dos.reset();
            writeMessage(msg, dos, multicast);
            Buffer buf=new Buffer(out_stream.getRawBuffer(), 0, out_stream.size());
            doSend(buf, dest, multicast);
        }
        finally {
            out_stream_lock.unlock();
        }
    }


    private void doSend(Buffer buf, Address dest, boolean multicast) throws Exception {
        if(stats) {
            num_msgs_sent++;
            num_bytes_sent+=buf.getLength();
        }
        if(multicast) {
            sendToAllMembers(buf.getBuf(), buf.getOffset(), buf.getLength());
        }
        else {
            sendToSingleMember(dest, buf.getBuf(), buf.getOffset(), buf.getLength());
        }
    }



    /**
     * This method needs to be synchronized on out_stream when it is called
     * @param msg
     * @return
     * @throws java.io.IOException
     */
    private static void writeMessage(Message msg, DataOutputStream dos, boolean multicast) throws Exception {
        byte flags=0;
        dos.writeShort(Version.version); // write the version
        if(multicast)
            flags+=MULTICAST;
        if(msg.isFlagSet(Message.OOB))
            flags+=OOB;
        dos.writeByte(flags);
        msg.writeTo(dos);
    }

    private Message readMessage(DataInputStream instream, Address dest, Address sender, boolean multicast) throws Exception {
        Message msg=new Message(false); // don't create headers, readFrom() will do this
        msg.readFrom(instream);
        postUnmarshalling(msg, dest, sender, multicast); // allows for optimization by subclass
        return msg;
    }



    private static void writeMessageList(List<Message> msgs, DataOutputStream dos, boolean multicast) throws Exception {
        Address src;
        byte flags=0;
        int len=msgs != null? msgs.size() : 0;
        boolean src_written=false;

        dos.writeShort(Version.version);
        flags+=LIST;
        if(multicast)
            flags+=MULTICAST;
        dos.writeByte(flags);
        dos.writeInt(len);
        if(msgs != null) {
            for(Message msg: msgs) {
                src=msg.getSrc();
                if(!src_written) {
                    Util.writeAddress(src, dos);
                    src_written=true;
                }
                msg.writeTo(dos);
            }
        }
    }

    private List<Message> readMessageList(DataInputStream instream, Address dest, boolean multicast) throws Exception {
        int           len;
        Message       msg;
        Address       src;

        len=instream.readInt();
        List<Message> list=new ArrayList<Message>(len);
        src=Util.readAddress(instream);
        for(int i=0; i < len; i++) {
            msg=new Message(false); // don't create headers, readFrom() will do this
            msg.readFrom(instream);
            postUnmarshallingList(msg, dest, multicast);
            msg.setSrc(src);
            list.add(msg);
        }
        return list;
    }



    protected Object handleDownEvent(Event evt) {
        switch(evt.getType()) {

        case Event.TMP_VIEW:
        case Event.VIEW_CHANGE:
            synchronized(members) {
                view=(View)evt.getArg();
                members.clear();

                if(!isSingleton()) {
                    Vector<Address> tmpvec=view.getMembers();
                    members.addAll(tmpvec);
                }
                else {
                    for(Protocol prot: up_prots.values()) {
                        if(prot instanceof ProtocolAdapter) {
                            ProtocolAdapter ad=(ProtocolAdapter)prot;
                            List<Address> tmp=ad.getMembers();
                            members.addAll(tmp);
                        }
                    }
                }
            }
            break;

        case Event.CONNECT:
        case Event.CONNECT_WITH_STATE_TRANSFER:    
            channel_name=(String)evt.getArg();
            header=new TpHeader(channel_name);
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

        case Event.CONFIG:
            if(log.isDebugEnabled()) log.debug("received CONFIG event: " + evt.getArg());
            handleConfigEvent((Map<String,Object>)evt.getArg());
            break;
        }
        return null;
    }




    protected void setThreadNames() {              
        if(diag_handler != null) {
            global_thread_factory.renameThread(DiagnosticsHandler.THREAD_NAME, diag_handler.getThread());
        }
    }


    protected void unsetThreadNames() {
        if(diag_handler != null && diag_handler.getThread() != null)
            diag_handler.getThread().setName(DiagnosticsHandler.THREAD_NAME);
    }

    private void setInAllThreadFactories(String cluster_name, Address local_address, String pattern) {
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

    
    protected void handleConfigEvent(Map<String,Object> map) {
        if(map == null) return;
        if(map.containsKey("additional_data")) {
            additional_data=(byte[])map.get("additional_data");
            if(local_addr instanceof IpAddress)
                ((IpAddress)local_addr).setAdditionalData(additional_data);
        }
    }


    protected static ExecutorService createThreadPool(int min_threads, int max_threads, long keep_alive_time, String rejection_policy,
                                                      BlockingQueue<Runnable> queue, final ThreadFactory factory) {

        ThreadPoolExecutor pool=new ThreadManagerThreadPoolExecutor(min_threads, max_threads, keep_alive_time, TimeUnit.MILLISECONDS, queue);
        pool.setThreadFactory(factory);

        //default
        RejectedExecutionHandler handler = new ThreadPoolExecutor.CallerRunsPolicy();
        if(rejection_policy != null) {
            if(rejection_policy.equals("abort"))
                handler = new ThreadPoolExecutor.AbortPolicy();
            else if(rejection_policy.equals("discard"))
                handler = new ThreadPoolExecutor.DiscardPolicy();
            else if(rejection_policy.equals("discardoldest"))
                handler = new ThreadPoolExecutor.DiscardOldestPolicy();         
        }
        pool.setRejectedExecutionHandler(new ShutdownRejectedExecutionHandler(handler));

        return pool;
    }


    private static void shutdownThreadPool(Executor thread_pool) {
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
    
    private void verifyRejectionPolicy(String str) throws Exception{
        if(!(str.equalsIgnoreCase("run") || str.equalsIgnoreCase("abort")|| str.equalsIgnoreCase("discard")|| str.equalsIgnoreCase("discardoldest"))) {
            log.error("rejection policy of " + str + " is unknown");
            throw new Exception("Unknown rejection policy " + str);
        }
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

    public void sendUpLocalAddressEvent() {
        if(up_prot != null)
            up(new Event(Event.SET_LOCAL_ADDRESS, local_addr));
        else {
            for(Map.Entry<String,Protocol> entry: up_prots.entrySet()) {
                String tmp=entry.getKey();
                if(tmp.startsWith(Global.DUMMY))
                    continue;
                Protocol prot=entry.getValue();
                prot.up(new Event(Event.SET_LOCAL_ADDRESS, local_addr));
            }
        }
    }

    /* ----------------------------- End of Private Methods ---------------------------------------- */



    /* ----------------------------- Inner Classes ---------------------------------------- */

    class IncomingPacket implements Runnable {
        final Address   dest, sender;
        final byte[]    buf;
        final int       offset, length;

        IncomingPacket(Address dest, Address sender, byte[] buf, int offset, int length) {
            this.dest=dest;
            this.sender=sender;
            this.buf=buf;
            this.offset=offset;
            this.length=length;
        }
        

        /** Code copied from handleIncomingPacket */
        public void run() {
            short                        version=0;
            boolean                      is_message_list, multicast;
            byte                         flags;
            ExposedByteArrayInputStream  in_stream=null;
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
                        log.warn(sb);
                    }
                    if(discard_incompatible_packets)
                        return;
                }

                flags=dis.readByte();
                is_message_list=(flags & LIST) == LIST;
                multicast=(flags & MULTICAST) == MULTICAST;

                if(is_message_list) { // used if message bundling is enabled
                    List<Message> msgs=readMessageList(dis, dest, multicast);
                    for(Message msg: msgs) {
                        if(msg.isFlagSet(Message.OOB)) {
                            log.warn("bundled message should not be marked as OOB");
                        }
                        handleMyMessage(msg, multicast);
                    }
                }
                else {
                    Message msg=readMessage(dis, dest, sender, multicast);
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

            Address src=msg.getSrc();
            if(loopback && multicast && src != null && src.equals(local_addr)) {
                return; // drop message that was already looped back and delivered
            }

            passMessageUp(msg, true);
        }
    }







    private class Bundler {
    	static final int 		   		   MIN_NUMBER_OF_BUNDLING_TASKS=2;
        /** HashMap<Address, List<Message>>. Keys are destinations, values are lists of Messages */
        final Map<Address,List<Message>>   msgs=new HashMap<Address,List<Message>>(36);
        @GuardedBy("lock")
        long                               count=0;    // current number of bytes accumulated
        int                                num_msgs=0;
        @GuardedBy("lock")
        int                                num_bundling_tasks=0;
        long                               last_bundle_time;        
        final ReentrantLock                lock=new ReentrantLock();
        final ExposedByteArrayOutputStream bundler_out_stream=new ExposedByteArrayOutputStream(INITIAL_BUFSIZE);
        final ExposedDataOutputStream      bundler_dos=new ExposedDataOutputStream(bundler_out_stream);


        private void send(Message msg, Address dest) throws Exception {
            long length=msg.size();
            checkLength(length);

            lock.lock();
            try {
                if(count + length >= max_bundle_size) {
                    if(!msgs.isEmpty()) {
                        sendBundledMessages(msgs);
                    }
                }
                addMessage(msg, dest);
                count+=length;
                if(num_bundling_tasks < MIN_NUMBER_OF_BUNDLING_TASKS) {
                    num_bundling_tasks++;
                    timer.schedule(new BundlingTimer(), max_bundle_timeout, TimeUnit.MILLISECONDS);
                }
            }
            finally {
                lock.unlock();
            }
        }

        /** Run with lock acquired */
        private void addMessage(Message msg, Address dest) { // no sync needed, always called with lock held
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
        private void sendBundledMessages(final Map<Address,List<Message>> msgs) {
            boolean   multicast;
            Buffer    buffer;
            Address   dst;

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

            for(Map.Entry<Address,List<Message>> entry: msgs.entrySet()) {
                List<Message> list=entry.getValue();
                if(list.isEmpty())
                    continue;
                dst=entry.getKey();
                multicast=dst == null || dst.isMulticastAddress();
                try {
                    bundler_out_stream.reset();
                    bundler_dos.reset();
                    writeMessageList(list, bundler_dos, multicast); // flushes output stream when done
                    buffer=new Buffer(bundler_out_stream.getRawBuffer(), 0, bundler_out_stream.size());
                    doSend(buffer, dst, multicast);
                }
                catch(Throwable e) {
                    if(log.isErrorEnabled()) log.error("exception sending msg: " + e.toString(), e.getCause());
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
                        sendBundledMessages(msgs);
                    }
                }
                finally {
                    num_bundling_tasks--;
                    lock.unlock();
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
                    }
                    return retval;
                }

                public String[] supportedKeys() {
                    return new String[]{"dump", "keys"};
                }
            });

            diag_sock=new MulticastSocket(diagnostics_port);
            // diag_sock=Util.createMulticastSocket(null, diagnostics_port, log);
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
                diag_sock.close();
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
                catch(IOException e) {
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
                if(!map.containsKey("local_addr"))
                    map.put("local_addr", local_addr != null? local_addr.toString() : "n/a");
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

    public static class ProtocolAdapter extends Protocol {
        final String cluster_name;
        final String transport_name;
        final TpHeader header;
        final List<Address> members=new ArrayList<Address>();
        final ThreadFactory factory;

        public ProtocolAdapter(String cluster_name, String transport_name, Protocol up, Protocol down, String pattern, Address addr) {
            this.cluster_name=cluster_name;
            this.transport_name=transport_name;
            this.up_prot=up;
            this.down_prot=down;
            this.header=new TpHeader(cluster_name);
            this.factory=new DefaultThreadFactory(Util.getGlobalThreadGroup(), "", false);
            factory.setPattern(pattern);
            if(addr != null)
                factory.setAddress(addr.toString());
        }

        @ManagedAttribute(description="Name of the cluster to which this adapter proxies")
        public String getCluster_name() {
            return cluster_name;
        }

        @ManagedAttribute(description="Name of the transport")
        public String getTransport_name() {
            return transport_name;
        }

        public List<Address> getMembers() {
            return Collections.unmodifiableList(members);
        }

        public ThreadFactory getThreadFactory() {
            return factory;
        }

        public Object down(Event evt) {
            switch(evt.getType()) {
                case Event.MSG:
                    Message msg=(Message)evt.getArg();
                    msg.putHeader(transport_name, header);
                    break;
                case Event.VIEW_CHANGE:
                    View view=(View)evt.getArg();
                    Vector<Address> tmp=view.getMembers();
                    members.clear();
                    members.addAll(tmp);
                    break;
                case Event.CONNECT:
                case Event.CONNECT_WITH_STATE_TRANSFER:
                    factory.setClusterName((String)evt.getArg());
                    break;
            }
            return down_prot.down(evt);
        }

        public Object up(Event evt) {
            switch(evt.getType()) {
                case Event.SET_LOCAL_ADDRESS:
                    Address addr=(Address)evt.getArg();
                    if(addr != null)
                        factory.setAddress(addr.toString());
                    break;
            }
            return up_prot.up(evt);
        }

        public String getName() {
            return "TP.ProtocolAdapter";
        }

        public String toString() {
            return cluster_name + " (" + transport_name + ")";
        }
    }
}
