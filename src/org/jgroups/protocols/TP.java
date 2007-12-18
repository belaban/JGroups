package org.jgroups.protocols;


import org.jgroups.*;
import org.jgroups.annotations.GuardedBy;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.*;
import org.jgroups.util.Queue;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
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
 * @version $Id: TP.java,v 1.165 2007/12/18 12:23:54 belaban Exp $
 */
public abstract class TP extends Protocol {

    /** The address (host and port) of this member */
    protected Address         local_addr=null;

    /** The name of the group to which this member is connected */
    protected String          channel_name=null;

    /** The interface (NIC) which should be used by this transport */
    protected InetAddress     bind_addr=null;

    /** Overrides bind_addr, -Djgroups.bind_addr and -Dbind.address: let's the OS return the local host address */
    boolean                   use_local_host=false;

    /** If true, the transport should use all available interfaces to receive multicast messages */
    boolean                   receive_on_all_interfaces=false;

    /** List<NetworkInterface> of interfaces to receive multicasts on. The multicast receive socket will listen
     * on all of these interfaces. This is a comma-separated list of IP addresses or interface names. E.g.
     * "192.168.5.1,eth1,127.0.0.1". Duplicates are discarded; we only bind to an interface once.
     * If this property is set, it override receive_on_all_interfaces.
     */
    List<NetworkInterface>    receive_interfaces=null;

    /** If true, the transport should use all available interfaces to send multicast messages. This means
     * the same multicast message is sent N times, so use with care */
    boolean                   send_on_all_interfaces=false;

    /** List<NetworkInterface> of interfaces to send multicasts on. The multicast send socket will send the
     * same multicast message on all of these interfaces. This is a comma-separated list of IP addresses or
     * interface names. E.g. "192.168.5.1,eth1,127.0.0.1". Duplicates are discarded.
     * If this property is set, it override send_on_all_interfaces.
     */
    List<NetworkInterface>    send_interfaces=null;


    /** The port to which the transport binds. 0 means to bind to any (ephemeral) port */
    int             bind_port=0;
    int				port_range=1; // 27-6-2003 bgooren, Only try one port by default

    boolean         prevent_port_reuse=false;

    /** The members of this group (updated when a member joins or leaves) */
    final protected Vector<Address>    members=new Vector<Address>(11);

    protected View                     view=null;

    final ExposedByteArrayInputStream  in_stream=new ExposedByteArrayInputStream(new byte[]{'0'});
    final DataInputStream              dis=new DataInputStream(in_stream);


    /** If true, messages sent to self are treated specially: unicast messages are
     * looped back immediately, multicast messages get a local copy first and -
     * when the real copy arrives - it will be discarded. Useful for Window
     * media (non)sense */
    boolean         loopback=false;


    /** Discard packets with a different version. Usually minor version differences are okay. Setting this property
     * to true means that we expect the exact same version on all incoming packets */
    protected boolean discard_incompatible_packets=false;

    /** Sometimes receivers are overloaded (they have to handle de-serialization etc).
     * Packet handler is a separate thread taking care of de-serialization, receiver
     * thread(s) simply put packet in queue and return immediately. Setting this to
     * true adds one more thread */
    boolean         use_incoming_packet_handler=true;

    /** Used by packet handler to store incoming DatagramPackets */
    Queue           incoming_packet_queue=null;

    /** Dequeues DatagramPackets from packet_queue, unmarshalls them and
     * calls <tt>handleIncomingUdpPacket()</tt> */
    IncomingPacketHandler   incoming_packet_handler=null;


    /** Used by packet handler to store incoming Messages */
    Queue                  incoming_msg_queue=null;

    IncomingMessageHandler incoming_msg_handler;


    boolean use_concurrent_stack=true;
    ThreadGroup pool_thread_group=new ThreadGroup(Util.getGlobalThreadGroup(), "Thread Pools");

    /**
     * Names the current thread. Valid values are "pcl":
     * p: include the previous (original) name, e.g. "Incoming thread-1", "UDP ucast receiver"
     * c: include the cluster name, e.g. "MyCluster"
     * l: include the local address of the current member, e.g. "192.168.5.1:5678"
     */
    protected ThreadNamingPattern thread_naming_pattern=new ThreadNamingPattern("cl");


    /** ================================== OOB thread pool ============================== */
    /** The thread pool which handles OOB messages */
    Executor oob_thread_pool;
    boolean oob_thread_pool_enabled=true;
    int oob_thread_pool_min_threads=2;
    int oob_thread_pool_max_threads=10;
    /** Number of milliseconds after which an idle thread is removed */
    long oob_thread_pool_keep_alive_time=30000;

    long num_oob_msgs_received=0;

    /** Used if oob_thread_pool is a ThreadPoolExecutor and oob_thread_pool_queue_enabled is true */
    BlockingQueue<Runnable> oob_thread_pool_queue=null;
    /** Whether of not to use a queue with ThreadPoolExecutor (ignored with direct executor) */
    boolean oob_thread_pool_queue_enabled=true;
    /** max number of elements in queue (bounded) */
    int oob_thread_pool_queue_max_size=500;
    /** Possible values are "Abort", "Discard", "DiscardOldest" and "Run". These values might change once we switch to
     * JDK 5's java.util.concurrent package */
    String oob_thread_pool_rejection_policy="Run";

    public Executor getOOBThreadPool() {
        return oob_thread_pool;
    }

    public void setOOBThreadPool(Executor oob_thread_pool) {
        if(this.oob_thread_pool != null) {
            shutdownThreadPool(oob_thread_pool);
        }
        this.oob_thread_pool=oob_thread_pool;
    }
    /** ================================== Regular thread pool ============================== */

    /** The thread pool which handles unmarshalling, version checks and dispatching of regular messages */
    Executor thread_pool;
    boolean thread_pool_enabled=true;
    int thread_pool_min_threads=2;
    int thread_pool_max_threads=10;
    /** Number of milliseconds after which an idle thread is removed */
    long thread_pool_keep_alive_time=30000;

    long num_incoming_msgs_received=0;

    /** Used if thread_pool is a ThreadPoolExecutor and thread_pool_queue_enabled is true */
    BlockingQueue<Runnable> thread_pool_queue=null;
    /** Whether of not to use a queue with ThreadPoolExecutor (ignored with directE decutor) */
    boolean thread_pool_queue_enabled=true;
    /** max number of elements in queue (bounded) */
    int thread_pool_queue_max_size=500;
    /** Possible values are "Abort", "Discard", "DiscardOldest" and "Run". These values might change once we switch to
     * JDK 5's java.util.concurrent package */
    String thread_pool_rejection_policy="Run";

    public Executor getDefaultThreadPool() {
        return thread_pool;
    }

    public void setDefaultThreadPool(Executor thread_pool) {
        if(this.thread_pool != null)
            shutdownThreadPool(this.thread_pool);
        this.thread_pool=thread_pool;
    } /** =============================== End of regular thread pool ============================= */



    /** If set it will be added to <tt>local_addr</tt>. Used to implement
     * for example transport independent addresses */
    byte[]          additional_data=null;

    /** Maximum number of bytes for messages to be queued until they are sent. This value needs to be smaller
        than the largest datagram packet size in case of UDP */
    int max_bundle_size=65535;

    /** Max number of milliseconds until queued messages are sent. Messages are sent when max_bundle_size or
     * max_bundle_timeout has been exceeded (whichever occurs faster)
     */
    long max_bundle_timeout=20;

    /** Enable bundling of smaller messages into bigger ones */
    boolean enable_bundling=false;

    /** Enable bundling for unicast messages. Ignored if enable_bundling is off */
    boolean enable_unicast_bundling=true;

    private Bundler    bundler=null;

    protected TimeScheduler      timer=null;

    private DiagnosticsHandler diag_handler=null;
    boolean enable_diagnostics=true;
    String diagnostics_addr="224.0.0.75";
    int    diagnostics_port=7500;

    /** If this transport is shared, identifies all the transport instances which are to be shared */
    String singleton_name=null;

    /** If singleton_name is enabled, this map is used to de-multiplex incoming messages according to their
     * cluster names (attached to the message by the transport anyway). The values are the next protocols above
     * the transports.
     */
    private final ConcurrentMap<String,Protocol> up_prots=new ConcurrentHashMap<String,Protocol>();

    TpHeader header;
    final String name=getName();

    protected PortsManager pm=null;
    protected String persistent_ports_file=null;
    protected long pm_expiry_time=30000L;

    static final byte LIST      = 1;  // we have a list of messages rather than a single message when set
    static final byte MULTICAST = 2;  // message is a multicast (versus a unicast) message when set
    static final byte OOB       = 4;  // message has OOB flag set (Message.OOB)

    long num_msgs_sent=0, num_msgs_received=0, num_bytes_sent=0, num_bytes_received=0;

    static  NumberFormat f;
    private static final int INITIAL_BUFSIZE=1024;

    static {
        f=NumberFormat.getNumberInstance();
        f.setGroupingUsed(false);
        f.setMaximumFractionDigits(2);
    }




    /**
     * Creates the TP protocol, and initializes the
     * state variables, does however not start any sockets or threads.
     */
    protected TP() {
    }

    /**
     * debug only
     */
    public String toString() {
        return name + "(local address: " + local_addr + ')';
    }

    public void resetStats() {
        num_msgs_sent=num_msgs_received=num_bytes_sent=num_bytes_received=0;
        num_oob_msgs_received=num_incoming_msgs_received=0;
    }

    public long getNumMessagesSent()     {return num_msgs_sent;}
    public long getNumMessagesReceived() {return num_msgs_received;}
    public long getNumBytesSent()        {return num_bytes_sent;}
    public long getNumBytesReceived()    {return num_bytes_received;}
    public String getBindAddress() {return bind_addr != null? bind_addr.toString() : "null";}
    public void setBindAddress(String bind_addr) throws UnknownHostException {
        this.bind_addr=InetAddress.getByName(bind_addr);
    }
    /** @deprecated Use {@link #isReceiveOnAllInterfaces()} instead */
    public boolean getBindToAllInterfaces() {return receive_on_all_interfaces;}
    public void setBindToAllInterfaces(boolean flag) {this.receive_on_all_interfaces=flag;}

    public boolean isReceiveOnAllInterfaces() {return receive_on_all_interfaces;}
    public java.util.List getReceiveInterfaces() {return receive_interfaces;}
    public boolean isSendOnAllInterfaces() {return send_on_all_interfaces;}
    public java.util.List getSendInterfaces() {return send_interfaces;}
    public boolean isDiscardIncompatiblePackets() {return discard_incompatible_packets;}
    public void setDiscardIncompatiblePackets(boolean flag) {discard_incompatible_packets=flag;}
    public boolean isEnableBundling() {return enable_bundling;}
    public void setEnableBundling(boolean flag) {enable_bundling=flag;}

    public boolean isEnable_unicast_bundling() {
        return enable_unicast_bundling;
    }

    public void setEnable_unicast_bundling(boolean enable_unicast_bundling) {
        this.enable_unicast_bundling=enable_unicast_bundling;
    }

    public int getMaxBundleSize() {return max_bundle_size;}
    public void setMaxBundleSize(int size) {max_bundle_size=size;}
    public long getMaxBundleTimeout() {return max_bundle_timeout;}
    public void setMaxBundleTimeout(long timeout) {max_bundle_timeout=timeout;}
    public Address getLocalAddress() {return local_addr;}
    public String getChannelName() {return channel_name;}
    public boolean isLoopback() {return loopback;}
    public void setLoopback(boolean b) {loopback=b;}
    public boolean isUseIncomingPacketHandler() {return use_incoming_packet_handler;}

    public ConcurrentMap<String, Protocol> getUpProtocols() {
        return up_prots;
    }

    public int getOOBMinPoolSize() {
        return oob_thread_pool instanceof ThreadPoolExecutor? ((ThreadPoolExecutor)oob_thread_pool).getCorePoolSize() : 0;
    }

    public void setOOBMinPoolSize(int size) {
        if(oob_thread_pool instanceof ThreadPoolExecutor)
            ((ThreadPoolExecutor)oob_thread_pool).setCorePoolSize(size);
    }

    public int getOOBMaxPoolSize() {
        return oob_thread_pool instanceof ThreadPoolExecutor? ((ThreadPoolExecutor)oob_thread_pool).getMaximumPoolSize() : 0;
    }

    public void setOOBMaxPoolSize(int size) {
        if(oob_thread_pool instanceof ThreadPoolExecutor)
            ((ThreadPoolExecutor)oob_thread_pool).setMaximumPoolSize(size);
    }

    public int getOOBPoolSize() {
        return oob_thread_pool instanceof ThreadPoolExecutor? ((ThreadPoolExecutor)oob_thread_pool).getPoolSize() : 0;
    }

    public long getOOBKeepAliveTime() {
        return oob_thread_pool instanceof ThreadPoolExecutor? ((ThreadPoolExecutor)oob_thread_pool).getKeepAliveTime(TimeUnit.MILLISECONDS) : 0;
    }

    public void setOOBKeepAliveTime(long time) {
        if(oob_thread_pool instanceof ThreadPoolExecutor)
            ((ThreadPoolExecutor)oob_thread_pool).setKeepAliveTime(time, TimeUnit.MILLISECONDS);
    }

    public long getOOBMessages() {
        return num_oob_msgs_received;
    }

    public int getOOBQueueSize() {
        return oob_thread_pool_queue.size();
    }

    public int getOOBMaxQueueSize() {
        return oob_thread_pool_queue_max_size;
    }




    public int getIncomingMinPoolSize() {
        return thread_pool instanceof ThreadPoolExecutor? ((ThreadPoolExecutor)thread_pool).getCorePoolSize() : 0;
    }

    public void setIncomingMinPoolSize(int size) {
        if(thread_pool instanceof ThreadPoolExecutor)
            ((ThreadPoolExecutor)thread_pool).setCorePoolSize(size);
    }

    public int getIncomingMaxPoolSize() {
        return thread_pool instanceof ThreadPoolExecutor? ((ThreadPoolExecutor)thread_pool).getMaximumPoolSize() : 0;
    }

    public void setIncomingMaxPoolSize(int size) {
        if(thread_pool instanceof ThreadPoolExecutor)
            ((ThreadPoolExecutor)thread_pool).setMaximumPoolSize(size);
    }

    public int getIncomingPoolSize() {
        return thread_pool instanceof ThreadPoolExecutor? ((ThreadPoolExecutor)thread_pool).getPoolSize() : 0;
    }

    public long getIncomingKeepAliveTime() {
        return thread_pool instanceof ThreadPoolExecutor? ((ThreadPoolExecutor)thread_pool).getKeepAliveTime(TimeUnit.MILLISECONDS) : 0;
    }

    public void setIncomingKeepAliveTime(long time) {
        if(thread_pool instanceof ThreadPoolExecutor)
            ((ThreadPoolExecutor)thread_pool).setKeepAliveTime(time, TimeUnit.MILLISECONDS);
    }

    public long getIncomingMessages() {
        return num_incoming_msgs_received;
    }

    public int getIncomingQueueSize() {
        return thread_pool_queue.size();
    }

    public int getIncomingMaxQueueSize() {
        return thread_pool_queue_max_size;
    }






    public Map<String,Object> dumpStats() {
        Map<String,Object> retval=super.dumpStats();
        if(retval == null)
            retval=new HashMap<String,Object>();
        retval.put("num_msgs_sent", new Long(num_msgs_sent));
        retval.put("num_msgs_received", new Long(num_msgs_received));
        retval.put("num_bytes_sent", new Long(num_bytes_sent));
        retval.put("num_bytes_received", new Long(num_bytes_received));
        return retval;
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


    private StringBuffer _getInfo() {
        StringBuffer sb=new StringBuffer();
        sb.append(local_addr).append(" (").append(channel_name).append(") ").append("\n");
        sb.append("local_addr=").append(local_addr).append("\n");
        sb.append("group_name=").append(channel_name).append("\n");
        sb.append("version=").append(Version.description).append(", cvs=\"").append(Version.cvs).append("\"\n");
        sb.append("view: ").append(view).append('\n');
        sb.append(getInfo());
        return sb;
    }


    private void handleDiagnosticProbe(SocketAddress sender, DatagramSocket sock, String request) {
        try {
            StringTokenizer tok=new StringTokenizer(request);
            String req=tok.nextToken();
            StringBuffer info=new StringBuffer("n/a");
            if(req.trim().toLowerCase().startsWith("query")) {
                ArrayList<String> l=new ArrayList<String>(tok.countTokens());
                while(tok.hasMoreTokens())
                    l.add(tok.nextToken().trim().toLowerCase());

                info=_getInfo();

                if(l.contains("jmx")) {
                    Channel ch=stack.getChannel();
                    if(ch != null) {
                        Map m=ch.dumpStats();
                        StringBuffer sb=new StringBuffer();
                        sb.append("stats:\n");
                        for(Iterator it=m.entrySet().iterator(); it.hasNext();) {
                            sb.append(it.next()).append("\n");
                        }
                        info.append(sb);
                    }
                }
                if(l.contains("props")) {
                    String p=stack.printProtocolSpecAsXML();
                    info.append("\nprops:\n").append(p);
                }
                if(l.contains("info")) {
                    Map<String, Object> tmp=stack.getChannel().getInfo();
                    info.append("INFO:\n");
                    for(Map.Entry<String,Object> entry: tmp.entrySet()) {
                        info.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
                    }
                }
            }


            byte[] diag_rsp=info.toString().getBytes();
            if(log.isDebugEnabled())
                log.debug("sending diag response to " + sender);
            sendResponse(sock, sender, diag_rsp);
        }
        catch(Throwable t) {
            if(log.isErrorEnabled())
                log.error("failed sending diag rsp to " + sender, t);
        }
    }

    private static void sendResponse(DatagramSocket sock, SocketAddress sender, byte[] buf) throws IOException {
        DatagramPacket p=new DatagramPacket(buf, 0, buf.length, sender);
        sock.send(p);
    }

    /* ------------------------------------------------------------------------------- */



    /*------------------------------ Protocol interface ------------------------------ */


    public void init() throws Exception {
        super.init();
        if(bind_addr != null) {
            Map<String, Object> m=new HashMap<String, Object>(1);
            m.put("bind_addr", bind_addr);
            up(new Event(Event.CONFIG, m));
        }

        HashMap<String, ThreadNamingPattern> map=new HashMap<String, ThreadNamingPattern>();
        map.put("thread_naming_pattern", thread_naming_pattern);
        up(new Event(Event.INFO, map));
    }


    /**
     * Creates the unicast and multicast sockets and starts the unicast and multicast receiver threads
     */
    public void start() throws Exception {
        timer=stack.timer;
        if(timer == null)
            throw new Exception("timer is null");

        if(enable_diagnostics) {
            diag_handler=new DiagnosticsHandler();
            diag_handler.start();
        }

        if(use_incoming_packet_handler && !use_concurrent_stack) {
            incoming_packet_queue=new Queue();
            incoming_packet_handler=new IncomingPacketHandler();
            incoming_packet_handler.start();
        }


        // ========================================== OOB thread pool ==============================
        if(oob_thread_pool_enabled) { // create a ThreadPoolExecutor for the unmarshaller thread pool
            if(oob_thread_pool_queue_enabled)
                oob_thread_pool_queue=new LinkedBlockingQueue<Runnable>(oob_thread_pool_queue_max_size);
            else
                oob_thread_pool_queue=new SynchronousQueue<Runnable>();
            oob_thread_pool=createThreadPool(oob_thread_pool_min_threads, oob_thread_pool_max_threads, oob_thread_pool_keep_alive_time,
                                             oob_thread_pool_rejection_policy, oob_thread_pool_queue, "OOB");
        }
        else { // otherwise use the caller's thread to unmarshal the byte buffer into a message
            oob_thread_pool=new DirectExecutor();
        }


        // ====================================== Regular thread pool ===========================
        if(thread_pool_enabled) { // create a ThreadPoolExecutor for the unmarshaller thread pool
            if(thread_pool_queue_enabled)
                thread_pool_queue=new LinkedBlockingQueue<Runnable>(thread_pool_queue_max_size);
            else
                thread_pool_queue=new SynchronousQueue<Runnable>();
            thread_pool=createThreadPool(thread_pool_min_threads, thread_pool_max_threads, thread_pool_keep_alive_time,
                                         thread_pool_rejection_policy, thread_pool_queue, "Incoming");
        }
        else { // otherwise use the caller's thread to unmarshal the byte buffer into a message
            thread_pool=new DirectExecutor();
        }


        if(loopback && !use_concurrent_stack) {
            incoming_msg_queue=new Queue();
            incoming_msg_handler=new IncomingMessageHandler();
            incoming_msg_handler.start();
        }

        if(enable_bundling) {
            bundler=new Bundler();
        }

        thread_naming_pattern.setAddress(local_addr);
        sendUpLocalAddressEvent();
    }


    public void stop() {
        if(diag_handler != null) {
            diag_handler.stop();
            diag_handler=null;
        }

        // 1. Stop the incoming packet handler thread
        if(incoming_packet_handler != null)
            incoming_packet_handler.stop();


        // 2. Stop the incoming message handler
        if(incoming_msg_handler != null)
            incoming_msg_handler.stop();

        // 3. Stop the thread pools

        if(oob_thread_pool instanceof ThreadPoolExecutor) {
            shutdownThreadPool(oob_thread_pool);
        }

        if(thread_pool instanceof ThreadPoolExecutor) {
            shutdownThreadPool(thread_pool);
        }
    }




    /**
     * Setup the Protocol instance according to the configuration string
     * @return true if no other properties are left.
     *         false if the properties still have data in them, ie ,
     *         properties are left over and not handled by the protocol stack
     */
    public boolean setProperties(Properties props) {
        super.setProperties(props);
        String str;

        try {
            bind_addr=Util.getBindAddress(props);
        }
        catch(UnknownHostException unknown) {
            log.fatal("failed getting bind_addr", unknown);
            return false;
        }
        catch(SocketException ex) {
            log.fatal("failed getting bind_addr", ex);
            return false;
        }

        str=props.getProperty("use_local_host");
        if(str != null) {
            use_local_host=Boolean.parseBoolean(str);
            props.remove("use_local_host");
        }

        str=props.getProperty("bind_to_all_interfaces");
        if(str != null) {
            receive_on_all_interfaces=Boolean.parseBoolean(str);
            props.remove("bind_to_all_interfaces");
            log.warn("bind_to_all_interfaces has been deprecated; use receive_on_all_interfaces instead");
        }

        str=props.getProperty("receive_on_all_interfaces");
        if(str != null) {
            receive_on_all_interfaces=Boolean.parseBoolean(str);
            props.remove("receive_on_all_interfaces");
        }

        str=props.getProperty("receive_interfaces");
        if(str != null) {
            try {
                receive_interfaces=Util.parseInterfaceList(str);
                props.remove("receive_interfaces");
            }
            catch(Exception e) {
                log.error("error determining interfaces (" + str + ")", e);
                return false;
            }
        }

        str=props.getProperty("send_on_all_interfaces");
        if(str != null) {
            send_on_all_interfaces=Boolean.parseBoolean(str);
            props.remove("send_on_all_interfaces");
        }

        str=props.getProperty("send_interfaces");
        if(str != null) {
            try {
                send_interfaces=Util.parseInterfaceList(str);
                props.remove("send_interfaces");
            }
            catch(Exception e) {
                log.error("error determining interfaces (" + str + ")", e);
                return false;
            }
        }

        str=props.getProperty("bind_port");
        if(str != null) {
            bind_port=Integer.parseInt(str);
            props.remove("bind_port");
        }

        str=props.getProperty("port_range");
        if(str != null) {
            port_range=Integer.parseInt(str);
            props.remove("port_range");
        }

        str=props.getProperty("persistent_ports_file");
        if(str != null) {
            persistent_ports_file=str;
            props.remove("persistent_ports_file");
        }

        str=props.getProperty("ports_expiry_time");
        if(str != null) {
            pm_expiry_time=Integer.parseInt(str);
            if(pm != null)
                pm.setExpiryTime(pm_expiry_time);
            props.remove("ports_expiry_time");
        }

        str=props.getProperty("persistent_ports");
        if(str != null) {
            if(Boolean.valueOf(str).booleanValue())
                pm=new PortsManager(pm_expiry_time, persistent_ports_file);
            props.remove("persistent_ports");
        }

        str=props.getProperty("prevent_port_reuse");
        if(str != null) {
            prevent_port_reuse=Boolean.valueOf(str);
            props.remove("prevent_port_reuse");
        }

        str=props.getProperty("loopback");
        if(str != null) {
            loopback=Boolean.valueOf(str).booleanValue();
            props.remove("loopback");
        }

        str=props.getProperty("discard_incompatible_packets");
        if(str != null) {
            discard_incompatible_packets=Boolean.valueOf(str).booleanValue();
            props.remove("discard_incompatible_packets");
        }

        // this is deprecated, just left for compatibility (use use_incoming_packet_handler)
        str=props.getProperty("use_packet_handler");
        if(str != null) {
            use_incoming_packet_handler=Boolean.valueOf(str).booleanValue();
            props.remove("use_packet_handler");
            if(log.isWarnEnabled()) log.warn("'use_packet_handler' is deprecated; use 'use_incoming_packet_handler' instead");
        }

        str=props.getProperty("use_incoming_packet_handler");
        if(str != null) {
            use_incoming_packet_handler=Boolean.valueOf(str).booleanValue();
            props.remove("use_incoming_packet_handler");
        }

        str=props.getProperty("use_concurrent_stack");
        if(str != null) {
            use_concurrent_stack=Boolean.valueOf(str).booleanValue();
            props.remove("use_concurrent_stack");
        }

        str=props.getProperty("thread_naming_pattern");
        if(str != null) {
            thread_naming_pattern=new ThreadNamingPattern(str);
            props.remove("thread_naming_pattern");
        }

        // ======================================= OOB thread pool =========================================
        str=props.getProperty("oob_thread_pool.enabled");
        if(str != null) {
            oob_thread_pool_enabled=Boolean.valueOf(str).booleanValue();
            props.remove("oob_thread_pool.enabled");
        }

        str=props.getProperty("oob_thread_pool.min_threads");
        if(str != null) {
            oob_thread_pool_min_threads=Integer.valueOf(str).intValue();
            props.remove("oob_thread_pool.min_threads");
        }

        str=props.getProperty("oob_thread_pool.max_threads");
        if(str != null) {
            oob_thread_pool_max_threads=Integer.valueOf(str).intValue();
            props.remove("oob_thread_pool.max_threads");
        }

        str=props.getProperty("oob_thread_pool.keep_alive_time");
        if(str != null) {
            oob_thread_pool_keep_alive_time=Long.valueOf(str).longValue();
            props.remove("oob_thread_pool.keep_alive_time");
        }

        str=props.getProperty("oob_thread_pool.queue_enabled");
        if(str != null) {
            oob_thread_pool_queue_enabled=Boolean.valueOf(str).booleanValue();
            props.remove("oob_thread_pool.queue_enabled");
        }

        str=props.getProperty("oob_thread_pool.queue_max_size");
        if(str != null) {
            oob_thread_pool_queue_max_size=Integer.valueOf(str).intValue();
            props.remove("oob_thread_pool.queue_max_size");
        }

        str=props.getProperty("oob_thread_pool.rejection_policy");
        if(str != null) {
            str=str.toLowerCase().trim();
            oob_thread_pool_rejection_policy=str;
            if(!(str.equals("run") || str.equals("abort")|| str.equals("discard")|| str.equals("discardoldest"))) {
                log.error("rejection policy of " + str + " is unknown");
                return false;
            }
            props.remove("oob_thread_pool.rejection_policy");
        }




        // ======================================= Regular thread pool =========================================
        str=props.getProperty("thread_pool.enabled");
        if(str != null) {
            thread_pool_enabled=Boolean.valueOf(str).booleanValue();
            props.remove("thread_pool.enabled");
        }

        str=props.getProperty("thread_pool.min_threads");
        if(str != null) {
            thread_pool_min_threads=Integer.valueOf(str).intValue();
            props.remove("thread_pool.min_threads");
        }

        str=props.getProperty("thread_pool.max_threads");
        if(str != null) {
            thread_pool_max_threads=Integer.valueOf(str).intValue();
            props.remove("thread_pool.max_threads");
        }

        str=props.getProperty("thread_pool.keep_alive_time");
        if(str != null) {
            thread_pool_keep_alive_time=Long.valueOf(str).longValue();
            props.remove("thread_pool.keep_alive_time");
        }

        str=props.getProperty("thread_pool.queue_enabled");
        if(str != null) {
            thread_pool_queue_enabled=Boolean.valueOf(str).booleanValue();
            props.remove("thread_pool.queue_enabled");
        }

        str=props.getProperty("thread_pool.queue_max_size");
        if(str != null) {
            thread_pool_queue_max_size=Integer.valueOf(str).intValue();
            props.remove("thread_pool.queue_max_size");
        }

        str=props.getProperty("thread_pool.rejection_policy");
        if(str != null) {
            str=str.toLowerCase().trim();
            thread_pool_rejection_policy=str;
            if(!(str.equals("run") || str.equals("abort")|| str.equals("discard")|| str.equals("discardoldest"))) {
                log.error("rejection policy of " + str + " is unknown");
                return false;
            }
            props.remove("thread_pool.rejection_policy");
        }


        str=props.getProperty("use_outgoing_packet_handler");
        if(str != null) {
            log.warn("Attribute \"use_outgoing_packet_handler\" has been deprecated and is ignored");
            props.remove("use_outgoing_packet_handler");
        }

        str=props.getProperty("outgoing_queue_max_size");
        if(str != null) {
            log.warn("Attribute \"use_outgoing_queue_max_size\" has been deprecated and is ignored");
            props.remove("outgoing_queue_max_size");
        }

        str=props.getProperty("max_bundle_size");
        if(str != null) {
            int bundle_size=Integer.parseInt(str);
            if(bundle_size > max_bundle_size) {
                if(log.isErrorEnabled()) log.error("max_bundle_size (" + bundle_size +
                        ") is greater than largest TP fragmentation size (" + max_bundle_size + ')');
                return false;
            }
            if(bundle_size <= 0) {
                if(log.isErrorEnabled()) log.error("max_bundle_size (" + bundle_size + ") is <= 0");
                return false;
            }
            max_bundle_size=bundle_size;
            props.remove("max_bundle_size");
        }

        str=props.getProperty("max_bundle_timeout");
        if(str != null) {
            max_bundle_timeout=Long.parseLong(str);
            if(max_bundle_timeout <= 0) {
                if(log.isErrorEnabled()) log.error("max_bundle_timeout of " + max_bundle_timeout + " is invalid");
                return false;
            }
            props.remove("max_bundle_timeout");
        }

        str=props.getProperty("enable_bundling");
        if(str != null) {
            enable_bundling=Boolean.valueOf(str).booleanValue();
            props.remove("enable_bundling");
        }

        str=props.getProperty("enable_unicast_bundling");
        if(str != null) {
            enable_unicast_bundling=Boolean.valueOf(str).booleanValue();
            props.remove("enable_unicast_bundling");
        }

        str=props.getProperty("enable_diagnostics");
        if(str != null) {
            enable_diagnostics=Boolean.valueOf(str).booleanValue();
            props.remove("enable_diagnostics");
        }

        str=props.getProperty("diagnostics_addr");
        if(str != null) {
            diagnostics_addr=str;
            props.remove("diagnostics_addr");
        }

        str=props.getProperty("diagnostics_port");
        if(str != null) {
            diagnostics_port=Integer.parseInt(str);
            props.remove("diagnostics_port");
        }

        str=props.getProperty(Global.SINGLETON_NAME);
        if(str != null) {
            singleton_name=str;
            props.remove(Global.SINGLETON_NAME);
        }

        return true;
    }




    /**
     * handle the UP event.
     * @param evt - the event being send from the stack
     */
    public Object up(Event evt) {
        switch(evt.getType()) {
        case Event.CONFIG:
            if(singleton_name != null)
                passToAllUpProtocols(evt);
            else
                up_prot.up(evt);
            if(log.isDebugEnabled()) log.debug("received CONFIG event: " + evt.getArg());
            handleConfigEvent((Map<String,Object>)evt.getArg());
            return null;
        }
        if(singleton_name != null) {
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
            if(log.isTraceEnabled()) log.trace(new StringBuffer("looping back message ").append(copy));
            // up_prot.up(new Event(Event.MSG, copy));

            // changed to fix http://jira.jboss.com/jira/browse/JGRP-506
            Executor pool=msg.isFlagSet(Message.OOB)? oob_thread_pool : thread_pool;
            pool.execute(new Runnable() {
                public void run() {
                    up_prot.up(new Event(Event.MSG, copy));
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
            boolean oob=false;
            byte oob_flag=data[Global.SHORT_SIZE]; // we need to skip the first 2 bytes (version)
            if((oob_flag & OOB) == OOB)
                oob=true;

            if(use_concurrent_stack) {
                if(oob) {
                    num_oob_msgs_received++;
                    dispatchToThreadPool(oob_thread_pool, dest, sender, data, offset, length);
                }
                else {
                    num_incoming_msgs_received++;
                    dispatchToThreadPool(thread_pool, dest, sender, data, offset, length);
                }
            }
            else {
                if(use_incoming_packet_handler) {
                    byte[] tmp=new byte[length];
                    System.arraycopy(data, offset, tmp, 0, length);
                    incoming_packet_queue.add(new IncomingPacket(dest, sender, tmp, 0, length));
                }
                else
                    handleIncomingPacket(dest, sender, data, offset, length);
            }
        }
        catch(Throwable t) {
            if(log.isErrorEnabled())
                log.error(new StringBuffer("failed handling data from ").append(sender), t);
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


    /**
     * Processes a packet read from either the multicast or unicast socket. Needs to be synchronized because
     * mcast or unicast socket reads can be concurrent.
     * Correction (bela April 19 2005): we access no instance variables, all vars are allocated on the stack, so
     * this method should be reentrant: removed 'synchronized' keyword
     */
    private void handleIncomingPacket(Address dest, Address sender, byte[] data, int offset, int length) {
        Message                msg=null;
        short                  version=0;
        boolean                is_message_list, multicast;
        byte                   flags;
        List<Message>          msgs;

        try {
            synchronized(in_stream) {
                in_stream.setData(data, offset, length);
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
                        StringBuffer sb=new StringBuffer();
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

                if(is_message_list)
                    msgs=readMessageList(dis, dest, multicast);
                else {
                    msg=readMessage(dis, dest, sender, multicast);
                    msgs=new LinkedList<Message>();
                    msgs.add(msg);
                }
            }

            Address src;
            for(Iterator it=msgs.iterator(); it.hasNext();) {
                msg=(Message)it.next();
                src=msg.getSrc();
                if(loopback) {
                    if(multicast && src != null && local_addr.equals(src)) { // discard own loopback multicast packets
                        it.remove();
                    }
                }
                else
                    handleIncomingMessage(msg);
            }
            if(incoming_msg_queue != null && !msgs.isEmpty())
                incoming_msg_queue.addAll(msgs);
        }
        catch(Throwable t) {
            if(log.isErrorEnabled())
                log.error("failed unmarshalling message", t);
        }
    }



    private void handleIncomingMessage(Message msg) {
        Event      evt;
        TpHeader   hdr;

        if(stats) {
            num_msgs_received++;
            num_bytes_received+=msg.getLength();
        }

        evt=new Event(Event.MSG, msg);
        if(log.isTraceEnabled()) {
            StringBuffer sb=new StringBuffer("message is ").append(msg).append(", headers are ").append(msg.printHeaders());
            log.trace(sb);
        }

        hdr=(TpHeader)msg.getHeader(name); // replaced removeHeader() with getHeader()
        if(hdr != null) {

            /* Discard all messages destined for a channel with a different name */
            String ch_name=hdr.channel_name;

            // Discard if message's group name is not the same as our group name unless the
            // message is a diagnosis message (special group name DIAG_GROUP)
            if(channel_name != null && !channel_name.equals(ch_name)) {
                if(log.isWarnEnabled())
                    log.warn(new StringBuffer("discarded message from different group \"").append(ch_name).
                            append("\" (our group is \"").append(channel_name).append("\"). Sender was ").append(msg.getSrc()));
                return;
            }
        }
        else {
            if(log.isTraceEnabled())
                log.trace(new StringBuffer("message does not have a transport header, msg is ").append(msg).
                          append(", headers are ").append(msg.printHeaders()).append(", will be discarded"));
            return;
        }
        up_prot.up(evt);
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

        ExposedByteArrayOutputStream out_stream=null;
        ExposedDataOutputStream      dos=null;
        Buffer                       buf;
        out_stream=new ExposedByteArrayOutputStream(INITIAL_BUFSIZE);
        dos=new ExposedDataOutputStream(out_stream);
        writeMessage(msg, dos, multicast);
        buf=new Buffer(out_stream.getRawBuffer(), 0, out_stream.size());
        doSend(buf, dest, multicast);
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
        List<Message> list=new LinkedList<Message>();
        int           len;
        Message       msg;
        Address       src;

        len=instream.readInt();
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
                Vector<Address> tmpvec=view.getMembers();
                members.addAll(tmpvec);
            }
            break;

        case Event.CONNECT:
        case Event.CONNECT_WITH_STATE_TRANSFER:    
            channel_name=(String)evt.getArg();
            header=new TpHeader(channel_name);            
            thread_naming_pattern.setClusterName(channel_name);
            setThreadNames();                               
            return null;

        case Event.DISCONNECT:
            unsetThreadNames();
            break;

        case Event.CONFIG:
            if(log.isDebugEnabled()) log.debug("received CONFIG event: " + evt.getArg());
            handleConfigEvent((Map<String,Object>)evt.getArg());
            break;
        }
        return null;
    }




    protected void setThreadNames() {
        if(incoming_packet_handler != null)
            thread_naming_pattern.renameThread(IncomingPacketHandler.THREAD_NAME,
                                               incoming_packet_handler.getThread());
        if(incoming_msg_handler != null)
            thread_naming_pattern.renameThread(IncomingMessageHandler.THREAD_NAME,
                                               incoming_msg_handler.getThread());
        if(diag_handler != null)
            thread_naming_pattern.renameThread(DiagnosticsHandler.THREAD_NAME,
                                               diag_handler.getThread());
    }


    protected void unsetThreadNames() {
        if(incoming_packet_handler != null && incoming_packet_handler.getThread() != null)
            incoming_packet_handler.getThread().setName(IncomingPacketHandler.THREAD_NAME);
        if(incoming_msg_handler != null && incoming_msg_handler.getThread() != null)
            incoming_msg_handler.getThread().setName(IncomingMessageHandler.THREAD_NAME);
        if(diag_handler != null && diag_handler.getThread() != null)
            diag_handler.getThread().setName(DiagnosticsHandler.THREAD_NAME);
    }
    
    protected void handleConfigEvent(Map<String,Object> map) {
        if(map == null) return;
        if(map.containsKey("additional_data")) {
            additional_data=(byte[])map.get("additional_data");
            if(local_addr instanceof IpAddress)
                ((IpAddress)local_addr).setAdditionalData(additional_data);
        }
    }


    protected ExecutorService createThreadPool(int min_threads, int max_threads, long keep_alive_time, String rejection_policy,
                                               BlockingQueue<Runnable> queue, final String thread_name) {

        ThreadPoolExecutor pool=new ThreadPoolExecutor(min_threads, max_threads, keep_alive_time, TimeUnit.MILLISECONDS, queue);
        pool.setThreadFactory(ProtocolStack.newThreadFactory(thread_naming_pattern, pool_thread_group, thread_name, false));

        if(rejection_policy != null) {
            if(rejection_policy.equals("abort"))
                pool.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());
            else if(rejection_policy.equals("discard"))
                pool.setRejectedExecutionHandler(new ThreadPoolExecutor.DiscardPolicy());
            else if(rejection_policy.equals("discardoldest"))
                pool.setRejectedExecutionHandler(new ThreadPoolExecutor.DiscardOldestPolicy());
            else
                pool.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        }

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
        Address   dest=null;
        Address   sender=null;
        byte[]    buf;
        int       offset, length;

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
                        StringBuffer sb=new StringBuffer();
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
        }


        private void handleMyMessage(Message msg, boolean multicast) {
            if(stats) {
                num_msgs_received++;
                num_bytes_received+=msg.getLength();
            }

            Address src=msg.getSrc();
            if(loopback && multicast && src != null && local_addr.equals(src)) {
                return; // drop message that was already looped back and delivered
            }

            TpHeader hdr=(TpHeader)msg.getHeader(name); // replaced removeHeader() with getHeader()
            if(hdr != null) {
                String ch_name=hdr.channel_name;
                if(singleton_name != null) {
                    Protocol up_prot=up_prots.get(ch_name);
                    if(up_prot != null) {
                        Event evt=new Event(Event.MSG, msg);
                        if(log.isTraceEnabled()) {
                            StringBuffer sb=new StringBuffer("message is ").append(msg).append(", headers are ").append(msg.printHeaders());
                            log.trace(sb);
                        }
                        up_prot.up(evt);
                        return;
                    }
                    else {
                        if(log.isWarnEnabled())
                            log.warn(new StringBuffer("discarded message from different group \"").append(ch_name).
                                    append("\" (our groups are ").append(up_prots.keySet()).append("). Sender was ").append(msg.getSrc()));
                        return;
                    }
                }
                else {
                    // Discard if message's group name is not the same as our group name
                    if(channel_name != null && !channel_name.equals(ch_name)) {
                        if(log.isWarnEnabled())
                            log.warn(new StringBuffer("discarded message from different group \"").append(ch_name).
                                    append("\" (our group is \"").append(channel_name).append("\"). Sender was ").append(msg.getSrc()));
                        return;
                    }
                }
            }
            else {
                if(channel_name == null) {
                    ;
                }
                else {
                    if(log.isTraceEnabled())
                        log.trace(new StringBuffer("message does not have a transport header, msg is ").append(msg).
                                append(", headers are ").append(msg.printHeaders()).append(", will be discarded"));
                    return;
                }
            }

            Event evt=new Event(Event.MSG, msg);
            if(log.isTraceEnabled()) {
                StringBuffer sb=new StringBuffer("message is ").append(msg).append(", headers are ").append(msg.printHeaders());
                log.trace(sb);
            }

            up_prot.up(evt);
        }


    }





    /**
     * This thread fetches byte buffers from the packet_queue, converts them into messages and passes them up
     * to the higher layer (done in handleIncomingUdpPacket()).
     */
    class IncomingPacketHandler implements Runnable {
    	
    	public static final String THREAD_NAME="IncomingPacketHandler"; 
        Thread t=null;

        Thread getThread(){
        	return t;
        }

        void start() {
            if(t == null || !t.isAlive()) {
                t=getProtocolStack().getThreadFactory().newThread(this, THREAD_NAME);                
                t.setDaemon(true);
                t.start();
            }
        }

        void stop() {
            incoming_packet_queue.close(true); // should terminate the packet_handler thread too
            if(t != null) {
                try {
                    t.join(Global.THREAD_SHUTDOWN_WAIT_TIME);
                }
                catch(InterruptedException e) {
                    Thread.currentThread().interrupt(); // set interrupt flag again
                }                
            }
        }

        public void run() {
            IncomingPacket entry;
            while(!incoming_packet_queue.closed() && Thread.currentThread().equals(t)) {
                try {
                    entry=(IncomingPacket)incoming_packet_queue.remove();
                    handleIncomingPacket(entry.dest, entry.sender, entry.buf, entry.offset, entry.length);
                }
                catch(QueueClosedException closed_ex) {
                    break;
                }
                catch(Throwable ex) {
                    if(log.isErrorEnabled())
                        log.error("error processing incoming packet", ex);
                }
            }
            if(log.isTraceEnabled()) log.trace("incoming packet handler terminating");
        }
    }


    class IncomingMessageHandler implements Runnable {
    	
    	public static final String THREAD_NAME = "IncomingMessageHandler"; 
        Thread t;

        Thread getThread(){
        	return t;
        }

        public void start() {
            if(t == null || !t.isAlive()) {
                t=getProtocolStack().getThreadFactory().newThread(this, THREAD_NAME);                
                t.setDaemon(true);
                t.start();
            }
        }


        public void stop() {
            incoming_msg_queue.close(true);            
            if(t != null) {
                try {
                    t.join(Global.THREAD_SHUTDOWN_WAIT_TIME);
                }
                catch(InterruptedException e) {
                    Thread.currentThread().interrupt(); // set interrupt flag again
                }                
            }
        }

        public void run() {
            Message msg;
            while(!incoming_msg_queue.closed() && Thread.currentThread().equals(t)) {
                try {
                    msg=(Message)incoming_msg_queue.remove();
                    handleIncomingMessage(msg);
                }
                catch(QueueClosedException closed_ex) {
                    break;
                }
                catch(Throwable ex) {
                    if(log.isErrorEnabled())
                        log.error("error processing incoming message", ex);
                }
            }
            if(log.isTraceEnabled()) log.trace("incoming message handler terminating");
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


        private void send(Message msg, Address dest) throws Exception {
            long length=msg.size();
            checkLength(length);
            Map<Address,List<Message>> bundled_msgs=null;

            lock.lock();
            try {
                if(count + length >= max_bundle_size) {                    
                    bundled_msgs=removeBundledMessages();
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

            if(bundled_msgs != null) {            	
                sendBundledMessages(bundled_msgs);
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


        /** Must always be called with lock held */
        private Map<Address,List<Message>> removeBundledMessages() {
            if(msgs.isEmpty())
                return null;
            Map<Address,List<Message>> copy=new HashMap<Address,List<Message>>(msgs);
            if(log.isTraceEnabled()) {
                long stop=System.currentTimeMillis();
                double percentage=100.0 / max_bundle_size * count;
                StringBuilder sb=new StringBuilder("sending ").append(num_msgs).append(" msgs (");
                num_msgs=0;
                sb.append(count).append(" bytes (" + f.format(percentage) + "% of max_bundle_size)");
                if(last_bundle_time > 0) {
                    sb.append(", collected in ").append(stop-last_bundle_time).append("ms) ");
                }
                sb.append(" to ").append(copy.size()).append(" destination(s)");
                if(copy.size() > 1) sb.append(" (dests=").append(copy.keySet()).append(")");
                log.trace(sb);
            }
            msgs.clear();
            count=0;
            return copy;
        }


        /**
         * Sends all messages from the map, all messages for the same destination are bundled into 1 message.
         * This method may be called by timer and bundler concurrently
         * @param msgs
         */
        private void sendBundledMessages(Map<Address,List<Message>> msgs) {
            boolean   multicast;
            Buffer    buffer;
            Map.Entry<Address,List<Message>> entry;
            Address   dst;
            ExposedByteArrayOutputStream out_stream=new ExposedByteArrayOutputStream(INITIAL_BUFSIZE);
            ExposedDataOutputStream      dos=new ExposedDataOutputStream(out_stream);
            boolean first=true;

            for(Iterator<Map.Entry<Address,List<Message>>> it=msgs.entrySet().iterator(); it.hasNext();) {
                entry=it.next();
                List<Message> list=entry.getValue();
                if(list.isEmpty())
                    continue;
                dst=entry.getKey();
                multicast=dst == null || dst.isMulticastAddress();
                try {
                    if(first) {
                        first=false;
                    }
                    else {
                        out_stream.reset();
                        dos.reset();
                    }
                    writeMessageList(list, dos, multicast); // flushes output stream when done
                    buffer=new Buffer(out_stream.getRawBuffer(), 0, out_stream.size());
                    doSend(buffer, dst, multicast);
                }
                catch(Throwable e) {
                    if(log.isErrorEnabled()) log.error("exception sending msg: " + e.toString(), e.getCause());
                }
            }
        }



        private void checkLength(long len) throws Exception {
            if(len > max_bundle_size)
                throw new Exception("message size (" + len + ") is greater than max bundling size (" + max_bundle_size +
                        "). Set the fragmentation/bundle size in FRAG and TP correctly");
        }


        private class BundlingTimer implements Runnable {

            public void run() {
                Map<Address, List<Message>> msgs=null;
                boolean unlocked=false;

                lock.lock();
                try {
                    msgs=removeBundledMessages();
                    if(msgs != null) {
                        lock.unlock();
                        unlocked=true;
                        sendBundledMessages(msgs);
                    }
                }
                finally {
                    if(unlocked)
                        lock.lock();
                    num_bundling_tasks--;
                    lock.unlock();
                }
            }
        }
    }


    private class DiagnosticsHandler implements Runnable {
    	public static final String THREAD_NAME = "DiagnosticsHandler"; 
        Thread thread=null;
        MulticastSocket diag_sock=null;

        DiagnosticsHandler() {
        }

        Thread getThread(){
        	return thread;
        }

        void start() throws IOException {
            diag_sock=new MulticastSocket(diagnostics_port);
            java.util.List interfaces=Util.getAllAvailableInterfaces();
            bindToInterfaces(interfaces, diag_sock);

            if(thread == null || !thread.isAlive()) {
                thread=getProtocolStack().getThreadFactory().newThread(this, THREAD_NAME);              
                thread.setDaemon(true);
                thread.start();
            }
        }

        void stop() {
            if(diag_sock != null)
                diag_sock.close();
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
            byte[] buf=new byte[1500]; // MTU on most LANs
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

        private void bindToInterfaces(java.util.List interfaces, MulticastSocket s) {
            SocketAddress group_addr=new InetSocketAddress(diagnostics_addr, diagnostics_port);
            for(Iterator it=interfaces.iterator(); it.hasNext();) {
                NetworkInterface i=(NetworkInterface)it.next();
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
}
