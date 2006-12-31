package org.jgroups.protocols;


import org.jgroups.*;
import org.jgroups.Channel;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.Protocol;
import org.jgroups.util.*;
import org.jgroups.util.List;
import org.jgroups.util.Queue;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.*;
import java.text.NumberFormat;
import java.util.*;
import java.util.concurrent.*;



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
 * @version $Id: TP.java,v 1.100 2006/12/31 06:26:58 belaban Exp $
 */
public abstract class TP extends Protocol {


    /** The address (host and port) of this member */
    protected Address         local_addr=null;

    /** The name of the group to which this member is connected */
    protected String          channel_name=null;

    /** The interface (NIC) which should be used by this transport */
    protected InetAddress     bind_addr=null;

    /** Overrides bind_addr, -Djgroups.bind_addr and -Dbind.address: let's the OS return the local host address */
    boolean         use_local_host=false;

    /** If true, the transport should use all available interfaces to receive multicast messages
     * @deprecated  Use {@link receive_on_all_interfaces} instead */
    boolean         bind_to_all_interfaces=false;

    /** If true, the transport should use all available interfaces to receive multicast messages */
    boolean         receive_on_all_interfaces=false;

    /** List<NetworkInterface> of interfaces to receive multicasts on. The multicast receive socket will listen
     * on all of these interfaces. This is a comma-separated list of IP addresses or interface names. E.g.
     * "192.168.5.1,eth1,127.0.0.1". Duplicates are discarded; we only bind to an interface once.
     * If this property is set, it override receive_on_all_interfaces.
     */
    java.util.List  receive_interfaces=null;

    /** If true, the transport should use all available interfaces to send multicast messages. This means
     * the same multicast message is sent N times, so use with care */
    boolean         send_on_all_interfaces=false;

    /** List<NetworkInterface> of interfaces to send multicasts on. The multicast send socket will send the
     * same multicast message on all of these interfaces. This is a comma-separated list of IP addresses or
     * interface names. E.g. "192.168.5.1,eth1,127.0.0.1". Duplicates are discarded.
     * If this property is set, it override send_on_all_interfaces.
     */
    java.util.List  send_interfaces=null;


    /** The port to which the transport binds. 0 means to bind to any (ephemeral) port */
    int             bind_port=0;
    int				port_range=1; // 27-6-2003 bgooren, Only try one port by default

    /** The members of this group (updated when a member joins or leaves) */
    final protected Vector    members=new Vector(11);

    protected View            view=null;

    /** Pre-allocated byte stream. Used for marshalling messages. Will grow as needed */
    final ExposedByteArrayOutputStream out_stream=new ExposedByteArrayOutputStream(1024);
    final ExposedBufferedOutputStream  buf_out_stream=new ExposedBufferedOutputStream(out_stream, 1024);
    final ExposedDataOutputStream      dos=new ExposedDataOutputStream(buf_out_stream);

    final ExposedByteArrayInputStream  in_stream=new ExposedByteArrayInputStream(new byte[]{'0'});
    final ExposedBufferedInputStream   buf_in_stream=new ExposedBufferedInputStream(in_stream);
    final DataInputStream              dis=new DataInputStream(buf_in_stream);


    /** If true, messages sent to self are treated specially: unicast messages are
     * looped back immediately, multicast messages get a local copy first and -
     * when the real copy arrives - it will be discarded. Useful for Window
     * media (non)sense */
    boolean         loopback=false;


    /** Discard packets with a different version. Usually minor version differences are okay. Setting this property
     * to true means that we expect the exact same version on all incoming packets */
    boolean         discard_incompatible_packets=false;

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
    BlockingQueue oob_thread_pool_queue=null;
    /** Whether of not to use a queue with ThreadPoolExecutor (ignored with direct executor) */
    boolean oob_thread_pool_queue_enabled=true;
    /** max number of elements in queue (bounded) */
    int oob_thread_pool_queue_max_size=500;
    /** Possible values are "Wait", "Abort", "Discard", "DiscardOldest". These values might change once we switch to
     * JDK 5's java.util.concurrent package */
    String oob_thread_pool_rejection_policy="Run";


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
    BlockingQueue thread_pool_queue=null;
    /** Whether of not to use a queue with ThreadPoolExecutor (ignored with directE decutor) */
    boolean thread_pool_queue_enabled=true;
    /** max number of elements in queue (bounded) */
    int thread_pool_queue_max_size=500;
    /** Possible values are "Wait", "Abort", "Discard", "DiscardOldest". These values might change once we switch to
     * JDK 5's java.util.concurrent package */
    String thread_pool_rejection_policy="Run";


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

    /** Enabled bundling of smaller messages into bigger ones */
    boolean enable_bundling=false;

    private Bundler    bundler=null;

    protected TimeScheduler      timer=null;

    private DiagnosticsHandler diag_handler=null;
    boolean enable_diagnostics=true;
    String diagnostics_addr="224.0.0.75";
    int    diagnostics_port=7500;


    /** HashMap<Address, Address>. Keys=senders, values=destinations. For each incoming message M with sender S, adds
     * an entry with key=S and value= sender's IP address and port.
     */
    HashMap addr_translation_table=new HashMap();

    boolean use_addr_translation=false;

    TpHeader header;

    final String name=getName();

    static final byte LIST      = 1;  // we have a list of messages rather than a single message when set
    static final byte MULTICAST = 2;  // message is a multicast (versus a unicast) message when set
    static final byte OOB       = 4;  // message has OOB flag set (Message.OOB)

    long num_msgs_sent=0, num_msgs_received=0, num_bytes_sent=0, num_bytes_received=0;

    static  NumberFormat f;
    private static final long POOL_SHUTDOWN_WAIT_TIME=3000;

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
    public int getMaxBundleSize() {return max_bundle_size;}
    public void setMaxBundleSize(int size) {max_bundle_size=size;}
    public long getMaxBundleTimeout() {return max_bundle_timeout;}
    public void setMaxBundleTimeout(long timeout) {max_bundle_timeout=timeout;}
    public Address getLocalAddress() {return local_addr;}
    public String getChannelName() {return channel_name;}
    public boolean isLoopback() {return loopback;}
    public void setLoopback(boolean b) {loopback=b;}
    public boolean isUseIncomingPacketHandler() {return use_incoming_packet_handler;}





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






    public Map dumpStats() {
        Map retval=super.dumpStats();
        if(retval == null)
            retval=new HashMap();
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
                ArrayList l=new ArrayList(tok.countTokens());
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
            Map m=new HashMap(1);
            m.put("bind_addr", bind_addr);
            passUp(new Event(Event.CONFIG, m));
        }
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

        if(use_incoming_packet_handler) {
            incoming_packet_queue=new Queue();
            incoming_packet_handler=new IncomingPacketHandler();
            incoming_packet_handler.start();
        }


        // ========================================== OOB thread pool ==============================
        if(oob_thread_pool_enabled) { // create a ThreadPoolExecutor for the unmarshaller thread pool
            if(oob_thread_pool_queue_enabled)
                oob_thread_pool_queue=new LinkedBlockingQueue(oob_thread_pool_queue_max_size);
            else
                oob_thread_pool_queue=new SynchronousQueue();
            oob_thread_pool=createThreadPool(oob_thread_pool_min_threads, oob_thread_pool_max_threads, oob_thread_pool_keep_alive_time,
                                             oob_thread_pool_rejection_policy, oob_thread_pool_queue, "OOB", "OOB Thread-");
        }
        else { // otherwise use the caller's thread to unmarshal the byte buffer into a message
            oob_thread_pool=new DirectExecutor();
        }


        // ====================================== Regular thread pool ===========================
        if(thread_pool_enabled) { // create a ThreadPoolExecutor for the unmarshaller thread pool
            if(thread_pool_queue_enabled)
                thread_pool_queue=new LinkedBlockingQueue(thread_pool_queue_max_size);
            else
                thread_pool_queue=new SynchronousQueue();
            thread_pool=createThreadPool(thread_pool_min_threads, thread_pool_max_threads, thread_pool_keep_alive_time,
                                         thread_pool_rejection_policy, thread_pool_queue, "Incoming", "Incoming Thread-");
        }
        else { // otherwise use the caller's thread to unmarshal the byte buffer into a message
            thread_pool=new DirectExecutor();
        }


        if(loopback) {
            incoming_msg_queue=new Queue();
            incoming_msg_handler=new IncomingMessageHandler();
            incoming_msg_handler.start();
        }

        if(enable_bundling) {
            bundler=new Bundler();
        }

        passUp(new Event(Event.SET_LOCAL_ADDRESS, local_addr));
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
            shutdownThreadPool((ThreadPoolExecutor)oob_thread_pool);
        }

        if(thread_pool instanceof ThreadPoolExecutor) {
            shutdownThreadPool((ThreadPoolExecutor)thread_pool);
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

        boolean ignore_systemprops=Util.isBindAddressPropertyIgnored();
        String str=Util.getProperty(new String[]{Global.BIND_ADDR, Global.BIND_ADDR_OLD}, props, "bind_addr",
                                    ignore_systemprops, null);

        if(str != null) {
            try {
                bind_addr=InetAddress.getByName(str);
            }
            catch(UnknownHostException unknown) {
                if(log.isFatalEnabled()) log.fatal("(bind_addr): host " + str + " not known");
                return false;
            }
            props.remove("bind_addr");
        }

        str=props.getProperty("use_local_host");
        if(str != null) {
            use_local_host=new Boolean(str).booleanValue();
            props.remove("use_local_host");
        }

        str=props.getProperty("bind_to_all_interfaces");
        if(str != null) {
            receive_on_all_interfaces=new Boolean(str).booleanValue();
            props.remove("bind_to_all_interfaces");
            log.warn("bind_to_all_interfaces has been deprecated; use receive_on_all_interfaces instead");
        }

        str=props.getProperty("receive_on_all_interfaces");
        if(str != null) {
            receive_on_all_interfaces=new Boolean(str).booleanValue();
            props.remove("receive_on_all_interfaces");
        }

        str=props.getProperty("receive_interfaces");
        if(str != null) {
            try {
                receive_interfaces=parseInterfaceList(str);
                props.remove("receive_interfaces");
            }
            catch(Exception e) {
                log.error("error determining interfaces (" + str + ")", e);
                return false;
            }
        }

        str=props.getProperty("send_on_all_interfaces");
        if(str != null) {
            send_on_all_interfaces=new Boolean(str).booleanValue();
            props.remove("send_on_all_interfaces");
        }

        str=props.getProperty("send_interfaces");
        if(str != null) {
            try {
                send_interfaces=parseInterfaceList(str);
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
            if(warn) log.warn("'use_packet_handler' is deprecated; use 'use_incoming_packet_handler' instead");
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
            if(!(str.equals("run") || str.equals("wait") || str.equals("abort")|| str.equals("discard")|| str.equals("discardoldest"))) {
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
            if(!(str.equals("run") || str.equals("wait") || str.equals("abort")|| str.equals("discard")|| str.equals("discardoldest"))) {
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

        str=props.getProperty("use_addr_translation");
        if(str != null) {
            use_addr_translation=Boolean.valueOf(str).booleanValue();
            props.remove("use_addr_translation");
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

        return true;
    }




    /**
     * handle the UP event.
     * @param evt - the event being send from the stack
     */
    public void up(Event evt) {
        switch(evt.getType()) {
        case Event.CONFIG:
            passUp(evt);
            if(log.isDebugEnabled()) log.debug("received CONFIG event: " + evt.getArg());
            handleConfigEvent((HashMap)evt.getArg());
            return;
        }
        passUp(evt);
    }

    /**
     * Caller by the layer above this layer. Usually we just put this Message
     * into the send queue and let one or more worker threads handle it. A worker thread
     * then removes the Message from the send queue, performs a conversion and adds the
     * modified Message to the send queue of the layer below it, by calling down()).
     */
    public void down(Event evt) {
        if(evt.getType() != Event.MSG) {  // unless it is a message handle it and respond
            handleDownEvent(evt);
            return;
        }

        Message msg=(Message)evt.getArg();
        if(header != null) {
            // added patch by Roland Kurmann (March 20 2003)
            // msg.putHeader(name, new TpHeader(channel_name));
            msg.putHeader(name, header);
        }

        setSourceAddress(msg); // very important !! listToBuffer() will fail with a null src address !!
        if(trace) {
            StringBuilder sb=new StringBuilder("sending msg to ").append(msg.getDest()).
                    append(" (src=").append(msg.getSrc()).append("), headers are ").append(msg.printHeaders());
            log.trace(sb.toString());
        }

        // Don't send if destination is local address. Instead, switch dst and src and put in up_queue.
        // If multicast message, loopback a copy directly to us (but still multicast). Once we receive this,
        // we will discard our own multicast message
        Address dest=msg.getDest();
        boolean multicast=dest == null || dest.isMulticastAddress();
        if(loopback && (multicast || dest.equals(local_addr))) {

            // we *have* to make a copy, or else passUp() might remove headers from msg which will then *not*
            // be available for marshalling further down (when sending the message)
            Message copy=msg.copy();
            if(trace) log.trace(new StringBuffer("looping back message ").append(copy));
            passUp(new Event(Event.MSG, copy));
            if(!multicast)
                return;
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
                log.error("failed sending message to " + dst + " (" + msg.getLength() + " bytes)", e.getCause());
            }
        }
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
        boolean mcast=dest == null || dest.isMulticastAddress();
        if(trace){
            StringBuilder sb=new StringBuilder("received (");
            sb.append(mcast? "mcast) " : "ucast) ").append(length).append(" bytes from ").append(sender);
            log.trace(sb.toString());
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
                    incoming_packet_queue.add(new IncomingPacket(dest, sender, tmp, offset, length));
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
            pool.execute(new IncomingPacket(dest, sender, tmp, offset, length));
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
        List                   l=null;  // used if bundling is enabled
        short                  version;
        boolean                is_message_list, multicast;
        byte                   flags;

        try {
            synchronized(in_stream) {
                in_stream.setData(data, offset, length);
                buf_in_stream.reset(length);
                version=dis.readShort();
                if(Version.compareTo(version) == false) {
                    if(warn) {
                        StringBuffer sb=new StringBuffer();
                        sb.append("packet from ").append(sender).append(" has different version (").append(version);
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
                    l=bufferToList(dis, dest, multicast);
                else
                    msg=bufferToMessage(dis, dest, sender, multicast);
            }

            LinkedList msgs=new LinkedList();
            if(is_message_list) {
                for(Enumeration en=l.elements(); en.hasMoreElements();)
                    msgs.add(en.nextElement());
            }
            else
                msgs.add(msg);

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
            if(incoming_msg_queue != null && msgs.size() > 0)
                incoming_msg_queue.addAll(msgs);
        }
        catch(QueueClosedException closed_ex) {
            ; // swallow exception
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
        if(trace) {
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
                if(warn)
                    log.warn(new StringBuffer("discarded message from different group \"").append(ch_name).
                            append("\" (our group is \"").append(channel_name).append("\"). Sender was ").append(msg.getSrc()));
                return;
            }
        }
        else {
            if(trace)
                log.trace(new StringBuffer("message does not have a transport header, msg is ").append(msg).
                          append(", headers are ").append(msg.printHeaders()).append(", will be discarded"));
            return;
        }
        passUp(evt);
    }


    /** Internal method to serialize and send a message. This method is not reentrant */
    private void send(Message msg, Address dest, boolean multicast) throws Exception {

        // bundle only regular messages; send OOB messages directly
        if(enable_bundling && !msg.isFlagSet(Message.OOB)) {
            bundler.send(msg, dest);
            return;
        }

        // Needs to be synchronized because we can have possible concurrent access, e.g.
        // Discovery uses a separate thread to send out discovery messages
        Buffer   buf;
        synchronized(out_stream) {
            buf=messageToBuffer(msg, multicast);
            doSend(buf, dest, multicast);
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
    private Buffer messageToBuffer(Message msg, boolean multicast) throws Exception {
        Buffer retval;
        byte flags=0;

        out_stream.reset();
        buf_out_stream.reset(out_stream.getCapacity());
        dos.reset();
        dos.writeShort(Version.version); // write the version
        if(multicast)
            flags+=MULTICAST;
        if(msg.isFlagSet(Message.OOB))
            flags+=OOB;
        dos.writeByte(flags);
        // preMarshalling(msg, dest, src);  // allows for optimization by subclass
        msg.writeTo(dos);
        // postMarshalling(msg, dest, src); // allows for optimization by subclass
        dos.flush();
        retval=new Buffer(out_stream.getRawBuffer(), 0, out_stream.size());
        return retval;
    }

    private Message bufferToMessage(DataInputStream instream, Address dest, Address sender, boolean multicast) throws Exception {
        Message msg=new Message(false); // don't create headers, readFrom() will do this
        msg.readFrom(instream);
        postUnmarshalling(msg, dest, sender, multicast); // allows for optimization by subclass
        return msg;
    }



    private Buffer listToBuffer(List l, boolean multicast) throws Exception {
        Buffer retval;
        Address src;
        Message msg;
        byte flags=0;
        int len=l != null? l.size() : 0;
        boolean src_written=false;
        out_stream.reset();
        buf_out_stream.reset(out_stream.getCapacity());
        dos.reset();
        dos.writeShort(Version.version);
        flags+=LIST;
        if(multicast)
            flags+=MULTICAST;
        dos.writeByte(flags);
        dos.writeInt(len);
        for(Enumeration en=l.elements(); en.hasMoreElements();) {
            msg=(Message)en.nextElement();
            src=msg.getSrc();
            if(!src_written) {
                Util.writeAddress(src, dos);
                src_written=true;
            }
            // msg.setSrc(null);
            msg.writeTo(dos);
            // msg.setSrc(src);
        }
        dos.flush();
        retval=new Buffer(out_stream.getRawBuffer(), 0, out_stream.size());
        return retval;
    }

    private List bufferToList(DataInputStream instream, Address dest, boolean multicast) throws Exception {
        List                    l=new List();
        DataInputStream         in=null;
        int                     len;
        Message                 msg;
        Address                 src;

        try {
            len=instream.readInt();
            src=Util.readAddress(instream);
            for(int i=0; i < len; i++) {
                msg=new Message(false); // don't create headers, readFrom() will do this
                msg.readFrom(instream);
                postUnmarshallingList(msg, dest, multicast);
                msg.setSrc(src);
                l.add(msg);
            }
            return l;
        }
        finally {
            Util.close(in);
        }
    }




    /**
     *
     * @param s
     * @return List<NetworkInterface>
     */
    private java.util.List parseInterfaceList(String s) throws Exception {
        java.util.List interfaces=new ArrayList(10);
        if(s == null)
            return null;

        StringTokenizer tok=new StringTokenizer(s, ",");
        String interface_name;
        NetworkInterface intf;

        while(tok.hasMoreTokens()) {
            interface_name=tok.nextToken();

            // try by name first (e.g. (eth0")
            intf=NetworkInterface.getByName(interface_name);

            // next try by IP address or symbolic name
            if(intf == null)
                intf=NetworkInterface.getByInetAddress(InetAddress.getByName(interface_name));

            if(intf == null)
                throw new Exception("interface " + interface_name + " not found");
            if(interfaces.contains(intf)) {
                log.warn("did not add interface " + interface_name + " (already present in " + print(interfaces) + ")");
            }
            else {
                interfaces.add(intf);
            }
        }
        return interfaces;
    }

    private static String print(java.util.List interfaces) {
        StringBuilder sb=new StringBuilder();
        boolean first=true;
        NetworkInterface intf;
        for(Iterator it=interfaces.iterator(); it.hasNext();) {
            intf=(NetworkInterface)it.next();
            if(first) {
                first=false;
            }
            else {
                sb.append(", ");
            }
            sb.append(intf.getName());
        }
        return sb.toString();
    }


    protected void handleDownEvent(Event evt) {
        switch(evt.getType()) {

        case Event.TMP_VIEW:
        case Event.VIEW_CHANGE:
            synchronized(members) {
                view=(View)evt.getArg();
                members.clear();
                Vector tmpvec=view.getMembers();
                members.addAll(tmpvec);
            }
            break;

        case Event.GET_LOCAL_ADDRESS:   // return local address -> Event(SET_LOCAL_ADDRESS, local)
            passUp(new Event(Event.SET_LOCAL_ADDRESS, local_addr));
            break;

        case Event.CONNECT:
            channel_name=(String)evt.getArg();
            header=new TpHeader(channel_name);
            setThreadNames();

            // removed March 18 2003 (bela), not needed (handled by GMS)
            // changed July 2 2003 (bela): we discard CONNECT_OK at the GMS level anyway, this might
            // be needed if we run without GMS though
            passUp(new Event(Event.CONNECT_OK));
            break;

        case Event.DISCONNECT:
            unsetThreadNames();
            passUp(new Event(Event.DISCONNECT_OK));
            break;

        case Event.CONFIG:
            if(log.isDebugEnabled()) log.debug("received CONFIG event: " + evt.getArg());
            handleConfigEvent((HashMap)evt.getArg());
            break;
        }
    }




    protected void setThreadNames() {
        if(channel_name != null) {
            String tmp, prefix=Global.THREAD_PREFIX;
            if(incoming_packet_handler != null) {
                tmp=incoming_packet_handler.getName();
                if(tmp != null && tmp.indexOf(prefix) == -1) {
                    tmp+=prefix + channel_name + ")";
                    incoming_packet_handler.setName(tmp);
                }
            }
            if(incoming_msg_handler != null) {
                tmp=incoming_msg_handler.getName();
                if(tmp != null && tmp.indexOf(prefix) == -1) {
                    tmp+=prefix + channel_name + ")";
                    incoming_msg_handler.setName(tmp);
                }
            }
            if(diag_handler != null) {
                tmp=diag_handler.getName();
                if(tmp != null && tmp.indexOf(prefix) == -1) {
                    tmp+=prefix + channel_name + ")";
                    diag_handler.setName(tmp);
                }
            }
        }
    }


    protected void unsetThreadNames() {
        if(channel_name != null) {
            String tmp, prefix=Global.THREAD_PREFIX;
            int index;

            tmp=incoming_packet_handler != null? incoming_packet_handler.getName() : null;
            if(tmp != null) {
                index=tmp.indexOf(prefix);
                if(index > -1) {
                    tmp=tmp.substring(0, index);
                    incoming_packet_handler.setName(tmp);
                }
            }

            tmp=incoming_msg_handler != null? incoming_msg_handler.getName() : null;
            if(tmp != null) {
                index=tmp.indexOf(prefix);
                if(index > -1) {
                    tmp=tmp.substring(0, index);
                    incoming_msg_handler.setName(tmp);
                }
            }

            tmp=diag_handler != null? diag_handler.getName() : null;
            if(tmp != null) {
                index=tmp.indexOf(prefix);
                if(index > -1) {
                    tmp=tmp.substring(0, index);
                    diag_handler.setName(tmp);
                }
            }
        }
    }



    protected void handleConfigEvent(HashMap map) {
        if(map == null) return;
        if(map.containsKey("additional_data")) {
            additional_data=(byte[])map.get("additional_data");
            if(local_addr instanceof IpAddress)
                ((IpAddress)local_addr).setAdditionalData(additional_data);
        }
    }


    protected Executor createThreadPool(int min_threads, int max_threads, long keep_alive_time, String rejection_policy,
                                        BlockingQueue queue,
                                        final String thread_group_name, final String thread_name_prefix) {
        ThreadPoolExecutor pool=null;
        pool=new ThreadPoolExecutor(min_threads, max_threads, keep_alive_time, TimeUnit.MILLISECONDS, queue);

        pool.setThreadFactory(new ThreadFactory() {
            int num=1;
            ThreadGroup unmarshaller_threads=new ThreadGroup(pool_thread_group, thread_group_name);
            public Thread newThread(Runnable command) {
                return new Thread(unmarshaller_threads, command, thread_name_prefix + num++);
            }
        });

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


    private void shutdownThreadPool(ThreadPoolExecutor thread_pool) {
        thread_pool.shutdownNow();
        try {
            thread_pool.awaitTermination(POOL_SHUTDOWN_WAIT_TIME, TimeUnit.MILLISECONDS);
        }
        catch(InterruptedException e) {
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
            short                        version;
            boolean                      is_message_list, multicast;
            byte                         flags;
            ExposedByteArrayInputStream  in_stream=null;
            ExposedBufferedInputStream   buf_in_stream=null;
            DataInputStream              dis=null;

            try {
                in_stream=new ExposedByteArrayInputStream(buf, offset, length);
                buf_in_stream=new ExposedBufferedInputStream(in_stream);
                dis=new DataInputStream(buf_in_stream);
                version=dis.readShort();
                if(Version.compareTo(version) == false) {
                    if(warn) {
                        StringBuffer sb=new StringBuffer();
                        sb.append("packet from ").append(sender).append(" has different version (").append(version);
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

                Message msg;
                if(is_message_list) { // used if message bundling is enabled
                    List l=bufferToList(dis, dest, multicast);
                    for(Enumeration en=l.elements(); en.hasMoreElements();) {
                        msg=(Message)en.nextElement();
                        if(msg.isFlagSet(Message.OOB)) {
                            log.warn("bundled message should not be marked as OOB");
                            System.out.println("");
                        }
                        handleMyMessage(msg, multicast);
                    }
                }
                else {
                    msg=bufferToMessage(dis, dest, sender, multicast);
                    handleMyMessage(msg, multicast);
                }
            }
            catch(Throwable t) {
                if(log.isErrorEnabled())
                    log.error("failed handling incoming message", t);
            }
            finally {
                Util.close(dis);
                Util.close(buf_in_stream);
                Util.close(in_stream);
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

                // Discard if message's group name is not the same as our group name
                if(channel_name != null && !channel_name.equals(ch_name)) {
                    if(warn)
                        log.warn(new StringBuffer("discarded message from different group \"").append(ch_name).
                                append("\" (our group is \"").append(channel_name).append("\"). Sender was ").append(msg.getSrc()));
                    return;
                }
            }
            else {
                if(trace)
                    log.trace(new StringBuffer("message does not have a transport header, msg is ").append(msg).
                            append(", headers are ").append(msg.printHeaders()).append(", will be discarded"));
                return;
            }

            Event evt=new Event(Event.MSG, msg);
            if(trace) {
                StringBuffer sb=new StringBuffer("message is ").append(msg).append(", headers are ").append(msg.printHeaders());
                log.trace(sb);
            }

            passUp(evt);
        }


    }





    /**
     * This thread fetches byte buffers from the packet_queue, converts them into messages and passes them up
     * to the higher layer (done in handleIncomingUdpPacket()).
     */
    class IncomingPacketHandler implements Runnable {
        Thread t=null;

        String getName() {
            return t != null? t.getName() : null;
        }

        void setName(String thread_name) {
            if(t != null)
                t.setName(thread_name);
        }

        void start() {
            if(t == null || !t.isAlive()) {
                t=new Thread(Util.getGlobalThreadGroup(), this, "IncomingPacketHandler");
                t.setDaemon(true);
                t.start();
            }
        }

        void stop() {
            Thread tmp=t;
            t=null;
            incoming_packet_queue.close(true); // should terminate the packet_handler thread too
            if(tmp != null) {
                try {
                    tmp.join(10000);
                }
                catch(InterruptedException e) {
                }
                if(tmp.isAlive()) {
                    if(log.isWarnEnabled())
                        log.warn("IncomingPacketHandler thread was interrupted, but is still alive");
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
            if(trace) log.trace("incoming packet handler terminating");
        }
    }


    class IncomingMessageHandler implements Runnable {
        Thread t;
        int i=0;


        String getName() {
            return t != null? t.getName() : null;
        }

        void setName(String thread_name) {
            if(t != null)
                t.setName(thread_name);
        }

        public void start() {
            if(t == null || !t.isAlive()) {
                t=new Thread(Util.getGlobalThreadGroup(), this, "IncomingMessageHandler");
                t.setDaemon(true);
                t.start();
            }
        }


        public void stop() {
            incoming_msg_queue.close(true);
            t=null;
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
            if(trace) log.trace("incoming message handler terminating");
        }
    }




    private class Bundler {
        /** HashMap<Address, List<Message>>. Keys are destinations, values are lists of Messages */
        final HashMap       msgs=new HashMap(36);
        long                count=0;    // current number of bytes accumulated
        int                 num_msgs=0;
        long                start=0;
        BundlingTimer       bundling_timer=null;


        private void send(Message msg, Address dest) throws Exception {
            long length=msg.size();
            checkLength(length);

            synchronized(this) {
                if(start == 0)
                    start=System.currentTimeMillis();
                if(count + length >= max_bundle_size) {
                    cancelTimer();
                    bundleAndSend();  // clears msgs and resets num_msgs
                }

                addMessage(msg, dest);
                count+=length;
                startTimer(); // start timer if not running
            }
        }

        /** Never called concurrently with cancelTimer - no need for synchronization */
        private void startTimer() {
            if(bundling_timer == null || bundling_timer.cancelled()) {
                bundling_timer=new BundlingTimer();
                timer.add(bundling_timer);
            }
        }

        /** Never called concurrently with startTimer() - no need for synchronization */
        private void cancelTimer() {
            if(bundling_timer != null) {
                bundling_timer.cancel();
                bundling_timer=null;
            }
        }

        private void addMessage(Message msg, Address dest) { // no sync needed, never called by multiple threads concurrently
            List    tmp;
            synchronized(msgs) {
                tmp=(List)msgs.get(dest);
                if(tmp == null) {
                    tmp=new List();
                    msgs.put(dest, tmp);
                }
                tmp.add(msg);
                num_msgs++;
            }
        }



        private void bundleAndSend() {
            Map.Entry      entry;
            Address        dst;
            Buffer         buffer;
            List           l;
            Map copy;

            synchronized(msgs) {
                if(msgs.size() == 0)
                    return;
                copy=new HashMap(msgs);
                if(trace) {
                    long stop=System.currentTimeMillis();
                    double percentage=100.0 / max_bundle_size * count;
                    StringBuilder sb=new StringBuilder("sending ").append(num_msgs).append(" msgs (");
                    num_msgs=0;
                    sb.append(count).append(" bytes (" + f.format(percentage) + "% of max_bundle_size), collected in "+
                            + (stop-start) + "ms) to ").append(copy.size()).
                            append(" destination(s)");
                    if(copy.size() > 1) sb.append(" (dests=").append(copy.keySet()).append(")");
                    log.trace(sb.toString());
                }
                msgs.clear();
                count=0;
            }

            try {
                boolean multicast;
                for(Iterator it=copy.entrySet().iterator(); it.hasNext();) {
                    entry=(Map.Entry)it.next();
                    l=(List)entry.getValue();
                    if(l.size() == 0)
                        continue;
                    dst=(Address)entry.getKey();
                    multicast=dst == null || dst.isMulticastAddress();
                    synchronized(out_stream) {
                        try {
                            buffer=listToBuffer(l, multicast);
                            doSend(buffer, dst, multicast);
                        }
                        catch(Throwable e) {
                            if(log.isErrorEnabled()) log.error("exception sending msg: " + e.toString(), e.getCause());
                        }
                    }
                }
            }
            finally {
                start=0;
            }
        }



        private void checkLength(long len) throws Exception {
            if(len > max_bundle_size)
                throw new Exception("message size (" + len + ") is greater than max bundling size (" + max_bundle_size +
                        "). Set the fragmentation/bundle size in FRAG and TP correctly");
        }

        private class BundlingTimer implements TimeScheduler.Task {
            boolean cancelled=false;

            void cancel() {
                cancelled=true;
            }

            public boolean cancelled() {
                return cancelled;
            }

            public long nextInterval() {
                return max_bundle_timeout;
            }

            public void run() {
                bundleAndSend();
                cancelled=true;
            }
        }
    }



    private class DiagnosticsHandler implements Runnable {
        Thread t=null;
        MulticastSocket diag_sock=null;

        DiagnosticsHandler() {
        }

        String getName() {
            return t != null? t.getName() : null;
        }

        void setName(String thread_name) {
            if(t != null)
                t.setName(thread_name);
        }

        void start() throws IOException {
            diag_sock=new MulticastSocket(diagnostics_port);
            java.util.List interfaces=Util.getAllAvailableInterfaces();
            bindToInterfaces(interfaces, diag_sock);

            if(t == null || !t.isAlive()) {
                t=new Thread(Util.getGlobalThreadGroup(), this, "DiagnosticsHandler");
                t.setDaemon(true);
                t.start();
            }
        }

        void stop() {
            if(diag_sock != null)
                diag_sock.close();
            t=null;
        }

        public void run() {
            byte[] buf=new byte[1500]; // MTU on most LANs
            DatagramPacket packet;
            while(!diag_sock.isClosed() && Thread.currentThread().equals(t)) {
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
                        if(trace)
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
