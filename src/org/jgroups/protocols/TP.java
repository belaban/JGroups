package org.jgroups.protocols;


import EDU.oswego.cs.dl.util.concurrent.BoundedLinkedQueue;
import org.jgroups.*;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.*;
import org.jgroups.util.List;
import org.jgroups.util.Queue;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.*;
import java.text.NumberFormat;
import java.util.*;



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
 * @version $Id: TP.java,v 1.77.2.9 2008/09/03 15:19:34 galderz Exp $
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
     * @deprecated  Use {@link #receive_on_all_interfaces} instead */
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


    /** Packets to be sent are stored in outgoing_queue and sent by a separate thread. Enabling this
     * value uses an additional thread */
    boolean               use_outgoing_packet_handler=false;

    /** Used by packet handler to store outgoing DatagramPackets */
    BoundedLinkedQueue    outgoing_queue=null;

    /** max number of elements in the bounded outgoing_queue */
    int                   outgoing_queue_max_size=2000;

    OutgoingPacketHandler outgoing_packet_handler=null;

    /** If set it will be added to <tt>local_addr</tt>. Used to implement
     * for example transport independent addresses */
    byte[]          additional_data=null;

    /** Maximum number of bytes for messages to be queued until they are sent. This value needs to be smaller
        than the largest datagram packet size in case of UDP */
    int max_bundle_size=64000;

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
    String diagnostics_addr="224.0.75.75";
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

    long num_msgs_sent=0, num_msgs_received=0, num_bytes_sent=0, num_bytes_received=0;

    static  NumberFormat f;

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
    public int getOutgoingQueueSize() {return outgoing_queue != null? outgoing_queue.size() : 0;}
    public int getIncomingQueueSize() {return incoming_packet_queue != null? incoming_packet_queue.size() : 0;}
    public Address getLocalAddress() {return local_addr;}
    public String getChannelName() {return channel_name;}
    public boolean isLoopback() {return loopback;}
    public void setLoopback(boolean b) {loopback=b;}
    public boolean isUseIncomingPacketHandler() {return use_incoming_packet_handler;}
    public boolean isUseOutgoingPacketHandler() {return use_outgoing_packet_handler;}
    public int getOutgoingQueueMaxSize() {return outgoing_queue != null? outgoing_queue_max_size : 0;}
    public void setOutgoingQueueMaxSize(int new_size) {
        if(outgoing_queue != null) {
            outgoing_queue.setCapacity(new_size);
            outgoing_queue_max_size=new_size;
        }
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

        if(loopback) {
            incoming_msg_queue=new Queue();
            incoming_msg_handler=new IncomingMessageHandler();
            incoming_msg_handler.start();
        }

        if(use_outgoing_packet_handler) {
            outgoing_queue=new BoundedLinkedQueue(outgoing_queue_max_size);
            outgoing_packet_handler=new OutgoingPacketHandler();
            outgoing_packet_handler.start();
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

        // 1. Stop the outgoing packet handler thread
        if(outgoing_packet_handler != null)
            outgoing_packet_handler.stop();


        // 2. Stop the incoming packet handler thread
        if(incoming_packet_handler != null)
            incoming_packet_handler.stop();


        // 3. Finally stop the incoming message handler
        if(incoming_msg_handler != null)
            incoming_msg_handler.stop();
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
            if(log.isWarnEnabled()) log.warn("'use_packet_handler' is deprecated; use 'use_incoming_packet_handler' instead");
        }

        str=props.getProperty("use_incoming_packet_handler");
        if(str != null) {
            use_incoming_packet_handler=Boolean.valueOf(str).booleanValue();
            props.remove("use_incoming_packet_handler");
        }

        str=props.getProperty("use_outgoing_packet_handler");
        if(str != null) {
            use_outgoing_packet_handler=Boolean.valueOf(str).booleanValue();
            props.remove("use_outgoing_packet_handler");
        }

        str=props.getProperty("outgoing_queue_max_size");
        if(str != null) {
            outgoing_queue_max_size=Integer.parseInt(str);
            props.remove("outgoing_queue_max_size");
            if(outgoing_queue_max_size <= 0) {
                if(log.isWarnEnabled())
                    log.warn("outgoing_queue_max_size of " + outgoing_queue_max_size + " is invalid, setting it to 1");
                outgoing_queue_max_size=1;
            }
        }

        str=props.getProperty("max_bundle_size");
        if(str != null) {
            int bundle_size=Integer.parseInt(str);
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

        if(enable_bundling) {
            //if (use_outgoing_packet_handler == false)
              //  if(log.isWarnEnabled()) log.warn("enable_bundling is true; setting use_outgoing_packet_handler=true");
            // use_outgoing_packet_handler=true;
        }

        return true;
    }



    /**
     * This prevents the up-handler thread to be created, which essentially is superfluous:
     * messages are received from the network rather than from a layer below.
     * DON'T REMOVE !
     */
    public void startUpHandler() {
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

        // Because we don't call Protocol.passDown(), we notify the observer directly (e.g. PerfObserver).
        // This way, we still have performance numbers for TP
        if(observer != null)
            observer.passDown(evt);

        setSourceAddress(msg); // very important !! listToBuffer() will fail with a null src address !!
        if(log.isTraceEnabled()) {
            StringBuffer sb=new StringBuffer("sending msg to ").append(msg.getDest()).
                    append(" (src=").append(msg.getSrc()).append("), headers are ").append(msg.getHeaders());
            log.trace(sb.toString());
        }

        // Don't send if destination is local address. Instead, switch dst and src and put in up_queue.
        // If multicast message, loopback a copy directly to us (but still multicast). Once we receive this,
        // we will discard our own multicast message
        Address dest=msg.getDest();
        boolean multicast=dest == null || dest.isMulticastAddress();
        if(loopback && (multicast || dest.equals(local_addr))) {
            Message copy=msg.copy();

            // copy.removeHeader(name); // we don't remove the header
            copy.setSrc(local_addr);
            // copy.setDest(dest);

            if(log.isTraceEnabled()) log.trace(new StringBuffer("looping back message ").append(copy));
            try {
                incoming_msg_queue.add(copy);
            }
            catch(QueueClosedException e) {
                // log.error("failed adding looped back message to incoming_msg_queue", e);
            }

            if(!multicast)
                return;
        }

        try {
            if(use_outgoing_packet_handler)
                outgoing_queue.put(msg);
            else
                send(msg, dest, multicast);
        }
        catch(QueueClosedException closed_ex) {
        }
        catch(InterruptedIOException ioex) {
        }
        catch(InterruptedException interruptedEx) {
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

//        if(length == 4) {  // received a diagnostics probe
//            if(data[offset] == 'd' && data[offset+1] == 'i' && data[offset+2] == 'a' && data[offset+3] == 'g') {
//                handleDiagnosticProbe(sender);
//                return;
//            }
//        }

        boolean mcast=dest == null || dest.isMulticastAddress();
        if(log.isTraceEnabled()){
            StringBuffer sb=new StringBuffer("received (");
            sb.append(mcast? "mcast) " : "ucast) ").append(length).append(" bytes from ").append(sender);
            log.trace(sb.toString());
        }

        try {
            if(use_incoming_packet_handler) {
                byte[] tmp=new byte[length];
                System.arraycopy(data, offset, tmp, 0, length);
                incoming_packet_queue.add(new IncomingQueueEntry(dest, sender, tmp, 0, length));
            }
            else
                handleIncomingPacket(dest, sender, data, offset, length);
        }
        catch(Throwable t) {
            if(log.isErrorEnabled())
                log.error(new StringBuffer("failed handling data from ").append(sender), t);
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
                if(Version.isBinaryCompatible(version) == false) {
                    if(log.isWarnEnabled()) {
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
        if(log.isTraceEnabled()) {
            StringBuffer sb=new StringBuffer("message is ").append(msg).append(", headers are ").append(msg.getHeaders());
            log.trace(sb);
        }

        /* Because Protocol.up() is never called by this bottommost layer, we call up() directly in the observer.
        * This allows e.g. PerfObserver to get the time of reception of a message */
        if(observer != null)
            observer.up(evt, up_queue.size());

        hdr=(TpHeader)msg.getHeader(name); // replaced removeHeader() with getHeader()
        if(hdr != null) {

            /* Discard all messages destined for a channel with a different name */
            String ch_name=hdr.channel_name;

            // Discard if message's group name is not the same as our group name unless the
            // message is a diagnosis message (special group name DIAG_GROUP)
            if(ch_name != null && channel_name != null && !channel_name.equals(ch_name) &&
                    !ch_name.equals(Util.DIAG_GROUP)) {
                if(log.isWarnEnabled())
                    log.warn(new StringBuffer("discarded message from different group \"").append(ch_name).
                            append("\" (our group is \"").append(channel_name).append("\"). Sender was ").append(msg.getSrc()));
                return;
            }
        }
        else {
            if(log.isTraceEnabled())
                log.trace(new StringBuffer("message does not have a transport header, msg is ").append(msg).
                          append(", headers are ").append(msg.getHeaders()).append(", will be discarded"));
            return;
        }
        passUp(evt);
    }


    /** Internal method to serialize and send a message. This method is not reentrant */
    private void send(Message msg, Address dest, boolean multicast) throws Exception {
        if(enable_bundling) {
            bundler.send(msg, dest);
            return;
        }

        // Needs to be synchronized because we can have possible concurrent access, e.g.
        // Discovery uses a separate thread to send out discovery messages
        // We would *not* need to sync between send(), OutgoingPacketHandler and BundlingOutgoingPacketHandler,
        // because only *one* of them is enabled
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
        StringBuffer sb=new StringBuffer();
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
            if(outgoing_packet_handler != null) {
                tmp=outgoing_packet_handler.getName();
                if(tmp != null && tmp.indexOf(prefix) == -1) {
                    tmp+=prefix + channel_name + ")";
                    outgoing_packet_handler.setName(tmp);
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

            tmp=outgoing_packet_handler != null? outgoing_packet_handler.getName() : null;
            if(tmp != null) {
                index=tmp.indexOf(prefix);
                if(index > -1) {
                    tmp=tmp.substring(0, index);
                    outgoing_packet_handler.setName(tmp);
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



    /* ----------------------------- End of Private Methods ---------------------------------------- */



    /* ----------------------------- Inner Classes ---------------------------------------- */

    static class IncomingQueueEntry {
        Address   dest=null;
        Address   sender=null;
        byte[]    buf;
        int       offset, length;

        IncomingQueueEntry(Address dest, Address sender, byte[] buf, int offset, int length) {
            this.dest=dest;
            this.sender=sender;
            this.buf=buf;
            this.offset=offset;
            this.length=length;
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
            IncomingQueueEntry entry;
            while(!incoming_packet_queue.closed() && Thread.currentThread().equals(t)) {
                try {
                    entry=(IncomingQueueEntry)incoming_packet_queue.remove();
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
            if(log.isTraceEnabled()) log.trace("incoming message handler terminating");
        }
    }


    /**
     * This thread fetches byte buffers from the outgoing_packet_queue, converts them into messages and sends them
     * using the unicast or multicast socket
     */
    class OutgoingPacketHandler implements Runnable {
        Thread             t=null;
        byte[]             buf;
        DatagramPacket     packet;


        String getName() {
            return t != null? t.getName() : null;
        }

        void setName(String thread_name) {
            if(t != null)
                t.setName(thread_name);
        }

        void start() {
            if(t == null || !t.isAlive()) {
                t=new Thread(Util.getGlobalThreadGroup(), this, "OutgoingPacketHandler");
                t.setDaemon(true);
                t.start();
            }
        }

        void stop() {
            Thread tmp=t;
            t=null;
            if(tmp != null) {
                tmp.interrupt();
            }
        }

        public void run() {
            Message msg;

            while(t != null && Thread.currentThread().equals(t)) {
                try {
                    msg=(Message)outgoing_queue.take();
                    handleMessage(msg);
                }
                catch(QueueClosedException closed_ex) {
                    break;
                }
                catch(InterruptedException interruptedEx) {
                }
                catch(Throwable th) {
                    if(log.isErrorEnabled()) log.error("exception sending packet", th);
                }
                msg=null; // let's give the garbage collector a hand... this is probably useless though
            }
            if(log.isTraceEnabled()) log.trace("outgoing message handler terminating");
        }

        protected void handleMessage(Message msg) throws Throwable {
            Address dest=msg.getDest();
            send(msg, dest, dest == null || dest.isMulticastAddress());
        }


    }




    /**
     * Bundles smaller messages into bigger ones. Collects messages in a list until
     * messages of a total of <tt>max_bundle_size bytes</tt> have accumulated, or until
     * <tt>max_bundle_timeout</tt> milliseconds have elapsed, whichever is first. Messages
     * are unbundled at the receiver.
     */
//    private class BundlingOutgoingPacketHandler extends OutgoingPacketHandler {
//        /** HashMap<Address, List<Message>>. Keys are destinations, values are lists of Messages */
//        final HashMap       msgs=new HashMap(11);
//        long                count=0;    // current number of bytes accumulated
//        int                 num_msgs=0;
//        long                start=0;
//        long                wait_time=0; // wait for removing messages from the queue
//
//
//
//        private void init() {
//            wait_time=start=count=0;
//        }
//
//        void start() {
//            init();
//            super.start();
//            t.setName("BundlingOutgoingPacketHandler");
//        }
//
//        void stop() {
//            // bundleAndSend();
//            super.stop();
//        }
//
//        public void run() {
//            Message msg;
//            long    length;
//            while(t != null && Thread.currentThread().equals(t)) {
//                try {
//                    msg=(Message)outgoing_queue.poll(wait_time);
//                    if(msg == null)
//                        throw new TimeoutException();
//                    length=msg.size();
//                    checkLength(length);
//                    if(start == 0)
//                        start=System.currentTimeMillis();
//
//                    if(count + length >= max_bundle_size) {
//                        bundleAndSend();
//                        count=0;
//                        start=System.currentTimeMillis();
//                    }
//
//                    addMessage(msg);
//                    count+=length;
//
//                    wait_time=max_bundle_timeout - (System.currentTimeMillis() - start);
//                    if(wait_time <= 0) {
//                        bundleAndSend();
//                        init();
//                    }
//                }
//                catch(QueueClosedException queue_closed_ex) {
//                    bundleAndSend();
//                    break;
//                }
//                catch(TimeoutException timeout_ex) {
//                    bundleAndSend();
//                    init();
//                }
//                catch(Throwable ex) {
//                    log.error("failure in bundling", ex);
//                }
//            }
//            if(log.isTraceEnabled()) log.trace("BundlingOutgoingPacketHandler thread terminated");
//        }
//
//
//
//
//        private void checkLength(long len) throws Exception {
//            if(len > max_bundle_size)
//                throw new Exception("message size (" + len + ") is greater than max bundling size (" + max_bundle_size +
//                        "). Set the fragmentation/bundle size in FRAG and TP correctly");
//        }
//
//
//        private void addMessage(Message msg) { // no sync needed, never called by multiple threads concurrently
//            List    tmp;
//            Address dst=msg.getDest();
//            tmp=(List)msgs.get(dst);
//            if(tmp == null) {
//                tmp=new List();
//                msgs.put(dst, tmp);
//            }
//            tmp.add(msg);
//            num_msgs++;
//        }
//
//
//
//        private void bundleAndSend() {
//            Map.Entry      entry;
//            Address        dst;
//            Buffer         buffer;
//            List           l;
//            long           stop_time=System.currentTimeMillis();
//
//            if(msgs.size() == 0)
//                return;
//
//            try {
//                if(log.isTraceEnabled()) {
//                    StringBuffer sb=new StringBuffer("sending ").append(num_msgs).append(" msgs (");
//                    sb.append(count).append(" bytes, ").append(stop_time-start).append("ms)");
//                    sb.append(" to ").append(msgs.size()).append(" destination(s)");
//                    if(msgs.size() > 1) sb.append(" (dests=").append(msgs.keySet()).append(")");
//                    log.trace(sb.toString());
//                }
//                boolean multicast;
//                for(Iterator it=msgs.entrySet().iterator(); it.hasNext();) {
//                    entry=(Map.Entry)it.next();
//                    l=(List)entry.getValue();
//                    if(l.size() == 0)
//                        continue;
//                    dst=(Address)entry.getKey();
//                    multicast=dst == null || dst.isMulticastAddress();
//                    synchronized(out_stream) {
//                        try {
//                            buffer=listToBuffer(l, multicast);
//                            doSend(buffer, dst, multicast);
//                        }
//                        catch(Throwable e) {
//                            if(log.isErrorEnabled()) log.error("exception sending msg", e);
//                        }
//                    }
//                }
//            }
//            finally {
//                msgs.clear();
//                num_msgs=0;
//            }
//        }
//    }



    private class Bundler {
        /** HashMap<Address, List<Message>>. Keys are destinations, values are lists of Messages */
        final HashMap       msgs=new HashMap(36);
        long                count=0;    // current number of bytes accumulated
        int                 num_msgs=0;
        long                start=0;
        int                 num_bundling_tasks=0;
        static final int    MIN_NUMBER_OF_BUNDLING_TASKS=2;


        private synchronized void send(Message msg, Address dest) throws Exception {
            long length=msg.size();
            checkLength(length);

            if(start == 0)
                start=System.currentTimeMillis();
            if(count + length >= max_bundle_size) {
                bundleAndSend();  // clears msgs and resets num_msgs
            }

            addMessage(msg, dest);
            count+=length;
            if(num_bundling_tasks < MIN_NUMBER_OF_BUNDLING_TASKS) {
                num_bundling_tasks++;
                timer.schedule(new BundlingTimer(this), max_bundle_timeout);
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
                if(msgs.isEmpty())
                    return;
                copy=new HashMap(msgs);
                if(log.isTraceEnabled()) {
                    long stop=System.currentTimeMillis();
                    double percentage=100.0 / max_bundle_size * count;
                    StringBuffer sb=new StringBuffer("sending ").append(num_msgs).append(" msgs (");
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

        private class BundlingTimer extends TimerTask {
            final Object mutex;

            public BundlingTimer(Object mutex) {
                this.mutex=mutex;
            }

            public void run() {
                try {
                    bundleAndSend();
                }
                finally {
                    synchronized(this) {
                        num_bundling_tasks--;
                    }
                }
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
            Thread tmp=t;
            t=null;
            try {tmp.join(500);} catch(InterruptedException e) {}
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
