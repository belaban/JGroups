package org.jgroups.protocols;


import org.jgroups.*;
import org.jgroups.stack.Protocol;
import org.jgroups.util.*;
import org.jgroups.util.List;
import org.jgroups.util.Queue;

import java.io.*;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.UnknownHostException;
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
 * @version $Id: TP.java,v 1.20 2005/08/11 11:43:15 belaban Exp $
 */
public abstract class TP extends Protocol {


    /** The address (host and port) of this member */
    Address         local_addr=null;

    /** The name of the group to which this member is connected */
    String          channel_name=null;

    /** The interface (NIC) which should be used by this transport */
    InetAddress     bind_addr=null;

    /** The transport should use all available interfaces if true */
    boolean         bind_to_all_interfaces=false;

    /** The port to which the transport binds. 0 means to bind to any (ephemeral) port */
    int             bind_port=0;
	int				port_range=1; // 27-6-2003 bgooren, Only try one port by default

    /** The members of this group (updated when a member joins or leaves) */
    final Vector    members=new Vector(11);

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
    boolean         loopback=true;

    /** Use a separate queue (plus a dedicated thread) for loopbacks */
    boolean         loopback_queue=false;

    /** Thread handling the loopback queue */
    LoopbackHandler loopback_handler=null;


    /** Discard packets with a different version. Usually minor version differences are okay. Setting this property
     * to true means that we expect the exact same version on all incoming packets */
    boolean         discard_incompatible_packets=false;

    /** Sometimes receivers are overloaded (they have to handle de-serialization etc).
     * Packet handler is a separate thread taking care of de-serialization, receiver
     * thread(s) simply put packet in queue and return immediately. Setting this to
     * true adds one more thread */
    boolean         use_incoming_packet_handler=false;

    /** Used by packet handler to store incoming DatagramPackets */
    Queue           incoming_queue=null;

    /** Dequeues DatagramPackets from packet_queue, unmarshalls them and
     * calls <tt>handleIncomingUdpPacket()</tt> */
    IncomingPacketHandler   incoming_packet_handler=null;

    /** Packets to be sent are stored in outgoing_queue and sent by a separate thread. Enabling this
     * value uses an additional thread */
    boolean         use_outgoing_packet_handler=false;

    /** Used by packet handler to store outgoing DatagramPackets */
    Queue           outgoing_queue=null;

    OutgoingPacketHandler outgoing_packet_handler=null;

    /** If set it will be added to <tt>local_addr</tt>. Used to implement
     * for example transport independent addresses */
    byte[]          additional_data=null;

    /** Maximum number of bytes for messages to be queued until they are sent. This value needs to be smaller
        than the largest datagram packet size in case of UDP */
    int max_bundle_size=AUTOCONF.senseMaxFragSizeStatic();

    /** Max number of milliseconds until queued messages are sent. Messages are sent when max_bundle_size or
     * max_bundle_timeout has been exceeded (whichever occurs faster)
     */
    long max_bundle_timeout=20;

    /** Enabled bundling of smaller messages into bigger ones */
    boolean enable_bundling=false;


    /** HashMap<Address, Address>. Keys=senders, values=destinations. For each incoming message M with sender S, adds
     * an entry with key=S and value= sender's IP address and port.
     */
    HashMap addr_translation_table=new HashMap();

    boolean use_addr_translation=false;

    TpHeader header;

    final String name=getName();

    static final String IGNORE_BIND_ADDRESS_PROPERTY="ignore.bind.address";



    long num_msgs_sent=0, num_msgs_received=0, num_bytes_sent=0, num_bytes_received=0;


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
    public boolean getBindToAllInterfaces() {return bind_to_all_interfaces;}
    public void setBindToAllInterfaces(boolean flag) {this.bind_to_all_interfaces=flag;}
    public boolean isDiscardIncompatiblePackets() {return discard_incompatible_packets;}
    public void setDiscardIncompatiblePackets(boolean flag) {discard_incompatible_packets=flag;}
    public boolean isEnableBundling() {return enable_bundling;}
    public void setEnableBundling(boolean flag) {enable_bundling=flag;}
    public int getMaxBundleSize() {return max_bundle_size;}
    public void setMaxBundleSize(int size) {max_bundle_size=size;}
    public long getMaxBundleTimeout() {return max_bundle_timeout;}
    public void setMaxBundleTimeout(long timeout) {max_bundle_timeout=timeout;}

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
     * @throws Throwable
     */
    public abstract void sendToAllMembers(byte[] data, int offset, int length) throws Exception;

    /**
     * Send to all members in the group. UDP would use an IP multicast message, whereas TCP would send N
     * messages, one for each member
     * @param dest Must be a non-null unicast address
     * @param data The data to be sent. This is not a copy, so don't modify it
     * @param offset
     * @param length
     * @throws Throwable
     */
    public abstract void sendToSingleMember(Address dest, byte[] data, int offset, int length) throws Exception;

    public abstract String getInfo();

    public abstract void preMarshalling(Message msg, Address dest, Address src);

    public abstract void postMarshalling(Message msg, Address dest, Address src);

    public abstract void postUnmarshalling(Message msg, Address dest, Address src);




    private void handleDiagnosticProbe(Address sender) {
        try {
            byte[] diag_rsp=getInfo().getBytes();
            if(log.isDebugEnabled())
                log.debug("sending diag response to " + sender);
            sendToSingleMember(sender, diag_rsp, 0, diag_rsp.length);
        }
        catch(Throwable t) {
            if(log.isErrorEnabled())
                log.error("failed sending diag rsp to " + sender, t);
        }
    }


    /* ------------------------------------------------------------------------------- */



    /*------------------------------ Protocol interface ------------------------------ */



    public void init() throws Exception {
        if(use_incoming_packet_handler) {
            incoming_queue=new Queue();
            incoming_packet_handler=new IncomingPacketHandler();
        }
        if(use_outgoing_packet_handler) {
            outgoing_queue=new Queue();
            if(enable_bundling) {
                outgoing_packet_handler=new BundlingOutgoingPacketHandler2();
            }
            else
                outgoing_packet_handler=new OutgoingPacketHandler();
        }
    }


    /**
     * Creates the unicast and multicast sockets and starts the unicast and multicast receiver threads
     */
    public void start() throws Exception {
        if(loopback_queue) {
            loopback_handler=new LoopbackHandler();
            loopback_handler.start();
        }
        startPacketHandlers();
        passUp(new Event(Event.SET_LOCAL_ADDRESS, local_addr));
    }


    public void stop() {
        stopPacketHandlers();
        if(loopback_handler != null) {
            loopback_handler.stop();
        }
    }



    /**
     * Setup the Protocol instance acording to the configuration string
     * @return true if no other properties are left.
     *         false if the properties still have data in them, ie ,
     *         properties are left over and not handled by the protocol stack
     */
    public boolean setProperties(Properties props) {
        String str;
        String tmp = null;

        super.setProperties(props);
        
        // PropertyPermission not granted if running in an untrusted environment with JNLP.
        try {
            tmp=System.getProperty("bind.address");
            if(Boolean.getBoolean(IGNORE_BIND_ADDRESS_PROPERTY)) {
                tmp=null;
            }
        }
        catch (SecurityException ex){
        }
        
        if(tmp != null)
            str=tmp;
        else
            str=props.getProperty("bind_addr");
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

        str=props.getProperty("bind_to_all_interfaces");
        if(str != null) {
            bind_to_all_interfaces=new Boolean(str).booleanValue();
            props.remove("bind_to_all_interfaces");
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

        str=props.getProperty("loopback_queue");
        if(str != null) {
            loopback_queue=Boolean.valueOf(str).booleanValue();
            props.remove("loopback_queue");
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

        str=props.getProperty("use_outgoing_packet_handler");
        if(str != null) {
            use_outgoing_packet_handler=Boolean.valueOf(str).booleanValue();
            props.remove("use_outgoing_packet_handler");
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

        if(enable_bundling) {
            if(use_outgoing_packet_handler == false)
                if(warn) log.warn("enable_bundling is true; setting use_outgoing_packet_handler=true");
            use_outgoing_packet_handler=true;
        }

        if(loopback_queue && loopback == false) {
            log.warn("loopback_queue is enabled, but loopback is false: setting loopback to true");
            loopback=true;
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

        setSourceAddress(msg);
        if(trace) {
            StringBuffer sb=new StringBuffer("sending msg to ").append(msg.getDest()).
                    append(" (src=").append(msg.getSrc()).append("), headers are ").append(msg.getHeaders());
            log.trace(sb.toString());
        }

        // Don't send if destination is local address. Instead, switch dst and src and put in up_queue.
        // If multicast message, loopback a copy directly to us (but still multicast). Once we receive this,
        // we will discard our own multicast message
        Address dest=msg.getDest();
        if(loopback && (dest == null || dest.equals(local_addr) || dest.isMulticastAddress())) {
            Message copy=msg.copy();
            // copy.removeHeader(name); // we don't remove the header
            copy.setSrc(local_addr);
            // copy.setDest(dest);
            evt=new Event(Event.MSG, copy);

            /* Because Protocol.up() is never called by this bottommost layer, we call up() directly in the observer.
               This allows e.g. PerfObserver to get the time of reception of a message */
            if(observer != null)
                observer.up(evt, up_queue.size());
            if(trace) log.trace(new StringBuffer("looping back message ").append(copy));
            if(loopback_queue)
                loopback_handler.add(evt);
            else
                passUp(evt);
            if(dest != null && !dest.isMulticastAddress())
                return;
        }

        try {
            if(use_outgoing_packet_handler)
                outgoing_queue.add(msg);
            else
                send(msg, dest);
        }
        catch(Throwable e) {
            if(log.isErrorEnabled()) log.error("failed sending message", e);
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

        if(length == 4) {  // received a diagnostics probe
            if(data[offset] == 'd' && data[offset+1] == 'i' && data[offset+2] == 'a' && data[offset+3] == 'g') {
                handleDiagnosticProbe(sender);
                return;
            }
        }

        boolean mcast=dest == null || dest.isMulticastAddress();
        if(trace){
            StringBuffer sb=new StringBuffer("received (");
            sb.append(mcast? "mcast)" : "ucast) ").append(length).append(" bytes from ").append(sender);
            log.trace(sb.toString());
        }

        try {
            if(use_incoming_packet_handler) {
                byte[] tmp=new byte[length];
                System.arraycopy(data, offset, tmp, 0, length);
                incoming_queue.add(new IncomingQueueEntry(dest, sender, tmp, offset, length));
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
        Message              msg=null;
        List                 l=null;  // used if bundling is enabled
        short                version;
        boolean              is_message_list;

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

                is_message_list=dis.readBoolean();
                if(is_message_list)
                    l=bufferToList(dis, dest);
                else
                    msg=bufferToMessage(dis, dest, sender);
            }

            if(is_message_list) {
                for(Enumeration en=l.elements(); en.hasMoreElements();) {
                    msg=(Message)en.nextElement();
                    try {
                        handleMessage(msg);
                    }
                    catch(Throwable t) {
                        if(log.isErrorEnabled())
                            log.error("failed unmarshalling message list", t);
                    }
                }
            }
            else {
                handleMessage(msg);
            }
        }
        catch(Throwable t) {
            if(log.isErrorEnabled())
                log.error("failed unmarshalling message", t);
        }
    }



    private void handleMessage(Message msg) {
        Event      evt;
        TpHeader   hdr;
        Address    dst=msg.getDest();

        if(stats) {
            num_msgs_received++;
            num_bytes_received+=msg.getLength();
        }

        // discard my own multicast loopback copy
        if(loopback) {
            Address src=msg.getSrc();
            if((dst == null || (dst.isMulticastAddress())) && src != null && local_addr.equals(src)) {
                if(trace)
                    log.trace("discarded own loopback multicast packet");
                return;
            }
        }

        evt=new Event(Event.MSG, msg);
        if(trace) {
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
                if(warn) log.warn("discarded message from different group (" +
                                                 ch_name + "). Sender was " + msg.getSrc());
                return;
            }
        }
        else {
            if(trace)
                log.trace(new StringBuffer("message does not have a transport header, msg is ").append(msg).
                          append(", headers are ").append(msg.getHeaders()).append(", will be discarded"));
            return;
        }
        passUp(evt);
    }


    /** Internal method to serialize and send a message. This method is not reentrant */
    private void send(Message msg, Address dest) throws Exception {
        Buffer   buf;
        Address  src=msg.getSrc();

        // Needs to be synchronized because we can have possible concurrent access, e.g.
        // Discovery uses a separate thread to send out discovery messages
        // We would *not* need to sync between send(), OutgoingPacketHandler and BundlingOutgoingPacketHandler,
        // because only *one* of them is enabled
        synchronized(out_stream) {
            buf=messageToBuffer(msg, dest, src);
            doSend(buf, dest);
        }
    }


    private void doSend(Buffer buf, Address dest) throws Exception {
        if(stats) {
            num_msgs_sent++;
            num_bytes_sent+=buf.getLength();
        }
        if(dest == null || dest.isMulticastAddress()) // 'null' means send to all group members
            sendToAllMembers(buf.getBuf(), buf.getOffset(), buf.getLength());
        else
            sendToSingleMember(dest, buf.getBuf(), buf.getOffset(), buf.getLength());
    }



    /**
     * This method needs to be synchronized on out_stream when it is called
     * @param msg
     * @param dest
     * @param src
     * @return
     * @throws java.io.IOException
     */
    private Buffer messageToBuffer(Message msg, Address dest, Address src) throws Exception {
        Buffer retval;
        out_stream.reset();
        buf_out_stream.reset(out_stream.getCapacity());
        dos.reset();
        dos.writeShort(Version.version); // write the version
        dos.writeBoolean(false); // single message, *not* a list of messages
        preMarshalling(msg, dest, src);  // allows for optimization by subclass
        msg.writeTo(dos);
        postMarshalling(msg, dest, src); // allows for optimization by subclass
        dos.flush();
        retval=new Buffer(out_stream.getRawBuffer(), 0, out_stream.size());
        return retval;
    }

    private Message bufferToMessage(DataInputStream instream, Address dest, Address sender) throws Exception {
        Message msg=new Message(false); // don't create headers, readFrom() will do this
        msg.readFrom(instream);
        postUnmarshalling(msg, dest, sender); // allows for optimization by subclass
        return msg;
    }


    void revertAddresses(Message msg, Address dest, Address src) {
        msg.setDest(dest);
        msg.setSrc(src);
    }




    private Buffer listToBuffer(List l, Address dest) throws Exception {
        Buffer retval;
        Address src;
        Message msg;
        int len=l != null? l.size() : 0;
        boolean src_written=false;
        out_stream.reset();
        buf_out_stream.reset(out_stream.getCapacity());
        dos.reset();
        dos.writeShort(Version.version);
        dos.writeBoolean(true);
        dos.writeInt(len);

        for(Enumeration en=l.elements(); en.hasMoreElements();) {
            msg=(Message)en.nextElement();
            src=msg.getSrc();
            if(!src_written) {
                Util.writeAddress(src, dos);
                src_written=true;
            }
            msg.setDest(null);
            msg.setSrc(null);
            msg.writeTo(dos);
            revertAddresses(msg, dest, src);
        }
        dos.flush();
        retval=new Buffer(out_stream.getRawBuffer(), 0, out_stream.size());
        return retval;
    }

    private List bufferToList(DataInputStream instream, Address dest) throws Exception {
        List            l=new List();
        DataInputStream in=null;
        int             len;
        Message         msg;
        Address         src;

        try {
            len=instream.readInt();
            src=Util.readAddress(instream);
            for(int i=0; i < len; i++) {
                msg=new Message(false); // don't create headers, readFrom() will do this
                msg.readFrom(instream);
                msg.setDest(dest);
                msg.setSrc(src);
                l.add(msg);
            }
            return l;
        }
        finally {
            Util.closeInputStream(in);
        }
    }




    /**
     * Starts the packet handlers
     */
    private void startPacketHandlers() throws Exception {
        if(use_outgoing_packet_handler)
            outgoing_packet_handler.start();
        if(use_incoming_packet_handler)
            incoming_packet_handler.start();
    }


    /**
     * Stops the packet handlers
     */
    private void stopPacketHandlers() {
        // 1. Stop the in_packet_handler thread
        if(incoming_packet_handler != null)
            incoming_packet_handler.stop();

        // 2. Stop the outgoing packet handler thread
        if(outgoing_packet_handler != null)
            outgoing_packet_handler.stop();
    }


    protected void handleDownEvent(Event evt) {
        switch(evt.getType()) {

        case Event.TMP_VIEW:
        case Event.VIEW_CHANGE:
            synchronized(members) {
                members.clear();
                Vector tmpvec=((View)evt.getArg()).getMembers();
                members.addAll(tmpvec);
            }
            break;

        case Event.GET_LOCAL_ADDRESS:   // return local address -> Event(SET_LOCAL_ADDRESS, local)
            passUp(new Event(Event.SET_LOCAL_ADDRESS, local_addr));
            break;

        case Event.CONNECT:
            channel_name=(String)evt.getArg();
            header=new TpHeader(channel_name);

            // removed March 18 2003 (bela), not needed (handled by GMS)
            // changed July 2 2003 (bela): we discard CONNECT_OK at the GMS level anyway, this might
            // be needed if we run without GMS though
            passUp(new Event(Event.CONNECT_OK));
            break;

        case Event.DISCONNECT:
            passUp(new Event(Event.DISCONNECT_OK));
            break;

        case Event.CONFIG:
            if(log.isDebugEnabled()) log.debug("received CONFIG event: " + evt.getArg());
            handleConfigEvent((HashMap)evt.getArg());
            break;
        }
    }


    protected void handleConfigEvent(HashMap map) {
        if(map == null) return;
        if(map.containsKey("additional_data"))
            additional_data=(byte[])map.get("additional_data");
    }



    /* ----------------------------- End of Private Methods ---------------------------------------- */



    /* ----------------------------- Inner Classes ---------------------------------------- */

    class IncomingQueueEntry {
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

        public void run() {
            IncomingQueueEntry entry;
            while(incoming_queue != null && incoming_packet_handler != null) {
                try {
                    entry=(IncomingQueueEntry)incoming_queue.remove();
                    handleIncomingPacket(entry.dest, entry.sender, entry.buf, entry.offset, entry.length);
                }
                catch(QueueClosedException closed_ex) {
                    if(trace) log.trace("packet_handler thread terminating");
                    break;
                }

            }
        }

        void start() {
            if(t == null || !t.isAlive()) {
                t=new Thread(this, "TP.IncomingPacketHandler thread");
                t.setDaemon(true);
                t.start();
            }
        }

        void stop() {
            if(incoming_queue != null)
                incoming_queue.close(false); // should terminate the packet_handler thread too
            t=null;
            incoming_queue=null;
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

        public void run() {
            Message msg;

            while(outgoing_queue != null && outgoing_packet_handler != null) {
                try {
                    msg=(Message)outgoing_queue.remove();
                    handleMessage(msg);
                }
                catch(QueueClosedException closed_ex) {
                    break;
                }
                catch(Throwable th) {
                    if(log.isErrorEnabled()) log.error("exception sending packet", th);
                }
                msg=null; // let's give the poor garbage collector a hand...
            }
            if(trace) log.trace("packet_handler thread terminating");
        }

        protected void handleMessage(Message msg) throws Throwable {
            Address dest=msg.getDest();
            send(msg, dest);
        }


        void start() {
            if(t == null || !t.isAlive()) {
                t=new Thread(this, "TP.OutgoingPacketHandler thread");
                t.setDaemon(true);
                t.start();
            }
        }

        void stop() {
            if(outgoing_queue != null)
                outgoing_queue.close(false); // should terminate the packet_handler thread too
            t=null;
        }
    }




    /**
     * Bundles smaller messages into bigger ones. Collects messages in a list until
     * messages of a total of <tt>max_bundle_size bytes</tt> have accumulated, or until
     * <tt>max_bundle_timeout</tt> milliseconds have elapsed, whichever is first. Messages
     * are unbundled at the receiver.
     */
//    private class BundlingOutgoingPacketHandler extends OutgoingPacketHandler {
//        long                total_bytes=0;
//        int                 num_msgs=0;
//        /** HashMap<Address, List<Message>>. Keys are destinations, values are lists of Messages */
//        final HashMap       msgs=new HashMap(11);
//
//
//        void start() {
//            super.start();
//            t.setName("TP.BundlingOutgoingPacketHandler thread");
//        }
//
//
//        public void run() {
//            Message msg, leftover=null;
//            long start=0;
//            while(outgoing_queue != null) {
//                try {
//                    total_bytes=0;
//                    msg=leftover != null? leftover : (Message)outgoing_queue.remove(); // blocks until message is available
//                    start=System.currentTimeMillis();
//                    leftover=waitForMessagesToAccumulate(msg, outgoing_queue, max_bundle_size, start, max_bundle_timeout);
//                    bundleAndSend(start);
//                }
//                catch(QueueClosedException closed_ex) {
//                    break;
//                }
//                catch(Throwable th) {
//                    if(log.isErrorEnabled()) log.error("exception sending packet", th);
//                }
//            }
//            bundleAndSend(start);
//            if(trace) log.trace("BundlingOutgoingPacketHandler thread terminated");
//        }
//
//
//        /**
//         * Waits until max_size bytes have accumulated in the queue, or max_time milliseconds have elapsed.
//         * When a message cannot be added to the ready-to-send bundle, it is returned, so the caller can
//         * re-submit it again next time.
//         * @param m
//         * @param q
//         * @param max_size
//         * @param max_time
//         * @return
//         */
//        private Message waitForMessagesToAccumulate(Message m, Queue q, long max_size, long start_time, long max_time) {
//            Message msg, leftover=null;
//            boolean running=true, size_exceeded, time_reached;
//            long    len, time_to_wait=max_time, waited_time;
//
//            while(running) {
//                try {
//                    msg=m != null? m : (Message)q.remove(time_to_wait);
//                    m=null; // necessary, otherwise we get 'm' again in subsequent iterations of the same loop !
//                    len=msg.size();
//                    checkLength(len);
//                    waited_time=System.currentTimeMillis() - start_time;
//                    time_to_wait=max_time - waited_time;
//                    size_exceeded=total_bytes + len > max_size;
//                    time_reached=time_to_wait <= 0;
//
//                    if(size_exceeded) {
//                        running=false;
//                        leftover=msg;
//                    }
//                    else {
//                        addMessage(msg);
//                        total_bytes+=len;
//                        if(time_reached)
//                            running=false;
//                    }
//                }
//                catch(TimeoutException timeout) {
//                    break;
//                }
//                catch(QueueClosedException closed) {
//                    break;
//                }
//                catch(Exception ex) {
//                    log.error("failure in bundling", ex);
//                }
//            }
//            return leftover;
//        }
//
//
//        private void checkLength(long len) throws Exception {
//            if(len > max_bundle_size)
//                throw new Exception("TP.BundlingOutgoingPacketHandler.handleMessage(): message size (" + len +
//                                    ") is greater than max bundling size (" + max_bundle_size + "). " +
//                                    "Set the fragmentation/bundle size in FRAG and TP correctly");
//        }
//
//
//        private void addMessage(Message msg) {
//            List    tmp;
//            Address dst=msg.getDest();
//            synchronized(msgs) {
//                tmp=(List)msgs.get(dst);
//                if(tmp == null) {
//                    tmp=new List();
//                    msgs.put(dst, tmp);
//                }
//                tmp.add(msg);
//                num_msgs++;
//            }
//        }
//
//
//
//        private void bundleAndSend(long start_time) {
//            Map.Entry      entry;
//            Address        dst;
//            Buffer         buffer;
//            List           l;
//            long           stop_time=System.currentTimeMillis();
//
//            synchronized(msgs) {
//                if(msgs.size() == 0)
//                    return;
//                if(start_time == 0)
//                    start_time=System.currentTimeMillis();
//
//                if(trace) {
//                    StringBuffer sb=new StringBuffer("sending ").append(num_msgs).append(" msgs (");
//                    sb.append(total_bytes).append(" bytes, ").append(stop_time-start_time).append("ms)");
//                    sb.append(" to ").append(msgs.size()).append(" destination(s)");
//                    if(msgs.size() > 1) sb.append(" (dests=").append(msgs.keySet()).append(")");
//                    log.trace(sb.toString());
//                }
//                for(Iterator it=msgs.entrySet().iterator(); it.hasNext();) {
//                    entry=(Map.Entry)it.next();
//                    dst=(Address)entry.getKey();
//                    l=(List)entry.getValue();
//                    try {
//                        if(l.size() > 0) {
//                            synchronized(out_stream) {
//                                buffer=listToBuffer(l, dst);
//                                doSend(buffer, dst);
//                            }
//                        }
//                    }
//                    catch(Exception e) {
//                        if(log.isErrorEnabled()) log.error("exception sending msg (dest=" + dst + ")", e);
//                    }
//                }
//                msgs.clear();
//                num_msgs=0;
//            }
//        }
//    }
//

    private class BundlingOutgoingPacketHandler2 extends OutgoingPacketHandler {
        /** HashMap<Address, List<Message>>. Keys are destinations, values are lists of Messages */
        final HashMap       msgs=new HashMap(11);
        long                count=0;    // current number of bytes accumulated
        int                 num_msgs=0;
        long                start=0;
        long                wait_time=0; // wait for removing messages from the queue



        private void init() {
            wait_time=start=count=0;
        }

        void start() {
            init();
            super.start();
            t.setName("TP.BundlingOutgoingPacketHandler2 thread");
        }


        public void run() {
            Message msg;
            long    length;
            while(outgoing_queue != null) {
                try {
                    msg=(Message)outgoing_queue.remove(wait_time);
                    length=msg.size();
                    checkLength(length);
                    if(start == 0)
                        start=System.currentTimeMillis();

                    if(count + length >= max_bundle_size) {
                        bundleAndSend();
                        count=0;
                        start=System.currentTimeMillis();
                    }

                    addMessage(msg);
                    count+=length;

                    wait_time=max_bundle_timeout - (System.currentTimeMillis() - start);
                    if(wait_time <= 0) {
                        bundleAndSend();
                        init();
                    }
                }
                catch(QueueClosedException queue_closed_ex) {
                    break;
                }
                catch(TimeoutException timeout_ex) {
                    bundleAndSend();
                    init();
                }
                catch(Throwable ex) {
                    log.error("failure in bundling", ex);
                }
            }
            if(trace) log.trace("BundlingOutgoingPacketHandler2 thread terminated");
        }




        private void checkLength(long len) throws Exception {
            if(len > max_bundle_size)
                throw new Exception("message size (" + len + ") is greater than max bundling size (" + max_bundle_size +
                        "). Set the fragmentation/bundle size in FRAG and TP correctly");
        }


        private void addMessage(Message msg) {
            List    tmp;
            Address dst=msg.getDest();
            tmp=(List)msgs.get(dst);
            if(tmp == null) {
                tmp=new List();
                msgs.put(dst, tmp);
            }
            tmp.add(msg);
            num_msgs++;
        }



        private void bundleAndSend() {
            Map.Entry      entry;
            Address        dst;
            Buffer         buffer;
            List           l;
            long           stop_time=System.currentTimeMillis();

            if(msgs.size() == 0)
                return;

            try {
                if(trace) {
                    StringBuffer sb=new StringBuffer("sending ").append(num_msgs).append(" msgs (");
                    sb.append(count).append(" bytes, ").append(stop_time-start).append("ms)");
                    sb.append(" to ").append(msgs.size()).append(" destination(s)");
                    if(msgs.size() > 1) sb.append(" (dests=").append(msgs.keySet()).append(")");
                    log.trace(sb.toString());
                }
                for(Iterator it=msgs.entrySet().iterator(); it.hasNext();) {
                    entry=(Map.Entry)it.next();
                    l=(List)entry.getValue();
                    if(l.size() == 0)
                        continue;
                    dst=(Address)entry.getKey();
                    synchronized(out_stream) {
                        try {
                            buffer=listToBuffer(l, dst);
                            doSend(buffer, dst);
                        }
                        catch(Throwable e) {
                            if(log.isErrorEnabled()) log.error("exception sending msg", e);
                        }
                    }
                }
            }
            finally {
                msgs.clear();
                num_msgs=0;
            }
        }
    }


    private class LoopbackHandler implements Runnable {
        private Queue queue=null;
        private Thread thread=null;


        void add(Event evt) {
            try {
                queue.add(evt);
            }
            catch(QueueClosedException e) {
                log.error("failed adding message to queue", e);
            }
        }

        void start() {
            queue=new Queue();
            thread=new Thread(this, "LoopbackHandler");
            thread.setDaemon(true);
            thread.start();
        }

        void stop() {
            if(thread != null && thread.isAlive()) {
                queue.close(false);
            }
        }

        public void run() {
            Event evt;
            while(thread != null && Thread.currentThread().equals(thread)) {
                try {
                    evt=(Event)queue.remove();
                    passUp(evt);
                }
                catch(QueueClosedException e) {
                    break;
                }
                catch(Throwable t) {
                    if(log.isErrorEnabled())
                        log.error("failed passing up message", t);
                }
            }
            if(trace)
                log.trace("LoopbackHandler thread terminated");
        }
    }


}
