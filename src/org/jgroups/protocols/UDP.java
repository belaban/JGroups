// $Id: UDP.java,v 1.34 2004/07/23 02:28:01 belaban Exp $

package org.jgroups.protocols;


import org.jgroups.*;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.Protocol;
import org.jgroups.util.List;
import org.jgroups.util.*;

import java.io.*;
import java.net.*;
import java.util.*;




/**
 * IP multicast transport based on UDP. Messages to the group (msg.dest == null) will
 * be multicast (to all group members), whereas point-to-point messages
 * (msg.dest != null) will be unicast to a single member. Uses a multicast and
 * a unicast socket.<p>
 * The following properties are being read by the UDP protocol<p>
 * param mcast_addr - the multicast address to use default is 228.8.8.8<br>
 * param mcast_port - (int) the port that the multicast is sent on default is 7600<br>
 * param ip_mcast - (boolean) flag whether to use IP multicast - default is true<br>
 * param ip_ttl - Set the default time-to-live for multicast packets sent out on this
 * socket. default is 32<br>
 * param use_packet_handler - If set, the mcast and ucast receiver threads just put
 * the datagram's payload (a byte buffer) into a queue, from where a separate thread
 * will dequeue and handle them (unmarshal and pass up). This frees the receiver
 * threads from having to do message unmarshalling; this time can now be spent
 * receiving packets. If you have lots of retransmissions because of network
 * input buffer overflow, consider setting this property to true (default is false).
 * @author Bela Ban
 */
public class UDP extends Protocol implements Runnable {

    /** Socket used for
     * <ol>
     * <li>sending unicast packets and
     * <li>receiving unicast packets
     * </ol>
     * The address of this socket will be our local address (<tt>local_addr</tt>) */
    DatagramSocket  sock=null;

    /** IP multicast socket for <em>receiving</em> multicast packets */
    MulticastSocket mcast_recv_sock=null;

    /** IP multicast socket for <em>sending</em> multicast packets */
    MulticastSocket mcast_send_sock=null;

    /** The address (host and port) of this member */
    IpAddress       local_addr=null;

    /** The name of the group to which this member is connected */
    String          group_addr=null;

    /** The multicast address (mcast address and port) this member uses */
    IpAddress       mcast_addr=null;

    /** The interface (NIC) to which the unicast and multicast sockets bind */
    InetAddress     bind_addr=null;

    /** The port to which the unicast receiver socket binds.
     * 0 means to bind to any (ephemeral) port */
    int             bind_port=0;
	int				port_range=1; // 27-6-2003 bgooren, Only try one port by default

    /** The multicast address used for sending and receiving packets */
    String          mcast_addr_name="228.8.8.8";

    /** The multicast port used for sending and receiving packets */
    int             mcast_port=7600;

    /** The multicast receiver thread */
    Thread          mcast_receiver=null;

    /** The unicast receiver thread */
    UcastReceiver   ucast_receiver=null;

    /** Whether to enable IP multicasting. If false, multiple unicast datagram
     * packets are sent rather than one multicast packet */
    boolean         ip_mcast=true;

    /** The time-to-live (TTL) for multicast datagram packets */
    int             ip_ttl=64;

    /** The members of this group (updated when a member joins or leaves) */
    Vector          members=new Vector(11);

    /** Pre-allocated byte stream. Used for serializing datagram packets. Will grow as needed */
    ByteArrayOutputStream out_stream=new ByteArrayOutputStream(65535);

    /** Send buffer size of the multicast datagram socket */
    int             mcast_send_buf_size=32000;

    /** Receive buffer size of the multicast datagram socket */
    int             mcast_recv_buf_size=64000;

    /** Send buffer size of the unicast datagram socket */
    int             ucast_send_buf_size=32000;

    /** Receive buffer size of the unicast datagram socket */
    int             ucast_recv_buf_size=64000;

    /** If true, messages sent to self are treated specially: unicast messages are
     * looped back immediately, multicast messages get a local copy first and -
     * when the real copy arrives - it will be discarded. Useful for Window
     * media (non)sense */
    boolean         loopback=true;

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
        than the largest UDP datagram packet size */
    int max_bundle_size=AUTOCONF.senseMaxFragSizeStatic();

    /** Max number of milliseconds until queued messages are sent. Messages are sent when max_bundle_size or
     * max_bundle_timeout has been exceeded (whichever occurs faster)
     */
    long max_bundle_timeout=20;

    /** Enabled bundling of smaller messages into bigger ones */
    boolean enable_bundling=false;

    /** Used by BundlingOutgoingPacketHandler */
    TimeScheduler timer=null;


    /** The name of this protocol */
    final String    name="UDP";


    final int VERSION_LENGTH=Version.getLength();



    //todo: remove
    long start, stop, num_msgs=0;




    /**
     * public constructor. creates the UDP protocol, and initializes the
     * state variables, does however not start any sockets or threads
     */
    public UDP() {
        ;
    }

    /**
     * debug only
     */
    public String toString() {
        return "Protocol UDP(local address: " + local_addr + ')';
    }




    /* ----------------------- Receiving of MCAST UDP packets ------------------------ */

    public void run() {
        DatagramPacket  packet;
        byte            receive_buf[]=new byte[65535];
        int             len;
        byte[]          tmp, data;

        // moved out of loop to avoid excessive object creations (bela March 8 2001)
        packet=new DatagramPacket(receive_buf, receive_buf.length);

        while(mcast_receiver != null && mcast_recv_sock != null) {
            try {
                packet.setData(receive_buf, 0, receive_buf.length);
                mcast_recv_sock.receive(packet);
                len=packet.getLength();
                data=packet.getData();
                if(len == 1 && data[0] == 0) {
                    if(log.isTraceEnabled()) log.trace("received dummy packet");
                    continue;
                }

                if(len == 4) {  // received a diagnostics probe
                    if(data[0] == 'd' && data[1] == 'i' && data[2] == 'a' && data[3] == 'g') {
                        handleDiagnosticProbe(packet.getAddress(), packet.getPort());
                        continue;
                    }
                }

                if(log.isTraceEnabled())
                    log.trace("received (mcast) " + packet.getLength() + " bytes from " +
                                                packet.getAddress() + ':' + packet.getPort() + " (size=" + len + " bytes)");
                if(len > receive_buf.length) {
                    if(log.isErrorEnabled()) log.error("size of the received packet (" + len + ") is bigger than " +
                                             "allocated buffer (" + receive_buf.length + "): will not be able to handle packet. " +
                                             "Use the FRAG protocol and make its frag_size lower than " + receive_buf.length);
                }

                if(Version.compareTo(data) == false) {
                    if(log.isWarnEnabled()) log.warn("packet from " + packet.getAddress() + ':' + packet.getPort() +
                               " has different version (" +
                               Version.printVersionId(data, Version.version_id.length) +
                               ") from ours (" + Version.printVersionId(Version.version_id) +
                               "). This may cause problems");
                }

                if(use_incoming_packet_handler) {
                    tmp=new byte[len];
                    System.arraycopy(data, 0, tmp, 0, len);
                    incoming_queue.add(tmp);
                }
                else
                    handleIncomingUdpPacket(data);
            }
            catch(SocketException sock_ex) {
                 if(log.isDebugEnabled()) log.debug("multicast socket is closed, exception=" + sock_ex);
                break;
            }
            catch(InterruptedIOException io_ex) { // thread was interrupted
                ; // go back to top of loop, where we will terminate loop
            }
            catch(Throwable ex) {
                if(log.isErrorEnabled()) log.error("exception=" + ex + ", stack trace=" + Util.printStackTrace(ex));
                Util.sleep(300); // so we don't get into 100% cpu spinning (should NEVER happen !)
            }
        }
         if(log.isDebugEnabled()) log.debug("multicast thread terminated");
    }

    void handleDiagnosticProbe(InetAddress sender, int port) {
        try {
            byte[] diag_rsp=getDiagResponse().getBytes();
            DatagramPacket rsp=new DatagramPacket(diag_rsp, 0, diag_rsp.length, sender, port);
            if(log.isDebugEnabled()) log.debug("sending diag response to " + sender + ':' + port);
            sock.send(rsp);
        }
        catch(Throwable t) {
            if(log.isErrorEnabled()) log.error("failed sending diag rsp to " + sender + ':' + port +
                                                       ", exception=" + t);
        }
    }

    String getDiagResponse() {
        StringBuffer sb=new StringBuffer();
        sb.append(local_addr).append(" (").append(group_addr).append(')');
        sb.append(" [").append(mcast_addr_name).append(':').append(mcast_port).append("]\n");
        sb.append("Version=").append(Version.version).append(", cvs=\"").append(Version.cvs).append("\"\n");
        sb.append("bound to ").append(bind_addr).append(':').append(bind_port).append('\n');
        sb.append("members: ").append(members).append('\n');

        return sb.toString();
    }

    /* ------------------------------------------------------------------------------- */



    /*------------------------------ Protocol interface ------------------------------ */

    public String getName() {
        return name;
    }


    public void init() throws Exception {
        if(use_incoming_packet_handler) {
            incoming_queue=new Queue();
            incoming_packet_handler=new IncomingPacketHandler();
        }
        if(use_outgoing_packet_handler) {
            outgoing_queue=new Queue();
            if(enable_bundling) {
                timer=stack != null? stack.timer : null;
                if(timer == null)
                    throw new Exception("UDP.init(): timer could not be retrieved");
                outgoing_packet_handler=new BundlingOutgoingPacketHandler();
            }
            else
                outgoing_packet_handler=new OutgoingPacketHandler();
        }
    }


    /**
     * Creates the unicast and multicast sockets and starts the unicast and multicast receiver threads
     */
    public void start() throws Exception {
         if(log.isDebugEnabled()) log.debug("creating sockets and starting threads");
        createSockets();
        passUp(new Event(Event.SET_LOCAL_ADDRESS, local_addr));
        startThreads();
    }


    public void stop() {
         if(log.isDebugEnabled()) log.debug("closing sockets and stopping threads");
        stopThreads();  // will close sockets, closeSockets() is not really needed anymore, but...
        closeSockets(); // ... we'll leave it in there for now (doesn't do anything if already closed)
    }


    /**
     * Setup the Protocol instance acording to the configuration string
     * The following properties are being read by the UDP protocol
     * param mcast_addr - the multicast address to use default is 228.8.8.8
     * param mcast_port - (int) the port that the multicast is sent on default is 7600
     * param ip_mcast - (boolean) flag whether to use IP multicast - default is true
     * param ip_ttl - Set the default time-to-live for multicast packets sent out on this socket. default is 32
     * @return true if no other properties are left.
     *         false if the properties still have data in them, ie ,
     *         properties are left over and not handled by the protocol stack
     *
     */
    public boolean setProperties(Properties props) {
        String str, tmp;

        super.setProperties(props);
        tmp=System.getProperty("bind.address");
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

        str=props.getProperty("bind_port");
        if(str != null) {
            bind_port=Integer.parseInt(str);
            props.remove("bind_port");
        }

		str=props.getProperty("start_port");
        if(str != null) {
            bind_port=Integer.parseInt(str);
            props.remove("start_port");
        }

		str=props.getProperty("port_range");
        if(str != null) {
            port_range=Integer.parseInt(str);
            props.remove("port_range");
        }

        str=props.getProperty("mcast_addr");
        if(str != null) {
            mcast_addr_name=str;
            props.remove("mcast_addr");
        }

        str=props.getProperty("mcast_port");
        if(str != null) {
            mcast_port=Integer.parseInt(str);
            props.remove("mcast_port");
        }

        str=props.getProperty("ip_mcast");
        if(str != null) {
            ip_mcast=Boolean.valueOf(str).booleanValue();
            props.remove("ip_mcast");
        }

        str=props.getProperty("ip_ttl");
        if(str != null) {
            ip_ttl=Integer.parseInt(str);
            props.remove("ip_ttl");
        }

        str=props.getProperty("mcast_send_buf_size");
        if(str != null) {
            mcast_send_buf_size=Integer.parseInt(str);
            props.remove("mcast_send_buf_size");
        }

        str=props.getProperty("mcast_recv_buf_size");
        if(str != null) {
            mcast_recv_buf_size=Integer.parseInt(str);
            props.remove("mcast_recv_buf_size");
        }

        str=props.getProperty("ucast_send_buf_size");
        if(str != null) {
            ucast_send_buf_size=Integer.parseInt(str);
            props.remove("ucast_send_buf_size");
        }

        str=props.getProperty("ucast_recv_buf_size");
        if(str != null) {
            ucast_recv_buf_size=Integer.parseInt(str);
            props.remove("ucast_recv_buf_size");
        }

        str=props.getProperty("loopback");
        if(str != null) {
            loopback=Boolean.valueOf(str).booleanValue();
            props.remove("loopback");
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

        str=props.getProperty("max_bundle_size");
        if(str != null) {
            int bundle_size=Integer.parseInt(str);
            if(bundle_size > max_bundle_size) {
                if(log.isErrorEnabled()) log.error("max_bundle_size (" + bundle_size +
                        ") is greater than largest UDP fragmentation size (" + max_bundle_size + ')');
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

        if(props.size() > 0) {
            System.err.println("UDP.setProperties(): the following properties are not recognized:");
            props.list(System.out);
            return false;
        }

        if(enable_bundling) {
            if(use_outgoing_packet_handler == false)
                if(log.isWarnEnabled()) log.warn("enable_bundling is true; setting use_outgoing_packet_handler=true");
            use_outgoing_packet_handler=true;
        }

        return true;
    }



    /**
     * DON'T REMOVE ! This prevents the up-handler thread to be created, which essentially is superfluous:
     * messages are received from the network rather than from a layer below.
     */
    public void startUpHandler() {
        ;
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
     * modified Message to the send queue of the layer below it, by calling Down).
     */
    public void down(Event evt) {
        Message msg;
        Object dest_addr;

        if(evt.getType() != Event.MSG) {  // unless it is a message handle it and respond
            handleDownEvent(evt);
            return;
        }




        // ****************** profiling ******************
        /*if(num_msgs == 0) {
            start=System.currentTimeMillis();
            num_msgs++;
        }
        else if(num_msgs >= 1000) {
            stop=System.currentTimeMillis();

            long total_time=stop-start;
            double msgs_per_msec=num_msgs / (double)total_time;


                if(log.isInfoEnabled()) log.info("UDP.down.profile()",
                        "total_time=" + total_time + ", msgs/ms=" + msgs_per_msec);
            num_msgs=0;
        }
        else {
            num_msgs++;
        }*/
        // ****************** profiling ******************


        msg=(Message)evt.getArg();

        if(group_addr != null) {
            // added patch by Roland Kurmann (March 20 2003)
            msg.putHeader(name, new UdpHeader(group_addr));
        }

        dest_addr=msg.getDest();

        // Because we don't call Protocol.passDown(), we notify the observer directly (e.g. PerfObserver).
        // This way, we still have performance numbers for UDP
        if(observer != null)
            observer.passDown(evt);

        if(dest_addr == null) { // 'null' means send to all group members
            if(ip_mcast) {
                if(mcast_addr == null) {
                    if(log.isErrorEnabled()) log.error("dest address of message is null, and " +
                                              "sending to default address fails as mcast_addr is null, too !" +
                                              " Discarding message " + Util.printEvent(evt));
                    return;
                }
                //if we want to use IP multicast, then set the destination of the message
                msg.setDest(mcast_addr);
            }
            else {
                //sends a separate UDP message to each address
                sendMultipleUdpMessages(msg, members);
                return;
            }
        }

        try {
            sendUdpMessage(msg);
        }
        catch(Exception e) {
            if(log.isErrorEnabled()) log.error("exception=" + e + ", msg=" + msg + ", mcast_addr=" + mcast_addr);
        }
    }






    /*--------------------------- End of Protocol interface -------------------------- */


    /* ------------------------------ Private Methods -------------------------------- */



    /**
     * If the sender is null, set our own address. We cannot just go ahead and set the address
     * anyway, as we might be sending a message on behalf of someone else ! E.g. in case of
     * retransmission, when the original sender has crashed, or in a FLUSH protocol when we
     * have to return all unstable messages with the FLUSH_OK response.
     */
    void setSourceAddress(Message msg) {
        if(msg.getSrc() == null)
            msg.setSrc(local_addr);
    }




    /**
     * Processes a packet read from either the multicast or unicast socket. Needs to be synchronized because
     * mcast or unicast socket reads can be concurrent
     */
    void handleIncomingUdpPacket(byte[] data) {
        ByteArrayInputStream inp_stream;
        ObjectInputStream    inp;
        Message              msg=null;
        List                 l;  // used if bundling is enabled

        try {
            // skip the first n bytes (default: 4), this is the version info
            inp_stream=new ByteArrayInputStream(data, VERSION_LENGTH, data.length - VERSION_LENGTH);
            inp=new ObjectInputStream(inp_stream);
            if(enable_bundling) {
                l=new List();
                l.readExternal(inp);
                for(Enumeration en=l.elements(); en.hasMoreElements();) {
                    msg=(Message)en.nextElement();
                    try {
                        handleMessage(msg);
                    }
                    catch(Throwable t) {
                        if(log.isErrorEnabled()) log.error("failure: " + t.toString());
                    }
                }
            }
            else {
                msg=new Message();
                msg.readExternal(inp);
                handleMessage(msg);
            }
        }
        catch(Throwable e) {
            if(log.isErrorEnabled()) log.error("exception=" + Util.getStackTrace(e));
        }
    }


    void handleMessage(Message msg) {
        Event evt;
        UdpHeader hdr;

        // discard my own multicast loopback copy
        if(loopback) {
            Address dst=msg.getDest();
            Address src=msg.getSrc();

            if(dst != null && dst.isMulticastAddress() && src != null && local_addr.equals(src)) {
                if(log.isTraceEnabled())
                    log.trace("discarded own loopback multicast packet");
                return;
            }
        }

        evt=new Event(Event.MSG, msg);
        if(log.isTraceEnabled())
            log.trace("message is " + msg + ", headers are " + msg.getHeaders());

        /* Because Protocol.up() is never called by this bottommost layer, we call up() directly in the observer.
        * This allows e.g. PerfObserver to get the time of reception of a message */
        if(observer != null)
            observer.up(evt, up_queue.size());

        hdr=(UdpHeader)msg.getHeader(name); // replaced removeHeader() with getHeader()
        if(hdr != null) {

            /* Discard all messages destined for a channel with a different name */
            String ch_name=null;

            if(hdr.group_addr != null)
                ch_name=hdr.group_addr;

            // Discard if message's group name is not the same as our group name unless the
            // message is a diagnosis message (special group name DIAG_GROUP)
            if(ch_name != null && group_addr != null && !group_addr.equals(ch_name) &&
                    !ch_name.equals(Util.DIAG_GROUP)) {
                if(log.isWarnEnabled()) log.warn("discarded message from different group (" +
                        ch_name + "). Sender was " + msg.getSrc());
                return;
            }
        }
        else {
            if(log.isErrorEnabled()) log.error("message does not have a UDP header");
        }
        passUp(evt);
    }


    /** Send a message to the address specified in dest */
    void sendUdpMessage(Message msg) throws Exception {
        IpAddress           dest;
        Message             copy;
        Event               evt;

        dest=(IpAddress)msg.getDest();  // guaranteed not to be null
        setSourceAddress(msg);

        if(log.isTraceEnabled())
            log.trace("sending message to " + msg.getDest() +
                        " (src=" + msg.getSrc() + "), headers are " + msg.getHeaders());

        // Don't send if destination is local address. Instead, switch dst and src and put in up_queue.
        // If multicast message, loopback a copy directly to us (but still multicast). Once we receive this,
        // we will discard our own multicast message
        if(loopback && (dest.equals(local_addr) || dest.isMulticastAddress())) {
            copy=msg.copy();
            // copy.removeHeader(name); // we don't remove the header
            copy.setSrc(local_addr);
            copy.setDest(dest);
            evt=new Event(Event.MSG, copy);

            /* Because Protocol.up() is never called by this bottommost layer, we call up() directly in the observer.
               This allows e.g. PerfObserver to get the time of reception of a message */
            if(observer != null)
                observer.up(evt, up_queue.size());
            if(log.isTraceEnabled()) log.trace("looped back local message " + copy);
            passUp(evt);
            if(!dest.isMulticastAddress())
                return;
        }

        if(use_outgoing_packet_handler) {
            outgoing_queue.add(msg);
            return;
        }

        send(msg);
    }


    /** Internal method to serialize and send a message. This method is not reentrant */
    void send(Message msg) throws Exception {
        IpAddress  dest=(IpAddress)msg.getDest();
        byte[]     buf=messageToBuffer(msg);
        doSend(buf, dest.getIpAddress(), dest.getPort());
    }

    void doSend(byte[] data, InetAddress dest, int port) throws IOException {
        DatagramPacket       packet;
        packet=new DatagramPacket(data, data.length, dest, port);

        if(dest.isMulticastAddress() && mcast_send_sock != null) { // mcast_recv_sock might be null if ip_mcast is false
            mcast_send_sock.send(packet);
        }
        else {
            if(sock != null)
                sock.send(packet);
        }
    }



    void sendMultipleUdpMessages(Message msg, Vector dests) {
        Address dest;

        for(int i=0; i < dests.size(); i++) {
            dest=(Address)dests.elementAt(i);
            msg.setDest(dest);

            try {
                sendUdpMessage(msg);
            }
            catch(Exception e) {
                if(log.isDebugEnabled()) log.debug("exception=" + e);
            }
        }
    }


    byte[] messageToBuffer(Message msg) throws Exception {
        ObjectOutputStream   out;
        // BufferedOutputStream bos;

        out_stream.reset();
        //bos=new BufferedOutputStream(out_stream);
        out_stream.write(Version.version_id, 0, Version.version_id.length); // write the version
        // bos.write(Version.version_id, 0, Version.version_id.length); // write the version
        out=new ObjectOutputStream(out_stream);
        // out=new ObjectOutputStream(bos);
        msg.writeExternal(out);
        out.flush(); // needed if out buffers its output to out_stream
        return out_stream.toByteArray();
    }

    /**
     * Create UDP sender and receiver sockets. Currently there are 2 sockets
     * (sending and receiving). This is due to Linux's non-BSD compatibility
     * in the JDK port (see DESIGN).
     */
    void createSockets() throws Exception {
        InetAddress tmp_addr=null;

        // bind_addr not set, try to assign one by default. This is needed on Windows

        // changed by bela Feb 12 2003: by default multicast sockets will be bound to all network interfaces

        // CHANGED *BACK* by bela March 13 2003: binding to all interfaces did not result in a correct
        // local_addr. As a matter of fact, comparison between e.g. 0.0.0.0:1234 (on hostA) and
        // 0.0.0.0:1.2.3.4 (on hostB) would fail !
        if(bind_addr == null) {
            InetAddress[] interfaces=InetAddress.getAllByName(InetAddress.getLocalHost().getHostAddress());
            if(interfaces != null && interfaces.length > 0)
                bind_addr=interfaces[0];
        }
        if(bind_addr == null)
            bind_addr=InetAddress.getLocalHost();

        if(bind_addr != null)
            if(log.isInfoEnabled()) log.info("unicast sockets will use interface " +
                                              bind_addr.getHostAddress());


        // 2. Create socket for receiving unicast UDP packets. The address and port
        //    of this socket will be our local address (local_addr)

		// 27-6-2003 bgooren, find available port in range (start_port, start_port+port_range)
        int rcv_port=bind_port, max_port=bind_port + port_range;
        while(rcv_port <= max_port) {

            try {
                sock=new DatagramSocket(rcv_port, bind_addr);
                break;
            }
            catch(SocketException bind_ex) {	// Cannot listen on this port
                rcv_port++;
            }
            catch(SecurityException sec_ex) { // Not allowed to list on this port
                rcv_port++;
            }

            // Cannot listen at all, throw an Exception
            if(rcv_port == max_port + 1) { // +1 due to the increment above
                throw new Exception("UDP.createSockets(): cannot list on any port in range " +
                        bind_port + '-' + (bind_port + port_range));
            }
        }
		//ucast_recv_sock=new DatagramSocket(bind_port, bind_addr);
        if(sock == null)
            throw new Exception("UDP.createSocket(): sock is null");

        local_addr=new IpAddress(sock.getLocalAddress(), sock.getLocalPort());
        if(additional_data != null)
            local_addr.setAdditionalData(additional_data);


        // 3. Create socket for receiving IP multicast packets
        if(ip_mcast) {
            // 3a. Create mcast receiver socket
            mcast_recv_sock=new MulticastSocket(mcast_port);
            mcast_recv_sock.setTimeToLive(ip_ttl);
            if(bind_addr != null)
                mcast_recv_sock.setInterface(bind_addr);
            tmp_addr=InetAddress.getByName(mcast_addr_name);
            mcast_addr=new IpAddress(tmp_addr, mcast_port);
            mcast_recv_sock.joinGroup(tmp_addr);

            // 3b. Create mcast sender socket
            mcast_send_sock=new MulticastSocket();
            mcast_send_sock.setTimeToLive(ip_ttl);
            if(bind_addr != null)
                mcast_send_sock.setInterface(bind_addr);
        }

        setBufferSizes();
        if(log.isInfoEnabled()) log.info("socket information:\n" + dumpSocketInfo());
    }


    String dumpSocketInfo() throws Exception {
        StringBuffer sb=new StringBuffer();
        sb.append("local_addr=").append(local_addr);
        sb.append(", mcast_addr=").append(mcast_addr);
        sb.append(", bind_addr=").append(bind_addr);
        sb.append(", ttl=").append(ip_ttl);

        if(sock != null) {
            sb.append("\nsock: bound to ");
            sb.append(sock.getLocalAddress().getHostAddress()).append(':').append(sock.getLocalPort());
            sb.append(", receive buffer size=").append(sock.getReceiveBufferSize());
            sb.append(", send buffer size=").append(sock.getSendBufferSize());
        }

        if(mcast_recv_sock != null) {
            sb.append("\nmcast_recv_sock: bound to ");
            sb.append(mcast_recv_sock.getInterface().getHostAddress()).append(':').append(mcast_recv_sock.getLocalPort());
            sb.append(", send buffer size=").append(mcast_recv_sock.getSendBufferSize());
            sb.append(", receive buffer size=").append(mcast_recv_sock.getReceiveBufferSize());
        }

         if(mcast_send_sock != null) {
            sb.append("\nmcast_send_sock: bound to ");
            sb.append(mcast_send_sock.getInterface().getHostAddress()).append(':').append(mcast_send_sock.getLocalPort());
            sb.append(", send buffer size=").append(mcast_send_sock.getSendBufferSize());
            sb.append(", receive buffer size=").append(mcast_send_sock.getReceiveBufferSize());
        }
        return sb.toString();
    }


    void setBufferSizes() {
        if(sock != null) {
            try {
                sock.setSendBufferSize(ucast_send_buf_size);
            }
            catch(Throwable ex) {
                if(log.isWarnEnabled()) log.warn("failed setting ucast_send_buf_size in sock: " + ex);
            }
            try {
                sock.setReceiveBufferSize(ucast_recv_buf_size);
            }
            catch(Throwable ex) {
                if(log.isWarnEnabled()) log.warn("failed setting ucast_recv_buf_size in sock: " + ex);
            }
        }

        if(mcast_recv_sock != null) {
            try {
                mcast_recv_sock.setSendBufferSize(mcast_send_buf_size);
            }
            catch(Throwable ex) {
                if(log.isWarnEnabled()) log.warn("failed setting mcast_send_buf_size in mcast_recv_sock: " + ex);
            }

            try {
                mcast_recv_sock.setReceiveBufferSize(mcast_recv_buf_size);
            }
            catch(Throwable ex) {
                if(log.isWarnEnabled()) log.warn("failed setting mcast_recv_buf_size in mcast_recv_sock: " + ex);
            }
        }

        if(mcast_send_sock != null) {
            try {
                mcast_send_sock.setSendBufferSize(mcast_send_buf_size);
            }
            catch(Throwable ex) {
                if(log.isWarnEnabled()) log.warn("failed setting mcast_send_buf_size in mcast_send_sock: " + ex);
            }

            try {
                mcast_send_sock.setReceiveBufferSize(mcast_recv_buf_size);
            }
            catch(Throwable ex) {
                if(log.isWarnEnabled()) log.warn("failed setting mcast_recv_buf_size in mcast_send_sock: " + ex);
            }
        }

    }


    /**
     * Closed UDP unicast and multicast sockets
     */
    void closeSockets() {
        // 1. Close multicast socket
        closeMulticastSocket();

        // 2. Close socket
        closeSocket();
    }


    void closeMulticastSocket() {
        if(mcast_recv_sock != null) {
            try {
                if(mcast_addr != null) {
                    // by sending a dummy packet the thread will be awakened
                    sendDummyPacket(mcast_addr.getIpAddress(), mcast_addr.getPort());
                    Util.sleep(300);
                    mcast_recv_sock.leaveGroup(mcast_addr.getIpAddress());
                }
                mcast_recv_sock.close(); // this will cause the mcast receiver thread to break out of its loop
                mcast_recv_sock=null;
                 if(log.isDebugEnabled()) log.debug("multicast receive socket closed");
            }
            catch(IOException ex) {
            }
            mcast_addr=null;
        }

        if(mcast_send_sock != null) {
            mcast_send_sock.close(); // this will cause the mcast receiver thread to break out of its loop
            mcast_send_sock=null;
            if(log.isDebugEnabled()) log.debug("multicast send socket closed");
        }
    }


    void closeSocket() {
        if(sock != null) {
            // by sending a dummy packet, the thread will terminate (if it was flagged as stopped before)
            sendDummyPacket(sock.getLocalAddress(), sock.getLocalPort());

            sock.close();
            sock=null;
             if(log.isDebugEnabled()) log.debug("socket closed");
        }
    }



    /**
     * Workaround for the problem encountered in certains JDKs that a thread listening on a socket
     * cannot be interrupted. Therefore we just send a dummy datagram packet so that the thread 'wakes up'
     * and realizes it has to terminate. Should be removed when all JDKs support Thread.interrupt() on
     * reads. Uses sock t send dummy packet, so this socket has to be still alive.
     * @param dest The destination host. Will be local host if null
     * @param port The destination port
     */
    void sendDummyPacket(InetAddress dest, int port) {
        DatagramPacket packet;
        byte[] buf={0};

        if(dest == null) {
            try {
                dest=InetAddress.getLocalHost();
            }
            catch(Exception e) {
            }
        }

        if(log.isTraceEnabled()) log.trace("sending packet to " + dest + ':' + port);

        if(sock == null || dest == null) {
            if(log.isWarnEnabled()) log.warn("sock was null or dest was null, cannot send dummy packet");
            return;
        }
        packet=new DatagramPacket(buf, buf.length, dest, port);
        try {
            sock.send(packet);
        }
        catch(Throwable e) {
            if(log.isErrorEnabled()) log.error("exception sending dummy packet to " + dest + ':' + port + ": " + e);
        }
    }


    /**
     * Starts the unicast and multicast receiver threads
     */
    void startThreads() throws Exception {
        if(ucast_receiver == null) {
            //start the listener thread of the ucast_recv_sock
            ucast_receiver=new UcastReceiver();
            ucast_receiver.start();
             if(log.isDebugEnabled()) log.debug("created unicast receiver thread");
        }

        if(ip_mcast) {
            if(mcast_receiver != null) {
                if(mcast_receiver.isAlive()) {

                        if(log.isDebugEnabled()) log.debug("did not create new multicastreceiver thread as existing " +
                                   "multicast receiver thread is still running");
                }
                else
                    mcast_receiver=null; // will be created just below...
            }

            if(mcast_receiver == null) {
                mcast_receiver=new Thread(this, "UDP mcast receiver");
                mcast_receiver.setPriority(Thread.MAX_PRIORITY); // needed ????
                mcast_receiver.setDaemon(true);
                mcast_receiver.start();
            }
        }
        if(use_outgoing_packet_handler)
            outgoing_packet_handler.start();
        if(use_incoming_packet_handler)
            incoming_packet_handler.start();
    }


    /**
     * Stops unicast and multicast receiver threads
     */
    void stopThreads() {
        Thread tmp;

        // 1. Stop the multicast receiver thread
        if(mcast_receiver != null) {
            if(mcast_receiver.isAlive()) {
                tmp=mcast_receiver;
                mcast_receiver=null;
                closeMulticastSocket();  // will cause the multicast thread to terminate
                tmp.interrupt();
                try {
                    tmp.join(100);
                }
                catch(Exception e) {
                }
                tmp=null;
            }
            mcast_receiver=null;
        }

        // 2. Stop the unicast receiver thread
        if(ucast_receiver != null) {
            ucast_receiver.stop();
            ucast_receiver=null;
        }

        // 3. Stop the in_packet_handler thread
        if(incoming_packet_handler != null)
            incoming_packet_handler.stop();
    }


    void handleDownEvent(Event evt) {
        switch(evt.getType()) {

            case Event.TMP_VIEW:
            case Event.VIEW_CHANGE:
                synchronized(members) {
                    members.removeAllElements();
                    Vector tmpvec=((View)evt.getArg()).getMembers();
                    for(int i=0; i < tmpvec.size(); i++)
                        members.addElement(tmpvec.elementAt(i));
                }
                break;

            case Event.GET_LOCAL_ADDRESS:   // return local address -> Event(SET_LOCAL_ADDRESS, local)
                passUp(new Event(Event.SET_LOCAL_ADDRESS, local_addr));
                break;

            case Event.CONNECT:
                group_addr=(String)evt.getArg();

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


    void handleConfigEvent(HashMap map) {
        if(map == null) return;
        if(map.containsKey("additional_data"))
            additional_data=(byte[])map.get("additional_data");
        if(map.containsKey("send_buf_size")) {
            mcast_send_buf_size=((Integer)map.get("send_buf_size")).intValue();
            ucast_send_buf_size=mcast_send_buf_size;
        }
        if(map.containsKey("recv_buf_size")) {
            mcast_recv_buf_size=((Integer)map.get("recv_buf_size")).intValue();
            ucast_recv_buf_size=mcast_recv_buf_size;
        }
        setBufferSizes();
    }


    /* ----------------------------- End of Private Methods ---------------------------------------- */

    /* ----------------------------- Inner Classes ---------------------------------------- */
    public class UcastReceiver implements Runnable {
        boolean running=true;
        Thread thread=null;


        public void start() {
            if(thread == null) {
                thread=new Thread(this, "UDP.UcastReceiverThread");
                thread.setDaemon(true);
                running=true;
                thread.start();
            }
        }


        public void stop() {
            Thread tmp;
            if(thread != null && thread.isAlive()) {
                running=false;
                tmp=thread;
                thread=null;
                closeSocket(); // this will cause the thread to break out of its loop
                tmp.interrupt();
                tmp=null;
            }
            thread=null;
        }


        public void run() {
            DatagramPacket  packet;
            byte            receive_buf[]=new byte[65535];
            int             len;
            byte[]          data, tmp;

            // moved out of loop to avoid excessive object creations (bela March 8 2001)
            packet=new DatagramPacket(receive_buf, receive_buf.length);

            while(running && thread != null && sock != null) {
                try {
                    packet.setData(receive_buf, 0, receive_buf.length);
                    sock.receive(packet);
                    len=packet.getLength();
                    data=packet.getData();
                    if(len == 1 && data[0] == 0) {
                        if(log.isTraceEnabled()) log.trace("received dummy packet");
                        continue;
                    }
                    if(log.isTraceEnabled())
                        log.trace("received (ucast) " + len + " bytes from " +
                                packet.getAddress() + ':' + packet.getPort());
                    if(len > receive_buf.length) {
                        if(log.isErrorEnabled()) log.error("size of the received packet (" + len + ") is bigger than " +
                                                               "allocated buffer (" + receive_buf.length + "): will not be able to handle packet. " +
                                                               "Use the FRAG protocol and make its frag_size lower than " + receive_buf.length);
                    }

                    if(Version.compareTo(data) == false) {
                        if(log.isWarnEnabled()) log.warn("packet from " + packet.getAddress() + ':' + packet.getPort() +
                                   " has different version (" +
                                   Version.printVersionId(data, Version.version_id.length) +
                                   ") from ours (" + Version.printVersionId(Version.version_id) +
                                   "). This may cause problems");
                    }

                    if(use_incoming_packet_handler) {
                        tmp=new byte[len];
                        System.arraycopy(data, 0, tmp, 0, len);
                        incoming_queue.add(tmp);
                    }
                    else
                        handleIncomingUdpPacket(data);
                }
                catch(SocketException sock_ex) {

                        if(log.isDebugEnabled()) log.debug("unicast receiver socket is closed, exception=" + sock_ex);
                    break;
                }
                catch(InterruptedIOException io_ex) { // thread was interrupted
                    ; // go back to top of loop, where we will terminate loop
                }
                catch(Throwable ex) {
                    if(log.isErrorEnabled()) log.error("[" + local_addr + "] exception=" + ex +
                                                           ", stack trace=" + Util.printStackTrace(ex));
                    Util.sleep(300); // so we don't get into 100% cpu spinning (should NEVER happen !)
                }
            }
             if(log.isDebugEnabled()) log.debug("unicast receiver thread terminated");
        }
    }


    /**
     * This thread fetches byte buffers from the packet_queue, converts them into messages and passes them up
     * to the higher layer (done in handleIncomingUdpPacket()).
     */
    class IncomingPacketHandler implements Runnable {
        Thread t=null;

        public void run() {
            byte[] data;
            while(incoming_queue != null && incoming_packet_handler != null) {
                try {
                    data=(byte[])incoming_queue.remove();
                }
                catch(QueueClosedException closed_ex) {
                     if(log.isDebugEnabled()) log.debug("packet_handler thread terminating");
                    break;
                }
                handleIncomingUdpPacket(data);
                data=null; // let's give the poor garbage collector a hand...
            }
        }

        void start() {
            if(t == null) {
                t=new Thread(this, "UDP.IncomingPacketHandler thread");
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
        ObjectOutputStream out;
        byte[]             buf;
        DatagramPacket     packet;
        IpAddress          dest;

        public void run() {
            Message msg;

            while(outgoing_queue != null && outgoing_packet_handler != null) {
                try {
                    msg=(Message)outgoing_queue.remove();
                    handleMessage(msg);
                }
                catch(QueueClosedException closed_ex) {
                     if(log.isDebugEnabled()) log.debug("packet_handler thread terminating");
                    break;
                }
                catch(Throwable th) {
                    if(log.isErrorEnabled()) log.error("exception sending packet: " + Util.printStackTrace(th));
                }
                msg=null; // let's give the poor garbage collector a hand...
            }
        }

        protected void handleMessage(Message msg) throws Exception {
            send(msg);
        }


        void start() {
            if(t == null) {
                t=new Thread(this, "UDP.OutgoingPacketHandler thread");
                t.setDaemon(true);
                t.start();
            }
        }

        void stop() {
            if(outgoing_queue != null)
                outgoing_queue.close(false); // should terminate the packet_handler thread too
            t=null;
            outgoing_queue=null;
        }
    }




    /**
     * Bundles smaller messages into bigger ones. Collects messages in a list until
     * messages of a total of <tt>max_bundle_size bytes</tt> have accumulated, or until
     * <tt>max_bundle_timeout</tt> milliseconds have elapsed, whichever is first. Messages
     * are unbundled at the receiver.
     */
    class BundlingOutgoingPacketHandler extends OutgoingPacketHandler {
        long          total_bytes=0;
        boolean       timer_running=false;

        /** HashMap<Address, List>. Keys are destinations, values are lists of Messages */
        HashMap       msgs=new HashMap(11);

        MyTask        task=new MyTask();


        class MyTask implements TimeScheduler.Task {

            public boolean cancelled() {
                return !timer_running;
            }

            public long nextInterval() {
                return max_bundle_timeout;
            }

            public void run() {
                if(timer_running) {
                    bundleAndSend();
                }
            }
        }


        void startTimer() {
            if(!timer_running) {
                timer_running=true;
                timer.add(task);
            }
        }

        void stopTimer() {
            timer_running=false;
        }


        void stop() {
            stopTimer();
            super.stop();
        }

        protected void handleMessage(Message msg) throws Exception {
            Address        dest=msg.getDest();
            long           len;
            List           tmp;

            len=msg.size(); // todo: use msg.getLength() instead of msg.getSize()
            if(len > max_bundle_size)
                throw new Exception("UDP.BundlingOutgoingPacketHandler.handleMessage(): " +
                        "message size (" + len + ") is greater than UDP fragmentation size. " +
                        "Set the fragmentation/bundle size in FRAG and UDP correctly");

            if(total_bytes + len >= max_bundle_size) {
                if(log.isTraceEnabled()) log.trace("sending " + total_bytes + " bytes");
                bundleAndSend(); // send all pending message and clear table
                total_bytes=0;
            }

            synchronized(msgs) {
                tmp=(List)msgs.get(dest);
                if(tmp == null) {
                    tmp=new List();
                    msgs.put(dest, tmp);
                }
                tmp.add(msg);
                total_bytes+=len;
            }

            if(!timer_running) // first message to be bundled
                startTimer();
        }


        void bundleAndSend() {
            Map.Entry            entry;
            IpAddress            dest;
            ObjectOutputStream   out;
            InetAddress          addr;
            int                  port;
            byte[]               data;
            List                 l;

            if(log.isDebugEnabled()) log.debug("\nsending msgs:\n" + dumpMessages(msgs));
            synchronized(msgs) {
                stopTimer();

                if(msgs.size() == 0)
                    return;

                for(Iterator it=msgs.entrySet().iterator(); it.hasNext();) {
                    entry=(Map.Entry)it.next();
                    dest=(IpAddress)entry.getKey();
                    addr=dest.getIpAddress();
                    port=dest.getPort();
                    l=(List)entry.getValue();
                    try {
                        out_stream.reset();
                        // BufferedOutputStream bos=new BufferedOutputStream(out_stream);
                        out_stream.write(Version.version_id, 0, Version.version_id.length); // write the version
                        //bos.write(Version.version_id, 0, Version.version_id.length); // write the version
                        out=new ObjectOutputStream(out_stream);
                        // out=new ObjectOutputStream(bos);
                        l.writeExternal(out);
                        out.close(); // needed if out buffers its output to out_stream
                        data=out_stream.toByteArray();
                        doSend(data, addr, port);
                    }
                    catch(IOException e) {
                        if(log.isErrorEnabled()) log.error("exception sending msg (to dest=" + dest + "): " + e);
                    }
                }
                msgs.clear();
            }
        }
    }

    String dumpMessages(HashMap map) {
        StringBuffer sb=new StringBuffer();
        Map.Entry    entry;
        List         l;
        if(map != null) {
            synchronized(map) {
                for(Iterator it=map.entrySet().iterator(); it.hasNext();) {
                    entry=(Map.Entry)it.next();
                    l=(List)entry.getValue();
                    sb.append(entry.getKey()).append(": ");
                    sb.append(l.size()).append(" msgs\n");
                }
            }
        }
        return sb.toString();
    }


//    class MessageList {
//        long  total_size=0;
//        List  l=new List();
//
//        void add(Message msg, int len) {
//            total_size+=len;
//            l.add(msg);
//        }
//
//        public void removeAll() {
//            l.removeAll();
//            total_size=0;
//        }
//
//        public long getTotalSize() {
//            return total_size;
//        }
//
//        public void setTotalSize(long s) {
//            total_size=s;
//        }
//
//        List getList() {
//            return l;
//        }
//
//        public int size() {
//            return l.size();
//        }
//
//        public String toString() {
//            return super.toString() + " [total size=" + total_size + "bytes]";
//        }
//    }
//

}
