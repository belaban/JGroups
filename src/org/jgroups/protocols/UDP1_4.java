package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.log.Trace;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Queue;
import org.jgroups.util.QueueClosedException;
import org.jgroups.util.Util;

import java.io.*;
import java.net.*;
import java.util.*;

/**
 * Multicast transport. Similar to UDP, but binds to multiple (or all) interfaces for sending and receiving
 * multicast and unicast traffic.<br/>
 * The list of interfaces can be set via a property (comma-delimited list of IP addresses or "all" for all
 * interfaces). Note that this class only works under JDK 1.4 and higher.<br/>
 * For each of the interfaces listed we create a Listener, which listens on the group multicast address and creates
 * a unicast datagram socket. The address of this member is determined at startup time, and is the host name plus
 * a timestamp (LogicalAddress). It does not change during the lifetime of the process. The LogicalAddress contains
 * a list of all unicast socket addresses to which we can send back unicast messages. When we send a message, the
 * Listener adds the sender's return address. When we receive a message, we add that address to our routing cache, which
 * contains logical addresses and physical addresses. When we need to send a unicast address, we first check whether
 * the logical address has a physical address associated with it in the cache. If so, we send a message to that address.
 * If not, we send the unicast message to <em>all</em> physical addresses contained in the LogicalAddress.<br/>
 * UDP1_4 guarantees that - in scenarios with multiple subnets and multi-homed machines - members do see each other.
 * There is some overhead in multicasting the same message on multiple interfaces, and potentially sending a unicast
 * on multiple interfaces as well, but the advantage is that we don't need stuff like bind_addr any longer. Plus,
 * the unicast routing caches should ensure that unicasts are only sent via 1 interface in almost all cases.
 * 
 * @author Bela Ban Oct 2003
 * @version $Id: UDP1_4.java,v 1.3 2003/12/19 18:29:38 belaban Exp $
 */
public class UDP1_4 extends Protocol implements Runnable {

    final String name="UDP1_4";

    
    /** Socket used for <em>sending</em> unicast datagram packets */
    // DatagramSocket  send_sock=null;

    /**
     * Socket used for <em>receiving</em> unicast datagram packets. The address of
     * this socket will be our local address (<tt>local_addr</tt>)
     */
    DatagramSocket ucast_sock=null;

    /** IP multicast socket for <em>sending</em> and <em>receiving</em> multicast packets */
    MulticastSocket mcast_sock=null;

    /** The address (host and port) of this member */
    IpAddress local_addr=null;

    /** The name of the group to which this member is connected */
    String group_addr=null;

    /** The multicast address (mcast address and port) this member uses */
    IpAddress mcast_addr=null;

    /** The interface (NIC) to which the unicast and multicast sockets bind */
    InetAddress bind_addr=null;

    /**
     * The port to which the unicast receiver socket binds.
     * 0 means to bind to any (ephemeral) port
     */
    int bind_port=0;
    int port_range=1; // 27-6-2003 bgooren, Only try one port by default

    /** The multicast address used for sending and receiving packets */
    String mcast_addr_name="228.8.8.8";

    /** The multicast port used for sending and receiving packets */
    int mcast_port=7600;

    /** The multicast receiver thread */
    Thread mcast_receiver=null;

    /** The unicast receiver thread */
    UcastReceiver ucast_receiver=null;

    /**
     * Whether to enable IP multicasting. If false, multiple unicast datagram
     * packets are sent rather than one multicast packet
     */
    boolean ip_mcast=true;

    /** The time-to-live (TTL) for multicast datagram packets */
    int ip_ttl=64;

    /** The members of this group (updated when a member joins or leaves) */
    Vector members=new Vector();

    /** Pre-allocated byte stream. Used for serializing datagram packets */
    ByteArrayOutputStream out_stream=new ByteArrayOutputStream(65535);

    /**
     * Header to be added to all messages sent via this protocol. It is
     * preallocated for efficiency
     */
    UdpHeader udp_hdr=null;

    /** Send buffer size of the multicast datagram socket */
    int mcast_send_buf_size=32000;

    /** Receive buffer size of the multicast datagram socket */
    int mcast_recv_buf_size=64000;

    /** Send buffer size of the unicast datagram socket */
    int ucast_send_buf_size=32000;

    /** Receive buffer size of the unicast datagram socket */
    int ucast_recv_buf_size=64000;

    /**
     * If true, messages sent to self are treated specially: unicast messages are
     * looped back immediately, multicast messages get a local copy first and -
     * when the real copy arrives - it will be discarded. Useful for Window
     * media (non)sense
     */
    boolean loopback=true;

    /**
     * Sometimes receivers are overloaded (they have to handle de-serialization etc).
     * Packet handler is a separate thread taking care of de-serialization, receiver
     * thread(s) simply put packet in queue and return immediately. Setting this to
     * true adds one more thread
     */
    boolean use_packet_handler=false;

    /** Used by packet handler to store incoming DatagramPackets */
    Queue packet_queue=null;

    /**
     * If set it will be added to <tt>local_addr</tt>. Used to implement
     * for example transport independent addresses
     */
    byte[] additional_data=null;

    /**
     * Dequeues DatagramPackets from packet_queue, unmarshalls them and
     * calls <tt>handleIncomingUdpPacket()</tt>
     */
    PacketHandler packet_handler=null;


    final int VERSION_LENGTH=Version.getLength();




    /**
     * public constructor. creates the UDP protocol, and initializes the
     * state variables, does however not start any sockets or threads
     */
    public UDP1_4() {
    }

    /**
     * debug only
     */
    public String toString() {
        return "Protocol UDP(local address: " + local_addr + ")";
    }




    /* ----------------------- Receiving of MCAST UDP packets ------------------------ */

    public void run() {
        DatagramPacket packet;
        byte receive_buf[]=new byte[65000];
        int len;
        byte[] tmp1, tmp2;

        // moved out of loop to avoid excessive object creations (bela March 8 2001)
        packet=new DatagramPacket(receive_buf, receive_buf.length);

        while(mcast_receiver != null && mcast_sock != null) {
            try {
                packet.setData(receive_buf, 0, receive_buf.length);
                mcast_sock.receive(packet);
                len=packet.getLength();
                if(len == 1 && packet.getData()[0] == 0) {
                    if(Trace.debug) Trace.info("UDP.run()", "received dummy packet");
                    continue;
                }

                if(len == 4) {  // received a diagnostics probe
                    byte[] tmp=packet.getData();
                    if(tmp[0] == 'd' && tmp[1] == 'i' && tmp[2] == 'a' && tmp[3] == 'g') {
                        handleDiagnosticProbe(packet.getAddress(), packet.getPort());
                        continue;
                    }
                }

                if(Trace.debug)
                    Trace.info("UDP.receive()", "received (mcast) " + packet.getLength() + " bytes from " +
                            packet.getAddress() + ":" + packet.getPort() + " (size=" + len + " bytes)");
                if(len > receive_buf.length) {
                    Trace.error("UDP.run()", "size of the received packet (" + len + ") is bigger than " +
                            "allocated buffer (" + receive_buf.length + "): will not be able to handle packet. " +
                            "Use the FRAG protocol and make its frag_size lower than " + receive_buf.length);
                }

                if(Version.compareTo(packet.getData()) == false) {
                    Trace.warn("UDP.run()",
                            "packet from " + packet.getAddress() + ":" + packet.getPort() +
                            " has different version (" +
                            Version.printVersionId(packet.getData(), Version.version_id.length) +
                            ") from ours (" + Version.printVersionId(Version.version_id) +
                            "). This may cause problems");
                }

                if(use_packet_handler) {
                    tmp1=packet.getData();
                    tmp2=new byte[len];
                    System.arraycopy(tmp1, 0, tmp2, 0, len);
                    packet_queue.add(tmp2);
                } else
                    handleIncomingUdpPacket(packet.getData());
            } catch(SocketException sock_ex) {
                if(Trace.trace) Trace.info("UDP.run()", "multicast socket is closed, exception=" + sock_ex);
                break;
            } catch(InterruptedIOException io_ex) { // thread was interrupted
                ; // go back to top of loop, where we will terminate loop
            } catch(Throwable ex) {
                Trace.error("UDP.run()", "exception=" + ex + ", stack trace=" + Util.printStackTrace(ex));
                Util.sleep(1000); // so we don't get into 100% cpu spinning (should NEVER happen !)
            }
        }
        if(Trace.trace) Trace.info("UDP.run()", "multicast thread terminated");
    }

    void handleDiagnosticProbe(InetAddress sender, int port) {
        try {
            byte[] diag_rsp=getDiagResponse().getBytes();
            DatagramPacket rsp=new DatagramPacket(diag_rsp, 0, diag_rsp.length, sender, port);
            if(Trace.trace)
                Trace.info("UDP.handleDiagnosticProbe()", "sending diag response to " + sender + ":" + port);
            ucast_sock.send(rsp);
        } catch(Throwable t) {
            Trace.error("UDP.handleDiagnosticProbe()", "failed sending diag rsp to " + sender + ":" + port +
                    ", exception=" + t);
        }
    }

    String getDiagResponse() {
        StringBuffer sb=new StringBuffer();
        sb.append(local_addr).append(" (").append(group_addr).append(")");
        sb.append(" [").append(mcast_addr_name).append(":").append(mcast_port).append("]\n");
        sb.append("Version=").append(Version.version).append(", cvs=\"").append(Version.cvs).append("\"\n");
        sb.append("bound to ").append(bind_addr).append(":").append(bind_port).append("\n");
        sb.append("members: ").append(members).append("\n");

        return sb.toString();
    }

    /* ------------------------------------------------------------------------------- */



    /*------------------------------ Protocol interface ------------------------------ */

    public String getName() {
        return name;
    }


    public void init() throws Exception {
        if(use_packet_handler) {
            packet_queue=new Queue();
            packet_handler=new PacketHandler();
        }
    }


    /**
     * Creates the unicast and multicast sockets and starts the unicast and multicast receiver threads
     */
    public void start() throws Exception {
        if(Trace.trace) Trace.info("UDP.start()", "creating sockets and starting threads");
        createSockets();
        passUp(new Event(Event.SET_LOCAL_ADDRESS, local_addr));
        startThreads();
    }


    public void stop() {
        if(Trace.trace) Trace.info("UDP.stop()", "closing sockets and stopping threads");
        stopThreads();  // will close sockets, closeSockets() is not really needed anymore, but...
        closeSockets(); // ... we'll leave it in there for now (doesn't do anything if already closed)
    }


    /**
     * Setup the Protocol instance acording to the configuration string
     * The following properties are being read by the UDP protocol
     * param mcast_addr - the multicast address to use default is 224.0.0.200
     * param mcast_port - (int) the port that the multicast is sent on default is 7500
     * param ip_mcast - (boolean) flag whether to use IP multicast - default is true
     * param ip_ttl - Set the default time-to-live for multicast packets sent out on this socket. default is 32
     * 
     * @return true if no other properties are left.
     *         false if the properties still have data in them, ie ,
     *         properties are left over and not handled by the protocol stack
     */
    public boolean setProperties(Properties props) {
        String str, tmp;

        tmp=System.getProperty("UDP.bind_addr");
        if(tmp != null)
            str=tmp;
        else
            str=props.getProperty("bind_addr");
        if(str != null) {
            try {
                bind_addr=InetAddress.getByName(str);
            } catch(UnknownHostException unknown) {
                Trace.fatal("UDP.setProperties()", "(bind_addr): host " + str + " not known");
                return false;
            }
            props.remove("bind_addr");
        }

        str=props.getProperty("bind_port");
        if(str != null) {
            bind_port=new Integer(str).intValue();
            props.remove("bind_port");
        }

        str=props.getProperty("start_port");
        if(str != null) {
            bind_port=new Integer(str).intValue();
            props.remove("start_port");
        }

        str=props.getProperty("port_range");
        if(str != null) {
            port_range=new Integer(str).intValue();
            props.remove("port_range");
        }

        str=props.getProperty("mcast_addr");
        if(str != null) {
            mcast_addr_name=new String(str);
            props.remove("mcast_addr");
        }

        str=props.getProperty("mcast_port");
        if(str != null) {
            mcast_port=new Integer(str).intValue();
            props.remove("mcast_port");
        }

        str=props.getProperty("ip_mcast");
        if(str != null) {
            ip_mcast=new Boolean(str).booleanValue();
            props.remove("ip_mcast");
        }

        str=props.getProperty("ip_ttl");
        if(str != null) {
            ip_ttl=new Integer(str).intValue();
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
            loopback=new Boolean(str).booleanValue();
            props.remove("loopback");
        }

        str=props.getProperty("use_packet_handler");
        if(str != null) {
            use_packet_handler=new Boolean(str).booleanValue();
            props.remove("use_packet_handler");
        }

        if(props.size() > 0) {
            System.err.println("UDP.setProperties(): the following properties are not recognized:");
            props.list(System.out);
            return false;
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
     * 
     * @param evt - the event being send from the stack
     */
    public void up(Event evt) {
        passUp(evt);

        switch(evt.getType()) {

            case Event.CONFIG:
                passUp(evt);
                if(Trace.trace) Trace.info("UDP.up()", "received CONFIG event: " + evt.getArg());
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

        msg=(Message)evt.getArg();

        if(udp_hdr != null && udp_hdr.group_addr != null) {
            // added patch by Roland Kurmann (March 20 2003)
            msg.putHeader(name, udp_hdr);
        }

        dest_addr=msg.getDest();

        // Because we don't call Protocol.passDown(), we notify the observer directly (e.g. PerfObserver).
        // This way, we still have performance numbers for UDP
        if(observer != null)
            observer.passDown(evt);

        if(dest_addr == null) { // 'null' means send to all group members
            if(ip_mcast) {
                if(mcast_addr == null) {
                    Trace.error("UDP.down()", "dest address of message is null, and " +
                            "sending to default address fails as mcast_addr is null, too !" +
                            " Discarding message " + Util.printEvent(evt));
                    return;
                }
                //if we want to use IP multicast, then set the destination of the message
                msg.setDest(mcast_addr);
            } else {
                //sends a separate UDP message to each address
                sendMultipleUdpMessages(msg, members);
                return;
            }
        }

        try {
            sendUdpMessage(msg);
        } catch(Exception e) {
            Trace.error("UDP.down()", "exception=" + e + ", msg=" + msg + ", mcast_addr=" + mcast_addr);
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
        ObjectInputStream inp;
        Message msg=null;
        UdpHeader hdr=null;
        Event evt;

        try {
            // skip the first n bytes (default: 4), this is the version info
            inp_stream=new ByteArrayInputStream(data, VERSION_LENGTH, data.length - VERSION_LENGTH);
            inp=new ObjectInputStream(inp_stream);
            msg=new Message();
            msg.readExternal(inp);

            // discard my own multicast loopback copy
            if(loopback) {
                Address dst=msg.getDest();
                Address src=msg.getSrc();

                if(dst != null && dst.isMulticastAddress() && src != null && local_addr.equals(src)) {
                    if(Trace.debug)
                        Trace.info("UDP.handleIncomingUdpPacket()", "discarded own loopback multicast packet");
                    return;
                }
            }

            evt=new Event(Event.MSG, msg);
            if(Trace.debug)
                Trace.info("UDP.handleIncomingUdpPacket()", "Message is " + msg + ", headers are " +
                        msg.getHeaders());

            /* Because Protocol.up() is never called by this bottommost layer, we call up() directly in the observer.
             * This allows e.g. PerfObserver to get the time of reception of a message */
            if(observer != null)
                observer.up(evt, up_queue.size());

            hdr=(UdpHeader)msg.removeHeader(name);
        } catch(Throwable e) {
            Trace.error("UDP.handleIncomingUdpPacket()", "exception=" + Trace.getStackTrace(e));
            return;
        }

        if(hdr != null) {

            /* Discard all messages destined for a channel with a different name */
            String ch_name=null;

            if(hdr.group_addr != null)
                ch_name=hdr.group_addr;

            // Discard if message's group name is not the same as our group name unless the
            // message is a diagnosis message (special group name DIAG_GROUP)
            if(ch_name != null && group_addr != null && !group_addr.equals(ch_name) &&
                    !ch_name.equals(Util.DIAG_GROUP)) {
                if(Trace.trace)
                    Trace.warn("UDP.handleIncomingUdpPacket()", "discarded message from different group (" +
                            ch_name + "). Sender was " + msg.getSrc());
                return;
            }
        }

        passUp(evt);
    }


    /**
     * Send a message to the address specified in dest
     */
    void sendUdpMessage(Message msg) throws Exception {
        IpAddress dest;
        ObjectOutputStream out;
        byte buf[];
        DatagramPacket packet;
        Message copy;
        Event evt;

        dest=(IpAddress)msg.getDest();  // guaranteed not to be null
        setSourceAddress(msg);

        if(Trace.debug)
            Trace.debug("UDP.sendUdpMessage()",
                    "sending message to " + msg.getDest() +
                    " (src=" + msg.getSrc() + "), headers are " + msg.getHeaders());

        // Don't send if destination is local address. Instead, switch dst and src and put in up_queue.
        // If multicast message, loopback a copy directly to us (but still multicast). Once we receive this,
        // we will discard our own multicast message
        if(loopback && (dest.equals(local_addr) || dest.isMulticastAddress())) {
            copy=msg.copy();
            copy.removeHeader(name);
            copy.setSrc(local_addr);
            copy.setDest(dest);
            evt=new Event(Event.MSG, copy);

            /* Because Protocol.up() is never called by this bottommost layer, we call up() directly in the observer.
               This allows e.g. PerfObserver to get the time of reception of a message */
            if(observer != null)
                observer.up(evt, up_queue.size());
            if(Trace.debug) Trace.info("UDP.sendUdpMessage()", "looped back local message " + copy);
            passUp(evt);
            if(!dest.isMulticastAddress())
                return;
        }

        out_stream.reset();
        out_stream.write(Version.version_id, 0, Version.version_id.length); // write the version
        out=new ObjectOutputStream(out_stream);
        msg.writeExternal(out);
        out.flush(); // needed if out buffers its output to out_stream
        buf=out_stream.toByteArray();
        packet=new DatagramPacket(buf, buf.length, dest.getIpAddress(), dest.getPort());

        if(ucast_sock != null) {
            try {
                ucast_sock.send(packet);
            } catch(Throwable e) {
                Trace.error("UDP.sendUdpMessage()", "exception sending ucast message: " + e);
            }
        } else {
            Trace.error("UDP.sendUdpMessage()", "(unicast) ucast_sock is null. Message is " + msg +
                    ", headers are " + msg.getHeaders());
        }
    }


    void sendMultipleUdpMessages(Message msg, Vector dests) {
        Address dest;

        for(int i=0; i < dests.size(); i++) {
            dest=(Address)dests.elementAt(i);
            msg.setDest(dest);

            try {
                sendUdpMessage(msg);
            } catch(Exception e) {
                Trace.debug("UDP.sendMultipleUdpMessages()", "exception=" + e);
            }
        }
    }


    /**
     * Create UDP sender and receiver sockets. Currently there are 2 sockets
     * (sending and receiving). This is due to Linux's non-BSD compatibility
     * in the JDK port (see DESIGN).
     */
    void createSockets() throws Exception {
        InetAddress tmp_addr=null;

        // bind_addr not set, try to assign one by default. This is needed on Windows
        // @todo: replace this code in JDK 1.4 with java.net.NetworkInterface API

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

        if(bind_addr != null && Trace.trace)
            Trace.info("UDP.createSockets()", "unicast sockets will use interface " +
                    bind_addr.getHostAddress());


        // 1. Create socket for sending unicast UDP packets (will use bind_addr
        // and bind_port if defined)

        // changed by bela March 27 2003; outgoing socket gets the bind address
        // dynamically from the routing table
        //send_sock=new DatagramSocket(bind_port, bind_addr);
        // Port needs to be chosen dynamically, otherwise if bind_port was used, we would
        // conflict with ucast_recv_sock - thanks to Bas Gooren for sending a patch
        //send_sock=new DatagramSocket(0, bind_addr);
        // send_sock=new DatagramSocket();


        // 2. Create socket for receiving unicast UDP packets. The address and port
        //    of this socket will be our local address (local_addr)

        // 27-6-2003 bgooren, find available port in range (start_port, start_port+port_range)
        int rcv_port=bind_port, max_port=bind_port + port_range;
        while(rcv_port <= max_port) {

            try {
                ucast_sock=new DatagramSocket(rcv_port, bind_addr);
                break;
            } catch(SocketException bind_ex) {	// Cannot listen on this port
                rcv_port++;
            } catch(SecurityException sec_ex) { // Not allowed to list on this port
                rcv_port++;
            }

            // Cannot listen at all, throw an Exception
            if(rcv_port == max_port + 1) { // +1 due to the increment above
                throw new Exception("UDP.createSockets(): cannot list on any port in range " +
                        bind_port + "-" + (bind_port + port_range));
            }
        }
        //ucast_recv_sock=new DatagramSocket(bind_port, bind_addr);
        if(ucast_sock == null)
            throw new Exception("UDP.createSocket(): ucast_recv_sock is null");

        local_addr=new IpAddress(ucast_sock.getLocalAddress(), ucast_sock.getLocalPort());
        if(additional_data != null)
            local_addr.setAdditionalData(additional_data);


        // 3. Create socket for receiving IP multicast packets
        if(ip_mcast) {
            mcast_sock=new MulticastSocket(mcast_port);
            mcast_sock.setTimeToLive(ip_ttl);
            if(bind_addr != null)
                mcast_sock.setInterface(bind_addr);
            tmp_addr=InetAddress.getByName(mcast_addr_name);
            mcast_addr=new IpAddress(tmp_addr, mcast_port);
            mcast_sock.joinGroup(tmp_addr);
        }

        setBufferSizes();

        if(Trace.trace)
            Trace.info("UDP.createSockets()", "socket information:\n" + dumpSocketInfo());
    }


    String dumpSocketInfo() throws Exception {
        StringBuffer sb=new StringBuffer();
        sb.append("local_addr=").append(local_addr);
        sb.append(", mcast_addr=").append(mcast_addr);
        sb.append(", bind_addr=").append(bind_addr);
        sb.append(", ttl=").append(ip_ttl);

        if(ucast_sock != null) {
            sb.append("\nreceive socket: bound to ");
            sb.append(ucast_sock.getLocalAddress().getHostAddress()).append(":").append(ucast_sock.getLocalPort());
            sb.append(", receive buffer size=").append(ucast_sock.getReceiveBufferSize());
        }

        if(mcast_sock != null) {
            sb.append("\nmulticast socket: bound to ");
            sb.append(mcast_sock.getInterface().getHostAddress()).append(":").append(mcast_sock.getLocalPort());
            sb.append(", send buffer size=").append(mcast_sock.getSendBufferSize());
            sb.append(", receive buffer size=").append(mcast_sock.getReceiveBufferSize());
        }
        return sb.toString();
    }


    void setBufferSizes() {
        if(ucast_sock != null) {
            try {
                ucast_sock.setReceiveBufferSize(ucast_recv_buf_size);
            } catch(Throwable ex) {
                Trace.warn("UDP.setBufferSizes()", "failed setting ucast_recv_buf_size in ucast_recv_sock: " + ex);
            }
        }

        if(mcast_sock != null) {
            try {
                mcast_sock.setSendBufferSize(mcast_send_buf_size);
            } catch(Throwable ex) {
                Trace.warn("UDP.setBufferSizes()", "failed setting mcast_send_buf_size in mcast_sock: " + ex);
            }

            try {
                mcast_sock.setReceiveBufferSize(mcast_recv_buf_size);
            } catch(Throwable ex) {
                Trace.warn("UDP.setBufferSizes()", "failed setting mcast_recv_buf_size in mcast_sock: " + ex);
            }
        }
    }


    /**
     * Closed UDP unicast and multicast sockets
     */
    void closeSockets() {
        // 1. Close multicast socket
        closeMulticastSocket();

        // 2. Close unicast receiver socket
        closeUnicastReceiverSocket();

        // 3. Close unicast sender socket
        // closeUnicastSenderSocket();
    }


    void closeMulticastSocket() {
        if(mcast_sock != null) {
            try {
                if(mcast_addr != null) {
                    // by sending a dummy packet the thread will be awakened
                    sendDummyPacket(mcast_addr.getIpAddress(), mcast_addr.getPort());
                    Util.sleep(300);
                    mcast_sock.leaveGroup(mcast_addr.getIpAddress());
                }
                mcast_sock.close(); // this will cause the mcast receiver thread to break out of its loop
                mcast_sock=null;
                if(Trace.trace) Trace.info("UDP.closeMulticastSocket()", "multicast socket closed");
            } catch(IOException ex) {
            }
            mcast_addr=null;
        }
    }


    void closeUnicastReceiverSocket() {
        if(ucast_sock != null) {
            // by sending a dummy packet, the thread will terminate (if it was flagged as stopped before)
            sendDummyPacket(ucast_sock.getLocalAddress(), ucast_sock.getLocalPort());

            ucast_sock.close();
            ucast_sock=null;
            if(Trace.trace) Trace.info("UDP.closeMulticastSocket()", "unicast receiver socket closed");
        }
    }


    /**
     * Workaround for the problem encountered in certains JDKs that a thread listening on a socket
     * cannot be interrupted. Therefore we just send a dummy datagram packet so that the thread 'wakes up'
     * and realizes it has to terminate. Should be removed when all JDKs support Thread.interrupt() on
     * reads. Uses send_sock t send dummy packet, so this socket has to be still alive.
     * 
     * @param dest The destination host. Will be local host if null
     * @param port The destination port
     */
    void sendDummyPacket(InetAddress dest, int port) {
        DatagramPacket packet;
        byte[] buf={0};

        if(dest == null) {
            try {
                dest=InetAddress.getLocalHost();
            } catch(Exception e) {
            }
        }

        if(Trace.debug) Trace.info("UDP.sendDummyPacket()", "sending packet to " + dest + ":" + port);

        if(ucast_sock == null || dest == null) {
            Trace.warn("UDP.sendDummyPacket()", "send_sock was null or dest was null, cannot send dummy packet");
            return;
        }
        packet=new DatagramPacket(buf, buf.length, dest, port);
        try {
            ucast_sock.send(packet);
        } catch(Throwable e) {
            Trace.error("UDP.sendDummyPacket()", "exception sending dummy packet to " +
                    dest + ":" + port + ": " + e);
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
            if(Trace.trace) Trace.info("UDP.startThreads()", "created unicast receiver thread");
        }

        if(ip_mcast) {
            if(mcast_receiver != null) {
                if(mcast_receiver.isAlive()) {
                    if(Trace.trace)
                        Trace.info("UDP.createThreads()",
                                "did not create new multicastreceiver thread as existing " +
                                "multicast receiver thread is still running");
                } else
                    mcast_receiver=null; // will be created just below...
            }

            if(mcast_receiver == null) {
                mcast_receiver=new Thread(this, "UDP mcast receiver");
                mcast_receiver.setPriority(Thread.MAX_PRIORITY); // needed ????
                mcast_receiver.setDaemon(true);
                mcast_receiver.start();
            }
        }
        if(use_packet_handler)
            packet_handler.start();
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
                } catch(Exception e) {
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
        if(packet_handler != null)
            packet_handler.stop();
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
                udp_hdr=new UdpHeader(group_addr);

                // removed March 18 2003 (bela), not needed (handled by GMS)
                // changed July 2 2003 (bela): we discard CONNECT_OK at the GMS level anyway, this might
                // be needed if we run without GMS though
                passUp(new Event(Event.CONNECT_OK));
                break;

            case Event.DISCONNECT:
                passUp(new Event(Event.DISCONNECT_OK));
                break;

            case Event.CONFIG:
                if(Trace.trace) Trace.info("UDP.down()", "received CONFIG event: " + evt.getArg());
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
                closeUnicastReceiverSocket(); // this will cause the thread to break out of its loop
                tmp.interrupt();
                tmp=null;
            }
            thread=null;
        }


        public void run() {
            DatagramPacket packet;
            byte receive_buf[]=new byte[65535];
            int len;
            byte[] tmp1, tmp2;

            // moved out of loop to avoid excessive object creations (bela March 8 2001)
            packet=new DatagramPacket(receive_buf, receive_buf.length);

            while(running && thread != null && ucast_sock != null) {
                try {
                    packet.setData(receive_buf, 0, receive_buf.length);
                    ucast_sock.receive(packet);
                    len=packet.getLength();
                    if(len == 1 && packet.getData()[0] == 0) {
                        if(Trace.debug) Trace.info("UDP.UcastReceiver.run()", "received dummy packet");
                        continue;
                    }
                    if(Trace.debug)
                        Trace.info("UDP.UcastReceiver.run()", "received (ucast) " + packet.getLength() + " bytes from " +
                                packet.getAddress() + ":" + packet.getPort() + " (size=" + packet.getLength() +
                                " bytes)");
                    if(len > receive_buf.length) {
                        Trace.error("UDP.UcastReceiver.run()", "size of the received packet (" + len + ") is bigger than " +
                                "allocated buffer (" + receive_buf.length + "): will not be able to handle packet. " +
                                "Use the FRAG protocol and make its frag_size lower than " + receive_buf.length);
                    }

                    if(Version.compareTo(packet.getData()) == false) {
                        Trace.warn("UDP.UcastReceiver.run()",
                                "packet from " + packet.getAddress() + ":" + packet.getPort() +
                                " has different version (" +
                                Version.printVersionId(packet.getData(), Version.version_id.length) +
                                ") from ours (" + Version.printVersionId(Version.version_id) +
                                "). This may cause problems");
                    }

                    if(use_packet_handler) {
                        tmp1=packet.getData();
                        tmp2=new byte[len];
                        System.arraycopy(tmp1, 0, tmp2, 0, len);
                        packet_queue.add(tmp2);
                    } else
                        handleIncomingUdpPacket(packet.getData());
                } catch(SocketException sock_ex) {
                    if(Trace.trace)
                        Trace.info("UDP.UcastReceiver.run()",
                                "unicast receiver socket is closed, exception=" + sock_ex);
                    break;
                } catch(InterruptedIOException io_ex) { // thread was interrupted
                    ; // go back to top of loop, where we will terminate loop
                } catch(Throwable ex) {
                    Trace.error("UDP.UcastReceiver.run()", "[" + local_addr + "] exception=" + ex +
                            ", stack trace=" + Util.printStackTrace(ex));
                    Util.sleep(1000); // so we don't get into 100% cpu spinning (should NEVER happen !)
                }
            }
            if(Trace.trace) Trace.info("UDP.UcastReceiver.run()", "unicast receiver thread terminated");
        }
    }


    /**
     * This thread fetches byte buffers from the packet_queue, converts them into messages and passes them up
     * to the higher layer (done in handleIncomingUdpPacket()).
     */
    class PacketHandler implements Runnable {
        Thread t=null;

        public void run() {
            byte[] data;
            while(packet_queue != null && packet_handler != null) {
                try {
                    data=(byte[])packet_queue.remove();
                } catch(QueueClosedException closed_ex) {
                    if(Trace.trace) Trace.info("UDP.PacketHandler.run()", "packet_handler thread terminating");
                    break;
                }
                handleIncomingUdpPacket(data);
                data=null; // let's give the poor garbage collector a hand...
            }
        }

        void start() {
            if(t == null) {
                t=new Thread(this, "UDP.PacketHandler thread");
                t.setDaemon(true);
                t.start();
            }
        }

        void stop() {
            if(packet_queue != null)
                packet_queue.close(false); // should terminate the packet_handler thread too
            t=null;
            packet_queue=null;
        }
    }

    /**
     * Manages a multicast and unicast socket on a given interface (NIC). The multicast socket is used
     * to listen for incoming multicast packets, the unicast socket is used to (1) listen for incoming
     * unicast packets, (2) to send unicast packets and (3) to send multicast packets
     */
    public static class Connector implements Runnable {

        Thread t=null;

        /** Interface on which ucast_sock and mcast_sender_sock are created */
        NetworkInterface bind_interface;


        /** Used for sending/receiving unicast/multicast packets. The reason we have to use a MulticastSocket versus a
         * DatagramSocket is that only MulticastSockets allow to set the interface over which a multicast
         * is sent: DatagramSockets consult the routing table to find the interface
         */
        MulticastSocket mcast_sock=null;


        /** Multicast address of group, plus port. Example: 230.1.2.3:7500 */
        SocketAddress mcast_group=null;

        /** Local port of the mcast_sock */
        SocketAddress local_addr=null;

        /** Local port for the multicast socket (0 = ephemeral port) */
        int local_port=0;

        int port_range=1;

        /** The receiver which handles incoming packets */
        Receiver receiver=null;

        boolean running=false;
        
        /** Buffer for incoming packets */
        byte[] receive_buffer=null;



        public Connector(String bind_interface,
                         String mcast_group,
                         int mcast_port,
                         int local_port, 
                         int port_range,
                         Receiver receiver, 
                         int receive_buffer_size) throws IOException {
            this.bind_interface=NetworkInterface.getByInetAddress(InetAddress.getByName(bind_interface));
            if(this.bind_interface == null)
                throw new IOException("UDP1_4.Connector(): no interface found for " + bind_interface);
            this.mcast_group=new InetSocketAddress(mcast_group,  mcast_port);
            this.local_port=local_port;
            this.port_range=port_range;
            if(bind_interface == null)
                this.bind_interface=determineBindInterface();
            this.receiver=receiver;
            this.receive_buffer=new byte[receive_buffer_size];

            // mcast_sock=createMulticastSocket();
            mcast_sock=new MulticastSocket(7500);
            System.out.println("ttl=" + mcast_sock.getTimeToLive());

            // todo: remove
            // mcast_sock.setNetworkInterface(this.bind_interface); // for outgoing multicasts

            this.local_port=mcast_sock.getLocalPort();
            local_addr=new InetSocketAddress(bind_interface, this.local_port);
            System.out.println("-- local_addr=" + local_addr);
        }



        public void start() throws Exception {
            if(running) {
                Trace.info("UDP1_4.Connector.start()", "Connector thread is already running");
                return;
            }

            if(mcast_group != null) {
                // mcast_sock.joinGroup(mcast_group, bind_interface);
                mcast_sock.joinGroup(InetAddress.getByName("230.1.2.3"));
                System.out.println("joined mcast group " + mcast_group + " on interface " + bind_interface);
            }

            t=new Thread(this, "ConnectorThread for " + local_addr);
            t.setDaemon(true);
            running=true;
            t.start();
        }

        public void stop() {
            if(!running)
                return;
            if(t != null && t.isAlive()) {
                if(mcast_sock != null) {
                    if(mcast_group != null)
                        try { mcast_sock.leaveGroup(mcast_group, bind_interface); } catch(IOException e) {}
                    mcast_sock.close(); // terminates the thread
                }
            }
            t=null;
            running=false;
        }



        /** Sends a message using mcast_sock */
        public void send(DatagramPacket msg) throws Exception {
            mcast_sock.send(msg);
            System.out.println("-- sent message to " + msg.getSocketAddress() + " via interface " +
                    mcast_sock.getNetworkInterface());
        }

        public void run() {
            DatagramPacket packet=new DatagramPacket(receive_buffer, receive_buffer.length);
            int            len;
            while(running) {
                try {
                    packet.setData(receive_buffer, 0, receive_buffer.length);
                    mcast_sock.receive(packet);
                    len=packet.getLength();
                    if(len == 1 && packet.getData()[0] == 0) {
                        if(Trace.debug) Trace.info("UDP1_4.Connector.run()", "received dummy packet");
                        continue;
                    }

                    if(len == 4) {  // received a diagnostics probe
                        byte[] tmp=packet.getData();
                        if(tmp[0] == 'd' && tmp[1] == 'i' && tmp[2] == 'a' && tmp[3] == 'g') {
                            handleDiagnosticProbe(packet.getAddress(), packet.getPort());
                            continue;
                        }
                    }
                    if(receiver != null)
                        receiver.receive(packet);
                }
                catch(Throwable t) {
                    if(t == null || mcast_sock == null || mcast_sock.isClosed())
                        break;
                }
            }
            running=false;
        }

        public String toString() {
            StringBuffer sb=new StringBuffer();
            sb.append("local_addr=").append(local_addr).append(", mcast_group=");
            sb.append(mcast_group);
            return sb.toString();
        }


        void handleDiagnosticProbe(InetAddress sender, int port) {
            try {
                byte[] diag_rsp=getDiagResponse().getBytes();
                DatagramPacket rsp=new DatagramPacket(diag_rsp, 0, diag_rsp.length, sender, port);
                if(Trace.trace)
                    Trace.info("UDP.handleDiagnosticProbe()", "sending diag response to " + sender + ":" + port);
                mcast_sock.send(rsp);
            } catch(Throwable t) {
                Trace.error("UDP.handleDiagnosticProbe()", "failed sending diag rsp to " + sender + ":" + port +
                        ", exception=" + t);
            }
        }

        String getDiagResponse() {
            StringBuffer sb=new StringBuffer();
            sb.append(local_addr).append(" (").append(local_addr).append(")");
            sb.append(" [").append(mcast_group).append("]\n");
            sb.append("Version=").append(Version.version).append(", cvs=\"").append(Version.cvs).append("\"\n");
            sb.append("bound to ").append(bind_interface).append(":").append(local_port).append("\n");
    //            sb.append("members: ").append(members).append("\n");
            return sb.toString();
        }



        /**
         * Get the first non-loopback interface
         * @return An interface
         */
        private NetworkInterface determineBindInterface() {
            return null;
        }

        private MulticastSocket createMulticastSocket() throws IOException {
            MulticastSocket sock=null;
            int             tmp_port=local_port;

            int max_port=tmp_port + port_range;
            while(tmp_port <= max_port) {
                try {
                    sock=new MulticastSocket(tmp_port);
                    break;
                }
                catch(Exception bind_ex) {
                    tmp_port++;
                }
            }
            if(sock == null)
                throw new IOException("could not create a MulticastSocket (port range: " + local_port +
                        " - " + (local_port+port_range));
            return sock;
        }
    }



    /** Manages a bunch of Connectors */
    public static class ConnectorTable implements Receiver {
        Receiver receiver=null;

        /** HashMap<Address,Connector>. Keys are ucast_sock addresses, values Connectors */
        HashMap connectors=new HashMap();



        public ConnectorTable(Receiver receiver) {
            this.receiver=receiver;
        }

        public Receiver getReceiver() {
            return receiver;
        }

        public void setReceiver(Receiver receiver) {
            this.receiver=receiver;
        }


        /** Get all interfaces, create one Connector per interface and call start() on it */
        public void start() throws Exception {
            Connector tmp;
            for(Iterator it=connectors.values().iterator(); it.hasNext();) {
                tmp=(Connector)it.next();
                tmp.start();
            }
        }

        public void stop() {
            Connector tmp;
            for(Iterator it=connectors.values().iterator(); it.hasNext();) {
                tmp=(Connector)it.next();
                tmp.stop();
            }
        }


        /** Sends a packet. If the destination is a multicast address, call send() on all connectors.
         * If destination is not null, send the message using <em>any</em> Connector: if we send a unicast
         * message, it doesn't matter to which interface we are bound; the kernel will choose the correct
         * interface based on the destination and the routing table. Note that the receiver will have the
         * interface which was chosen by the kernel to send the message as the receiver's address, so the
         * correct Connector will receive a possible response.
         * @param msg
         * @throws Exception
         */
        public void send(DatagramPacket msg) throws Exception {
            InetAddress dest;
            if(msg == null)
                return;
            dest=msg.getAddress();
            if(dest == null)
                throw new IOException("UDP1_4.ConnectorTable.send(): destination address is null");

            if(dest.isMulticastAddress()) {
                // send to all Connectors
                for(Iterator it=connectors.values().iterator(); it.hasNext();) {
                    ((Connector)it.next()).send(msg);
                }
            }
            else {
                // send to a random connector
                Collection conns=connectors.values();
                Connector c=pickRandomConnector(conns);
                c.send(msg);
            }
        }

        // todo: make this more efficient (iteration is bad) ! Use array rather than HashMap for connectors
        private Connector pickRandomConnector(Collection conns) {
            int size=conns.size();
            int index=((int)(Util.random(size))) -1;
            Iterator it=conns.iterator();
            Connector ret=null;
            for(int i=0; i <= index; i++) {
                ret=(Connector)it.next();
            }
            return ret;
        }

        public void addConnector(String bind_interface, String mcast_group, int mcast_port,
                                 int local_port, int port_range, int receive_buffer_size) throws IOException {
            if(bind_interface == null)
                return;
            Connector tmp=(Connector)connectors.get(bind_interface);
            if(tmp != null) {
                Trace.warn("UDP1_4.ConnectorTable.addConnector()", "connector for interface " + bind_interface +
                        " is already present (will be overwritten): " + tmp);
            }
            tmp=new Connector(bind_interface, mcast_group, mcast_port, local_port, port_range, this, receive_buffer_size);
            connectors.put(bind_interface, tmp);
        }


        public void receive(DatagramPacket packet) {
            if(receiver != null) {
                receiver.receive(packet);
            }
        }
    }



    public static interface Receiver {

        /** Called when data has been received on a socket. When the callback returns, the buffer will be
         * reused: therefore, if <code>buf</code> must be processed on a separate thread, it needs to be copied.
         * This method might be called concurrently by multiple threads, so it has to be reentrant
         * @param packet
         */
        void receive(DatagramPacket packet);
    }


    public static class MyReceiver implements Receiver {
        ConnectorTable t=null;

        public MyReceiver() {

        }

        public void setConnectorTable(ConnectorTable t) {
            this.t=t;
        }

        public void receive(DatagramPacket packet) {
            System.out.println("-- received " + packet.getLength() + " bytes from " + packet.getSocketAddress());
            InetAddress sender=packet.getAddress();
            byte[] buf=packet.getData();
            int len=packet.getLength();
            String tmp=new String(buf, 0, len);
            if(len > 4) {
                if(tmp.startsWith("rsp:")) {
                    System.out.println("-- received respose: \"" + tmp + "\"");
                    return;
                }
            }

            byte[] rsp_buf=("rsp: this is a response to " + tmp).getBytes();
            DatagramPacket response=new DatagramPacket(rsp_buf, rsp_buf.length, sender, packet.getPort());

            try {
                t.send(response);
            }
            catch(Exception e) {
                e.printStackTrace();
                System.err.println("MyReceiver: problem sending response to " + sender);
            }
        }
    }

    static void help() {
        System.out.println("UDP1_4 [-help] [-bind_addrs <list of interfaces>]");
    }



    public static void main(String[] args) {
        MyReceiver r=new MyReceiver();
        ConnectorTable ct=new ConnectorTable(r);
        r.setConnectorTable(ct);
        String mcast_group="230.1.2.3";
        int    mcast_port=7500;
        String line;
        BufferedReader  in=null;
        DatagramPacket packet;
        byte[]   send_buf;
        int      receive_buffer_size=65000;

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-help")) {
                help();
                continue;
            }
            if(args[i].equals("-bind_addrs")) {

                while(++i < args.length && !args[i].trim().startsWith("-")) {
                    try {
                        ct.addConnector(args[i], mcast_group, mcast_port, 0, 0, receive_buffer_size);
                    }
                    catch(IOException e) {
                        e.printStackTrace();
                        return;
                    }
                }
            }
        }


        try {
            ct.start(); // starts all Connectors in turn
            in=new BufferedReader(new InputStreamReader(System.in));
            while(true) {
                System.out.print("> "); System.out.flush();
                line=in.readLine();
                if(line.startsWith("quit") || line.startsWith("exit"))
                    break;
                send_buf=line.getBytes();
                // packet=new DatagramPacket(send_buf, send_buf.length, InetAddress.getByName(mcast_group), mcast_port);
                packet=new DatagramPacket(send_buf, send_buf.length, InetAddress.getByName(mcast_group), mcast_port);
                ct.send(packet);
            }
        }
        catch(Exception e) {
            e.printStackTrace();
        }
        finally {
            if(ct != null)
                ct.stop();
        }
    }

}
