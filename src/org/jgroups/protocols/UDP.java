// $Id: UDP.java,v 1.1 2003/09/09 01:24:11 belaban Exp $

package org.jgroups.protocols;


import java.io.*;
import java.net.*;
import java.util.HashMap;
import java.util.Properties;
import java.util.Vector;

import org.jgroups.*;
import org.jgroups.util.*;
import org.jgroups.stack.*;
import org.jgroups.log.Trace;




/**
 * IP multicast transport based on UDP. Messages to the group (msg.dest == null) will
 * be multicast (to all group members), whereas point-to-point messages
 * (msg.dest != null) will be unicast to a single member. Uses a multicast and
 * a unicast socket.<p>
 * The following properties are being read by the UDP protocol<p>
 * param mcast_addr - the multicast address to use default is 224.0.0.200<br>
 * param mcast_port - (int) the port that the multicast is sent on default is 7500<br>
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
    /** Socket used for <em>sending</em> unicast datagram packets */
    DatagramSocket  send_sock=null;

    /** Socket used for <em>receiving</em> unicast datagram packets. The address of
     * this socket will be our local address (<tt>local_addr</tt>) */
    DatagramSocket  ucast_recv_sock=null;

    /** IP multicast socket for <em>sending</em> and <em>receiving</em> multicast packets */
    MulticastSocket mcast_sock=null;

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
    Vector          members=new Vector();

    /** Pre-allocated byte stream. Used for serializing datagram packets */
    ByteArrayOutputStream out_stream=new ByteArrayOutputStream(65535);

    /** Header to be added to all messages sent via this protocol. It is
     * preallocated for efficiency */
    UdpHeader       udp_hdr=null;

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
    boolean         use_packet_handler=false;

    /** Used by packet handler to store incoming DatagramPackets */
    Queue           packet_queue=null;

    /** If set it will be added to <tt>local_addr</tt>. Used to implement
     * for example transport independent addresses */
    byte[]          additional_data=null;

    /** Dequeues DatagramPackets from packet_queue, unmarshalls them and
     * calls <tt>handleIncomingUdpPacket()</tt> */
    PacketHandler   packet_handler=null;

    /** The name of this protocol */
    final String    name="UDP";


    final int VERSION_LENGTH=Version.getLength();


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
        return "Protocol UDP(local address: " + local_addr + ")";
    }




    /* ----------------------- Receiving of MCAST UDP packets ------------------------ */

    public void run() {
        DatagramPacket  packet;
        byte            receive_buf[]=new byte[65000];
        int             len;
        byte[]          tmp1, tmp2;

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
                }
                else
                    handleIncomingUdpPacket(packet.getData());
            }
            catch(SocketException sock_ex) {
                if(Trace.trace) Trace.info("UDP.run()", "multicast socket is closed, exception=" + sock_ex);
                break;
            }
            catch(InterruptedIOException io_ex) { // thread was interrupted
                ; // go back to top of loop, where we will terminate loop
            }
            catch(Throwable ex) {
                Trace.error("UDP.run()", "exception=" + ex + ", stack trace=" + Util.printStackTrace(ex));
                Util.sleep(1000); // so we don't get into 100% cpu spinning (should NEVER happen !)
            }
        }
        if(Trace.trace) Trace.info("UDP.run()", "multicast thread terminated");
    }

    void handleDiagnosticProbe(InetAddress sender, int port) {
        try {
            byte[]      diag_rsp=getDiagResponse().getBytes();
            DatagramPacket rsp=new DatagramPacket(diag_rsp, 0, diag_rsp.length, sender, port);
            if(Trace.trace)
                Trace.info("UDP.handleDiagnosticProbe()", "sending diag response to " + sender + ":" + port);
            send_sock.send(rsp);
        }
        catch(Throwable t) {
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
     * @return true if no other properties are left.
     *         false if the properties still have data in them, ie ,
     *         properties are left over and not handled by the protocol stack
     *
     */
    public boolean setProperties(Properties props) {
        String str;

        str=props.getProperty("bind_addr");
        if(str != null) {
            try {
                bind_addr=InetAddress.getByName(str);
            }
            catch(UnknownHostException unknown) {
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
        ObjectInputStream    inp;
        Message              msg=null;
        UdpHeader            hdr=null;
        Event                evt;

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
        }
        catch(Throwable e) {
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


    /** Send a message to the address specified in dest */
    void sendUdpMessage(Message msg) throws Exception {
        IpAddress           dest;
        ObjectOutputStream  out;
        byte                buf[];
        DatagramPacket      packet;
        Message             copy;
        Event               evt;

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

        if(dest.getIpAddress().isMulticastAddress()) { // multicast message
            try {
                mcast_sock.send(packet);
            }
            catch(Throwable e) {
                Trace.error("UDP.sendUdpMessage()", "exception sending mcast message: " + e);
            }
        }
        else {                                         // unicast message
            if(send_sock != null) {
                try {
                    send_sock.send(packet);
                }
                catch(Throwable e) {
                    Trace.error("UDP.sendUdpMessage()", "exception sending ucast message: " + e);
                }
            }
            else {
                Trace.error("UDP.sendUdpMessage()", "(unicast) send_sock is null. Message is " + msg +
                                                    ", headers are " + msg.getHeaders());
            }
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
        send_sock=new DatagramSocket(0, bind_addr);
        // send_sock=new DatagramSocket();


        // 2. Create socket for receiving unicast UDP packets. The address and port
        //    of this socket will be our local address (local_addr)

		// 27-6-2003 bgooren, find available port in range (start_port, start_port+port_range)
		int rcv_port=bind_port, max_port=bind_port+port_range;
        while(rcv_port <= max_port) {

			try {
                ucast_recv_sock=new DatagramSocket(rcv_port, bind_addr);
				break;
            } catch(SocketException bind_ex) {	// Cannot listen on this port
                rcv_port++;
            } catch(SecurityException sec_ex) { // Not allowed to list on this port
				rcv_port++;
            }

			// Cannot listen at all, throw an Exception
			if(rcv_port == max_port+1) { // +1 due to the increment above
				throw new Exception("UDP.createSockets(): cannot list on any port in range "+
                        bind_port+"-"+(bind_port+port_range));
			}
        }
		//ucast_recv_sock=new DatagramSocket(bind_port, bind_addr);

        local_addr=new IpAddress(ucast_recv_sock.getLocalAddress(), ucast_recv_sock.getLocalPort());
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

        if(send_sock != null) {
            sb.append("\nsend socket: bound to ");
            sb.append(send_sock.getLocalAddress().getHostAddress()).append(":").append(send_sock.getLocalPort());
            sb.append(", send buffer size=").append(send_sock.getSendBufferSize());
        }

        if(ucast_recv_sock != null) {
            sb.append("\nreceive socket: bound to ");
            sb.append(ucast_recv_sock.getLocalAddress().getHostAddress()).append(":").append(ucast_recv_sock.getLocalPort());
            sb.append(", receive buffer size=").append(ucast_recv_sock.getReceiveBufferSize());
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
        if(send_sock != null) {
            try {
                send_sock.setSendBufferSize(ucast_send_buf_size);
            }
            catch(Throwable ex) {
                Trace.warn("UDP.setBufferSizes()", "failed setting ucast_send_buf_size in send_sock: " + ex);
            }
        }

        if(ucast_recv_sock != null) {
            try {
                ucast_recv_sock.setReceiveBufferSize(ucast_recv_buf_size);
            }
            catch(Throwable ex) {
                Trace.warn("UDP.setBufferSizes()", "failed setting ucast_recv_buf_size in ucast_recv_sock: " + ex);
            }
        }

        if(mcast_sock != null) {
            try {
                mcast_sock.setSendBufferSize(mcast_send_buf_size);
            }
            catch(Throwable ex) {
                Trace.warn("UDP.setBufferSizes()", "failed setting mcast_send_buf_size in mcast_sock: " + ex);
            }

            try {
                mcast_sock.setReceiveBufferSize(mcast_recv_buf_size);
            }
            catch(Throwable ex) {
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
        closeUnicastSenderSocket();
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
            }
            catch(IOException ex) {
            }
            mcast_addr=null;
        }
    }


    void closeUnicastReceiverSocket() {
        if(ucast_recv_sock != null) {
            // by sending a dummy packet, the thread will terminate (if it was flagged as stopped before)
            sendDummyPacket(ucast_recv_sock.getLocalAddress(), ucast_recv_sock.getLocalPort());

            ucast_recv_sock.close();
            ucast_recv_sock=null;
            if(Trace.trace) Trace.info("UDP.closeMulticastSocket()", "unicast receiver socket closed");
        }
    }

    void closeUnicastSenderSocket() {
        if(send_sock != null) {
            send_sock.close();
            send_sock=null;
            if(Trace.trace) Trace.info("UDP.closeMulticastSocket()", "unicast sender socket closed");
        }
    }


    /**
     * Workaround for the problem encountered in certains JDKs that a thread listening on a socket
     * cannot be interrupted. Therefore we just send a dummy datagram packet so that the thread 'wakes up'
     * and realizes it has to terminate. Should be removed when all JDKs support Thread.interrupt() on
     * reads. Uses send_sock t send dummy packet, so this socket has to be still alive.
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

        if(Trace.debug) Trace.info("UDP.sendDummyPacket()", "sending packet to " + dest + ":" + port);

        if(send_sock == null || dest == null) {
            Trace.warn("UDP.sendDummyPacket()", "send_sock was null or dest was null, cannot send dummy packet");
            return;
        }
        packet=new DatagramPacket(buf, buf.length, dest, port);
        try {
            send_sock.send(packet);
        }
        catch(Throwable e) {
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
            DatagramPacket  packet;
            byte            receive_buf[]=new byte[65535];
            int             len;
            byte[]          tmp1, tmp2;

            // moved out of loop to avoid excessive object creations (bela March 8 2001)
            packet=new DatagramPacket(receive_buf, receive_buf.length);

            while(running && thread != null && ucast_recv_sock != null) {
                try {
                    packet.setData(receive_buf, 0, receive_buf.length);
                    ucast_recv_sock.receive(packet);
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
                    }
                    else
                        handleIncomingUdpPacket(packet.getData());
                }
                catch(SocketException sock_ex) {
                    if(Trace.trace)
                        Trace.info("UDP.UcastReceiver.run()",
                                   "unicast receiver socket is closed, exception=" + sock_ex);
                    break;
                }
                catch(InterruptedIOException io_ex) { // thread was interrupted
                    ; // go back to top of loop, where we will terminate loop
                }
                catch(Throwable ex) {
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
                }
                catch(QueueClosedException closed_ex) {
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


}
