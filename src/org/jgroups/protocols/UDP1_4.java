package org.jgroups.protocols;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.*;
import org.jgroups.stack.LogicalAddress1_4;
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
 * a timestamp (LogicalAddress1_4). It does not change during the lifetime of the process. The LogicalAddress1_4 contains
 * a list of all unicast socket addresses to which we can send back unicast messages. When we send a message, the
 * Listener adds the sender's return address. When we receive a message, we add that address to our routing cache, which
 * contains logical addresses and physical addresses. When we need to send a unicast address, we first check whether
 * the logical address has a physical address associated with it in the cache. If so, we send a message to that address.
 * If not, we send the unicast message to <em>all</em> physical addresses contained in the LogicalAddress1_4.<br/>
 * UDP1_4 guarantees that - in scenarios with multiple subnets and multi-homed machines - members do see each other.
 * There is some overhead in multicasting the same message on multiple interfaces, and potentially sending a unicast
 * on multiple interfaces as well, but the advantage is that we don't need stuff like bind_addr any longer. Plus,
 * the unicast routing caches should ensure that unicasts are only sent via 1 interface in almost all cases.
 * 
 * @author Bela Ban Oct 2003
 * @version $Id: UDP1_4.java,v 1.17 2004/07/05 05:51:25 belaban Exp $
 * todo: sending of dummy packets
 */
public class UDP1_4 extends Protocol implements  Receiver {

    final String name="UDP1_4";

    /** Maintains a list of Connectors, one for each interface we're listening on */
    ConnectorTable ct=null;

    /** A List<String> of bind addresses, we create 1 Connector for each interface */
    List bind_addrs=null;

    /** The name of the group to which this member is connected */
    String group_name=null;

    /** The multicast address (mcast address and port) this member uses (default: 230.1.2.3:7500) */
    InetSocketAddress mcast_addr=null;

    /** The address of this member. Valid for the lifetime of the JVM in which this member runs */
    LogicalAddress1_4 local_addr=new LogicalAddress1_4(null, null);

    /** Logical address without list of physical addresses */
    LogicalAddress1_4 local_addr_canonical=local_addr.copy();

    /** Pre-allocated byte stream. Used for serializing datagram packets */
    ByteArrayOutputStream out_stream=new ByteArrayOutputStream(65535);

    /**
     * The port to which the unicast receiver socket binds.
     * 0 means to bind to any (ephemeral) port
     */
    int local_bind_port=0;
    int port_range=1; // 27-6-2003 bgooren, Only try one port by default


    /**
     * Whether to enable IP multicasting. If false, multiple unicast datagram
     * packets are sent rather than one multicast packet
     */
    boolean ip_mcast=true;

    /** The time-to-live (TTL) for multicast datagram packets */
    int ip_ttl=32;

    /** The members of this group (updated when a member joins or leaves) */
    Vector members=new Vector();

    /**
     * Header to be added to all messages sent via this protocol. It is
     * preallocated for efficiency
     */
    UdpHeader udp_hdr=null;

    /** Send buffer size of the multicast datagram socket */
    int mcast_send_buf_size=300000;

    /** Receive buffer size of the multicast datagram socket */
    int mcast_recv_buf_size=300000;

    /** Send buffer size of the unicast datagram socket */
    int ucast_send_buf_size=300000;

    /** Receive buffer size of the unicast datagram socket */
    int ucast_recv_buf_size=300000;

    /**
     * If true, messages sent to self are treated specially: unicast messages are
     * looped back immediately, multicast messages get a local copy first and -
     * when the real copy arrives - it will be discarded. Useful for Window
     * media (non)sense
     * @deprecated This is used by default now
     */
    boolean loopback=true; //todo: remove

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

    protected static Log log=LogFactory.getLog(UDP1_4.class);


    final int VERSION_LENGTH=Version.getLength();

    /** Number of bytes to allocate to receive a packet. Needs to be set to be higher than frag_size
     * (handle CONFIG event)
     */
    final int DEFAULT_RECEIVE_BUFFER_SIZE=120000;  // todo: make settable and/or use CONFIG event




    /**
     * Public constructor. creates the UDP protocol, and initializes the
     * state variables, does however not start any sockets or threads
     */
    public UDP1_4() {
    }

    /**
     * debug only
     */
    public String toString() {
        return "Protocol UDP(local address: " + local_addr + ')';
    }


    public void receive(DatagramPacket packet) {
        int           len=packet.getLength();
        byte[]        data=packet.getData();
        SocketAddress sender=packet.getSocketAddress();

        if(len == 4) {  // received a diagnostics probe
            if(data[0] == 'd' && data[1] == 'i' && data[2] == 'a' && data[3] == 'g') {
                handleDiagnosticProbe(sender);
                return;
            }
        }

        if(log.isTraceEnabled())
            log.trace("received " + len + " bytes from " + sender);

        if(Version.compareTo(packet.getData()) == false) {
            if(log.isWarnEnabled()) log.warn("packet from " + sender + " has different version (" +
                    Version.printVersionId(data, Version.version_id.length) +
                    ") from ours (" + Version.printVersionId(Version.version_id) +
                    "). This may cause problems");
        }

        if(use_packet_handler && packet_queue != null) {
            byte[] tmp=new byte[len];
            System.arraycopy(data, 0, tmp, 0, len);
            try {
                Object[] arr=new Object[]{tmp, sender};
                packet_queue.add(arr);
                return;
            }
            catch(QueueClosedException e) {
                if(log.isWarnEnabled()) log.warn("packet queue for packet handler thread is closed");
                // pass through to handleIncomingPacket()
            }
        }

        handleIncomingUdpPacket(data, sender);
    }


    /* ----------------------- Receiving of MCAST UDP packets ------------------------ */

//    public void run() {
//        DatagramPacket packet;
//        byte receive_buf[]=new byte[65000];
//        int len;
//        byte[] tmp1, tmp2;
//
//        // moved out of loop to avoid excessive object creations (bela March 8 2001)
//        packet=new DatagramPacket(receive_buf, receive_buf.length);
//
//        while(mcast_receiver != null && mcast_sock != null) {
//            try {
//                packet.setData(receive_buf, 0, receive_buf.length);
//                mcast_sock.receive(packet);
//                len=packet.getLength();
//                if(len == 1 && packet.getData()[0] == 0) {
//                    if(log.isTraceEnabled()) if(log.isInfoEnabled()) log.info("UDP1_4.run()", "received dummy packet");
//                    continue;
//                }
//
//                if(len == 4) {  // received a diagnostics probe
//                    byte[] tmp=packet.getData();
//                    if(tmp[0] == 'd' && tmp[1] == 'i' && tmp[2] == 'a' && tmp[3] == 'g') {
//                        handleDiagnosticProbe(null, null);
//                        continue;
//                    }
//                }
//
//                if(log.isTraceEnabled())
//                    if(log.isInfoEnabled()) log.info("UDP1_4.receive()", "received (mcast) " + packet.getLength() + " bytes from " +
//                            packet.getAddress() + ":" + packet.getPort() + " (size=" + len + " bytes)");
//                if(len > receive_buf.length) {
//                    if(log.isErrorEnabled()) log.error("UDP1_4.run()", "size of the received packet (" + len + ") is bigger than " +
//                            "allocated buffer (" + receive_buf.length + "): will not be able to handle packet. " +
//                            "Use the FRAG protocol and make its frag_size lower than " + receive_buf.length);
//                }
//
//                if(Version.compareTo(packet.getData()) == false) {
//                    if(log.isWarnEnabled()) log.warn("UDP1_4.run()",
//                            "packet from " + packet.getAddress() + ":" + packet.getPort() +
//                            " has different version (" +
//                            Version.printVersionId(packet.getData(), Version.version_id.length) +
//                            ") from ours (" + Version.printVersionId(Version.version_id) +
//                            "). This may cause problems");
//                }
//
//                if(use_packet_handler) {
//                    tmp1=packet.getData();
//                    tmp2=new byte[len];
//                    System.arraycopy(tmp1, 0, tmp2, 0, len);
//                    packet_queue.add(tmp2);
//                } else
//                    handleIncomingUdpPacket(packet.getData());
//            } catch(SocketException sock_ex) {
//                 if(log.isInfoEnabled()) log.info("UDP1_4.run()", "multicast socket is closed, exception=" + sock_ex);
//                break;
//            } catch(InterruptedIOException io_ex) { // thread was interrupted
//                ; // go back to top of loop, where we will terminate loop
//            } catch(Throwable ex) {
//                if(log.isErrorEnabled()) log.error("UDP1_4.run()", "exception=" + ex + ", stack trace=" + Util.printStackTrace(ex));
//                Util.sleep(1000); // so we don't get into 100% cpu spinning (should NEVER happen !)
//            }
//        }
//         if(log.isInfoEnabled()) log.info("UDP1_4.run()", "multicast thread terminated");
//    }

    void handleDiagnosticProbe(SocketAddress sender) {
        try {
            byte[] diag_rsp=getDiagResponse().getBytes();
            DatagramPacket rsp=new DatagramPacket(diag_rsp, 0, diag_rsp.length, sender);

                if(log.isInfoEnabled()) log.info("sending diag response to " + sender);
            ct.send(rsp);
        } catch(Throwable t) {
            if(log.isErrorEnabled()) log.error("failed sending diag rsp to " + sender + ", exception=" + t);
        }
    }

    String getDiagResponse() {
        StringBuffer sb=new StringBuffer();
        sb.append(local_addr).append(" (").append(group_name).append(')');
        sb.append(" [").append(mcast_addr).append("]\n");
        sb.append("Version=").append(Version.version).append(", cvs=\"").append(Version.cvs).append("\"\n");
        sb.append("physical addresses: ").append(local_addr.getPhysicalAddresses()).append('\n');
        sb.append("members: ").append(members).append('\n');

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
         if(log.isInfoEnabled()) log.info("creating sockets and starting threads");
        if(ct == null) {
            ct=new ConnectorTable(mcast_addr, DEFAULT_RECEIVE_BUFFER_SIZE, mcast_recv_buf_size, ip_mcast, this);

            for(Iterator it=bind_addrs.iterator(); it.hasNext();) {
                String bind_addr=(String)it.next();
                ct.listenOn(bind_addr, local_bind_port, port_range, DEFAULT_RECEIVE_BUFFER_SIZE, ucast_recv_buf_size,
                        ucast_send_buf_size, ip_ttl, this);
            }

            // add physical addresses to local_addr
            List physical_addrs=ct.getConnectorAddresses(); // must be non-null and size() >= 1
            for(Iterator it=physical_addrs.iterator(); it.hasNext();) {
                SocketAddress address=(SocketAddress)it.next();
                local_addr.addPhysicalAddress(address);
            }

            if(additional_data != null)
                local_addr.setAdditionalData(additional_data);

            ct.start();

            passUp(new Event(Event.SET_LOCAL_ADDRESS, local_addr));
            if(use_packet_handler)
            packet_handler.start();
        }
    }


    public void stop() {
         if(log.isInfoEnabled()) log.info("closing sockets and stopping threads");
        if(packet_handler != null)
            packet_handler.stop();
        if(ct != null) {
            ct.stop();
            ct=null;
        }
        local_addr.removeAllPhysicalAddresses();
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
        String  str;
        List    exclude_list=null;
        String  mcast_addr_name="230.8.8.8";
        int     mcast_port=7500;

        super.setProperties(props);
        str=props.getProperty("bind_addrs");
        if(str != null) {
            str=str.trim();
            if("all".equals(str.toLowerCase())) {
                try {
                    bind_addrs=determineAllBindInterfaces();
                }
                catch(SocketException e) {
                    e.printStackTrace();
                    bind_addrs=null;
                }
            }
            else {
                bind_addrs=Util.parseCommaDelimitedStrings(str);
            }
            props.remove("bind_addrs");
        }

        str=props.getProperty("bind_addrs_exclude");
        if(str != null) {
            str=str.trim();
            exclude_list=Util.parseCommaDelimitedStrings(str);
            props.remove("bind_addrs_exclude");
        }

        str=props.getProperty("bind_port");
        if(str != null) {
            local_bind_port=Integer.parseInt(str);
            props.remove("bind_port");
        }

        str=props.getProperty("start_port");
        if(str != null) {
            local_bind_port=Integer.parseInt(str);
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

        str=props.getProperty("use_packet_handler");
        if(str != null) {
            use_packet_handler=Boolean.valueOf(str).booleanValue();
            props.remove("use_packet_handler");
        }


        // determine mcast_addr
        mcast_addr=new InetSocketAddress(mcast_addr_name, mcast_port);

        // handling of bind_addrs
        if(bind_addrs == null)
            bind_addrs=new ArrayList();
        if(bind_addrs.size() == 0) {
            try {
                String default_bind_addr=determineDefaultBindInterface();
                bind_addrs.add(default_bind_addr);
            }
            catch(SocketException ex) {
                if(log.isErrorEnabled()) log.error("failed determining the default bind interface: " + ex);
            }
        }
        if(exclude_list != null) {
            bind_addrs.removeAll(exclude_list);
        }
        if(bind_addrs.size() == 0) {
            if(log.isErrorEnabled()) log.error("no valid bind interface found, unable to listen for network traffic");
            return false;
        }
        else {

                if(log.isInfoEnabled()) log.info("bind interfaces are " + bind_addrs);
        }

        if(props.size() > 0) {
            System.err.println("UDP1_4.setProperties(): the following properties are not recognized:");
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
                 if(log.isInfoEnabled()) log.info("received CONFIG event: " + evt.getArg());
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
            if(ip_mcast == false) {
                //sends a separate UDP message to each address
                sendMultipleUdpMessages(msg, members);
                return;
            }
        }

        try {
            sendUdpMessage(msg); // either unicast (dest != null) or multicast (dest == null)
        }
        catch(Exception e) {
            if(log.isErrorEnabled()) log.error("exception=" + e + ", msg=" + msg + ", mcast_addr=" + mcast_addr);
        }
    }






    /*--------------------------- End of Protocol interface -------------------------- */


    /* ------------------------------ Private Methods -------------------------------- */


    void handleMessage(Message msg) {

    }


    /**
     * Processes a packet read from either the multicast or unicast socket. Needs to be synchronized because
     * mcast or unicast socket reads can be concurrent
     */
    void handleIncomingUdpPacket(byte[] data, SocketAddress sender) {
        ByteArrayInputStream inp_stream;
        ObjectInputStream    inp;
        Message              msg=null;
        UdpHeader            hdr=null;
        Event                evt;
        Address              dst, src;

        try {
            // skip the first n bytes (default: 4), this is the version info
            inp_stream=new ByteArrayInputStream(data, VERSION_LENGTH, data.length - VERSION_LENGTH);
            inp=new ObjectInputStream(inp_stream);
            msg=new Message();
            msg.readExternal(inp);
            dst=msg.getDest();
            src=msg.getSrc();
            if(src == null) {
                if(log.isErrorEnabled()) log.error("sender's address is null");
            }
            else {
                ((LogicalAddress1_4)src).setPrimaryPhysicalAddress(sender);
            }

            // discard my own multicast loopback copy
            if((dst == null || dst.isMulticastAddress()) && src != null && local_addr.equals(src)) {
                if(log.isTraceEnabled())
                    log.trace("discarded own loopback multicast packet");

                // System.out.println("-- discarded " + msg.getObject());

                return;
            }

            evt=new Event(Event.MSG, msg);
            if(log.isTraceEnabled())
                log.trace("Message is " + msg + ", headers are " + msg.getHeaders());

            /* Because Protocol.up() is never called by this bottommost layer, we call up() directly in the observer.
             * This allows e.g. PerfObserver to get the time of reception of a message */
            if(observer != null)
                observer.up(evt, up_queue.size());

            hdr=(UdpHeader)msg.removeHeader(name);
        } catch(Throwable e) {
            if(log.isErrorEnabled()) log.error("exception=" + Util.getStackTrace(e));
            return;
        }

        if(hdr != null) {

            /* Discard all messages destined for a channel with a different name */
            String ch_name=null;

            if(hdr.group_addr != null)
                ch_name=hdr.group_addr;

            // Discard if message's group name is not the same as our group name unless the
            // message is a diagnosis message (special group name DIAG_GROUP)
            if(ch_name != null && group_name != null && !group_name.equals(ch_name) &&
                    !ch_name.equals(Util.DIAG_GROUP)) {

                    if(log.isWarnEnabled()) log.warn("discarded message from different group (" +
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
        Address            dest, src;
        ObjectOutputStream out;
        byte               buf[];
        DatagramPacket     packet;
        Message            copy;
        Event              evt; // for loopback messages

        dest=msg.getDest();  // if non-null: unicast, else multicast
        src=msg.getSrc();
        if(src == null) {
            src=local_addr_canonical; // no physical addresses present
            msg.setSrc(src);
        }

        if(log.isTraceEnabled())
            log.trace("sending message to " + msg.getDest() +
                    " (src=" + msg.getSrc() + "), headers are " + msg.getHeaders());

        // Don't send if destination is local address. Instead, switch dst and src and put in up_queue.
        // If multicast message, loopback a copy directly to us (but still multicast). Once we receive this,
        // we will discard our own multicast message
        if(dest == null || dest.isMulticastAddress() || dest.equals(local_addr)) {
            copy=msg.copy();
            copy.removeHeader(name);
            evt=new Event(Event.MSG, copy);

            /* Because Protocol.up() is never called by this bottommost layer, we call up() directly in the observer.
               This allows e.g. PerfObserver to get the time of reception of a message */
            if(observer != null)
                observer.up(evt, up_queue.size());
            if(log.isTraceEnabled()) log.trace("looped back local message " + copy);

            // System.out.println("\n-- passing up packet id=" + copy.getObject());
            passUp(evt);
            // System.out.println("-- passed up packet id=" + copy.getObject());

            if(dest != null && !dest.isMulticastAddress())
                return; // it is a unicast message to myself, no need to put on the network
        }

        out_stream.reset();
        out_stream.write(Version.version_id, 0, Version.version_id.length); // write the version
        out=new ObjectOutputStream(out_stream);
        msg.writeExternal(out);
        out.flush(); // needed if out buffers its output to out_stream
        buf=out_stream.toByteArray();
        packet=new DatagramPacket(buf, buf.length, mcast_addr);

        //System.out.println("-- sleeping 4 secs");
        // Thread.sleep(4000);


        // System.out.println("\n-- sending packet " + msg.getObject());
        ct.send(packet);
        // System.out.println("-- sent " + msg.getObject());
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





//
//    /**
//     * Workaround for the problem encountered in certains JDKs that a thread listening on a socket
//     * cannot be interrupted. Therefore we just send a dummy datagram packet so that the thread 'wakes up'
//     * and realizes it has to terminate. Should be removed when all JDKs support Thread.interrupt() on
//     * reads. Uses send_sock t send dummy packet, so this socket has to be still alive.
//     *
//     * @param dest The destination host. Will be local host if null
//     * @param port The destination port
//     */
//    void sendDummyPacket(InetAddress dest, int port) {
//        DatagramPacket packet;
//        byte[] buf={0};
//
//        if(dest == null) {
//            try {
//                dest=InetAddress.getLocalHost();
//            } catch(Exception e) {
//            }
//        }
//
//        if(log.isTraceEnabled()) if(log.isInfoEnabled()) log.info("UDP1_4.sendDummyPacket()", "sending packet to " + dest + ":" + port);
//
//        if(ucast_sock == null || dest == null) {
//            if(log.isWarnEnabled()) log.warn("UDP1_4.sendDummyPacket()", "send_sock was null or dest was null, cannot send dummy packet");
//            return;
//        }
//        packet=new DatagramPacket(buf, buf.length, dest, port);
//        try {
//            ucast_sock.send(packet);
//        } catch(Throwable e) {
//            if(log.isErrorEnabled()) log.error("UDP1_4.sendDummyPacket()", "exception sending dummy packet to " +
//                    dest + ":" + port + ": " + e);
//        }
//    }





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
                group_name=(String)evt.getArg();
                udp_hdr=new UdpHeader(group_name);

                // removed March 18 2003 (bela), not needed (handled by GMS)
                // changed July 2 2003 (bela): we discard CONNECT_OK at the GMS level anyway, this might
                // be needed if we run without GMS though
                passUp(new Event(Event.CONNECT_OK));
                break;

            case Event.DISCONNECT:
                passUp(new Event(Event.DISCONNECT_OK));
                break;

            case Event.CONFIG:
                 if(log.isInfoEnabled()) log.info("received CONFIG event: " + evt.getArg());
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
    }


    /** Return the first non-loopback interface */
    public String determineDefaultBindInterface() throws SocketException {
        for(Enumeration en=NetworkInterface.getNetworkInterfaces(); en.hasMoreElements();) {
            NetworkInterface ni=(NetworkInterface)en.nextElement();
            for(Enumeration en2=ni.getInetAddresses(); en2.hasMoreElements();) {
                InetAddress bind_addr=(InetAddress)en2.nextElement();
                if(!bind_addr.isLoopbackAddress()) {
                    return bind_addr.getHostAddress();
                }
            }
        }
        return null;
    }

    public List determineAllBindInterfaces() throws SocketException {
        List ret=new ArrayList();
        for(Enumeration en=NetworkInterface.getNetworkInterfaces(); en.hasMoreElements();) {
            NetworkInterface ni=(NetworkInterface)en.nextElement();
            for(Enumeration en2=ni.getInetAddresses(); en2.hasMoreElements();) {
                InetAddress bind_addr=(InetAddress)en2.nextElement();
                ret.add(bind_addr.getHostAddress());
            }
        }

        return ret;
    }

    /* ----------------------------- End of Private Methods ---------------------------------------- */



    /* ----------------------------- Inner Classes ---------------------------------------- */


    /**
     * This thread fetches byte buffers from the packet_queue, converts them into messages and passes them up
     * to the higher layer (done in handleIncomingUdpPacket()).
     */
    class PacketHandler implements Runnable {
        Thread t=null;

        public void run() {
            byte[] data;
            SocketAddress sender;

            while(packet_queue != null && packet_handler != null) {
                try {
                    Object[] arr=(Object[])packet_queue.remove();
                    data=(byte[])arr[0];
                    sender=(SocketAddress)arr[1];
                } catch(QueueClosedException closed_ex) {
                     if(log.isInfoEnabled()) log.info("packet_handler thread terminating");
                    break;
                }
                handleIncomingUdpPacket(data, sender);
                data=null; // let's give the poor garbage collector a hand...
            }
        }

        void start() {
            if(t == null) {
                t=new Thread(this, "UDP1_4.PacketHandler thread");
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

        protected Thread t=null;

        protected SenderThread sender_thread=null;

        /** Interface on which ucast_sock and mcast_sender_sock are created */
        NetworkInterface bind_interface;


        /** Used for sending/receiving unicast/multicast packets. The reason we have to use a MulticastSocket versus a
         * DatagramSocket is that only MulticastSockets allow to set the interface over which a multicast
         * is sent: DatagramSockets consult the routing table to find the interface
         */
        MulticastSocket mcast_sock=null;

        /** Local port of the mcast_sock */
        SocketAddress local_addr=null;

        /** The receiver which handles incoming packets */
        Receiver receiver=null;

        /** Buffer for incoming unicast packets */
        protected byte[] receive_buffer=null;


        Queue send_queue=new Queue();


        class SenderThread extends Thread {


            public void run() {
                Object[] arr;
                byte[] buf;
                SocketAddress dest;

                while(send_queue != null) {
                    try {
                        arr=(Object[])send_queue.remove();
                        buf=(byte[])arr[0];
                        dest=(SocketAddress)arr[1];
                        mcast_sock.send(new DatagramPacket(buf, buf.length, dest));
                    }
                    catch(QueueClosedException e) {
                        break;
                    }
                    catch(SocketException e) {
                        e.printStackTrace();
                    }
                    catch(IOException e) {
                        e.printStackTrace();
                    }

                }
            }
        }



        public Connector(NetworkInterface bind_interface, int local_bind_port,
                         int port_range,  int receive_buffer_size,
                         int receive_sock_buf_size, int send_sock_buf_size,
                         int ip_ttl, Receiver receiver) throws IOException {
            this.bind_interface=bind_interface;
            this.receiver=receiver;
            this.receive_buffer=new byte[receive_buffer_size];

            mcast_sock=createMulticastSocket(local_bind_port, port_range);

            // changed Bela Dec 31 2003: if loopback is disabled other members on the same machine won't be able
            // to receive our multicasts
            // mcast_sock.setLoopbackMode(true); // we don't want to receive our own multicasts
            mcast_sock.setReceiveBufferSize(receive_sock_buf_size);
            mcast_sock.setSendBufferSize(send_sock_buf_size);
            mcast_sock.setTimeToLive(ip_ttl);
            System.out.println("ttl=" + mcast_sock.getTimeToLive());
            mcast_sock.setNetworkInterface(this.bind_interface); // for outgoing multicasts
            local_addr=mcast_sock.getLocalSocketAddress();
            System.out.println("-- local_addr=" + local_addr);
            System.out.println("-- mcast_sock: send_bufsize=" + mcast_sock.getSendBufferSize() +
                    ", recv_bufsize=" + mcast_sock.getReceiveBufferSize());
        }


        public SocketAddress getLocalAddress() {
            return local_addr;
        }

        public NetworkInterface getBindInterface() {
            return bind_interface;
        }

        public void start() throws Exception {
            if(mcast_sock == null)
                throw new Exception("UDP1_4.Connector.start(): connector has been stopped (start() cannot be called)");

            if(t != null && t.isAlive()) {
                if(log.isWarnEnabled()) log.warn("connector thread is already running");
                return;
            }
            t=new Thread(this, "ConnectorThread for " + local_addr);
            t.setDaemon(true);
            t.start();

            sender_thread=new SenderThread();
            sender_thread.start();
        }

        /** Stops the connector. After this call, start() cannot be called, but a new connector has to
         * be created
         */
        public void stop() {
            if(mcast_sock != null)
                mcast_sock.close(); // terminates the thread if running
            t=null;
            mcast_sock=null;
        }



        /** Sends a message using mcast_sock */
        public void send(DatagramPacket packet) throws Exception {
            //mcast_sock.send(packet);

            byte[] buf=(byte[])packet.getData().clone();
            Object[] arr=new Object[]{buf, packet.getSocketAddress()};
            send_queue.add(arr);
        }

        public void run() {
            DatagramPacket packet=new DatagramPacket(receive_buffer, receive_buffer.length);
            while(t != null) {
                try {
                    packet.setData(receive_buffer, 0, receive_buffer.length);
                    ConnectorTable.receivePacket(packet, mcast_sock, receiver);
                }
                catch(Throwable t) {
                    if(t == null || mcast_sock == null || mcast_sock.isClosed())
                        break;
                    if(log.isErrorEnabled()) log.error("[" + local_addr + "] exception=" + t);
                    Util.sleep(300); // so we don't get into 100% cpu spinning (should NEVER happen !)
                }
            }
            t=null;
        }




        public String toString() {
            StringBuffer sb=new StringBuffer();
            sb.append("local_addr=").append(local_addr).append(", mcast_group=");
            return sb.toString();
        }





        // 27-6-2003 bgooren, find available port in range (start_port, start_port+port_range)
        private MulticastSocket createMulticastSocket(int local_bind_port, int port_range) throws IOException {
            MulticastSocket sock=null;
            int             tmp_port=local_bind_port;

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
                throw new IOException("could not create a MulticastSocket (port range: " + local_bind_port +
                        " - " + (local_bind_port+port_range));
            return sock;
        }
    }





    /** Manages a bunch of Connectors */
    public static class ConnectorTable implements Receiver, Runnable {

        Thread t=null;

        /** Socket to receive multicast packets. Will be bound to n interfaces */
        MulticastSocket mcast_sock=null;

        /** The multicast address which mcast_sock will join (e.g. 230.1.2.3:7500) */
        InetSocketAddress mcast_addr=null;

        Receiver receiver=null;

        /** Buffer for incoming packets */
        byte[] receive_buffer=null;

        /** Vector<Connector>. A list of Connectors, one for each interface we listen on */
        Vector connectors=new Vector();

        boolean running=false;





        public ConnectorTable(InetSocketAddress mcast_addr,
                              int receive_buffer_size, int receive_sock_buf_size,
                              boolean ip_mcast, Receiver receiver) throws IOException {
            this.receiver=receiver;
            this.mcast_addr=mcast_addr;
            this.receive_buffer=new byte[receive_buffer_size];

            if(ip_mcast) {
                mcast_sock=new MulticastSocket(mcast_addr.getPort());
                // changed Bela Dec 31 2003: if loopback is disabled other members on the same machine won't be able
                // to receive our multicasts
                // mcast_sock.setLoopbackMode(true); // do not get own multicasts
                mcast_sock.setReceiveBufferSize(receive_sock_buf_size);
            }
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
            if(running)
                return;

            if(mcast_sock != null) {
                // Start the thread servicing the incoming multicasts
                t=new Thread(this, "ConnectorTable thread");
                t.setDaemon(true);
                t.start();
            }


            // Start all Connectors
            for(Iterator it=connectors.iterator(); it.hasNext();) {
                tmp=(Connector)it.next();
                tmp.start();
            }

            running=true;
        }


        public void stop() {
            Connector tmp;
            for(Iterator it=connectors.iterator(); it.hasNext();) {
                tmp=(Connector)it.next();
                tmp.stop();
            }
            connectors.clear();
            t=null;
            if(mcast_sock != null) {
                mcast_sock.close();
                mcast_sock=null;
            }
            running=false;
        }


        public void run() {
            // receive mcast packets on any interface of the list of interfaces we're listening on
            DatagramPacket p=new DatagramPacket(receive_buffer, receive_buffer.length);
            while(t != null && mcast_sock != null && !mcast_sock.isClosed()) {
                p.setData(receive_buffer, 0, receive_buffer.length);
                try {
                    receivePacket(p, mcast_sock, this);
                }
                catch(Throwable t) {
                    if(t == null || mcast_sock == null || mcast_sock.isClosed())
                        break;
                    if(log.isErrorEnabled()) log.error("exception=" + t);
                    Util.sleep(300); // so we don't get into 100% cpu spinning (should NEVER happen !)
                }
            }
            t=null;
        }


        /**
         * Returns a list of local addresses (one for each Connector)
         * @return List<SocketAddress>
         */
        public List getConnectorAddresses() {
            Connector c;
            ArrayList ret=new ArrayList();
            for(Iterator it=connectors.iterator(); it.hasNext();) {
                c=(Connector)it.next();
                ret.add(c.getLocalAddress());
            }
            return ret;
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
                for(int i=0; i < connectors.size(); i++) {
                    ((Connector)connectors.get(i)).send(msg);
                }
            }
            else {
                // send to a random connector
                Connector c=pickRandomConnector(connectors);
                c.send(msg);
            }
        }

        private Connector pickRandomConnector(Vector conns) {
            int size=conns.size();
            int index=((int)(Util.random(size))) -1;
            return (Connector)conns.get(index);
        }

        /**
         * Adds the given interface address to the list of interfaces on which the receiver mcast
         * socket has to listen.
         * Also creates a new Connector. Calling this method twice on the same interface will throw an exception
         * @param bind_interface
         * @param local_port
         * @param port_range
         * @param receive_buffer_size
         * @throws IOException
         */
        public void listenOn(String bind_interface, int local_port, int port_range, 
                             int receive_buffer_size, int receiver_sock_buf_size, int send_sock_buf_size,
                             int ip_ttl, Receiver receiver) throws IOException {
            if(bind_interface == null)
                return;

            NetworkInterface ni=NetworkInterface.getByInetAddress(InetAddress.getByName(bind_interface));
            if(ni == null)
                throw new IOException("UDP1_4.ConnectorTable.listenOn(): bind interface for " +
                        bind_interface + " not found");

            Connector tmp=findConnector(ni);
            if(tmp != null) {
                if(log.isWarnEnabled()) log.warn("connector for interface " + bind_interface +
                        " is already present (will be skipped): " + tmp);
                return;
            }

            // 1. join the group on this interface
            if(mcast_sock != null) {
                mcast_sock.joinGroup(mcast_addr, ni);

                    if(log.isInfoEnabled()) log.info("joining " + mcast_addr + " on interface " + ni);
            }

            // 2. create a new Connector
            tmp=new Connector(ni, local_port, port_range, receive_buffer_size, receiver_sock_buf_size,
                    send_sock_buf_size, ip_ttl, receiver);
            connectors.add(tmp);
        }

        private Connector findConnector(NetworkInterface ni) {
            for(int i=0; i < connectors.size(); i++) {
                Connector c=(Connector)connectors.elementAt(i);
                if(c.getBindInterface().equals(ni))
                    return c;
            }
            return null;
        }


        public void receive(DatagramPacket packet) {
            if(receiver != null) {
                receiver.receive(packet);
            }
        }


        public String toString() {
            StringBuffer sb=new StringBuffer();
            sb.append("*** todo: implement ***");
            return sb.toString();
        }


        public static void receivePacket(DatagramPacket packet, DatagramSocket sock, Receiver receiver) throws IOException {
            int len;

            sock.receive(packet);
            len=packet.getLength();
            if(len == 1 && packet.getData()[0] == 0) {
                if(log.isTraceEnabled()) log.trace("received dummy packet");
                return;
            }
            if(receiver != null)
                receiver.receive(packet);
        }
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
                    System.out.println("-- received respose: \"" + tmp + '\"');
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



    public static class MulticastReceiver implements Runnable {
        Unmarshaller m=null;
        DatagramSocket sock=null; // may be DatagramSocket or MulticastSocket

        public void run() {
            // receives packet from socket
            // calls Unmarshaller.receive()
        }

    }

    public static class Unmarshaller {
        Queue q=null;

        void receive(byte[] data, SocketAddress sender) {
            // if (q) --> q.add()
            // unserialize and call handleMessage()
        }
    }



    public static class Mailman {
        
    }


    static void help() {
        System.out.println("UDP1_4 [-help] [-bind_addrs <list of interfaces>]");
    }



    public static void main(String[] args) {
        MyReceiver        r=new MyReceiver();
        ConnectorTable    ct;
        String            line;
        InetSocketAddress mcast_addr;
        BufferedReader    in=null;
        DatagramPacket    packet;
        byte[]            send_buf;
        int               receive_buffer_size=65000;
        boolean           ip_mcast=true;

        try {
            mcast_addr=new InetSocketAddress("230.1.2.3", 7500);
            ct=new ConnectorTable(mcast_addr, receive_buffer_size, 120000, ip_mcast, r);
            r.setConnectorTable(ct);
        }
        catch(Throwable t) {
            t.printStackTrace();
            return;
        }

        for(int i=0; i < args.length; i++) {
            if("-help".equals(args[i])) {
                help();
                continue;
            }
            if("-bind_addrs".equals(args[i])) {
                while(++i < args.length && !args[i].trim().startsWith("-")) {
                    try {
                        ct.listenOn(args[i], 0, 1, receive_buffer_size, 120000, 12000, 32, r);
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
                packet=new DatagramPacket(send_buf, send_buf.length, mcast_addr);
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


interface Receiver {

    /** Called when data has been received on a socket. When the callback returns, the buffer will be
     * reused: therefore, if <code>buf</code> must be processed on a separate thread, it needs to be copied.
     * This method might be called concurrently by multiple threads, so it has to be reentrant
     * @param packet
     */
    void receive(DatagramPacket packet);
}
