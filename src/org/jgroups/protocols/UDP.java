package org.jgroups.protocols;


import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Global;
import org.jgroups.PhysicalAddress;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.AsciiString;
import org.jgroups.util.SuppressLog;
import org.jgroups.util.Util;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.*;
import java.util.Collection;
import java.util.Formatter;
import java.util.List;
import java.util.Map;


/**
 * IP multicast transport based on UDP. Messages to the group (msg.dest == null)
 * will be multicast (to all group members), whereas point-to-point messages
 * (msg.dest != null) will be unicast to a single member. Uses a multicast and a
 * unicast socket.
 * <p>
 * The following properties are read by the UDP protocol:
 * <ul>
 * <li> param mcast_addr - the multicast address to use; default is 228.8.8.8.
 * <li> param mcast_port - (int) the port that the multicast is sent on; default
 * is 7600
 * <li> param ip_mcast - (boolean) flag whether to use IP multicast; default is
 * true.
 * <li> param ip_ttl - the default time-to-live for multicast packets sent out
 * on this socket; default is 8.
 * <li> param use_packet_handler - boolean, defaults to false. If set, the mcast
 * and ucast receiver threads just put the datagram's payload (a byte buffer)
 * into a queue, from where a separate thread will dequeue and handle them
 * (unmarshal and pass up). This frees the receiver threads from having to do
 * message unmarshalling; this time can now be spent receiving packets. If you
 * have lots of retransmissions because of network input buffer overflow,
 * consider setting this property to true.
 * </ul>
 * 
 * @author Bela Ban
 */
public class UDP extends TP {

    /* ------------------------------------------ Properties  ------------------------------------------ */

    /**
     * Traffic class for sending unicast and multicast datagrams. Valid values
     * are (check {@link DatagramSocket#setTrafficClass(int)} ); for details):
     * <UL>
     * <LI><CODE>IPTOS_LOWCOST (0x02)</CODE>, <b>decimal 2</b></LI>
     * <LI><CODE>IPTOS_RELIABILITY (0x04)</CODE><, <b>decimal 4</b>/LI>
     * <LI><CODE>IPTOS_THROUGHPUT (0x08)</CODE>, <b>decimal 8</b></LI>
     * <LI><CODE>IPTOS_LOWDELAY (0x10)</CODE>, <b>decimal</b> 16</LI>
     * </UL>
     */
    @Property(description="Traffic class for sending unicast and multicast datagrams. Default is 8")
    protected int tos=8; // valid values: 2, 4, 8 (default), 16

    @Property(name="mcast_addr", description="The multicast address used for sending and receiving packets",
              defaultValueIPv4="228.8.8.8", defaultValueIPv6="ff0e::8:8:8",
              systemProperty=Global.UDP_MCAST_ADDR,writable=false)
    protected InetAddress mcast_group_addr=null;

    @Property(description="The multicast port used for sending and receiving packets. Default is 7600",
              systemProperty=Global.UDP_MCAST_PORT, writable=false)
    protected int mcast_port=7600;

    @Property(description="Multicast toggle. If false multiple unicast datagrams are sent instead of one multicast. " +
            "Default is true", writable=false)
    protected boolean ip_mcast=true;

    @Property(description="The time-to-live (TTL) for multicast datagram packets. Default is 8",systemProperty=Global.UDP_IP_TTL)
    protected int ip_ttl=8;

    @Property(description="Send buffer size of the multicast datagram socket. Default is 100'000 bytes")
    protected int mcast_send_buf_size=100000;

    @Property(description="Receive buffer size of the multicast datagram socket. Default is 500'000 bytes")
    protected int mcast_recv_buf_size=500000;

    @Property(description="Send buffer size of the unicast datagram socket. Default is 100'000 bytes")
    protected int ucast_send_buf_size=100000;

    @Property(description="Receive buffer size of the unicast datagram socket. Default is 64'000 bytes")
    protected int ucast_recv_buf_size=64000;

    @Property(description="If true, disables IP_MULTICAST_LOOP on the MulticastSocket (for sending and receiving of " +
      "multicast packets). IP multicast packets send on a host P will therefore not be received by anyone on P. Use with caution.")
    protected boolean disable_loopback=false;

    @Property(description="Suppresses warnings on Mac OS (for now) about not enough buffer space when sending " +
      "a datagram packet")
    protected long suppress_time_out_of_buffer_space=60000;


    /* --------------------------------------------- Fields ------------------------------------------------ */



    /** The multicast address (mcast address and port) this member uses */
    protected IpAddress       mcast_addr;

    /**
     * Socket used for
     * <ol>
     * <li>sending unicast and multicast packets and
     * <li>receiving unicast packets
     * </ol>
     * The address of this socket will be our local address (<tt>local_addr</tt>)
     */
    protected MulticastSocket  sock;

    /** IP multicast socket for <em>receiving</em> multicast packets */
    protected MulticastSocket mcast_sock=null;

    /** Runnable to receive multicast packets */
    protected PacketReceiver  mcast_receiver=null;

    /** Runnable to receive unicast packets */
    protected PacketReceiver  ucast_receiver=null;

    protected SuppressLog<InetAddress> suppress_log_out_of_buffer_space;

    protected static final boolean is_android, is_mac;


    static  {
        is_android=Util.checkForAndroid();
        is_mac=Util.checkForMac();
    }



    public boolean supportsMulticasting() {
        return ip_mcast;
    }

    public void setMulticastAddress(InetAddress addr) {this.mcast_group_addr=addr;}
    public InetAddress getMulticastAddress() {return mcast_group_addr;}
    public int getMulticastPort() {return mcast_port;}
    public void setMulticastPort(int mcast_port) {this.mcast_port=mcast_port;}
    public void setMcastPort(int mcast_port) {this.mcast_port=mcast_port;}

    /**
     * Set the ttl for multicast socket
     * @param ttl the time to live for the socket.
     * @throws IOException
     */
    public void setMulticastTTL(int ttl) {
        this.ip_ttl=ttl;
        setTimeToLive(ttl, sock);
    }

    /**
     * Getter for current multicast TTL
     * @return
     */
    public int getMulticastTTL() {
        return ip_ttl;
    }

    public void setMaxBundleSize(int size) {
        super.setMaxBundleSize(size);
        if(size > Global.MAX_DATAGRAM_PACKET_SIZE)
            throw new IllegalArgumentException("max_bundle_size (" + size + ") cannot exceed the max datagram " +
                                                 "packet size of " + Global.MAX_DATAGRAM_PACKET_SIZE);
    }

    @ManagedAttribute(description="Number of messages dropped when sending because of insufficient buffer space")
    public int getDroppedMessage() {
        return suppress_log_out_of_buffer_space != null? suppress_log_out_of_buffer_space.getCache().size() : 0;
    }

    @ManagedOperation(description="Clears the cache for dropped messages")
    public void clearDroppedMessagesCache() {
        if(suppress_log_out_of_buffer_space != null)
            suppress_log_out_of_buffer_space.getCache().clear();
    }

    public String getInfo() {
        StringBuilder sb=new StringBuilder();
        sb.append("group_addr=").append(mcast_group_addr.getHostName()).append(':').append(mcast_port).append("\n");
        return sb.toString();
    }

    public void sendMulticast(AsciiString cluster_name, byte[] data, int offset, int length) throws Exception {
        if(ip_mcast && mcast_addr != null) {
            _send(mcast_addr.getIpAddress(), mcast_addr.getPort(), true, data, offset, length);
        }
        else {
            if(!isSingleton())
                sendToMembers(members, data, offset, length);
            else {
                Collection<Address> mbrs=members;
                if(cluster_name != null && up_prots != null) {
                    ProtocolAdapter prot_ad=(ProtocolAdapter)up_prots.get(cluster_name);
                    if(prot_ad != null)
                        mbrs=prot_ad.getMembers();
                }
                sendToMembers(mbrs, data, offset, length);
            }
        }
    }

    public void sendUnicast(PhysicalAddress dest, byte[] data, int offset, int length) throws Exception {
        _send(((IpAddress)dest).getIpAddress(), ((IpAddress)dest).getPort(), false, data, offset, length);
    }


    protected void _send(InetAddress dest, int port, boolean mcast, byte[] data, int offset, int length) throws Exception {
        DatagramPacket packet=new DatagramPacket(data, offset, length, dest, port);
        // using the datagram socket to send multicasts or unicasts (https://issues.jboss.org/browse/JGRP-1765)
        if(sock != null) {
            try {
                sock.send(packet);
            }
            catch(IOException ex) {
                if(suppress_log_out_of_buffer_space != null)
                    suppress_log_out_of_buffer_space.log(SuppressLog.Level.warn, dest, suppress_time_out_of_buffer_space,
                                                         local_addr, dest == null? "cluster" : dest, ex);
                else
                    throw ex;
            }
        }
    }


    /* ------------------------------------------------------------------------------- */



    /*------------------------------ Protocol interface ------------------------------ */

    @Override
    protected Object handleDownEvent(Event evt) {
        Object retval=super.handleDownEvent(evt);
        switch(evt.getType()) {
            case Event.VIEW_CHANGE:
                if(suppress_log_out_of_buffer_space != null)
                    suppress_log_out_of_buffer_space.removeExpired(suppress_time_out_of_buffer_space);
                break;
        }
        return retval;
    }

    public void init() throws Exception {
        super.init();
        if(is_mac && suppress_time_out_of_buffer_space > 0)
            suppress_log_out_of_buffer_space=new SuppressLog<>(log, "FailureSendingToPhysAddr", "SuppressMsg");
    }

    /**
     * Creates the unicast and multicast sockets and starts the unicast and multicast receiver threads
     */
    public void start() throws Exception {
        try {
            createSockets();
            super.start();
        }
        catch(Exception ex) {
            destroySockets();
            throw ex;
        }
        ucast_receiver=new PacketReceiver(sock, "unicast receiver",
                                          new Runnable() {public void run() {closeUnicastSocket();}});

        if(ip_mcast)
            mcast_receiver=new PacketReceiver(mcast_sock, "multicast receiver",
                                              new Runnable() {public void run() {closeMulticastSocket();}});
    }


    public void stop() {
        if(log.isDebugEnabled()) log.debug("closing sockets and stopping threads");
        stopThreads();  // will close sockets, closeSockets() is not really needed anymore, but...
        super.stop();
    }

    public void destroy() {
        super.destroy();
        destroySockets();
    }

    protected void handleConnect() throws Exception {
        if(isSingleton()) {
            if(connect_count == 0) {
                startThreads();
            }
            super.handleConnect();
        }
        else
            startThreads();
    }

    protected void handleDisconnect() {
        if(isSingleton()) {
            super.handleDisconnect();
            if(connect_count == 0) {
                stopThreads();
            }
        }
        else
            stopThreads();
    }

    /*--------------------------- End of Protocol interface -------------------------- */


    /* ------------------------------ Private Methods -------------------------------- */

    protected static Method findMethod(Class<?> clazz, String method_name, Class<?> ... parameters) {
        try {
            Method method=clazz.getDeclaredMethod(method_name, parameters);
            method.setAccessible(true);
            return method;
        }
        catch(Throwable t) {
            return null;
        }
    }


    /** Creates the  UDP sender and receiver sockets */
    protected void createSockets() throws Exception {
        if(bind_addr == null)
            throw new IllegalArgumentException("bind_addr cannot be null") ;

        Util.checkIfValidAddress(bind_addr, getName());
        if(log.isDebugEnabled()) log.debug("sockets will use interface " + bind_addr.getHostAddress());

        // Create socket for receiving unicast UDP packets and sending of IP multicast packets. The bind address
        // and port of this socket will be our local physical address (local_addr)

        // 1: sock is bound to bind_addr:bind_port, which means it will *receive* packets only on bind_addr:bind_port
        // 2: sock's setInterface() method is used to determine the interface to *send* multicasts (unicasts use the
        //    interface determined by consulting the routing table)

        if(bind_port > 0)
            sock=createMulticastSocketWithBindPort();
        else
            sock=createMulticastSocket("jgroups.udp.sock", 0);

        setTimeToLive(ip_ttl, sock);

        if(tos > 0) {
            try {
                sock.setTrafficClass(tos);
            }
            catch(SocketException e) {
                log.warn(Util.getMessage("TrafficClass"), tos, e);
            }
        }

        // 3. Create socket for receiving IP multicast packets
        if(ip_mcast) {
            // https://jira.jboss.org/jira/browse/JGRP-777 - this doesn't work on MacOS, and we don't have
            // cross talking on Windows anyway, so we just do it for Linux. (How about Solaris ?)

            // If possible, the MulticastSocket(SocketAddress) ctor is used which binds to mcast_addr:mcast_port.
            // This acts like a filter, dropping multicasts to different multicast addresses
            if(can_bind_to_mcast_addr)
                mcast_sock=Util.createMulticastSocket(getSocketFactory(), "jgroups.udp.mcast_sock", mcast_group_addr, mcast_port, log);
            else
                mcast_sock=getSocketFactory().createMulticastSocket("jgroups.udp.mcast_sock", mcast_port);

            if(disable_loopback)
                mcast_sock.setLoopbackMode(disable_loopback);

            mcast_addr=new IpAddress(mcast_group_addr, mcast_port);

            // check that we're not using the same mcast address and port as the diagnostics socket
            if(enable_diagnostics && diagnostics_addr.equals(mcast_group_addr) && diagnostics_port == mcast_port)
                throw new IllegalArgumentException("diagnostics_addr:diagnostics_port and mcast_addr:mcast_port " +
                                                     "have to be different");

            if(tos > 0) {
                try {
                    mcast_sock.setTrafficClass(tos);
                }
                catch(SocketException e) {
                    log.warn(Util.getMessage("TrafficClass"), tos, e);
                }
            }

            if(receive_on_all_interfaces || (receive_interfaces != null && !receive_interfaces.isEmpty())) {
                List<NetworkInterface> interfaces;
                if(receive_interfaces != null)
                    interfaces=receive_interfaces;
                else
                    interfaces=Util.getAllAvailableInterfaces();
                bindToInterfaces(interfaces, mcast_sock, mcast_addr.getIpAddress());
            }
            else {
                if(bind_addr != null)
                    setInterface(bind_addr, mcast_sock); // not strictly needed for receiving, only for sending of mcasts
                 mcast_sock.joinGroup(mcast_group_addr);
            }
        }

        setBufferSizes();
        log.debug("socket information:\n%s", dumpSocketInfo());
    }


    protected void destroySockets() {
        closeMulticastSocket();
        closeUnicastSocket();
    }

    protected IpAddress createLocalAddress() {
        if(sock == null || sock.isClosed())
            return null;
        if(external_addr != null) {
            if(external_port > 0)
                return new IpAddress(external_addr, external_port);
            return new IpAddress(external_addr, sock.getLocalPort());
        }
        return new IpAddress(sock.getLocalAddress(), sock.getLocalPort());
    }

    protected void setTimeToLive(int ttl, MulticastSocket s) {
        try {
            if(s != null)
                s.setTimeToLive(ttl);
        }
        catch(Throwable ex) { // just in case Windows throws an exception (DualStack impl, not implemented)
            log.error("failed setting ip_ttl to %d: %s", ttl, ex);
        }
    }

    protected void setInterface(InetAddress intf, MulticastSocket s) {
        try {
            if(s != null && intf != null)
                s.setInterface(intf);
        }
        catch(Throwable ex) {
            log.error("failed setting interface to %s: %s", intf, ex);
        }
    }


    protected PhysicalAddress getPhysicalAddress() {
        return createLocalAddress();
    }

    /**
     *
     * @param interfaces List<NetworkInterface>. Guaranteed to have no duplicates
     * @param s
     * @param mcastAddr
     * @throws IOException
     */
    protected void bindToInterfaces(List<NetworkInterface> interfaces,
                                  MulticastSocket s,
                                  InetAddress mcastAddr) {
        SocketAddress tmp_mcast_addr=new InetSocketAddress(mcastAddr, mcast_port);
        for(NetworkInterface intf: interfaces) {

            //[ JGRP-680] - receive_on_all_interfaces requires every NIC to be configured
            try {
                s.joinGroup(tmp_mcast_addr, intf);
                log.trace("joined %s on %s", tmp_mcast_addr, intf.getName());
            }
            catch(IOException e) {
                if(log.isWarnEnabled())
                    log.warn(Util.getMessage("InterfaceJoinFailed"), tmp_mcast_addr, intf.getName());
            }
        }
    }



    /**
     * Creates a DatagramSocket when bind_port > 0. Attempts to allocate the socket with port == bind_port, and
     * increments until it finds a valid port, or until port_range has been exceeded
     * @return DatagramSocket The newly created socket
     * @throws Exception
     */
    protected MulticastSocket createMulticastSocketWithBindPort() throws Exception {
        MulticastSocket tmp=null;
        // 27-6-2003 bgooren, find available port in range (start_port, start_port+port_range)
        int rcv_port=bind_port, max_port=bind_port + port_range;
        while(rcv_port <= max_port) {
            try {
                return createMulticastSocket("jgroups.udp.sock", rcv_port);
            }
            catch(SocketException | SecurityException bind_ex) {	// Cannot listen on this port
                rcv_port++;
            }
        }

        // Cannot listen at all, throw an Exception
        if(rcv_port >= max_port + 1) // +1 due to the increment above
            throw new Exception("failed to open a port in range " + bind_port + '-' + max_port);
        return tmp;
    }

    protected MulticastSocket createMulticastSocket(String service_name, int port) throws Exception {
        // Creates an *unbound* multicast socket (because SocketAddress is null)
        MulticastSocket retval=getSocketFactory().createMulticastSocket(service_name, null); // causes *no* binding !
        if(bind_addr != null)
            setInterface(bind_addr, retval);
        retval.setReuseAddress(false); // so we get a conflict if binding to the same port and increment the port
        retval.bind(new InetSocketAddress(bind_addr, port));
        return retval;
    }

    protected String dumpSocketInfo() throws Exception {
        StringBuilder sb=new StringBuilder(128);
        Formatter formatter=new Formatter(sb);
        formatter.format("mcast_addr=%s, bind_addr=%s, ttl=%d", mcast_addr, bind_addr, ip_ttl);

        if(sock != null)
            formatter.format("\nsock: bound to %s:%d, receive buffer size=%d, send buffer size=%d",
                             sock.getLocalAddress().getHostAddress(), sock.getLocalPort(),
                             sock.getReceiveBufferSize(), sock.getSendBufferSize());

        if(mcast_sock != null)
            formatter.format("\nmcast_sock: bound to %s:%d, send buffer size=%d, receive buffer size=%d",
                             mcast_sock.getInterface().getHostAddress(), mcast_sock.getLocalPort(),
                             mcast_sock.getSendBufferSize(), mcast_sock.getReceiveBufferSize());
        return sb.toString();
    }


    void setBufferSizes() {
        if(sock != null)
            setBufferSize(sock, ucast_send_buf_size, ucast_recv_buf_size);

        if(mcast_sock != null)
            setBufferSize(mcast_sock, mcast_send_buf_size, mcast_recv_buf_size);
    }

    protected void setBufferSize(DatagramSocket sock, int send_buf_size, int recv_buf_size) {
        try {
            sock.setSendBufferSize(send_buf_size);
            int actual_size=sock.getSendBufferSize();
            if(actual_size < send_buf_size && log.isWarnEnabled()) {
                log.warn(Util.getMessage("IncorrectBufferSize"), "send", sock.getClass().getSimpleName(),
                                         Util.printBytes(send_buf_size), Util.printBytes(actual_size), "send", "net.core.wmem_max");
            }
        }
        catch(Throwable ex) {
            log.warn(Util.getMessage("BufferSizeFailed"), "send", send_buf_size, sock, ex);
        }

        try {
            sock.setReceiveBufferSize(recv_buf_size);
            int actual_size=sock.getReceiveBufferSize();
            if(actual_size < recv_buf_size && log.isWarnEnabled()) {
                log.warn(Util.getMessage("IncorrectBufferSize"), "receive", sock.getClass().getSimpleName(),
                                         Util.printBytes(recv_buf_size), Util.printBytes(actual_size), "receive", "net.core.rmem_max");
            }
        }
        catch(Throwable ex) {
            log.warn(Util.getMessage("BufferSizeFailed"), "receive", recv_buf_size, sock, ex);
        }
    }



    void closeMulticastSocket() {
        if(mcast_sock != null) {
            try {
                if(mcast_addr != null) {
                    mcast_sock.leaveGroup(mcast_addr.getIpAddress());
                }
                getSocketFactory().close(mcast_sock); // this will cause the mcast receiver thread to break out of its loop
                mcast_sock=null;
                if(log.isDebugEnabled()) log.debug("multicast socket closed");
            }
            catch(IOException ex) {
            }
            mcast_addr=null;
        }
    }


    protected void closeUnicastSocket() {
        getSocketFactory().close(sock);
    }



    /**
     * Starts the unicast and multicast receiver threads
     */
    void startThreads() throws Exception {
        ucast_receiver.start();
        if(mcast_receiver != null)
            mcast_receiver.start();
    }


    /**
     * Stops unicast and multicast receiver threads
     */
    void stopThreads() {
        if(mcast_receiver != null)
            mcast_receiver.stop();
        if(ucast_receiver != null)
            ucast_receiver.stop();
    }


    protected void handleConfigEvent(Map<String,Object> map) {
        boolean set_buffers=false;
        if(map == null) return;

        if(map.containsKey("send_buf_size")) {
            mcast_send_buf_size=(Integer)map.get("send_buf_size");
            ucast_send_buf_size=mcast_send_buf_size;
            set_buffers=true;
        }
        if(map.containsKey("recv_buf_size")) {
            mcast_recv_buf_size=(Integer)map.get("recv_buf_size");
            ucast_recv_buf_size=mcast_recv_buf_size;
            set_buffers=true;
        }
        if(set_buffers)
            setBufferSizes();
    }

    /* ----------------------------- End of Private Methods ---------------------------------------- */



    /* ----------------------------- Inner Classes ---------------------------------------- */


    public class PacketReceiver implements Runnable {
        private       Thread         thread=null;
        private final DatagramSocket receiver_socket;
        private final String         name;
        private final Runnable       close_strategy;

        public PacketReceiver(DatagramSocket socket, String name, Runnable close_strategy) {
            this.receiver_socket=socket;
            this.name=name;
            this.close_strategy=close_strategy;
        }

        public synchronized void start() {
            if(thread == null || !thread.isAlive()) {
                thread=getThreadFactory().newThread(this, name);
                thread.start();
            }
        }

        public synchronized void stop() {
            Thread tmp=thread;
            thread=null;
            try {
                close_strategy.run();
            }
            catch(Exception e1) {
            }
            finally {
                Util.close(receiver_socket); // second line of defense
            }

            if(tmp != null && tmp.isAlive()) {
                tmp.interrupt();
                try {
                    tmp.join(Global.THREAD_SHUTDOWN_WAIT_TIME);
                }
                catch(InterruptedException e) {
                    Thread.currentThread().interrupt(); // set interrupt flag again
                }
            }
        }


        public void run() {
            final byte           receive_buf[]=new byte[66000]; // to be on the safe side (IPv6 == 65575 bytes, IPv4 = 65535)
            final DatagramPacket packet=new DatagramPacket(receive_buf, receive_buf.length);

            while(thread != null && Thread.currentThread().equals(thread)) {
                try {

                    // solves Android ISSUE #24748 - DatagramPacket truncated UDP in ICS
                    if(is_android)
                        packet.setLength(receive_buf.length);

                    receiver_socket.receive(packet);
                    int len=packet.getLength();
                    if(len > receive_buf.length) {
                        if(log.isErrorEnabled())
                            log.error(Util.getMessage("SizeOfTheReceivedPacket"),len, receive_buf.length, receive_buf.length);
                    }

                    receive(new IpAddress(packet.getAddress(), packet.getPort()),
                            receive_buf, packet.getOffset(), len);
                }
                catch(SocketException sock_ex) {
                    if(receiver_socket.isClosed()) {
                        if(log.isDebugEnabled()) log.debug("receiver socket is closed, exception=" + sock_ex);
                        break;
                    }
                    log.error(Util.getMessage("FailedReceivingPacket"), sock_ex);
                }
                catch(Throwable ex) {
                    if(log.isErrorEnabled())
                        log.error(Util.getMessage("FailedReceivingPacket"), ex);
                }
            }
            if(log.isDebugEnabled()) log.debug(name + " thread terminated");
        }

        public String toString() {
            return receiver_socket != null? receiver_socket.getLocalSocketAddress().toString() : "null";
        }
    }
}
