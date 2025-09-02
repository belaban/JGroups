package org.jgroups.protocols;


import org.jgroups.Event;
import org.jgroups.Global;
import org.jgroups.PhysicalAddress;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.conf.AttributeType;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.SuppressLog;
import org.jgroups.util.Util;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.*;
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
 * <li> param mcast_addr - the multicast address to use; default is 239.8.8.8.
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
     * <ul>
     * <li>{@code IPTOS_LOWCOST (0x02)}, <b>decimal 2</b></li>
     * <li>{@code IPTOS_RELIABILITY (0x04)}, <b>decimal 4</b></li>
     * <li>{@code IPTOS_THROUGHPUT (0x08)}, <b>decimal 8</b></li>
     * <li>{@code IPTOS_LOWDELAY (0x10)}, <b>decimal 16</b></li>
     * </ul>
     */
    @Property(description="Traffic class for sending unicast and multicast datagrams")
    protected int tos; // valid values: 0, 2, 4, 8, 16

    protected static final String UCAST_NAME="ucast-receiver";
    protected static final String MCAST_NAME="mcast-receiver";

    @Property(name="mcast_addr", description="The multicast address used for sending and receiving packets",
              defaultValueIPv4="239.8.8.8", defaultValueIPv6="ff0e::8:8:8",
              systemProperty=Global.UDP_MCAST_ADDR,writable=false)
    protected InetAddress mcast_group_addr;

    @Property(description="The multicast port used for sending and receiving packets. Default is 7600",
              systemProperty=Global.UDP_MCAST_PORT, writable=false)
    protected int mcast_port=7600;

    @Property(description="Multicast toggle. If false multiple unicast datagrams are sent instead of one multicast. " +
            "Default is true", writable=false)
    protected boolean ip_mcast=true;

    @Property(description="The time-to-live (TTL) for multicast datagram packets. Default is 8",systemProperty=Global.UDP_IP_TTL)
    protected int ip_ttl=8;

    @Property(description="Send buffer size of the multicast datagram socket",type=AttributeType.BYTES)
    protected int mcast_send_buf_size=(Global.MAX_DATAGRAM_PACKET_SIZE + MSG_OVERHEAD) * 100;

    @Property(description="Receive buffer size of the multicast datagram socket",type=AttributeType.BYTES)
    protected int mcast_recv_buf_size=5_000_000;

    @Property(description="Send buffer size of the unicast datagram socket",type=AttributeType.BYTES)
    protected int ucast_send_buf_size=200_000;

    @Property(description="Receive buffer size of the unicast datagram socket",type=AttributeType.BYTES)
    protected int ucast_recv_buf_size=5_000_000;

    @Property(description="If true, disables IP_MULTICAST_LOOP on the MulticastSocket (for sending and receiving of " +
      "multicast packets). IP multicast packets send on a host P will therefore not be received by anyone on P. Use with caution.")
    protected boolean disable_loopback=false;

    @Property(description="Suppresses warnings on Mac OS (for now) about not enough buffer space when sending " +
      "a datagram packet",type=AttributeType.TIME)
    protected long suppress_time_out_of_buffer_space=60000;

    protected int unicast_receiver_threads=1;
    protected int multicast_receiver_threads=1;


    /* --------------------------------------------- Fields ------------------------------------------------ */



    /** The multicast address (mcast address and port) this member uses */
    protected IpAddress       mcast_addr;

    /**
     * Socket used for
     * <ol>
     * <li>sending unicast and multicast packets and
     * <li>receiving unicast packets
     * </ol>
     * The address of this socket will be our local address (local_addr)
     */
    protected MulticastSocket  sock;

    /** IP multicast socket for <em>receiving</em> multicast packets */
    protected MulticastSocket   mcast_sock;

    /** Runnable to receive multicast packets */
    protected PacketReceiver[]  mcast_receivers;

    /** Runnable to receive unicast packets */
    protected PacketReceiver[]  ucast_receivers;

    protected SuppressLog<InetAddress> suppress_log_out_of_buffer_space;

    protected static final boolean is_android, is_mac;


    static  {
        is_android=Util.checkForAndroid();
        is_mac=Util.checkForMac();
    }



    public boolean           supportsMulticasting()             {return ip_mcast;}
    public <T extends UDP> T setMulticasting(boolean fl)        {this.ip_mcast=fl; return (T)this;}
    public <T extends UDP> T setMulticastAddress(InetAddress a) {this.mcast_group_addr=a; return (T)this;}
    public InetAddress       getMulticastAddress()              {return mcast_group_addr;}
    public int               getMulticastPort()                 {return mcast_port;}
    public <T extends UDP> T setMulticastPort(int mcast_port)   {this.mcast_port=mcast_port; return (T)this;}

    public int getTos() {return tos;}
    public UDP setTos(int t) {this.tos=t; return this;}

    public InetAddress getMcastGroupAddr() {return mcast_group_addr;}
    public UDP setMcastGroupAddr(InetAddress m) {this.mcast_group_addr=m; return this;}

    public boolean ipMcast() {return ip_mcast;}
    public UDP ipMcast(boolean i) {this.ip_mcast=i; return this;}

    public int getIpTTL()      {return ip_ttl;}
    public UDP setIpTTL(int i) {this.ip_ttl=i; return this;}

    public int getMcastSendBufSize() {return mcast_send_buf_size;}
    public UDP setMcastSendBufSize(int m) {this.mcast_send_buf_size=m; return this;}

    public int getMcastRecvBufSize() {return mcast_recv_buf_size;}
    public UDP setMcastRecvBufSize(int m) {this.mcast_recv_buf_size=m; return this;}

    public int getUcastSendBufSize() {return ucast_send_buf_size;}
    public UDP setUcastSendBufSize(int u) {this.ucast_send_buf_size=u; return this;}

    public int getUcastRecvBufSize() {return ucast_recv_buf_size;}
    public UDP setUcastRecvBufSize(int u) {this.ucast_recv_buf_size=u; return this;}

    public boolean disableLoopback() {return disable_loopback;}
    public UDP disableLoopback(boolean d) {this.disable_loopback=d; return this;}

    public long getSuppressTimeOutOfBufferSpace() {return suppress_time_out_of_buffer_space;}
    public UDP setSuppressTimeOutOfBufferSpace(long s) {this.suppress_time_out_of_buffer_space=s; return this;}


    /**
     * Set the ttl for multicast socket
     * @param ttl the time to live for the socket.
     */
    public <T extends UDP> T setMulticastTTL(int ttl) {
        this.ip_ttl=ttl;
        setTimeToLive(ttl, sock);
        return (T)this;
    }

    public int getMulticastTTL() {
        return ip_ttl;
    }


    @ManagedAttribute(description="Number of messages dropped when sending because of insufficient buffer space"
      ,type=AttributeType.SCALAR)
    public int getDroppedMessages() {
        return suppress_log_out_of_buffer_space != null? suppress_log_out_of_buffer_space.getCache().size() : 0;
    }

    @ManagedOperation(description="Clears the cache for dropped messages")
    public <T extends UDP> T clearDroppedMessagesCache() {
        if(suppress_log_out_of_buffer_space != null)
            suppress_log_out_of_buffer_space.getCache().clear();
        return (T)this;
    }

    @Property(description="Number of unicast receiver threads, all reading from the same DatagramSocket. " +
      "If de-serialization is slow, increasing the number of receiver threads might yield better performance.")
    public <T extends UDP> T setUcastReceiverThreads(int num) {
        if(unicast_receiver_threads != num) {
            unicast_receiver_threads=num;
            if(ucast_receivers != null) {
                stopUcastReceiverThreads();
                ucast_receivers=createReceivers(unicast_receiver_threads, sock, UCAST_NAME);
                startUcastReceiverThreads();
            }
        }
        return (T)this;
    }

    @Property(description="Number of unicast receiver threads, all reading from the same DatagramSocket. " +
      "If de-serialization is slow, increasing the number of receiver threads might yield better performance.")
    public int getUcastReceiverThreads() {
        return unicast_receiver_threads;
    }

    @Property(description="Number of multicast receiver threads, all reading from the same MulticastSocket. " +
          "If de-serialization is slow, increasing the number of receiver threads might yield better performance.")
    public <T extends UDP> T setMcastReceiverThreads(int num) {
        if(multicast_receiver_threads != num) {
            multicast_receiver_threads=num;
            if(mcast_receivers != null) {
                stopMcastReceiverThreads();
                mcast_receivers=createReceivers(multicast_receiver_threads, mcast_sock, MCAST_NAME);
                startMcastReceiverThreads();
            }
        }
        return (T)this;
    }

    @Property(description="Number of multicast receiver threads, all reading from the same MulticastSocket. " +
      "If de-serialization is slow, increasing the number of receiver threads might yield better performance.")
    public int getMcastReceiverThreads() {
        return multicast_receiver_threads;
    }

    public String getInfo() {
        return String.format("group_addr=%s:%d\n", mcast_group_addr.getHostName(), mcast_port);
    }

    @Override
    public void sendToAll(byte[] data, int offset, int length) throws Exception {
        if(ip_mcast && mcast_addr != null) {
            if(local_transport != null) {
                try {
                    local_transport.sendToAll(data, offset, length);
                }
                catch(Exception ex) {
                    log.warn("failed sending group message via local transport, sending it via regular transport", ex);
                }
            }
            _send(mcast_addr.getIpAddress(), mcast_addr.getPort(), data, offset, length);
        }
        else
            super.sendToAll(data, offset, length);
    }

    public void sendUnicast(PhysicalAddress dest, byte[] data, int offset, int length) throws Exception {
        _send(((IpAddress)dest).getIpAddress(), ((IpAddress)dest).getPort(), data, offset, length);
    }


    protected void _send(InetAddress dest, int port, byte[] data, int offset, int length) throws Exception {
        DatagramPacket packet=new DatagramPacket(data, offset, length, dest, port);
        // using the datagram socket to send multicasts or unicasts (https://issues.redhat.com/browse/JGRP-1765)
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
    public Object down(Event evt) {
        Object retval=super.down(evt);
        if(evt.getType() == Event.VIEW_CHANGE) {
            if(suppress_log_out_of_buffer_space != null)
                suppress_log_out_of_buffer_space.removeExpired(suppress_time_out_of_buffer_space);
            if(local_transport != null) {
                try {
                    // if we have local members, we send the multicast through the local transport, and do *not* need
                    // to receive a copy on the local host
                    sock.setLoopbackMode(true);
                    mcast_sock.setLoopbackMode(true);
                }
                catch(SocketException e) {
                    log.error("failed enabling loopback-mode to", e);
                }
            }
        }
        return retval;
    }

    public void init() throws Exception {
        super.init();
        if(bundler.getMaxSize() > Global.MAX_DATAGRAM_PACKET_SIZE)
            throw new IllegalArgumentException("bundler.max_size (" + bundler.getMaxSize() + ") cannot exceed the max " +
                                                 "datagram packet size of " + Global.MAX_DATAGRAM_PACKET_SIZE);
        if(is_mac && suppress_time_out_of_buffer_space > 0)
            suppress_log_out_of_buffer_space=new SuppressLog<>(log, "FailureSendingToPhysAddr");
    }

    /** Creates the unicast and multicast sockets and starts the unicast and multicast receiver threads */
    public void start() throws Exception {
        try {
            createSockets();
            super.start();
        }
        catch(Exception ex) {
            destroySockets();
            throw ex;
        }
        ucast_receivers=createReceivers(unicast_receiver_threads, sock, UCAST_NAME);
        if(ip_mcast)
            mcast_receivers=createReceivers(multicast_receiver_threads, mcast_sock, MCAST_NAME);
    }


    public void stop() {
        super.stop();
        log.debug("%s: closing sockets and stopping threads", local_addr);
        destroySockets();
        stopThreads();
    }

    protected void handleConnect() throws Exception {
        startThreads();
    }

    protected void setCorrectSocketBufferSize(MulticastSocket s, int buf_size, int new_size, boolean send, boolean mcast)
      throws SocketException {
        String so=String.format("%s%s", mcast? "mcast ": "", send? "send":"receive");
        log.warn("%s: setting %s socket buffer size (%s) to %s (size of the max datagram packet)",
                 local_addr, so, Util.printBytes(buf_size), Util.printBytes(new_size));
        if(send)
            s.setSendBufferSize(new_size);
        else
            s.setReceiveBufferSize(new_size);
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
            throw new IllegalArgumentException("bind_addr cannot be null");

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
            // https://issues.redhat.com/browse/JGRP-777 - this doesn't work on MacOS, and we don't have
            // cross talking on Windows anyway, so we just do it for Linux. (How about Solaris ?)

            // If possible, the MulticastSocket(SocketAddress) ctor is used which binds to mcast_addr:mcast_port.
            // This acts like a filter, dropping multicasts to different multicast addresses
            if(Util.can_bind_to_mcast_addr)
                mcast_sock=Util.createMulticastSocket(getSocketFactory(), "jgroups.udp.mcast_sock", mcast_group_addr, mcast_port, log);
            else
                mcast_sock=getSocketFactory().createMulticastSocket("jgroups.udp.mcast_sock", mcast_port);

            if(disable_loopback) {
                mcast_sock.setLoopbackMode(true);
                sock.setLoopbackMode(true);
            }

            mcast_addr=new IpAddress(mcast_group_addr, mcast_port);

            // check that we're not using the same mcast address and port as the diagnostics socket
            if(diag_handler.isEnabled() && diag_handler.getMcastAddress().equals(mcast_group_addr)
              && diag_handler.getPort() == mcast_port)
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
                joinGroupOnInterfaces(interfaces, mcast_sock, mcast_addr.getIpAddress());
            }
            else {
                if(bind_addr != null)
                    setNetworkInterface(bind_addr, mcast_sock); // not strictly needed for receiving, only for sending of mcasts
                mcast_sock.joinGroup(new InetSocketAddress(mcast_group_addr, mcast_port),
                                     bind_addr == null? null : NetworkInterface.getByInetAddress(bind_addr));
            }
        }

        setBufferSizes();
        log.debug("socket information:\n%s", dumpSocketInfo());
    }


    protected void destroySockets() {
        closeMulticastSocket();
        closeUnicastSocket();
    }

    protected PacketReceiver[] createReceivers(int num, DatagramSocket sock, String name) {
        PacketReceiver[] receivers=new PacketReceiver[num];
        for(int i=0; i < num; i++)
            receivers[i]=new PacketReceiver(sock, name);
        return receivers;
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

    protected <T extends UDP> T setTimeToLive(int ttl, MulticastSocket s) {
        try {
            if(s != null)
                s.setTimeToLive(ttl);
        }
        catch(Throwable ex) { // just in case Windows throws an exception (DualStack impl, not implemented)
            log.error("failed setting ip_ttl to %d: %s", ttl, ex);
        }
        return (T)this;
    }

    protected <T extends UDP> T setNetworkInterface(InetAddress addr, MulticastSocket s) {
        NetworkInterface intf=null;
        try {
            if(s != null && addr != null) {
                intf=NetworkInterface.getByInetAddress(addr);
                s.setNetworkInterface(intf);
                if (log.isDebugEnabled()) {
                    log.debug(String.format("multicast_socket on %s", intf.getName()));
                }
            }
        }
        catch(Throwable ex) {
            log.error("failed setting interface to %s (%s): %s", intf, addr, ex);
        }
        return (T)this;
    }


    protected PhysicalAddress getPhysicalAddress() {
        return createLocalAddress();
    }

    /**
     * Joins a multicast address on all interfaces
     * @param interfaces The interfaces to join mcast_addr:mcast_port
     * @param s The MulticastSocket to join on
     * @param mcast_addr The multicast address to join
     */
    protected void joinGroupOnInterfaces(List<NetworkInterface> interfaces, MulticastSocket s, InetAddress mcast_addr) {
        SocketAddress tmp_mcast_addr=new InetSocketAddress(mcast_addr, mcast_port);
        for(NetworkInterface intf: interfaces) {

            //[ JGRP-680] - receive_on_all_interfaces requires every NIC to be configured
            try {
                s.joinGroup(tmp_mcast_addr, intf);
                log.debug("joined %s on %s", tmp_mcast_addr, intf.getName());
            }
            catch(IOException e) {
                log.warn(Util.getMessage("InterfaceJoinFailed"), tmp_mcast_addr, intf.getName());
            }
        }
    }



    /**
     * Creates a DatagramSocket when bind_port &gt; 0. Attempts to allocate the socket with port == bind_port, and
     * increments until it finds a valid port, or until port_range has been exceeded
     * @return DatagramSocket The newly created socket
     * @throws Exception
     */
    protected MulticastSocket createMulticastSocketWithBindPort() throws Exception {
        MulticastSocket tmp=null;
        Exception saved_exception=null;
        // 27-6-2003 bgooren, find available port in range (start_port, start_port+port_range)
        int rcv_port=bind_port, max_port=bind_port + port_range;
        while(rcv_port <= max_port) {
            try {
                return createMulticastSocket("jgroups.udp.sock", rcv_port);
            }
            catch(SocketException | SecurityException bind_ex) {	// Cannot listen on this port
                rcv_port++;
                saved_exception=bind_ex;
            }
        }

        // Cannot listen at all, throw an Exception
        if(rcv_port >= max_port + 1) // +1 due to the increment above
            throw new Exception(String.format("failed to open a port in range %d-%d (last exception: %s)",
                                              bind_port, max_port, saved_exception));
        return tmp;
    }

    protected MulticastSocket createMulticastSocket(String service_name, int port) throws Exception {
        // Creates an *unbound* multicast socket (because SocketAddress is null)
        MulticastSocket retval=getSocketFactory().createMulticastSocket(service_name, null); // causes *no* binding !
        if(bind_addr != null)
            setNetworkInterface(bind_addr, retval);
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
                             mcast_sock.getLocalAddress(), mcast_sock.getLocalPort(),
                             mcast_sock.getSendBufferSize(), mcast_sock.getReceiveBufferSize());
        NetworkInterface nic=bind_addr != null? NetworkInterface.getByInetAddress(bind_addr) : null;
        String nic_name=nic != null? nic.getName() : "n/a";
        if(bind_port > 0)
            formatter.format("\n%s: using network interface '%s' with port range '%s-%s'", bind_addr, nic_name, bind_port, (bind_port + port_range));
        else
            formatter.format("\n%s: using network interface '%s' to any (ephemeral) port", bind_addr, nic_name);
        return sb.toString();
    }


    void setBufferSizes() throws SocketException {
        if(sock != null) {
            setBufferSize(sock, ucast_send_buf_size, ucast_recv_buf_size);
            if(ucast_send_buf_size <= 0)
                ucast_send_buf_size=getBufferSize(sock, true);
            if(ucast_recv_buf_size <= 0)
                ucast_recv_buf_size=getBufferSize(sock, false);
        }

        if(mcast_sock != null) {
            setBufferSize(mcast_sock, mcast_send_buf_size, mcast_recv_buf_size);
            if(mcast_send_buf_size <= 0)
                mcast_send_buf_size=getBufferSize(mcast_sock, true);
            if(mcast_recv_buf_size <= 0)
                mcast_recv_buf_size=getBufferSize(mcast_sock, false);
        }

        int max_size=Global.MAX_DATAGRAM_PACKET_SIZE + MSG_OVERHEAD;
        if(sock != null) {
            if(sock.getSendBufferSize() < max_size)
                setCorrectSocketBufferSize(sock, sock.getSendBufferSize(), max_size, true, false);
            if(sock.getReceiveBufferSize() < max_size)
                setCorrectSocketBufferSize(sock, sock.getReceiveBufferSize(), max_size, false, false);
        }
        if(mcast_sock != null) {
            if(mcast_sock.getSendBufferSize() < max_size)
                setCorrectSocketBufferSize(mcast_sock, mcast_sock.getSendBufferSize(), max_size, true, true);
            if(mcast_sock.getReceiveBufferSize() < max_size)
                setCorrectSocketBufferSize(mcast_sock, mcast_sock.getReceiveBufferSize(), max_size, false, true);
        }
    }

    protected void setBufferSize(DatagramSocket sock, int send_buf_size, int recv_buf_size) {
        if(send_buf_size > 0) {
            try {
                sock.setSendBufferSize(send_buf_size);
                int actual_size=sock.getSendBufferSize();
                if(actual_size < send_buf_size && log.isWarnEnabled()) {
                    log.warn(Util.getMessage("IncorrectBufferSize"), "send", sock.getClass().getSimpleName(),
                             Util.printBytes(send_buf_size), Util.printBytes(actual_size));
                }
            }
            catch(Throwable ex) {
                log.warn(Util.getMessage("BufferSizeFailed"), "send", send_buf_size, sock, ex);
            }
        }
        if(recv_buf_size > 0) {
            try {
                sock.setReceiveBufferSize(recv_buf_size);
                int actual_size=sock.getReceiveBufferSize();
                if(actual_size < recv_buf_size && log.isWarnEnabled()) {
                    log.warn(Util.getMessage("IncorrectBufferSize"), "receive", sock.getClass().getSimpleName(),
                             Util.printBytes(recv_buf_size), Util.printBytes(actual_size));
                }
            }
            catch(Throwable ex){
                log.warn(Util.getMessage("BufferSizeFailed"), "receive", recv_buf_size, sock, ex);
            }
        }
    }

    protected static int getBufferSize(DatagramSocket s, boolean send) {
        try {
            return send? s.getSendBufferSize() : s.getReceiveBufferSize();
        }
        catch(SocketException e) {
            return 0;
        }
    }

    void closeMulticastSocket() {
        if(mcast_sock != null) {
            try {
                if(mcast_addr != null) {
                    SocketAddress addr=new InetSocketAddress(mcast_addr.getIpAddress(), mcast_addr.getPort());
                    mcast_sock.leaveGroup(addr, bind_addr == null? null : NetworkInterface.getByInetAddress(bind_addr));
                }
                getSocketFactory().close(mcast_sock); // this will cause the mcast receiver thread to break out of its loop
                mcast_sock=null;
                if(log.isDebugEnabled()) log.debug("%s: multicast socket closed", local_addr);
            }
            catch(IOException ignored) {
            }
            mcast_addr=null;
        }
    }


    protected void closeUnicastSocket() {
        getSocketFactory().close(sock);
    }


    protected void startThreads() throws Exception {
        startUcastReceiverThreads();
        startMcastReceiverThreads();
    }

    protected void startUcastReceiverThreads() {
        if(ucast_receivers != null)
            for(PacketReceiver r: ucast_receivers)
                r.start();
    }

    protected void startMcastReceiverThreads() {
        if(mcast_receivers != null)
            for(PacketReceiver r: mcast_receivers)
                r.start();
    }

    protected void stopThreads() {
        stopMcastReceiverThreads();
        stopUcastReceiverThreads();
    }

    protected void stopUcastReceiverThreads() {Util.close(ucast_receivers);}
    protected void stopMcastReceiverThreads() {Util.close(mcast_receivers);}

    protected void handleConfigEvent(Map<String,Object> map) throws SocketException {
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


    public class PacketReceiver implements Runnable, Closeable {
        private       Thread         thread;
        private final DatagramSocket receiver_socket;
        private final String         name;

        public PacketReceiver(DatagramSocket socket, String name) {
            this.receiver_socket=socket;
            this.name=name;
        }

        public synchronized void start() {
            if(thread == null || !thread.isAlive()) {
                thread=getThreadFactory().newThread(this, name);
                thread.start();
            }
        }

        public void close() throws IOException {stop();}

        public synchronized void stop() {
            Thread tmp=thread;
            thread=null;

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
            final byte[]         receive_buf=new byte[66000]; // to be on the safe side (IPv6 == 65575 bytes, IPv4 = 65535)
            final DatagramPacket packet=new DatagramPacket(receive_buf, receive_buf.length);

            while(Thread.currentThread().equals(thread)) {
                try {
                    // solves Android ISSUE #24748 - DatagramPacket truncated UDP in ICS
                    if(is_android)
                        packet.setLength(receive_buf.length);

                    receiver_socket.receive(packet);
                    int len=packet.getLength();
                    if(len > receive_buf.length && log.isErrorEnabled())
                        log.error(Util.getMessage("SizeOfTheReceivedPacket"), len, receive_buf.length, receive_buf.length);
                    receive(new IpAddress(packet.getAddress(), packet.getPort()),
                            receive_buf, packet.getOffset(), len);
                }
                catch(SocketException sock_ex) {
                    if(receiver_socket.isClosed()) {
                        log.debug("%s: receiver socket is closed, exception=%s", local_addr, sock_ex.getMessage());
                        break;
                    }
                    log.error(Util.getMessage("FailedReceivingPacket"), sock_ex);
                }
                catch(Throwable ex) {
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
