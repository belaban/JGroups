package org.jgroups.protocols;


import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.Message;
import org.jgroups.annotations.DeprecatedProperty;
import org.jgroups.annotations.Property;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.BoundedList;
import org.jgroups.util.Util;

import java.io.IOException;
import java.net.*;
import java.util.ArrayList;
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
 * on this socket; default is 32.
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
 * @version $Id: UDP.java,v 1.186 2008/09/26 16:07:03 belaban Exp $
 */
@DeprecatedProperty(names={"num_last_ports","null_src_addresses", "send_on_all_interfaces", "send_interfaces"})
public class UDP extends TP {

    
    /**
     * BoundedList<Integer> of the last 100 ports used. This is to avoid
     * reusing a port for DatagramSocket
     */
    private static final BoundedList<Integer> last_ports_used=new BoundedList<Integer>(100);
    
    private static final boolean is_linux; // are we running on Linux ?


    static {
        is_linux=Util.checkForLinux();
    }

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
    private int tos=8; // valid values: 2, 4, 8 (default), 16   

    @Property(name="mcast_addr", description="The multicast address used for sending and receiving packets. Default is 228.8.8.8")
    private String mcast_addr_name="228.8.8.8";

    @Property(description="The multicast port used for sending and receiving packets. Default is 7600")
    private int mcast_port=7600;

    @Property(description="Multicast toggle. If false multiple unicast datagrams are sent instead of one multicast. Default is tru")
    private boolean ip_mcast=true;

    @Property(description="The time-to-live (TTL) for multicast datagram packets. Default is 8")
    private int ip_ttl=8;

    @Property(description="Send buffer size of the multicast datagram socket. Default is 100'000 bytes")
    private int mcast_send_buf_size=100000;

    @Property(description="Receive buffer size of the multicast datagram socket. Default is 500'000 bytes")
    private int mcast_recv_buf_size=500000;

    @Property(description="Send buffer size of the unicast datagram socket. Default is 100'000 bytes")
    private int ucast_send_buf_size=100000;

    @Property(description="Receive buffer size of the unicast datagram socket. Default is 64'000 bytes")
    private int ucast_recv_buf_size=64000;

    
   
    
    /* --------------------------------------------- Fields ------------------------------------------------ */

    
    
    /** The multicast address (mcast address and port) this member uses */
    private IpAddress mcast_addr=null;

    /**
     * Socket used for
     * <ol>
     * <li>sending unicast and multicast packets and
     * <li>receiving unicast packets
     * </ol>
     * The address of this socket will be our local address (<tt>local_addr</tt>)
     */
    private DatagramSocket sock=null;

    /** IP multicast socket for <em>receiving</em> multicast packets */
    private MulticastSocket mcast_sock=null;

    /** Runnable to receive multicast packets */
    private PacketReceiver mcast_receiver=null;

    /** Runnable to receive unicast packets */
    private PacketReceiver ucast_receiver=null;  


    /**
     * Usually, src addresses are nulled, and the receiver simply sets them to
     * the address of the sender. However, for multiple addresses on a Windows
     * loopback device, this doesn't work (see
     * http://jira.jboss.com/jira/browse/JGRP-79 and the JGroups wiki for
     * details). This must be the same value for all members of the same group.
     * Default is true, for performance reasons
     */
    // private boolean null_src_addresses=true;



    /**
     * Creates the UDP protocol, and initializes the state variables, does however not start any sockets or threads.
     */
    public UDP() {
    }


    public void setMulticastAddress(String addr) {this.mcast_addr_name=addr;}
    public String getMulticastAddress() {return mcast_addr_name;}
    public int getMulticastPort() {return mcast_port;}
    public void setMulticastPort(int mcast_port) {this.mcast_port=mcast_port;}



    public String getInfo() {
        StringBuilder sb=new StringBuilder();
        sb.append("group_addr=").append(mcast_addr_name).append(':').append(mcast_port).append("\n");
        return sb.toString();
    }

    public void sendToAllMembers(byte[] data, int offset, int length) throws Exception {
        if(ip_mcast && mcast_addr != null) {
            _send(mcast_addr.getIpAddress(), mcast_addr.getPort(), true, data, offset, length);
        }
        else {
            List<Address> mbrs;
            synchronized(members) {
                mbrs=new ArrayList<Address>(members);
            }
            for(Address mbr: mbrs) {
                _send(((IpAddress)mbr).getIpAddress(), ((IpAddress)mbr).getPort(), false, data, offset, length);
            }
        }
    }

    public void sendToSingleMember(Address dest, byte[] data, int offset, int length) throws Exception {
        _send(((IpAddress)dest).getIpAddress(), ((IpAddress)dest).getPort(), false, data, offset, length);
    }


    public void postUnmarshalling(Message msg, Address dest, Address src, boolean multicast) {
         if(multicast)
            msg.setDest(null);
        else
            msg.setDest(dest);
    }

    public void postUnmarshallingList(Message msg, Address dest, boolean multicast) {
         if(multicast)
            msg.setDest(null);
        else
            msg.setDest(dest);
    }

    private void _send(InetAddress dest, int port, boolean mcast, byte[] data, int offset, int length) throws Exception {
        DatagramPacket packet=new DatagramPacket(data, offset, length, dest, port);
        try {
            if(mcast) {
                if(mcast_sock != null)
                    mcast_sock.send(packet);
            }
            else {
                if(sock != null)
                    sock.send(packet);
            }
        }
        catch(Exception ex) {
            throw new Exception("dest=" + dest + ":" + port + " (" + length + " bytes)", ex);
        }
    }


    /* ------------------------------------------------------------------------------- */



    /*------------------------------ Protocol interface ------------------------------ */

    public String getName() {
        return "UDP";
    }

    public void init() throws Exception {
        super.init();

        String str=Util.getProperty(new String[]{Global.UDP_MCAST_ADDR, "jboss.partition.udpGroup"},
                                    null, "mcast_addr", false, null);
        if(str != null)
            mcast_addr_name=str;

        str=Util.getProperty(new String[]{Global.UDP_MCAST_PORT, "jboss.partition.udpPort"},
                             null, "mcast_port", false, null);

        if(str != null)
            mcast_port=Integer.parseInt(str);

        str=Util.getProperty(new String[]{Global.UDP_IP_TTL}, null, "ip_ttl", false, null);
        if(str != null)
            ip_ttl=Integer.parseInt(str);

        Util.checkBufferSize("UDP.mcast_send_buf_size", mcast_send_buf_size);
        Util.checkBufferSize("UDP.mcast_recv_buf_size", mcast_recv_buf_size);
        Util.checkBufferSize("UDP.ucast_send_buf_size", ucast_send_buf_size);
        Util.checkBufferSize("UDP.ucast_recv_buf_size", ucast_recv_buf_size);
    }




    /**
     * Creates the unicast and multicast sockets and starts the unicast and multicast receiver threads
     */
    public void start() throws Exception {
        if(log.isDebugEnabled()) log.debug("creating sockets and starting threads");
        try {
            createSockets();
        }
        catch(Exception ex) {
            String tmp="problem creating sockets (bind_addr=" + bind_addr + ", mcast_addr=" + mcast_addr + ")";
            throw new Exception(tmp, ex);
        }
        super.start();

        ucast_receiver=new PacketReceiver(sock,
                                          local_addr,
                                          "unicast receiver",
                                          new Runnable() {
                                              public void run() {
                                                  closeUnicastSocket();
                                              }
                                          });

        if(ip_mcast)
            mcast_receiver=new PacketReceiver(mcast_sock,
                                              mcast_addr,
                                              "multicast receiver",
                                              new Runnable() {
                                                  public void run() {
                                                      closeMulticastSocket();
                                                  }
                                              });
    }


    public void stop() {
        if(log.isDebugEnabled()) log.debug("closing sockets and stopping threads");
        stopThreads();  // will close sockets, closeSockets() is not really needed anymore, but...
        super.stop();
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





    /**
     * Create UDP sender and receiver sockets. Currently there are 2 sockets
     * (sending and receiving). This is due to Linux's non-BSD compatibility
     * in the JDK port (see DESIGN).
     */
    private void createSockets() throws Exception {
        InetAddress tmp_addr;

        // bind_addr not set, try to assign one by default. This is needed on Windows

        // changed by bela Feb 12 2003: by default multicast sockets will be bound to all network interfaces

        // CHANGED *BACK* by bela March 13 2003: binding to all interfaces did not result in a correct
        // local_addr. As a matter of fact, comparison between e.g. 0.0.0.0:1234 (on hostA) and
        // 0.0.0.0:1.2.3.4 (on hostB) would fail !
//        if(bind_addr == null) {
//            InetAddress[] interfaces=InetAddress.getAllByName(InetAddress.getLocalHost().getHostAddress());
//            if(interfaces != null && interfaces.length > 0)
//                bind_addr=interfaces[0];
//        }

        if(bind_addr == null && !use_local_host) {
            bind_addr=Util.getFirstNonLoopbackAddress();
        }
        if(bind_addr == null)
            bind_addr=InetAddress.getLocalHost();

        if(bind_addr != null)
            if(log.isDebugEnabled()) log.debug("sockets will use interface " + bind_addr.getHostAddress());


        // 2. Create socket for receiving unicast UDP packets. The address and port
        //    of this socket will be our local address (local_addr)
        if(bind_port > 0) {
            sock=createDatagramSocketWithBindPort();
        }
        else {
            DatagramSocket tmp_sock=null;
            if(prevent_port_reuse) {
                tmp_sock=new DatagramSocket(0, bind_addr);
            }
            try {
                sock=createEphemeralDatagramSocket();
            }
            finally {
                Util.close(tmp_sock);
            }
        }
        if(tos > 0) {
            try {
                sock.setTrafficClass(tos);
            }
            catch(SocketException e) {
                log.warn("traffic class of " + tos + " could not be set, will be ignored");
                if(log.isDebugEnabled())
                    log.debug("Cause of failure to set traffic class:", e);
            }
        }

        if(sock == null)
            throw new Exception("UDP.createSocket(): sock is null");

        local_addr=createLocalAddress();
        if(additional_data != null)
            ((IpAddress)local_addr).setAdditionalData(additional_data);


        // 3. Create socket for receiving IP multicast packets
        if(ip_mcast) {
            // 3a. Create mcast receiver socket
            tmp_addr=InetAddress.getByName(mcast_addr_name);

            // https://jira.jboss.org/jira/browse/JGRP-777 - this doesn't work on MacOS, and we don't have
            // cross talking on Windows anyway, so we just do it for Linux. (How about Solaris ?)
            if(is_linux)
                mcast_sock=Util.createMulticastSocket(tmp_addr, mcast_port, log);
            else
                mcast_sock=new MulticastSocket(mcast_port);

            mcast_sock.setTimeToLive(ip_ttl);

            mcast_addr=new IpAddress(tmp_addr, mcast_port);
            if(tos > 0) {
                try {
                    mcast_sock.setTrafficClass(tos);
                }
                catch(SocketException e) {
                    log.warn("traffic class of " + tos + " could not be set, will be ignored", e);
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
                    mcast_sock.setInterface(bind_addr);
                 mcast_sock.joinGroup(tmp_addr);
            }
        }

        setBufferSizes();
        if(log.isDebugEnabled()) log.debug("socket information:\n" + dumpSocketInfo());
    }


    protected Address createLocalAddress() {
        return new IpAddress(sock.getLocalAddress(), sock.getLocalPort());
    }



    /**
     *
     * @param interfaces List<NetworkInterface>. Guaranteed to have no duplicates
     * @param s
     * @param mcastAddr
     * @throws IOException
     */
    private void bindToInterfaces(List<NetworkInterface> interfaces,
                                  MulticastSocket s,
                                  InetAddress mcastAddr) {
        SocketAddress tmp_mcast_addr=new InetSocketAddress(mcastAddr, mcast_port);
        for(NetworkInterface intf:interfaces) {

            //[ JGRP-680] - receive_on_all_interfaces requires every NIC to be configured
            try {
                s.joinGroup(tmp_mcast_addr, intf);
                if(log.isTraceEnabled())
                    log.trace("joined " + tmp_mcast_addr + " on " + intf.getName());
            }
            catch(IOException e) {
                if(log.isWarnEnabled())
                    log.warn("Could not join " + tmp_mcast_addr + " on interface " + intf.getName());
            }
        }
    }



    /** Creates a DatagramSocket with a random port. Because in certain operating systems, ports are reused,
     * we keep a list of the n last used ports, and avoid port reuse */
    protected DatagramSocket createEphemeralDatagramSocket() throws SocketException {
        DatagramSocket tmp;
        int localPort=0;
        while(true) {
            try {
                tmp=new DatagramSocket(localPort, bind_addr); // first time localPort is 0
            }
            catch(SocketException socket_ex) {
                // Vladimir May 30th 2007
                // special handling for Linux 2.6 kernel which sometimes throws BindException while we probe for a random port
                localPort++;
                continue;
            }
            localPort=tmp.getLocalPort();
            if(last_ports_used.contains(localPort)) {
                if(log.isDebugEnabled())
                    log.debug("local port " + localPort + " already seen in this session; will try to get other port");
                try {tmp.close();} catch(Throwable e) {}
                localPort++;
            }
            else {
                last_ports_used.add(localPort);
                break;
            }
        }
        return tmp;
    }




    /**
     * Creates a DatagramSocket when bind_port > 0. Attempts to allocate the socket with port == bind_port, and
     * increments until it finds a valid port, or until port_range has been exceeded
     * @return DatagramSocket The newly created socket
     * @throws Exception
     */
    protected DatagramSocket createDatagramSocketWithBindPort() throws Exception {
        DatagramSocket tmp=null;
        // 27-6-2003 bgooren, find available port in range (start_port, start_port+port_range)
        int rcv_port=bind_port, max_port=bind_port + port_range;
        if(pm != null && bind_port > 0) {
            rcv_port=pm.getNextAvailablePort(rcv_port);
        }
        while(rcv_port <= max_port) {
            try {
                tmp=new DatagramSocket(rcv_port, bind_addr);
                return tmp;
            }
            catch(SocketException bind_ex) {	// Cannot listen on this port
                rcv_port++;
            }
            catch(SecurityException sec_ex) { // Not allowed to listen on this port
                rcv_port++;
            }
        }

        // Cannot listen at all, throw an Exception
        if(rcv_port >= max_port + 1) { // +1 due to the increment above
            throw new Exception("failed to open a port in range " + bind_port + '-' + max_port);
        }
        return tmp;
    }


    private String dumpSocketInfo() throws Exception {
        StringBuilder sb=new StringBuilder(128);
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

        if(mcast_sock != null) {
            sb.append("\nmcast_sock: bound to ");
            sb.append(mcast_sock.getInterface().getHostAddress()).append(':').append(mcast_sock.getLocalPort());
            sb.append(", send buffer size=").append(mcast_sock.getSendBufferSize());
            sb.append(", receive buffer size=").append(mcast_sock.getReceiveBufferSize());
        }
        return sb.toString();
    }


    void setBufferSizes() {
        if(sock != null)
            setBufferSize(sock, ucast_send_buf_size, ucast_recv_buf_size);

        if(mcast_sock != null)
            setBufferSize(mcast_sock, mcast_send_buf_size, mcast_recv_buf_size);
    }

    private void setBufferSize(DatagramSocket sock, int send_buf_size, int recv_buf_size) {
        try {
            sock.setSendBufferSize(send_buf_size);
        }
        catch(Throwable ex) {
            if(log.isWarnEnabled()) log.warn("failed setting send buffer size of " + send_buf_size + " in " + sock + ": " + ex);
        }

        try {
            sock.setReceiveBufferSize(recv_buf_size);
        }
        catch(Throwable ex) {
            if(log.isWarnEnabled()) log.warn("failed setting receive buffer size of " + recv_buf_size + " in " + sock + ": " + ex);
        }
    }



    void closeMulticastSocket() {
        if(mcast_sock != null) {
            try {
                if(mcast_addr != null) {
                    mcast_sock.leaveGroup(mcast_addr.getIpAddress());
                }
                mcast_sock.close(); // this will cause the mcast receiver thread to break out of its loop
                mcast_sock=null;
                if(log.isDebugEnabled()) log.debug("multicast socket closed");
            }
            catch(IOException ex) {
            }
            mcast_addr=null;
        }
    }


    private void closeUnicastSocket() {
        if(sock != null) {
            if(pm != null && bind_port > 0) {
                int port=local_addr != null? ((IpAddress)local_addr).getPort() : sock.getLocalPort();
                pm.removePort(port);
            }
            sock.close();
            sock=null;
            if(log.isDebugEnabled()) log.debug("socket closed");
        }
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
        ucast_receiver.stop();
    }


    protected void handleConfigEvent(Map<String,Object> map) {
        boolean set_buffers=false;
        super.handleConfigEvent(map);
        if(map == null) return;

        if(map.containsKey("send_buf_size")) {
            mcast_send_buf_size=((Integer)map.get("send_buf_size")).intValue();
            ucast_send_buf_size=mcast_send_buf_size;
            set_buffers=true;
        }
        if(map.containsKey("recv_buf_size")) {
            mcast_recv_buf_size=((Integer)map.get("recv_buf_size")).intValue();
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
        private final Address        dest;
        private final String         name;
        private final Runnable       close_strategy;

        public PacketReceiver(DatagramSocket socket, Address dest, String name, Runnable close_strategy) {
            this.receiver_socket=socket;
            this.dest=dest;
            this.name=name;
            this.close_strategy=close_strategy;
        }

        public synchronized void start() {
            if(thread == null || !thread.isAlive()) {
                thread=getThreadFactory().newThread(this, name);
                thread.start();
                if(log.isDebugEnabled())
                    log.debug("created " + name + " thread ");
            }
        }

        public synchronized void stop() {
            try {
                close_strategy.run();
            }
            catch(Exception e1) {
            }
            finally {
                Util.close(receiver_socket); // second line of defense
            }

            if(thread != null && thread.isAlive()) {
                Thread tmp=thread;
                thread=null;
                tmp.interrupt();
                try {
                    tmp.join(Global.THREAD_SHUTDOWN_WAIT_TIME);
                }
                catch(InterruptedException e) {
                    Thread.currentThread().interrupt(); // set interrupt flag again
                }
            }
            thread=null;
        }


        public void run() {
            final byte           receive_buf[]=new byte[65535];
            final DatagramPacket packet=new DatagramPacket(receive_buf, receive_buf.length);

            while(thread != null && Thread.currentThread().equals(thread)) {
                try {
                    receiver_socket.receive(packet);
                    int len=packet.getLength();
                    if(len > receive_buf.length) {
                        if(log.isErrorEnabled())
                            log.error("size of the received packet (" + len + ") is bigger than allocated buffer (" +
                                      receive_buf.length + "): will not be able to handle packet. " +
                                      "Use the FRAG2 protocol and make its frag_size lower than " + receive_buf.length);
                    }

                    receive(dest,
                            new IpAddress(packet.getAddress(), packet.getPort()),
                            receive_buf,
                            packet.getOffset(),
                            len);
                }
                catch(SocketException sock_ex) {
                    if(log.isDebugEnabled()) log.debug("unicast receiver socket is closed, exception=" + sock_ex);
                    break;
                }
                catch(Throwable ex) {
                    if(log.isErrorEnabled())
                        log.error("[" + local_addr + "] failed receiving unicast packet", ex);
                }
            }
            if(log.isDebugEnabled()) log.debug(name + " thread terminated");
        }

        public String toString() {
            return receiver_socket != null? receiver_socket.getLocalSocketAddress().toString() : "null";
        }
    }
}
