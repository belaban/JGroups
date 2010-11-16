package org.jgroups.protocols;

import org.jgroups.Event;
import org.jgroups.Global;
import org.jgroups.Message;
import org.jgroups.annotations.Property;
import org.jgroups.annotations.DeprecatedProperty;
import org.jgroups.conf.PropertyConverters;
import org.jgroups.util.*;

import java.io.*;
import java.net.*;
import java.util.*;

/**
 * Uses its own IP multicast socket to send and receive discovery requests/responses. Can be used in
 * conjuntion with a non-UDP transport, e.g. TCP.<p>
 * The discovery is <em>assymetric</em>: discovery requests are broadcast via the multicast socket, and
 * received via the multicast socket by everyone in the group. However, the discovery responses are sent
 * back via the regular transport (e.g. TCP) to the sender (discovery request contained sender's regular address,
 * e.g. 192.168.0.2:7800).
 * @author Bela Ban
 */
@DeprecatedProperty(names="bind_to_all_interfaces")
public class MPING extends PING implements Runnable {
    
    private static final boolean can_bind_to_mcast_addr; // are we running on Linux ?


    static {
        can_bind_to_mcast_addr=Util.checkForLinux() || Util.checkForSolaris() || Util.checkForHp();
    }


    /* -----------------------------------------    Properties     -------------------------------------------------- */

    @Property(description="Bind address for multicast socket. " +
            "The following special values are also recognized: GLOBAL, SITE_LOCAL, LINK_LOCAL and NON_LOOPBACK",
              systemProperty={Global.BIND_ADDR, Global.BIND_ADDR_OLD},
              defaultValueIPv4=Global.NON_LOOPBACK_ADDRESS, defaultValueIPv6=Global.NON_LOOPBACK_ADDRESS)
    InetAddress bind_addr=null;
    
    @Property(name="bind_interface", converter=PropertyConverters.BindInterface.class, 
    		description="The interface (NIC) which should be used by this transport",dependsUpon="bind_addr")
    protected String bind_interface_str=null;
 

    @Property(description="Time to live for discovery packets. Default is 8", systemProperty=Global.MPING_IP_TTL)
    int ip_ttl=8;

    @Property(name="mcast_addr", systemProperty=Global.MPING_MCAST_ADDR,
              defaultValueIPv4="230.5.6.7", defaultValueIPv6="ff0e::5:6:7")
    InetAddress mcast_addr=null;


    @Property(description="Multicast port for discovery packets. Default is 7555", systemProperty=Global.MPING_MCAST_PORT)
    int mcast_port=7555;

    @Property(description="If true, the transport should use all available interfaces to receive multicast messages. Default is false")
    boolean receive_on_all_interfaces=false;

    /**
     * List<NetworkInterface> of interfaces to receive multicasts on. The
     * multicast receive socket will listen on all of these interfaces. This is
     * a comma-separated list of IP addresses or interface names. E.g.
     * "192.168.5.1,eth1,127.0.0.1". Duplicates are discarded; we only bind to
     * an interface once. If this property is set, it override
     * receive_on_all_interfaces.
     */
    @Property(converter=PropertyConverters.NetworkInterfaceList.class, description="List of interfaces to receive multicasts on")
    List<NetworkInterface> receive_interfaces=null;

    /**
     * If true, the transport should use all available interfaces to send
     * multicast messages. This means the same multicast message is sent N
     * times, so use with care
     */
    @Property(description="Whether send messages are sent on all interfaces. Default is false")
    boolean send_on_all_interfaces=false;

    /**
     * List<NetworkInterface> of interfaces to send multicasts on. The
     * multicast send socket will send the same multicast message on all of
     * these interfaces. This is a comma-separated list of IP addresses or
     * interface names. E.g. "192.168.5.1,eth1,127.0.0.1". Duplicates are
     * discarded. If this property is set, it override send_on_all_interfaces.
     */
    @Property(converter=PropertyConverters.NetworkInterfaceList.class, description="List of interfaces to send multicasts on")
    List<NetworkInterface> send_interfaces=null;
    
    
    
    /* --------------------------------------------- Fields ------------------------------------------------------ */

    
    private MulticastSocket mcast_sock=null;

    /**
     * If we have multiple mcast send sockets, e.g. send_interfaces or
     * send_on_all_interfaces enabled
     */
    private MulticastSocket[] mcast_send_sockets=null;
    
    private volatile Thread receiver=null;



    public MPING() {        
    }

    public InetAddress getBindAddr() {
        return bind_addr;
    }

    public void setBindAddr(InetAddress bind_addr) {
        this.bind_addr=bind_addr;
    }

    public List<NetworkInterface> getReceiveInterfaces() {
        return receive_interfaces;
    }

    public List<NetworkInterface> getSendInterfaces() {
        return send_interfaces;
    }

    public boolean isReceiveOnAllInterfaces() {
        return receive_on_all_interfaces;
    }

    public boolean isSendOnAllInterfaces() {
        return send_on_all_interfaces;
    }

    public int getTTL() {
        return ip_ttl;
    }

    public void setTTL(int ip_ttl) {
        this.ip_ttl=ip_ttl;
    }

    public InetAddress getMcastAddr() {
        return mcast_addr;
    }

    public void setMcastAddr(InetAddress mcast_addr) {
        this.mcast_addr=mcast_addr;
    }

    public void setMulticastAddress(String addr) throws UnknownHostException {
        mcast_addr=InetAddress.getByName(addr);
    }

    public int getMcastPort() {
        return mcast_port;
    }

    public void setMcastPort(int mcast_port) {
        this.mcast_port=mcast_port;
    }



    @SuppressWarnings("unchecked")
    public Object up(Event evt) {
        if(evt.getType() == Event.CONFIG) {
            if(bind_addr == null) {
                Map<String,Object> config=(Map<String,Object>)evt.getArg();
                bind_addr=(InetAddress)config.get("bind_addr");
            }
            return up_prot.up(evt);
        }
        return super.up(evt);
    }


    public void init() throws Exception {
        super.init();
        if(log.isDebugEnabled())
            log.debug("bind_addr=" + bind_addr + " mcast_addr=" + mcast_addr + ", mcast_port=" + mcast_port);
    }

    public void start() throws Exception {
        if(can_bind_to_mcast_addr) // https://jira.jboss.org/jira/browse/JGRP-836 - prevent cross talking on Linux
            mcast_sock=Util.createMulticastSocket(getSocketFactory(), Global.MPING_MCAST_SOCK, mcast_addr, mcast_port, log);
        else
            mcast_sock=getSocketFactory().createMulticastSocket(Global.MPING_MCAST_SOCK, mcast_port);
        
        mcast_sock.setTimeToLive(ip_ttl);

        if(receive_on_all_interfaces || (receive_interfaces != null && !receive_interfaces.isEmpty())) {
            List<NetworkInterface> interfaces;
            if(receive_interfaces != null)
                interfaces=receive_interfaces;
            else
                interfaces=Util.getAllAvailableInterfaces();
            bindToInterfaces(interfaces, mcast_sock, mcast_addr);
        }
        else {
            if(bind_addr != null)
                mcast_sock.setInterface(bind_addr);
            mcast_sock.joinGroup(mcast_addr);
        }


        // 3b. Create mcast sender socket
        if(send_on_all_interfaces || (send_interfaces != null && !send_interfaces.isEmpty())) {
            List interfaces;
            NetworkInterface intf;
            if(send_interfaces != null)
                interfaces=send_interfaces;
            else
                interfaces=Util.getAllAvailableInterfaces();
            mcast_send_sockets=new MulticastSocket[interfaces.size()];
            int index=0;
            for(Iterator it=interfaces.iterator(); it.hasNext();) {
                intf=(NetworkInterface)it.next();
                mcast_send_sockets[index]=new MulticastSocket();
                mcast_send_sockets[index].setNetworkInterface(intf);
                mcast_send_sockets[index].setTimeToLive(ip_ttl);
                index++;
            }
        }


        startReceiver();
        super.start();
    }


    private void bindToInterfaces(List<NetworkInterface> interfaces, MulticastSocket s, InetAddress mcast_addr) throws IOException {
        SocketAddress tmp_mcast_addr=new InetSocketAddress(mcast_addr, mcast_port);
        for(Iterator it=interfaces.iterator(); it.hasNext();) {
            NetworkInterface i=(NetworkInterface)it.next();
            for(Enumeration en2=i.getInetAddresses(); en2.hasMoreElements();) {
                InetAddress addr=(InetAddress)en2.nextElement();
                s.joinGroup(tmp_mcast_addr, i);
                if(log.isTraceEnabled())
                    log.trace("joined " + tmp_mcast_addr + " on " + i.getName() + " (" + addr + ")");
                break;
            }
        }
    }



    private void startReceiver() {
        if(receiver == null || !receiver.isAlive()) {
            receiver=new Thread(Util.getGlobalThreadGroup(), this, "ReceiverThread");
            receiver.setDaemon(true);
            receiver.start();
            if(log.isTraceEnabled())
                log.trace("receiver thread started");
        }
    }

    public void stop() {
        Util.close(mcast_sock);
        mcast_sock=null;
        receiver=null;
        super.stop();
    }

    void sendMcastDiscoveryRequest(Message msg) {
        Buffer           buf;
        DatagramPacket   packet;
        DataOutputStream out=null;

        try {
            if(msg.getSrc() == null)
                msg.setSrc(local_addr);
            ExposedByteArrayOutputStream out_stream=new ExposedByteArrayOutputStream(128);
            out=new DataOutputStream(out_stream);
            msg.writeTo(out);
            out.flush(); // flushes contents to out_stream
            buf=new Buffer(out_stream.getRawBuffer(), 0, out_stream.size());
            packet=new DatagramPacket(buf.getBuf(), buf.getOffset(), buf.getLength(), mcast_addr, mcast_port);
            discovery_reception.reset();
            if(mcast_send_sockets != null) {
                MulticastSocket s;
                for(int i=0; i < mcast_send_sockets.length; i++) {
                    s=mcast_send_sockets[i];
                    try {
                        s.send(packet);
                    }
                    catch(Exception e) {
                        log.error("failed sending packet on socket " + s);
                    }
                }
            }
            else { // DEFAULT path
                if(mcast_sock != null)
                    mcast_sock.send(packet);
            }
            waitForDiscoveryRequestReception();
        }
        catch(IOException ex) {
            log.error("failed sending discovery request", ex);
        }
        finally {
            Util.close(out);
        }
    }



    public void run() {
        final byte[]         receive_buf=new byte[65535];
        DatagramPacket       packet=new DatagramPacket(receive_buf, receive_buf.length);
        byte[]               data;
        ByteArrayInputStream inp_stream;
        DataInputStream      inp=null;
        Message              msg;

        while(mcast_sock != null && receiver != null && Thread.currentThread().equals(receiver)) {
            packet.setData(receive_buf, 0, receive_buf.length);
            try {
                mcast_sock.receive(packet);
                data=packet.getData();
                inp_stream=new ExposedByteArrayInputStream(data, 0, data.length);
                inp=new DataInputStream(inp_stream);
                msg=new Message();
                msg.readFrom(inp);
                up(new Event(Event.MSG, msg));
            }
            catch(SocketException socketEx) {
                break;
            }
            catch(Throwable ex) {
                log.error("failed receiving packet (from " + packet.getSocketAddress() + ")", ex);
            }
            finally {
                Util.close(inp);
            }
        }
        if(log.isTraceEnabled())
            log.trace("receiver thread terminated");
    }


}
