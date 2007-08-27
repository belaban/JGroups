package org.jgroups.protocols;

import org.jgroups.Event;
import org.jgroups.Global;
import org.jgroups.Message;
import org.jgroups.util.Buffer;
import org.jgroups.util.ExposedByteArrayOutputStream;
import org.jgroups.util.Util;

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
 * @version $Id: MPING.java,v 1.29 2007/08/27 08:09:19 belaban Exp $
 */
public class MPING extends PING implements Runnable {
    MulticastSocket        mcast_sock=null;

    /** If we have multiple mcast send sockets, e.g. send_interfaces or send_on_all_interfaces enabled */
    MulticastSocket[]      mcast_send_sockets=null;
    Thread                 receiver=null;
    InetAddress            bind_addr=null;
    int                    ip_ttl=8;
    InetAddress            mcast_addr=null;
    int                    mcast_port=7555;

     /** If true, the transport should use all available interfaces to receive multicast messages */
    boolean                receive_on_all_interfaces=false;

     /** List<NetworkInterface> of interfaces to receive multicasts on. The multicast receive socket will listen
     * on all of these interfaces. This is a comma-separated list of IP addresses or interface names. E.g.
     * "192.168.5.1,eth1,127.0.0.1". Duplicates are discarded; we only bind to an interface once.
     * If this property is set, it override receive_on_all_interfaces.
     */
    List<NetworkInterface> receive_interfaces=null;

    /** If true, the transport should use all available interfaces to send multicast messages. This means
     * the same multicast message is sent N times, so use with care */
    boolean                send_on_all_interfaces=false;

    /** List<NetworkInterface> of interfaces to send multicasts on. The multicast send socket will send the
     * same multicast message on all of these interfaces. This is a comma-separated list of IP addresses or
     * interface names. E.g. "192.168.5.1,eth1,127.0.0.1". Duplicates are discarded.
     * If this property is set, it override send_on_all_interfaces.
     */
    List<NetworkInterface> send_interfaces=null;

    /** Pre-allocated byte stream. Used for serializing datagram packets. Will grow as needed */
    final ExposedByteArrayOutputStream out_stream=new ExposedByteArrayOutputStream(128);
    byte                receive_buf[]=new byte[1024];


    public String getName() {
        return "MPING";
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

    public int getMcastPort() {
        return mcast_port;
    }

    public void setMcastPort(int mcast_port) {
        this.mcast_port=mcast_port;
    }


    public boolean setProperties(Properties props) {
        String str;

        try {
            bind_addr=Util.getBindAddress(props);
        }
        catch(UnknownHostException unknown) {
            log.fatal("failed getting bind_addr", unknown);
            return false;
        }
        catch(SocketException ex) {
            log.fatal("failed getting bind_addr", ex);
            return false;
        }

        str=Util.getProperty(new String[]{Global.MPING_MCAST_ADDR}, props, "mcast_addr", false, "230.5.6.7");
        if(str != null) {
            try {
                mcast_addr=InetAddress.getByName(str);
            }
            catch(UnknownHostException e) {
                log.error("could not resolve " + str, e);
                return false;
            }
            props.remove("mcast_addr");
        }

        str=Util.getProperty(new String[]{Global.MPING_MCAST_PORT}, props, "mcast_port", false, "7555");
        if(str != null) {
            mcast_port=Integer.parseInt(str);
            props.remove("mcast_port");
        }

        str=Util.getProperty(new String[]{Global.MPING_IP_TTL}, props, "ip_ttl", false, "16");
        if(str != null) {
            ip_ttl=Integer.parseInt(str);
            props.remove("ip_ttl");
        }

        str=props.getProperty("bind_to_all_interfaces");
        if(str != null) {
            receive_on_all_interfaces=Boolean.parseBoolean(str);
            props.remove("bind_to_all_interfaces");
            log.warn("bind_to_all_interfaces has been deprecated; use receive_on_all_interfaces instead");
            props.remove("bind_to_all_interfaces");
        }

        str=props.getProperty("receive_on_all_interfaces");
        if(str != null) {
            receive_on_all_interfaces=Boolean.parseBoolean(str);
            props.remove("receive_on_all_interfaces");
        }

        str=props.getProperty("receive_interfaces");
        if(str != null) {
            try {
                receive_interfaces=Util.parseInterfaceList(str);
                props.remove("receive_interfaces");
            }
            catch(Exception e) {
                log.error("error determining interfaces (" + str + ")", e);
                return false;
            }
        }

        str=props.getProperty("send_on_all_interfaces");
        if(str != null) {
            send_on_all_interfaces=Boolean.parseBoolean(str);
            props.remove("send_on_all_interfaces");
        }

        str=props.getProperty("send_interfaces");
        if(str != null) {
            try {
                send_interfaces=Util.parseInterfaceList(str);
                props.remove("send_interfaces");
            }
            catch(Exception e) {
                log.error("error determining interfaces (" + str + ")", e);
                return false;
            }
        }

        if(mcast_addr == null) {
            try {
                mcast_addr=InetAddress.getByName("230.5.6.7");
            }
            catch(UnknownHostException e) {
                log.error("failed getting default mcast address", e);
                return false;
            }
        }
        return super.setProperties(props);
    }


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


    public void start() throws Exception {
        mcast_sock=new MulticastSocket(mcast_port);
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
        mcast_sock.close();
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
            out_stream.reset();
            out=new DataOutputStream(out_stream);
            msg.writeTo(out);
            out.flush(); // flushes contents to out_stream
            buf=new Buffer(out_stream.getRawBuffer(), 0, out_stream.size());
            packet=new DatagramPacket(buf.getBuf(), buf.getOffset(), buf.getLength(), mcast_addr, mcast_port);
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
        }
        catch(IOException ex) {
            log.error("failed sending discovery request", ex);
        }
        finally {
            Util.close(out);
        }
    }



    public void run() {
        DatagramPacket       packet=new DatagramPacket(receive_buf, receive_buf.length);
        byte[]               data;
        ByteArrayInputStream inp_stream=null;
        DataInputStream      inp=null;
        Message              msg;

        while(mcast_sock != null && receiver != null && Thread.currentThread().equals(receiver)) {
            packet.setData(receive_buf, 0, receive_buf.length);
            try {
                mcast_sock.receive(packet);
                data=packet.getData();
                inp_stream=new ByteArrayInputStream(data, 0, data.length);
                inp=new DataInputStream(inp_stream);
                msg=new Message();
                msg.readFrom(inp);
                up(new Event(Event.MSG, msg));
            }
            catch(SocketException socketEx) {
                break;
            }
            catch(Exception ex) {
                log.error("failed receiving packet", ex);
            }
            finally {
                closeInputStream(inp);
                closeInputStream(inp_stream);
            }
        }
        if(log.isTraceEnabled())
            log.trace("receiver thread terminated");
    }

    private static void closeInputStream(InputStream inp) {
        if(inp != null)
            try {inp.close();} catch(IOException e) {}
    }
}
