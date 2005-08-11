package org.jgroups.protocols;

import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.util.Buffer;
import org.jgroups.util.ExposedByteArrayOutputStream;
import org.jgroups.util.Util;

import java.io.*;
import java.net.*;
import java.util.Enumeration;
import java.util.Properties;

/**
 * Uses its own IP multicast socket to send and receive discovery requests/responses. Can be used in
 * conjuntion with a non-UDP transport, e.g. TCP.<p>
 * The discovery is <em>assymetric</em>: discovery requests are broadcast via the multicast socket, and
 * received via the multicast socket by everyone in the group. However, the discovery responses are sent
 * back via the regular transport (e.g. TCP) to the sender (discovery request contained sender's regular address,
 * e.g. 192.168.0.2:7800).
 * @author Bela Ban
 * @version $Id: MPING.java,v 1.12 2005/08/11 12:43:47 belaban Exp $
 */
public class MPING extends PING implements Runnable {
    MulticastSocket     mcast_sock=null;
    Thread              receiver=null;
    InetAddress         bind_addr=null;
    boolean             bind_to_all_interfaces=true;
    int                 ip_ttl=16;
    InetAddress         mcast_addr=null;
    int                 mcast_port=7555;

    /** Pre-allocated byte stream. Used for serializing datagram packets. Will grow as needed */
    final ExposedByteArrayOutputStream out_stream=new ExposedByteArrayOutputStream(512);
    byte                receive_buf[]=new byte[1024];
    static final String IGNORE_BIND_ADDRESS_PROPERTY="ignore.bind.address";


    public String getName() {
        return "MPING";
    }

    public InetAddress getBindAddr() {
        return bind_addr;
    }

    public void setBindAddr(InetAddress bind_addr) {
        this.bind_addr=bind_addr;
    }

    public boolean isBindToAllInterfaces() {
        return bind_to_all_interfaces;
    }

    public void setBindToAllInterfaces(boolean bind_to_all_interfaces) {
        this.bind_to_all_interfaces=bind_to_all_interfaces;
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
        String tmp=null, str;

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

        str=props.getProperty("mcast_addr");
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

        str=props.getProperty("mcast_port");
        if(str != null) {
            mcast_port=Integer.parseInt(str);
            props.remove("mcast_port");
        }

        str=props.getProperty("ip_ttl");
        if(str != null) {
            ip_ttl=Integer.parseInt(str);
            props.remove("ip_ttl");
        }

        str=props.getProperty("bind_to_all_interfaces");
        if(str != null) {
            bind_to_all_interfaces=new Boolean(str).booleanValue();
            props.remove("bind_to_all_interfaces");
        }

        if(mcast_addr == null)
            try {
                mcast_addr=InetAddress.getByName("230.5.6.7");
            }
            catch(UnknownHostException e) {
                log.error("failed getting default mcast address", e);
                return false;
            }

        return super.setProperties(props);
    }


    public void start() throws Exception {
        mcast_sock=new MulticastSocket(mcast_port);
        mcast_sock.setTimeToLive(ip_ttl);

        if(bind_to_all_interfaces) {
            bindToAllInterfaces();
            // interface for outgoing packets
            if(bind_addr != null)
                mcast_sock.setNetworkInterface(NetworkInterface.getByInetAddress(bind_addr));
        }
        else {
            if(bind_addr == null) {
                InetAddress[] interfaces=InetAddress.getAllByName(InetAddress.getLocalHost().getHostAddress());
                if(interfaces != null && interfaces.length > 0)
                    bind_addr=interfaces[0];
            }
            if(bind_addr == null)
                bind_addr=InetAddress.getLocalHost();

            if(bind_addr != null)
                if(log.isInfoEnabled()) log.info("sockets will use interface " + bind_addr.getHostAddress());


            if(bind_addr != null) {
                mcast_sock.setInterface(bind_addr);
                // mcast_sock.setNetworkInterface(NetworkInterface.getByInetAddress(bind_addr)); // JDK 1.4 specific
            }
            mcast_sock.joinGroup(mcast_addr);
        }

        startReceiver();
        super.start();
    }




    private void bindToAllInterfaces() throws IOException {
        SocketAddress tmp_mcast_addr=new InetSocketAddress(mcast_addr, mcast_port);
        Enumeration en=NetworkInterface.getNetworkInterfaces();
        while(en.hasMoreElements()) {
            NetworkInterface i=(NetworkInterface)en.nextElement();
            for(Enumeration en2=i.getInetAddresses(); en2.hasMoreElements();) {
                InetAddress addr=(InetAddress)en2.nextElement();
                // if(addr.isLoopbackAddress())
                   // continue;
                mcast_sock.joinGroup(tmp_mcast_addr, i);
                if(trace)
                    log.trace("joined " + tmp_mcast_addr + " on interface " + i.getName() + " (" + addr + ")");
                break;
            }
        }
    }

    private void startReceiver() {
        if(receiver == null || !receiver.isAlive()) {
            receiver=new Thread(this, "ReceiverThread");
            receiver.setDaemon(true);
            receiver.start();
            if(trace)
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
            mcast_sock.send(packet);
        }
        catch(IOException ex) {
            log.error("failed sending discovery request", ex);
        }
        finally {
            Util.closeOutputStream(out);
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
        if(trace)
            log.trace("receiver thread terminated");
    }

    private void closeInputStream(InputStream inp) {
        if(inp != null)
            try {inp.close();} catch(IOException e) {}
    }
}
