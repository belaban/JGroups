package org.jgroups.protocols;

import org.jgroups.Event;
import org.jgroups.Global;
import org.jgroups.Message;
import org.jgroups.annotations.Property;
import org.jgroups.util.ByteArrayDataInputStream;
import org.jgroups.util.ByteArrayDataOutputStream;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;

/**
 * Broadcast PING. Uses UDP broadcasts to discover initial membership. This protocol is useless in IPv6 environments, as
 * IPv6 has no notion of broadcast addresses. Use IP multicasts instead (e.g. PING or MPING) when running in IPv6.
 * @author Bela Ban
 * @since 2.12
 */
public class BPING extends PING implements Runnable {


    /* -----------------------------------------    Properties     -------------------------------------------------- */

    @Property(description="Target address for broadcasts. This should be restricted to the local subnet, e.g. 192.168.1.255")
    protected String dest="255.255.255.255";

    @Property(description="Port for discovery packets", systemProperty=Global.BPING_BIND_PORT)
    protected int bind_port=8555;

    @Property(description="Sends discovery packets to ports 8555 to (8555+port_range)")
    protected int port_range=5;



    /* --------------------------------------------- Fields ------------------------------------------------------ */
    protected DatagramSocket  sock=null;
    protected volatile Thread receiver=null;
    protected InetAddress     dest_addr;



    public BPING() {
    }

    public int getBindPort() {
        return bind_port;
    }

    public void setBindPort(int bind_port) {
        this.bind_port=bind_port;
    }



    public void init() throws Exception {
        super.init();
        dest_addr=InetAddress.getByName(dest);
        if(log.isDebugEnabled())
            log.debug("listening on " + bind_port);
    }

    public void start() throws Exception {
        for(int i=bind_port; i <= bind_port+port_range; i++) {
            try {
                sock=getSocketFactory().createDatagramSocket("jgroups.bping.sock", i);
                break;
            }
            catch(Throwable t) {
                if(i > bind_port+port_range)
                    throw new RuntimeException("failed to open a port in range [" + bind_port + " - " + (bind_port+port_range) + "]", t);
            }
        }

        sock.setBroadcast(true);
        startReceiver();
        super.start();
    }


    private void startReceiver() {
        if(receiver == null || !receiver.isAlive()) {
            receiver=new Thread(this, "ReceiverThread");
            receiver.setDaemon(true);
            receiver.start();
            if(log.isTraceEnabled())
                log.trace("receiver thread started");
        }
    }

    public void stop() {
        Util.close(sock);
        sock=null;
        receiver=null;
        super.stop();
    }

    @Override
    protected void sendMcastDiscoveryRequest(Message msg) {
        try {
            if(msg.getSrc() == null)
                msg.setSrc(local_addr);
            ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(128);
            msg.writeTo(out);
            for(int i=bind_port; i <= bind_port+port_range; i++) {
                DatagramPacket packet=new DatagramPacket(out.buffer(), 0, out.position(), dest_addr, i);
                sock.send(packet);
            }
        }
        catch(Exception ex) {
            log.error("failed sending discovery request", ex);
        }
    }



    public void run() {
        final byte[]    receive_buf=new byte[65535];
        DatagramPacket  packet=new DatagramPacket(receive_buf, receive_buf.length);
        DataInput       inp;

        while(sock != null && receiver != null && Thread.currentThread().equals(receiver)) {
            packet.setData(receive_buf, 0, receive_buf.length);
            try {
                sock.receive(packet);
                inp=new ByteArrayDataInputStream(packet.getData(), packet.getOffset(), packet.getLength());
                Message msg=new Message();
                msg.readFrom(inp);
                up(new Event(Event.MSG, msg));
            }
            catch(SocketException socketEx) {
                break;
            }
            catch(Throwable ex) {
                log.error("failed receiving packet (from " + packet.getSocketAddress() + ")", ex);
            }
        }
        if(log.isTraceEnabled())
            log.trace("receiver thread terminated");
    }


}
