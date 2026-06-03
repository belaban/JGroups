package org.jgroups.tests.rt.transports;

import org.jgroups.Global;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.tests.rt.RtReceiver;
import org.jgroups.tests.rt.RtTransport;
import org.jgroups.util.DefaultThreadFactory;
import org.jgroups.util.ThreadFactory;
import org.jgroups.util.Util;

import java.io.IOException;
import java.net.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Transport based on UDP datagrams. Note that this transport is not reliable: if a packet is dropped, the sender will
 * wait for the response forever and block.
 * @author Bela Ban
 * @since  4.0
 */
public class UdpTransport extends RtTransport {
    protected DatagramSocket      sock;
    protected RtReceiver          receiver;
    protected InetAddress         host;
    protected int                 port=7800;
    protected boolean             server;
    protected final Log           log=LogFactory.getLog(UdpTransport.class);
    protected List<SocketAddress> members=new ArrayList<>();
    protected ThreadFactory       factory;

    public UdpTransport() {
    }

    public String[] options() {
        return new String[]{"-host <host>", "-port <port>", "-server"};
    }

    public UdpTransport options(String... options) throws Exception {
        if(options == null)
            return this;
        for(int i=0; i < options.length; i++) {
            if(options[i].equals("-server")) {
                server=true;
                continue;
            }
            if(options[i].equals("-host")) {
                host=InetAddress.getByName(options[++i]);
                continue;
            }
            if(options[i].equals("-port")) {
                port=Integer.parseInt(options[++i]);
            }
        }
        if(host == null)
            host=InetAddress.getLocalHost();
        return this;
    }

    public UdpTransport receiver(RtReceiver receiver) {
        this.receiver=receiver;
        return this;
    }

    public Object localAddress() {return members != null? members.get(0) : null;}

    public List<? extends Object> clusterMembers() {
        return members;
    }

    public void start(String ... options) throws Exception {
        options(options);
        factory=new DefaultThreadFactory("receiver", false, true).useVirtualThreads(vthreads);
        if(server) { // simple single threaded server, can only handle a single connection at a time
            sock=new DatagramSocket(port, host);
            System.out.println("server started (ctrl-c to kill)");
        }
        else {
            sock=new DatagramSocket();
            members.add(sock.getLocalSocketAddress());
            members.add(new InetSocketAddress(host, port));
        }
        Thread receiver_thread=factory.newThread(new Receiver(), "receiver");
        receiver_thread.start();
    }

    public void stop() {
        Util.close(sock);
    }

    public void send(Object dest, byte[] buf, int offset, int length) throws Exception {
        SocketAddress target=dest != null? (SocketAddress)dest : new InetSocketAddress(host, port);
        DatagramPacket packet=new DatagramPacket(buf, offset, length, target);
        sock.send(packet);
    }


    protected class Receiver implements Runnable {
        public void run() {
            byte[] buf=new byte[Global.MAX_DATAGRAM_PACKET_SIZE];
            DatagramPacket packet=new DatagramPacket(buf, 0, buf.length);
            for(;;) {
                try {
                    sock.receive(packet);
                    if(receiver != null)
                        receiver.receive(packet.getSocketAddress(), buf, packet.getOffset(), packet.getLength());
                }
                catch(IOException ex) {
                    break;
                }
                catch(Exception e) {
                    e.printStackTrace();
                }

            }
        }
    }


}
