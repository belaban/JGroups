package org.jgroups.tests.perf.transports;


import org.jgroups.stack.IpAddress;
import org.jgroups.tests.perf.Receiver;
import org.jgroups.tests.perf.Transport;
import org.jgroups.tests.perf.Configuration;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.Properties;
import java.util.Map;
import java.util.List;

/**
 * @author Bela Ban Jan 22
 * @author 2004
 * @version $Id: UdpTransport.java,v 1.11 2008/09/11 17:43:12 belaban Exp $
 */
public class UdpTransport implements Transport {
    Receiver         receiver=null;
    Properties       config=null;
    Configuration    cfg=null;
    InetAddress      mcast_addr=null;
    int              mcast_port=7500;
    InetAddress      bind_addr=null;
    MulticastSocket  mcast_sock=null;
    DatagramSocket   ucast_sock=null;
    IpAddress        local_addr=null;
    ReceiverThread   mcast_receiver=null;
    ReceiverThread   ucast_receiver=null;
    int              max_receiver_buffer_size=500000;
    int              max_send_buffer_size=500000;


    public UdpTransport() {
    }

    public Object getLocalAddress() {
        return local_addr;
    }


    public String help() {
        return null;
    }

    public void create(Properties properties) throws Exception {
        this.config=properties;
        String mcast_addr_str=System.getProperty("udp.mcast_addr", config.getProperty("mcast_addr"));
        if(mcast_addr_str == null)
            mcast_addr_str="228.8.8.8";
        mcast_addr=InetAddress.getByName(mcast_addr_str);

        String bind_addr_str=System.getProperty("udp.bind_addr", config.getProperty("bind_addr"));
        if(bind_addr_str != null) {
            bind_addr=InetAddress.getByName(bind_addr_str);
        }
        else
            bind_addr=InetAddress.getLocalHost();

        ucast_sock=new DatagramSocket(0, bind_addr);
        ucast_sock.setReceiveBufferSize(max_receiver_buffer_size);
        ucast_sock.setSendBufferSize(max_send_buffer_size);
        mcast_sock=new MulticastSocket(mcast_port);
        mcast_sock.setReceiveBufferSize(max_receiver_buffer_size);
        mcast_sock.setSendBufferSize(max_send_buffer_size);
        if(bind_addr != null)
            mcast_sock.setInterface(bind_addr);
        mcast_sock.joinGroup(mcast_addr);
        local_addr=new IpAddress(ucast_sock.getLocalAddress(), ucast_sock.getLocalPort());
        System.out.println("-- local_addr is " + local_addr);
    }

    public void create(Configuration config) throws Exception {
        this.cfg=config;
        String[] args=config.getTransportArgs();
        if(args != null) {
            // todo: process
        }
    }


    public void start() throws Exception {
        mcast_receiver=new ReceiverThread(mcast_sock);
        ucast_receiver=new ReceiverThread(ucast_sock);
        mcast_receiver.start();
        ucast_receiver.start();
    }

    public void stop() {
        if(mcast_receiver != null)
            mcast_receiver.stop();
        if(ucast_receiver != null)
            ucast_receiver.stop();
    }

    public void destroy() {
        if(mcast_sock != null)
            mcast_sock.close();
        if(ucast_sock != null)
            ucast_sock.close();
    }

    public void setReceiver(Receiver r) {
        this.receiver=r;
    }

    public Map dumpStats() {
        return null;
    }

    public void send(Object destination, byte[] payload, boolean oob) throws Exception {
        DatagramPacket p;
        if(destination == null) {
            p=new DatagramPacket(payload, payload.length, mcast_addr, mcast_port);
        }
        else {
            IpAddress addr=(IpAddress)destination;
            p=new DatagramPacket(payload, payload.length, addr.getIpAddress(), addr.getPort());

        }
        ucast_sock.send(p);
    }







    class ReceiverThread implements Runnable {
        DatagramSocket sock;
        Thread         t=null;

        ReceiverThread(DatagramSocket sock) {
            this.sock=sock;
        }

        void start() throws Exception {
            t=new Thread(this, "ReceiverThread for " + sock.getLocalAddress() + ':' + sock.getLocalPort());
            t.start();
        }

        void stop() {
            t=null;
            if(sock != null)
                sock.close();
        }

        public void run() {
            byte[]         buf=new byte[128000];
            DatagramPacket p;

            while(t != null) {
                p=new DatagramPacket(buf, buf.length);
                try {
                    sock.receive(p);
                    if(receiver != null) {
                        IpAddress addr=new IpAddress(p.getAddress(), p.getPort());
                        receiver.receive(addr, p.getData());
                    }
                }
                catch(IOException e) {
                    if(sock == null)
                        t=null;
                }
            }
            t=null;
        }
    }
}
