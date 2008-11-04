package org.jgroups.tests.perf.transports;


import org.jgroups.stack.IpAddress;
import org.jgroups.tests.perf.Receiver;
import org.jgroups.tests.perf.Transport;
import org.jgroups.tests.perf.Configuration;
import org.jgroups.tests.perf.IPerf;
import org.jgroups.Global;

import java.io.IOException;
import java.net.*;
import java.util.Properties;
import java.util.Map;
import java.nio.ByteBuffer;

/**
 * @author Bela Ban Jan 22
 * @author 2004
 * @version $Id: UdpTransport.java,v 1.14 2008/11/04 16:04:47 belaban Exp $
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
    int              max_chunk_size=65000;


    public UdpTransport() {
    }

    public Object getLocalAddress() {
        return local_addr;
    }


    public String help() {
        return "-mcast_addr <addr> -mcast_port <port> -max_chunk_size <bytes>";
    }

    public void create(Properties properties) throws Exception {
        this.config=properties;
        String mcast_addr_str=System.getProperty("udp.mcast_addr", config.getProperty("mcast_addr"));
        if(mcast_addr_str == null)
            mcast_addr_str="233.3.4.5";
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
        bind_addr=cfg.getBindAddress();

        String[] args=config.getTransportArgs();
        if(args != null) {
            for(int i=0; i < args.length; i++) {
                if(args[i].equals("-mcast_addr")) {
                    mcast_addr=InetAddress.getByName(args[++i]);
                    continue;
                }
                if(args[i].equals("-mcast_port")) {
                    mcast_port=Integer.parseInt(args[++i]);
                    continue;
                }
                if(args[i].equals("-max_chunk_size")) {
                    int tmp=Integer.parseInt(args[++i]);
                    if(tmp > max_chunk_size || tmp < 1000)
                        throw new IllegalArgumentException("-max_chunk_size must be <= 65K and >= 1000");
                    else
                        max_chunk_size=tmp;
                    continue;
                }
                help();
                return;
            }
        }

        if(mcast_addr == null)
            mcast_addr=InetAddress.getByName("232.5.5.5");

        if(bind_addr == null)
            bind_addr=InetAddress.getLocalHost();

        ucast_sock=new DatagramSocket(0, bind_addr);
        ucast_sock.setReceiveBufferSize(max_receiver_buffer_size);
        ucast_sock.setSendBufferSize(max_send_buffer_size);
        mcast_sock=new MulticastSocket(mcast_port);
        mcast_sock.setReceiveBufferSize(max_receiver_buffer_size);
        mcast_sock.setSendBufferSize(max_send_buffer_size);
        mcast_sock.setInterface(bind_addr);
        mcast_sock.joinGroup(mcast_addr);
        local_addr=new IpAddress(ucast_sock.getLocalAddress(), ucast_sock.getLocalPort());
        System.out.println("-- local_addr is " + local_addr);
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
        int original_length=payload.length;

        if(payload.length > max_chunk_size) {
            int offset=0;
            int length=max_chunk_size;

            ByteBuffer data=ByteBuffer.wrap(payload, 0, Global.BYTE_SIZE + Global.INT_SIZE);
            IPerf.Type type=IPerf.Type.getType(data.get());
            if(type != IPerf.Type.DATA)
                throw new IllegalArgumentException("only DATA requests can exceed the chunk size");
            original_length=data.getInt();

            while(offset < original_length) {
                ByteBuffer chunk=ByteBuffer.allocate(length + Global.BYTE_SIZE + Global.INT_SIZE);
                chunk.put(IPerf.Type.DATA.getByte());
                chunk.putInt(length);
                _send(destination, chunk.array(), 0, length + Global.BYTE_SIZE + Global.INT_SIZE);
                offset+=length;
                int remaining=original_length - offset;
                length=Math.min(remaining, max_chunk_size);
                if(length == 0)
                    break;
            }
        }
        else
            _send(destination, payload, 0, original_length);
    }

    private void _send(Object destination, byte[] payload, int offset, int length) throws Exception {
        DatagramPacket p;
        SocketAddress dest=destination == null?
                new InetSocketAddress(mcast_addr, mcast_port) :
                new InetSocketAddress(((IpAddress)destination).getIpAddress(), ((IpAddress)destination).getPort());
        p=new DatagramPacket(payload, offset, length, dest);
        if(destination == null)
            mcast_sock.send(p);
        else
            ucast_sock.send(p);
    }







    class ReceiverThread implements Runnable {
        DatagramSocket recv_sock;
        Thread         t=null;

        ReceiverThread(DatagramSocket sock) {
            this.recv_sock=sock;
        }

        void start() throws Exception {
            t=new Thread(this, "ReceiverThread for " + recv_sock.getLocalAddress() + ':' + recv_sock.getLocalPort());
            t.start();
        }

        void stop() {
            t=null;
            if(recv_sock != null)
                recv_sock.close();
        }

        public void run() {
            byte[]         buf=new byte[65535];
            DatagramPacket p;

            while(t != null) {
                p=new DatagramPacket(buf, buf.length);
                try {
                    recv_sock.receive(p);
                    if(receiver != null) {
                        IpAddress addr=new IpAddress(p.getAddress(), p.getPort());
                        receiver.receive(addr, p.getData());
                    }
                }
                catch(IOException e) {
                    if(recv_sock == null)
                        t=null;
                }
            }
            t=null;
        }
    }
}
