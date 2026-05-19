package org.jgroups.tests.rt.transports;

import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.tests.rt.RtReceiver;
import org.jgroups.tests.rt.RtTransport;
import org.jgroups.util.Util;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class NioTransport extends RtTransport {
    protected ServerSocketChannel srv_channel;
    protected SocketChannel       client_channel;
    protected Receiver            receiver_thread;
    protected RtReceiver          receiver;
    protected InetAddress         host;
    protected int                 port=7800;
    protected boolean             server, tcp_nodelay, direct_buffers=true; // use direct memory to receive msgs
    protected final Log           log=LogFactory.getLog(NioTransport.class);
    protected ByteBuffer          send_length_buf;
    protected final Lock          lock=new ReentrantLock();


    public NioTransport() {
    }

    public String[] options() {
        return new String[]{"-host <host>", "-port <port>", "-server",
          "-direct <boolean>",
          "-tcp-nodelay <boolean>"};
    }

    public NioTransport options(String... options) throws Exception {
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
                continue;
            }
            if(options[i].equals("-tcp-nodelay")) {
                tcp_nodelay=Boolean.parseBoolean(options[++i]);
                continue;
            }
            if(options[i].equals("-direct")) {
                direct_buffers=Boolean.parseBoolean(options[++i]);
            }
        }
        if(host == null)
            host=InetAddress.getLocalHost();
        return this;
    }

    public NioTransport receiver(RtReceiver receiver) {
        this.receiver=receiver;
        return this;
    }

    public Object localAddress() {return null;}

    public List<? extends Object> clusterMembers() {
        return null;
    }

    public void start(String ... options) throws Exception {
        options(options);
        send_length_buf=direct_buffers? ByteBuffer.allocateDirect(4) : ByteBuffer.allocate(4);
        if(server) { // simple single threaded server, can only handle a single connection at a time
            srv_channel=ServerSocketChannel.open();
            srv_channel.bind(new InetSocketAddress(host, port), 50);
            System.out.println("server started (ctrl-c to kill)");
            for(;;) {
                client_channel=srv_channel.accept();
                client_channel.socket().setTcpNoDelay(tcp_nodelay); // we're concerned about latency
                receiver_thread=new Receiver();
                receiver_thread.start();
            }
        }
        else {
            client_channel=SocketChannel.open();
            client_channel.socket().setTcpNoDelay(tcp_nodelay);
            client_channel.connect(new InetSocketAddress(host, port));
            receiver_thread=new Receiver();
            receiver_thread.start();
        }
    }

    public void stop() {
        Util.close(srv_channel, client_channel);
    }

    public void send(Object dest, byte[] buf, int offset, int length) throws Exception {
        ByteBuffer sbuf=ByteBuffer.wrap(buf, offset, length);
        send(null, sbuf);
    }

    @Override
    public void send(Object dest, ByteBuffer buf) throws Exception {
        lock.lock();
        try {
            send_length_buf.putInt(0, buf.remaining()).clear();
            client_channel.write(send_length_buf);
            client_channel.write(buf);
        }
        finally {
            lock.unlock();
        }
    }

    protected class Receiver extends Thread {

        public void run() {
            ByteBuffer buf=createBuffer(round_trip.size());
            ByteBuffer length_buf=createBuffer(Integer.BYTES);
            for(;;) {
                try {
                    length_buf.clear();
                    int num=client_channel.read(length_buf);
                    if(num == -1)
                        break;
                    if(num != 4)
                        throw new IllegalStateException("expected length (4 bytes), but received " + num);
                    int length=length_buf.getInt(0);
                    if(length > buf.capacity())
                        buf=createBuffer(length);
                    buf.position(0).limit(length);
                    num=client_channel.read(buf);
                    if(num == -1)
                        break;
                    buf.flip();
                    if(num != length)
                        throw new IllegalStateException("expected " + length + " bytes, but got only " + num);
                    if(receiver != null) {
                        int offset=buf.hasArray()? buf.arrayOffset() + buf.position() : buf.position(), len=buf.remaining();
                        if(buf.hasArray())
                            receiver.receive(null, buf.array(), offset, length);
                        else
                            receiver.receive(null, buf);
                    }
                }
                catch(ClosedChannelException cce) {
                    break;
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            }
            Util.close(client_channel);
        }
    }

    protected ByteBuffer createBuffer(int size) {
        return direct_buffers? ByteBuffer.allocateDirect(size) : ByteBuffer.allocate(size);
    }


}
