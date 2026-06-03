package org.jgroups.tests.rt.transports;

import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.tests.rt.RtReceiver;
import org.jgroups.tests.rt.RtTransport;
import org.jgroups.util.DefaultThreadFactory;
import org.jgroups.util.Runner;
import org.jgroups.util.ThreadFactory;
import org.jgroups.util.Util;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.nio.channels.SelectionKey.OP_READ;

/**
 * Transport implementation using non-blocking NIO channels. Important: this is only for *one* server and *one*
 * client; multiple clients would hit non-reentrant code in the server! Reason: used for perftesting only.
 */
public class NioTransportNonBlocking extends RtTransport {
    protected ServerSocketChannel srv_channel;
    protected SocketChannel       client_channel;
    protected RtReceiver          receiver;
    protected InetAddress         host;
    protected int                 port=7800;
    protected boolean             server, tcp_nodelay=true;
    protected final Log           log=LogFactory.getLog(NioTransportNonBlocking.class);
    protected ByteBuffer          recv_length, recv_buf;
    protected ByteBuffer          send_length, send_buf;
    protected ByteBuffer[]        buffers;
    protected final Lock          lock=new ReentrantLock();
    protected ThreadFactory       factory;
    protected Runner              selector_handler;
    protected Selector            selector;


    public NioTransportNonBlocking() {
    }

    public String[] options() {
        return new String[]{"-host <host>", "-port <port>", "-server", "-tcp-nodelay <boolean>"};
    }

    public NioTransportNonBlocking options(String... options) throws Exception {
        if(options == null)
            return this;
        for(int i=0; i < options.length; i++) {
            switch(options[i]) {
                case "-server" ->      server=true;
                case "-host" ->        host=InetAddress.getByName(options[++i]);
                case "-port" ->        port=Integer.parseInt(options[++i]);
                case "-tcp-nodelay" -> tcp_nodelay=Boolean.parseBoolean(options[++i]);
                default -> throw new IllegalArgumentException(String.format("option '%s' not valid", options[i]));
            }
        }
        if(host == null)
            host=InetAddress.getLocalHost();
        return this;
    }

    public NioTransportNonBlocking receiver(RtReceiver receiver) {
        this.receiver=receiver;
        return this;
    }

    public Object localAddress() {return null;}

    public List<? extends Object> clusterMembers() {
        return null;
    }

    public void start(String ... options) throws Exception {
        options(options);
        factory=new DefaultThreadFactory("receiver", false, true).useVirtualThreads(vthreads);
        selector=Selector.open();
        send_length=createBuffer(4);
        recv_length=createBuffer(4);
        recv_buf=createBuffer(round_trip.size());
        send_buf=createBuffer(round_trip.size());
        buffers=new ByteBuffer[]{send_length, null};
        if(server) { // simple single threaded server, can only handle a single connection at a time
            srv_channel=ServerSocketChannel.open();
            srv_channel.bind(new InetSocketAddress(host, port), 50);
            srv_channel.configureBlocking(false);
            System.out.println("server started (ctrl-c to kill)");
            srv_channel.register(selector, SelectionKey.OP_ACCEPT);
        }
        else {
            client_channel=SocketChannel.open();
            client_channel.setOption(StandardSocketOptions.TCP_NODELAY, tcp_nodelay);
            client_channel.connect(new InetSocketAddress(host, port));
            client_channel.configureBlocking(false);
            client_channel.register(selector, OP_READ);
        }
        selector_handler=new Runner(factory, "Selector", () -> {
            try {
                handleSelector();
            }
            catch(IOException e) {
                throw new RuntimeException(e);
            }
        }, () -> Util.close(srv_channel));
        selector_handler.start();
    }

    public void stop() {
        Util.close(srv_channel, client_channel, selector, selector_handler);
    }

    public void send(Object dest, byte[] buf, int offset, int length) throws Exception {
        ByteBuffer sbuf=ByteBuffer.wrap(buf, offset, length);
        send(null, sbuf);
    }

    @Override
    public void send(Object dest, ByteBuffer buf) throws Exception {
        write(client_channel, buf);
    }

    protected void handleSelector() throws IOException {
        Iterator<SelectionKey> it=null;
        while(selector.select() >= 0) {
            try {
                it=selector.selectedKeys().iterator();
            }
            catch(Throwable ex) {
                continue;
            }
            while(it.hasNext()) {
                SelectionKey k=it.next();
                try {
                    if(!k.isValid())
                        continue;
                    if(k.isAcceptable()) // srv_channel
                        handleAccept(k);
                    else {
                        if(k.isReadable())
                            read(client_channel);
                        if(k.isValid() && k.isWritable())
                            write(client_channel, send_buf);
                    }
                }
                catch(Throwable ex) {
                    ex.printStackTrace();
                }
                finally {
                    if(k.isValid())
                        it.remove();
                }
            }
        }
    }

    protected void handleAccept(SelectionKey ignored) throws IOException {
        client_channel=srv_channel.accept();
        if(client_channel == null) return; // can happen if no connection is available to accept
        client_channel.setOption(StandardSocketOptions.TCP_NODELAY, tcp_nodelay);
        client_channel.configureBlocking(false);
        client_channel.register(selector, OP_READ);
    }

    protected void read(SocketChannel ch) {
        int len=0;
        try {
            if(recv_length.remaining() > 0) {
                int read=ch.read(recv_length);
                if(read == -1) {
                    Util.close(ch);
                    return;
                }
                if(recv_length.remaining() > 0)
                    return;
                len=recv_length.getInt(0);
                if(len > recv_buf.capacity())
                    recv_buf=createBuffer(len);
                recv_buf.position(0).limit(len);
            }
            if(recv_buf.remaining() > 0) {
                int read=ch.read(recv_buf);
                if(read == -1) {
                    Util.close(ch);
                    return;
                }
                if(recv_buf.remaining() == 0) {
                    recv_length.clear();
                    if(receiver != null)
                        receiver.receive(null, recv_buf.flip());
                }
            }
        }
        catch(IOException ioex) {
            Util.close(ch);
        }
    }

    protected void write(SocketChannel ch, ByteBuffer buf) {
        int len=buf.remaining();
        if(len == 0)
            return;
        lock.lock();
        try {
            int length=buf.remaining();
            send_length.putInt(0, length);
            buffers[1]=buf;
            long written=ch.write(buffers);

            // todo: queue send buffer(s) and register OP_WRITE with the selector; replace the polling code below
            if(written != length + send_length.capacity()) {
                System.err.printf("-- expected to write %d bytes, but wrote %d\n", length + send_length.capacity(), written);
                do {
                    long tmp=ch.write(buffers);
                    if(tmp == -1)
                        break;
                    written+=tmp;
                    Thread.yield();
                }
                while(written != length + send_length.capacity());
            }
        }
        catch(IOException e) {
            throw new RuntimeException(e);
        }
        finally {
            send_length.clear();
            lock.unlock();
        }
    }

    protected ByteBuffer createBuffer(int size) {
        return direct_memory? ByteBuffer.allocateDirect(size) : java.nio.ByteBuffer.allocate(size);
    }


}
