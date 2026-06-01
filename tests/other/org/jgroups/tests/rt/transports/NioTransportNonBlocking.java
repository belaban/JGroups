package org.jgroups.tests.rt.transports;

import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.tests.rt.RtReceiver;
import org.jgroups.tests.rt.RtTransport;
import org.jgroups.util.DefaultThreadFactory;
import org.jgroups.util.Runner;
import org.jgroups.util.ThreadFactory;
import org.jgroups.util.Util;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class NioTransportNonBlocking extends RtTransport {
    protected ServerSocketChannel srv_channel;
    protected SocketChannel       client_channel;
    protected RtReceiver          receiver;
    protected InetAddress         host;
    protected int                 port=7800;
    protected boolean             server, tcp_nodelay=true, direct_buffers=true; // use direct memory to receive msgs
    protected boolean             vthreads=true;
    protected final Log           log=LogFactory.getLog(NioTransportNonBlocking.class);
    protected ByteBuffer          send_length_buf;
    protected ByteBuffer[]        buffers;
    protected final Lock          lock=new ReentrantLock();
    protected ThreadFactory       factory;
    protected Runner              acceptor;


    public NioTransportNonBlocking() {
    }

    public String[] options() {
        return new String[]{"-host <host>", "-port <port>", "-server",
          "-direct <boolean>", "-tcp-nodelay <boolean>", "-vthreads <boolean>"};
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
                case "-direct" ->      direct_buffers=Boolean.parseBoolean(options[++i]);
                case "-vthreads" ->    vthreads=Boolean.parseBoolean(options[++i]);
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
        send_length_buf=createBuffer(4);
        buffers=new ByteBuffer[]{send_length_buf, null};
        if(server) { // simple single threaded server, can only handle a single connection at a time
            srv_channel=ServerSocketChannel.open();
            srv_channel.bind(new InetSocketAddress(host, port), 50);
            System.out.println("server started (ctrl-c to kill)");
            acceptor=new Runner(factory, "tcp-acceptor", () -> {
                try {
                    accept();
                }
                catch(IOException e) {
                    throw new RuntimeException(e);
                }
            }, () -> Util.close(srv_channel));
            acceptor.start();
        }
        else {
            client_channel=SocketChannel.open();
            client_channel.configureBlocking(false);
            client_channel.setOption(StandardSocketOptions.TCP_NODELAY, tcp_nodelay);
            client_channel.connect(new InetSocketAddress(host, port));
            Thread receiver_thread=factory.newThread(new Receiver(), "receiver");
            receiver_thread.start();
        }
    }

    public void stop() {
        Util.close(srv_channel, client_channel, acceptor);
    }

    public void send(Object dest, byte[] buf, int offset, int length) throws Exception {
        ByteBuffer sbuf=ByteBuffer.wrap(buf, offset, length);
        send(null, sbuf);
    }

    @Override
    public void send(Object dest, ByteBuffer buf) throws Exception {
        lock.lock();
        try {
            int length=buf.remaining();
            send_length_buf.putInt(0, length);
            buffers[1]=buf;
            client_channel.write(buffers);
        }
        finally {
            send_length_buf.clear();
            lock.unlock();
        }
    }

    protected void accept() throws IOException {
        client_channel=srv_channel.accept();
        client_channel.setOption(StandardSocketOptions.TCP_NODELAY, tcp_nodelay);
        Thread receiver_thread=factory.newThread(new Receiver(), "receiver");
        receiver_thread.start();
    }

    protected class Receiver implements Runnable {

        public void run() {
            ByteBuffer buf=createBuffer(round_trip.size());
            ByteBuffer length_buf=createBuffer(Integer.BYTES);
            for(;;) {
                try {
                    length_buf.clear();
                    readFully(client_channel, 4, length_buf);
                    int length=length_buf.getInt(0);
                    if(length > buf.capacity())
                        buf=createBuffer(length);
                    buf.position(0).limit(length);
                    readFully(client_channel, length, buf);
                    buf.flip();
                    if(receiver != null) {
                        int offset=buf.hasArray()? buf.arrayOffset() + buf.position() : buf.position(), len=buf.remaining();
                        if(buf.hasArray())
                            receiver.receive(null, buf.array(), offset, len);
                        else
                            receiver.receive(null, buf);
                    }
                }
                catch(IOException cce) {
                    break;
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            }
            Util.close(client_channel);
        }
    }

    protected static int readFully(SocketChannel ch, int expected, ByteBuffer buf) throws IOException {
        int read=0;
        do {
            int tmp=ch.read(buf);
            if(tmp == -1)
                throw new EOFException();
            read+=tmp;
        }
        while(read < expected);
        return read;
    }

    protected ByteBuffer createBuffer(int size) {
        return direct_buffers? ByteBuffer.allocateDirect(size) : ByteBuffer.allocate(size);
    }


}
