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

import static org.jgroups.tests.RoundTrip.PAYLOAD;

public class NioTransport implements RtTransport {
    protected ServerSocketChannel srv_channel;
    protected SocketChannel       client_channel;
    protected Receiver            receiver_thread;
    protected RtReceiver          receiver;
    protected InetAddress         host;
    protected int                 port=7800;
    protected boolean             server, tcp_nodelay, direct_buffers=true; // use direct memory to receive msgs
    protected final Log           log=LogFactory.getLog(NioTransport.class);


    public NioTransport() {
    }

    public String[] options() {
        return new String[]{"-host <host>", "-port <port>", "-server",
          "-direct <boolean>",
          "-tcp-nodelay <boolean>"};
    }

    public void options(String... options) throws Exception {
        if(options == null)
            return;
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
    }

    public void receiver(RtReceiver receiver) {
        this.receiver=receiver;
    }

    public Object localAddress() {return null;}

    public List<? extends Object> clusterMembers() {
        return null;
    }

    public void start(String ... options) throws Exception {
        options(options);
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
        client_channel.write(sbuf);
    }

    @Override
    public void send(Object dest, ByteBuffer buf) throws Exception {
        client_channel.write(buf);
    }

    protected class Receiver extends Thread {

        public void run() {
            ByteBuffer buf=direct_buffers? ByteBuffer.allocateDirect(PAYLOAD) : ByteBuffer.allocate(PAYLOAD);
            // use buf.order(ByteOrder.nativeOrder() to measure perf between systems of the *same* native byte order
            for(;;) {
                try {
                    buf.position(0);
                    int num=client_channel.read(buf);
                    if(num == -1)
                        break;
                    buf.flip();
                    if(num != PAYLOAD)
                        throw new IllegalStateException("expected " + PAYLOAD + " bytes, but got only " + num);
                    if(receiver != null) {
                        int offset=buf.hasArray()? buf.arrayOffset() + buf.position() : buf.position(), len=buf.remaining();
                        if(buf.hasArray())
                            receiver.receive(null, buf.array(), offset, len);
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


}
