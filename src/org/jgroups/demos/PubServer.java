package org.jgroups.demos;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.blocks.cs.*;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Bits;
import org.jgroups.util.DefaultSocketFactory;
import org.jgroups.util.DefaultThreadFactory;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.net.InetAddress;
import java.nio.ByteBuffer;

/**
 * Demo which starts an {@link NioServer} that accepts messages from clients ({@link PubClient}) and forwards them to
 * all connected clients
 * @author Bela Ban
 * @since  3.6.5
 */
public class PubServer implements Receiver {
    protected BaseServer server;
    protected final Log  log=LogFactory.getLog(PubServer.class);


    protected void start(InetAddress bind_addr, int port, boolean nio, int recv_buf_size) throws Exception {
        server=nio?
          new NioServer(new DefaultThreadFactory("pubsrv", false), new DefaultSocketFactory(),
                        bind_addr, port, port+50, null, 0, recv_buf_size)
          : new TcpServer(new DefaultThreadFactory("pubsrv", false), new DefaultSocketFactory(),
                          bind_addr, port, port+50, null, 0, recv_buf_size);
        server.receiver(this);
        server.start();
        JmxConfigurator.register(server, Util.getMBeanServer(), "pub:name=pub-server");
        int local_port=server.localAddress() instanceof IpAddress? ((IpAddress)server.localAddress()).getPort(): 0;
        System.out.printf("\nPubServer listening at %s:%s\n", bind_addr != null? bind_addr : "0.0.0.0",  local_port);
    }


    @Override
    public void receive(Address sender, ByteBuffer buf) {
        try {
            server.send(null, buf);
        }
        catch(Exception ex) {
            log.error("failed publishing message", ex);
        }
    }

    @Override
    public void receive(Address sender, byte[] buf, int offset, int length) {
        try {
            server.send(null, buf, offset, length);
        }
        catch(Exception ex) {
            log.error("failed publishing message", ex);
        }
    }

    public void receive(Address sender, DataInput in) throws Exception {
        int len=in.readInt();
        byte[] buf=new byte[len + Global.INT_SIZE];
        Bits.writeInt(len, buf, 0);
        in.readFully(buf, Global.INT_SIZE, len);
        server.send(null, buf, 0, buf.length);
    }

    public static void main(String[] args) throws Exception {
        int         port=7500, recv_buf_size=0;
        InetAddress bind_addr=null;
        boolean     nio=true;

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-port")) {
                port=Integer.parseInt(args[++i]);
                continue;
            }
            if(args[i].equals("-bind_addr")) {
                bind_addr=InetAddress.getByName(args[++i]);
                continue;
            }
            if(args[i].equals("-nio")) {
                nio=Boolean.parseBoolean(args[++i]);
                continue;
            }
            if(args[i].equals("-recv_buf_size")) {
                recv_buf_size=Integer.parseInt(args[++i]);
                continue;
            }
            help();
            return;
        }
        PubServer server=new PubServer();
        server.start(bind_addr, port, nio, recv_buf_size);
    }

    protected static void help() {
        System.out.println("PubServer [-port port] [-bind_addr bind_addr] [-nio true|false] [-recv_buf_size bytes]");
    }
}
