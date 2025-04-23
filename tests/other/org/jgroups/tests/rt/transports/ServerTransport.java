package org.jgroups.tests.rt.transports;

import org.jgroups.Address;
import org.jgroups.PhysicalAddress;
import org.jgroups.blocks.cs.*;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.tests.RoundTrip;
import org.jgroups.tests.rt.RtReceiver;
import org.jgroups.tests.rt.RtTransport;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.net.InetAddress;
import java.util.List;

/**
 * @author Bela Ban
 * @since  4.0
 */
public class ServerTransport extends ReceiverAdapter implements RtTransport {
    protected BaseServer   srv;
    protected RtReceiver   receiver;
    protected InetAddress  host;
    protected int          port=7800;
    protected boolean      server, nio, tcp_nodelay;
    protected int          out_buf_size=8192, in_buf_size=8192;
    protected final Log    log=LogFactory.getLog(ServerTransport.class);


    public ServerTransport() {
    }

    public String[] options() {
        return new String[]{"-host <host>", "-port <port>", "-server", "-nio", "-tcp-nodelay <boolean>",
          "-outbuf <size>", "-inbuf <size>"};
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
            if(options[i].equals("-nio")) {
                nio=true;
                continue;
            }
            if(options[i].equals("-tcp-nodelay")) {
                tcp_nodelay=Boolean.parseBoolean(options[++i]);
                continue;
            }
            if(options[i].equals("-outbuf")) {
                out_buf_size=Integer.parseInt(options[++i]);
                continue;
            }
            if(options[i].equals("-inbuf")) {
                in_buf_size=Integer.parseInt(options[++i]);
            }
        }
        if(host == null)
            host=InetAddress.getLocalHost();
    }


    public void receiver(RtReceiver receiver) {
        this.receiver=receiver;
    }

    public Object localAddress() {
        return null;
    }

    public List<Object> clusterMembers() {
        return null;
    }

    public void start(String ... options) throws Exception {
        options(options);
        if(server) {
            srv=nio? new NioServer(host, port) : new TcpServer(host, port)
              .connExpireTimeout(0)
              .tcpNodelay(tcp_nodelay)
              .receiver(this);
            if(srv instanceof TcpBaseServer)
                ((TcpBaseServer)srv).setBufferedOutputStreamSize(out_buf_size).setBufferedInputStreamSize(in_buf_size);
            srv.start();
            System.out.printf("server started on %s (ctrl-c to terminate)\n", srv.localAddress());
        }
        else {
            srv=nio? new NioClient(null, 0, host, port) : new TcpClient(null, 0, host, port)
              .tcpNodelay(false)
              .receiver(this);
            if(srv instanceof TcpBaseServer)
                ((TcpBaseServer)srv).setBufferedOutputStreamSize(out_buf_size).setBufferedInputStreamSize(in_buf_size);
            srv.start();
        }
    }

    public void stop() {
        Util.close(this.srv);
    }

    public void send(Object dest, byte[] buf, int offset, int length) throws Exception {
        srv.send((PhysicalAddress)dest, buf, offset, buf.length);
    }

    public void receive(Address sender, byte[] buf, int offset, int length) {
        if(receiver != null) {
            receiver.receive(sender, buf, offset, length);
        }
    }

    public void receive(Address sender, DataInput in, int length) throws Exception {
        if(receiver == null)
            return;
        byte[] buf=new byte[RoundTrip.PAYLOAD];
        in.readFully(buf);
        receiver.receive(sender, buf, 0, buf.length);
    }
}
