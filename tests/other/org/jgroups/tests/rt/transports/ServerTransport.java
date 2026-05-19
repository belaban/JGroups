package org.jgroups.tests.rt.transports;

import org.jgroups.Address;
import org.jgroups.blocks.cs.*;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.tests.rt.RtReceiver;
import org.jgroups.tests.rt.RtTransport;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.net.InetAddress;
import java.util.List;

public class ServerTransport extends RtTransport implements Receiver {
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

    public ServerTransport options(String... options) throws Exception {
        if(options == null)
            return this;
        for(int i=0; i < options.length; i++) {
            switch(options[i]) {
                case "-server" ->      server=true;
                case "-host" ->        host=InetAddress.getByName(options[++i]);
                case "-port" ->        port=Integer.parseInt(options[++i]);
                case "-nio" ->         nio=true;
                case "-tcp-nodelay" -> tcp_nodelay=Boolean.parseBoolean(options[++i]);
                case "-outbuf" ->      out_buf_size=Integer.parseInt(options[++i]);
                case "-inbuf" ->       in_buf_size=Integer.parseInt(options[++i]);
            }
        }
        if(host == null)
            host=InetAddress.getLocalHost();
        return this;
    }


    public ServerTransport receiver(RtReceiver receiver) {
        this.receiver=receiver;
        return this;
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
            srv=nio? new NioServer(host, port) : new TcpServer(host, port);
            srv.connExpireTimeout(0).tcpNodelay(tcp_nodelay).receiver(this);
            if(srv instanceof TcpBaseServer)
                ((TcpBaseServer)srv).setBufferedOutputStreamSize(out_buf_size).setBufferedInputStreamSize(in_buf_size);
            srv.start();
            System.out.printf("server started on %s (ctrl-c to terminate)\n", srv.localAddress());
        }
        else {
            srv=nio? new NioClient(null, 0, host, port) : new TcpClient(null, 0, host, port);
            srv.tcpNodelay(false).receiver(this);
            if(srv instanceof TcpBaseServer)
                ((TcpBaseServer)srv).setBufferedOutputStreamSize(out_buf_size).setBufferedInputStreamSize(in_buf_size);
            srv.start();
        }
    }

    public void stop() {
        Util.close(this.srv);
    }

    public void send(Object dest, byte[] buf, int offset, int length) throws Exception {
        srv.send((Address)dest, buf, offset, length);
    }

    public void receive(Address sender, byte[] buf, int offset, int length) {
        if(receiver != null) {
            receiver.receive(sender, buf, offset, length);
        }
    }

    public void receive(Address sender, DataInput in, int length) throws Exception {
        if(receiver == null)
            return;
        byte[] buf=new byte[length];
        in.readFully(buf);
        receiver.receive(sender, buf, 0, length);
    }
}
