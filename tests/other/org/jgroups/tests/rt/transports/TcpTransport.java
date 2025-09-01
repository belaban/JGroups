package org.jgroups.tests.rt.transports;

import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.tests.RoundTrip;
import org.jgroups.tests.rt.RtReceiver;
import org.jgroups.tests.rt.RtTransport;
import org.jgroups.util.Util;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;

import static java.lang.System.out;

public class TcpTransport implements RtTransport {
    protected ServerSocket srv_sock;
    protected Socket       sock;
    protected OutputStream output;
    protected InputStream  input;
    protected Receiver     receiver_thread;
    protected RtReceiver   receiver;
    protected InetAddress  host=null;
    protected int          port=7800;
    protected int          out_buf_size=8192, in_buf_size=8192;
    protected boolean      server, tcp_nodelay;
    protected final Log    log=LogFactory.getLog(TcpTransport.class);


    public TcpTransport() {
    }

    public String[] options() {
        return new String[]{"-host <host>", "-port <port>", "-server", "-tcp-nodelay <boolean>", "-outbuf <size>", "-inbuf <size>"};
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

    public Object localAddress() {return null;}

    public List<? extends Object> clusterMembers() {
        return null;
    }

    public void start(String ... options) throws Exception {
        options(options);
        if(server) { // simple single threaded server, can only handle a single connection at a time
            srv_sock=new ServerSocket(port, 50, host);
            out.println("server started (ctrl-c to kill)");
            for(;;) {
                Socket s=srv_sock.accept();
                s.setTcpNoDelay(tcp_nodelay); // we're concerned about latency
                input=in_buf_size > 0? new BufferedInputStream(s.getInputStream(), in_buf_size) : s.getInputStream();
                output=out_buf_size > 0? new BufferedOutputStream(s.getOutputStream(), out_buf_size) : s.getOutputStream();
                receiver_thread=new Receiver(input);
                receiver_thread.start();
            }
        }
        else {
            sock=new Socket();
            sock.setTcpNoDelay(tcp_nodelay);
            sock.connect(new InetSocketAddress(host, port));
            input=in_buf_size > 0? new BufferedInputStream(sock.getInputStream(), in_buf_size) : sock.getInputStream();
            output=out_buf_size > 0? new BufferedOutputStream(sock.getOutputStream(), out_buf_size) : sock.getOutputStream();
            receiver_thread=new Receiver(input);
            receiver_thread.start();
        }
    }

    public void stop() {
        Util.close(sock, srv_sock, input, output);
    }

    public void send(Object dest, byte[] buf, int offset, int length) throws Exception {
        doSend(buf, offset, length);
        flush();
    }

    public void doSend(byte[] buf, int offset, int length) throws Exception {
        output.write(buf, offset, length);
    }

    public void flush() throws IOException {
        output.flush();
    }




    protected class Receiver extends Thread {
        protected final InputStream in;

        public Receiver(InputStream in) {
            this.in=in;
        }

        public void run() {
            byte[] buf=new byte[RoundTrip.PAYLOAD];
            for(;;) {
                try {
                    int num=in.read(buf, 0, buf.length);
                    if(num == -1)
                        return;
                    if(num != buf.length)
                        throw new IllegalStateException("expected " + buf.length + " bytes, but got only " + num);
                    if(receiver != null)
                        receiver.receive(null, buf, 0, buf.length);
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }


}
