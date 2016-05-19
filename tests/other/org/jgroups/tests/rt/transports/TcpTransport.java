package org.jgroups.tests.rt.transports;

import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.tests.RoundTrip;
import org.jgroups.tests.rt.RtReceiver;
import org.jgroups.tests.rt.RtTransport;
import org.jgroups.util.Util;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;

import static java.lang.System.out;

/**
 * @author Bela Ban
 * @since  4.0
 */
public class TcpTransport implements RtTransport {
    protected ServerSocket srv_sock;
    protected Socket       sock;
    protected OutputStream output;
    protected InputStream  input;
    protected Receiver     receiver_thread;
    protected RtReceiver   receiver;
    protected InetAddress  host=null;
    protected int          port=7800;
    protected boolean      server, tcp_nodelay;
    protected final Log    log=LogFactory.getLog(TcpTransport.class);


    public TcpTransport() {
    }

    public String[] options() {
        return new String[]{"-host <host>", "-port <port>", "-server", "-tcp-nodelay"};
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
                tcp_nodelay=true;
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
                Socket client_sock=srv_sock.accept();
                client_sock.setTcpNoDelay(tcp_nodelay); // we're concerned about latency
                input=client_sock.getInputStream();
                output=client_sock.getOutputStream();
                receiver_thread=new Receiver(input);
                receiver_thread.start();
            }
        }
        else {
            sock=new Socket();
            sock.setTcpNoDelay(tcp_nodelay);
            sock.connect(new InetSocketAddress(host, port));
            output=sock.getOutputStream();
            input=sock.getInputStream();
            receiver_thread=new Receiver(input);
            receiver_thread.start();
        }
    }

    public void stop() {
        Util.close(sock, srv_sock, input, output);
    }

    public void send(Object dest, byte[] buf, int offset, int length) throws Exception {
        output.write(buf, offset, length);
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
