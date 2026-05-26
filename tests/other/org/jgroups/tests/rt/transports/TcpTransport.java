package org.jgroups.tests.rt.transports;

import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.tests.rt.RtReceiver;
import org.jgroups.tests.rt.RtTransport;
import org.jgroups.util.DefaultThreadFactory;
import org.jgroups.util.ThreadFactory;
import org.jgroups.util.Util;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.lang.System.out;

public class TcpTransport extends RtTransport {
    protected ServerSocket        srv_sock;
    protected Socket              sock;
    protected DataOutputStream    output;
    protected DataInputStream     input;
    protected RtReceiver          receiver;
    protected InetAddress         host;
    protected int                 port=7800;
    protected int                 out_buf_size=8192, in_buf_size=8192;
    protected boolean             server, tcp_nodelay=true;
    protected final Log           log=LogFactory.getLog(TcpTransport.class);
    protected final Lock          lock=new ReentrantLock();
    protected final ThreadFactory factory=new DefaultThreadFactory("receiver", true, true).useVirtualThreads(true);


    public TcpTransport() {
    }

    public String[] options() {
        return new String[]
          {"-host <host>", "-port <port>", "-server", "-tcp-nodelay <boolean>", "-outbuf <size>", "-inbuf <size>"};
    }

    public TcpTransport options(String... options) throws Exception {
        if(options == null)
            return this;
        for(int i=0; i < options.length; i++) {
            switch(options[i]) {
                case "-server" ->      server=true;
                case "-host" ->        host=InetAddress.getByName(options[++i]);
                case "-port" ->        port=Integer.parseInt(options[++i]);
                case "-tcp-nodelay" -> tcp_nodelay=Boolean.parseBoolean(options[++i]);
                case "-outbuf" ->      out_buf_size=Integer.parseInt(options[++i]);
                case "-inbuf" ->       in_buf_size=Integer.parseInt(options[++i]);
            }
        }
        if(host == null)
            host=InetAddress.getLocalHost();
        return this;
    }

    public TcpTransport receiver(RtReceiver receiver) {
        this.receiver=receiver;
        return this;
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
                input=createInput(s, in_buf_size);
                output=createOutput(s, out_buf_size);
                Thread receiver_thread=factory.newThread(new Receiver(this.input), "receiver");
                receiver_thread.start();
            }
        }
        else {
            sock=new Socket();
            sock.setTcpNoDelay(tcp_nodelay);
            sock.connect(new InetSocketAddress(host, port));
            input=createInput(sock, in_buf_size);
            output=createOutput(sock, out_buf_size);
            Thread receiver_thread=factory.newThread(new Receiver(this.input), "receiver");
            receiver_thread.start();
        }
    }

    public void stop() {
        Util.close(sock, srv_sock, input, output);
    }

    public void send(Object dest, byte[] buf, int offset, int length) throws Exception {
        lock.lock(); // to prevent multiple senders from corrupting each other's data
        try {
            output.writeInt(length);
            output.write(buf, offset, length);
            output.flush();
        }
        finally {
            lock.unlock();
        }
    }

    protected static DataOutputStream createOutput(Socket s, int size) throws IOException {
        return new DataOutputStream(size > 0? new BufferedOutputStream(s.getOutputStream(), size) : s.getOutputStream());
    }

    protected static DataInputStream createInput(Socket s, int size) throws IOException {
        return new DataInputStream(size > 0? new BufferedInputStream(s.getInputStream(), size) : s.getInputStream());
    }

    protected class Receiver implements Runnable {
        protected final DataInputStream in;

        public Receiver(DataInputStream in) {
            this.in=in;
        }

        public void run() {
            byte[] buf=new byte[round_trip.size()];
            for(;;) {
                try {
                    int length=in.readInt();
                    if(length > buf.length)
                        buf=new byte[length];
                    in.readFully(buf, 0, length);
                    if(receiver != null)
                        receiver.receive(null, buf, 0, length);
                }
                catch(IOException ioe) {
                    break;
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }


}
