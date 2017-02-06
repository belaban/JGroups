package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.Experimental;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.*;

import java.io.*;
import java.net.*;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Bare-bones thread-per-connection TCP-based transport. Only used to compare with {@link TCP} or {@link TCP_NIO2},
 * don't use in production!
 * @author Bela Ban
 * @since  4.0
 */
@Experimental
@MBean(description="Simple TCP based transport")
public class SimpleTCP extends TP {

    @Property(description="size in bytes of TCP receiver window")
    protected int recv_buf_size=500000;

    @Property(description="size in bytes of TCP send window")
    protected int send_buf_size=500000;

    @Property(description="Size of the buffer of the BufferedInputStream in TcpConnection. A read always tries to read " +
      "ahead as much data as possible into the buffer. 0: default size")
    protected int buffered_input_stream_size=8192;

    @Property(description="Size of the buffer of the BufferedOutputStream in TcpConnection. Smaller messages are " +
      " buffered until this size is exceeded or flush() is called. Bigger messages are sent immediately. 0: default size")
    protected int buffered_output_stream_size=8192;


    protected ServerSocket                        srv_sock;
    protected Acceptor                            acceptor;
    protected final Map<SocketAddress,Connection> connections=new ConcurrentHashMap<>();
    protected final Map<Address,SocketAddress>    addr_table=new ConcurrentHashMap<>();

    public boolean supportsMulticasting() {return false;}

    @ManagedOperation(description="dumps the address table")
    public String printAddressTable() {
        return addr_table.entrySet().stream()
          .collect(StringBuilder::new,
                   (sb, e) -> sb.append(e.getKey()).append(": ").append(e.getValue()).append("\n"),
                   (l, r) ->{}).toString();
    }

    public void sendMulticast(byte[] data, int offset, int length) throws Exception {
        // not needed, implemented in down()
    }

    public void sendUnicast(PhysicalAddress dest, byte[] data, int offset, int length) throws Exception {
        // not needed, implemented in down()
    }

    public String getInfo() {
        return "SimpleTCP";
    }

    public void init() throws Exception {
        super.init();
        srv_sock=Util.createServerSocket(new DefaultSocketFactory(), "srv-sock", bind_addr, bind_port, bind_port+50);
        acceptor=new Acceptor(bind_addr, bind_port);
    }

    public void start() throws Exception {
        super.start();
        acceptor.start();
    }

    public void stop() {
        super.stop();
        Util.close(srv_sock);
        acceptor.stop();
    }

    public void destroy() {
        super.destroy();
        acceptor.stop();
        connections.values().forEach(Util::close);
        connections.clear();
    }

    public Object down(Event evt) {
        Object retval=super.down(evt);
        switch(evt.type()) {
            case Event.ADD_PHYSICAL_ADDRESS:
                Tuple<Address,PhysicalAddress> tuple=evt.arg();
                IpAddress val=(IpAddress)tuple.getVal2();
                addr_table.put(tuple.getVal1(), new InetSocketAddress(val.getIpAddress(), val.getPort()));
                break;
            case Event.VIEW_CHANGE:

                for(Iterator<Map.Entry<Address,SocketAddress>> it=addr_table.entrySet().iterator(); it.hasNext();) {
                    Map.Entry<Address,SocketAddress> entry=it.next();
                    if(!view.containsMember(entry.getKey())) {
                        SocketAddress sock_addr=entry.getValue();
                        it.remove();
                        Connection conn=connections.remove(sock_addr);
                        Util.close(conn);
                    }
                }
                break;
        }
        return retval;
    }

    public Object down(Message msg) {
        try {
            return _down(msg);
        }
        catch(Exception e) {
            log.error("failure passing message down", e);
            return null;
        }
    }

    protected Object _down(Message msg) throws Exception {
        Address dest=msg.dest();
        setSourceAddress(msg); // very important !! listToBuffer() will fail with a null src address !!
        int size=(int)msg.size();
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(size + Global.INT_SIZE);
        out.writeInt(size);
        msg.writeTo(out);

        if(dest != null) // unicast
            sendTo(dest, out.buffer(), 0, out.position());
        else { // multicast
            Collection<Address> dests=view != null? view.getMembers(): addr_table.keySet();
            for(Address dst: dests) {
                try {
                    sendTo(dst, out.buffer(), 0, out.position());
                }
                catch(Throwable t) {
                    log.error("failed sending multicast message to " + dst, t);
                }
            }
        }
        return null;
    }


    protected void sendTo(Address dest, byte[] buffer, int offset, int length) throws Exception {
        SocketAddress physical_dest=null;

        if(dest instanceof IpAddress) {
            IpAddress ip_addr=(IpAddress)dest;
            physical_dest=new InetSocketAddress(ip_addr.getIpAddress(), ip_addr.getPort());
        }
        else
            physical_dest=addr_table.get(dest);
        if(physical_dest == null)
            throw new Exception(String.format("physical address for %s not found", dest));
        Connection conn=getConnection(physical_dest);
        conn.send(buffer, offset, length);
    }

    protected Connection getConnection(SocketAddress dest) throws Exception {
        Connection conn=connections.get(dest);
        if(conn != null)
            return conn;
        Socket dest_sock=new Socket();
        dest_sock.setSendBufferSize(send_buf_size);
        dest_sock.setReceiveBufferSize(recv_buf_size);
        dest_sock.connect(dest);

        Connection c=connections.putIfAbsent(dest, conn=new Connection(dest_sock).start());
        if(c != null) {
            Util.close(conn);
            return c;
        }
        return conn;
    }

    protected boolean addPhysicalAddressToCache(Address logical_addr, PhysicalAddress physical_addr) {
        IpAddress tmp=(IpAddress)physical_addr;
        addr_table.put(logical_addr, new InetSocketAddress(tmp.getIpAddress(), tmp.getPort()));
        return super.addPhysicalAddressToCache(logical_addr, physical_addr);
    }

    protected PhysicalAddress getPhysicalAddress() {
        return new IpAddress((InetSocketAddress)srv_sock.getLocalSocketAddress());
    }


    /** Calls ServerSocket.accept() and creates new Connection objects */
    protected class Acceptor implements Runnable {
        protected final InetAddress  bind;
        protected final int          port;
        protected Runner             runner;

        public Acceptor(InetAddress bind, int port) throws Exception {
            this.bind=bind;
            this.port=port;
            runner=new Runner(new DefaultThreadFactory("tcp", true, true),
                              "acceptor", this, null);

        }

        protected void start() throws Exception {
            runner.start();
        }

        protected void stop() {
            runner.stop();
        }

        public void run() {
            try {
                Socket client_sock=srv_sock.accept();
                client_sock.setSendBufferSize(send_buf_size);
                client_sock.setReceiveBufferSize(recv_buf_size);
                Connection c;
                Connection existing=connections.putIfAbsent(client_sock.getRemoteSocketAddress(),
                                                            c=new Connection(client_sock).start());
                if(existing != null)
                    Util.close(c);

            }
            catch(Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /** 1 connection per peer to send and receive messages */
    protected class Connection implements Runnable, Closeable {
        protected final Socket           sock;
        protected final IpAddress        peer_addr;
        protected final DataInputStream  in;
        protected final DataOutputStream out;
        protected final Runner           runner;
        protected byte[]                 buffer=new byte[1024];
        protected final AtomicInteger    writers=new AtomicInteger(0); // to determine if a flush() is needed

        public Connection(Socket sock) throws Exception {
            this.sock=sock;
            peer_addr=new IpAddress((InetSocketAddress)sock.getRemoteSocketAddress());
            in=new DataInputStream(createBufferedInputStream(sock.getInputStream()));
            out=new DataOutputStream(createBufferedOutputStream(sock.getOutputStream()));
            runner=new Runner(new DefaultThreadFactory("tcp", false, true),
                              "conn-" + sock.getLocalPort(), this, null);
        }

        protected Connection start() {
            runner.start();
            return this;
        }

        public void close() {
            runner.stop();
            Util.close(in, out, sock);
        }

        protected void send(byte[] buffer, int offset, int length) throws Exception {
            writers.incrementAndGet();
            out.write(buffer, offset, length); // a write is synchronized (on the file descriptor)
            // if another writer is active, we don't need to flush as the other writer will (or somebody else)
            if(writers.decrementAndGet() == 0)
                out.flush();
        }

        public void run() {
            // System.out.printf("[%s] reading from sock, conn: %s\n", Thread.currentThread().getName(), this);
            try {
                int len=in.readInt();
                if(buffer == null || buffer.length < len)
                    buffer=new byte[len];
                in.readFully(buffer, 0, len);
                ByteArrayDataInputStream input=new ByteArrayDataInputStream(buffer, 0, len);
                Message msg=new Message(false);
                msg.readFrom(input);
                thread_pool.execute(() -> up_prot.up(msg));
            }
            catch(IOException io_ex) {
                runner.stop();
                throw new RuntimeException(io_ex);
            }
            catch(Exception ex) {
                if(sock.isClosed())
                    runner.stop();
                throw new RuntimeException(ex);
            }
        }

        public String toString() {
            return String.format("%s -> %s", sock.getLocalSocketAddress(), peer_addr);
        }

        protected BufferedOutputStream createBufferedOutputStream(OutputStream out) {
            int size=buffered_output_stream_size;
            return size == 0? new BufferedOutputStream(out) : new BufferedOutputStream(out, size);
        }

        protected BufferedInputStream createBufferedInputStream(InputStream in) {
            int size=buffered_input_stream_size;
            return size == 0? new BufferedInputStream(in) : new BufferedInputStream(in, size);
        }
    }

}
