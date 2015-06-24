package org.jgroups.nio;

import org.jgroups.Address;
import org.jgroups.blocks.BaseServer;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.*;

import java.net.InetAddress;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Server for sending and receiving messages via NIO channels. Uses only a single thread to accept, connect, write and
 * read connections. Read messages are passed to a receiver, which typically uses a thread pool to process messages.<p/>
 * Note that writes can get dropped, e.g. in the case where we have a previous write pending and a new write is received.
 * This is typically not an issue as JGroups retransmits messages, but might become one when using NioServer standalone,
 * ie. outside of JGroups.
 * @author Bela Ban
 * @since  3.6.5
 */
public class NioServer extends BaseServer<Address,NioConnection> {
    protected ServerSocketChannel channel;  // used to accept connections from peers
    protected Selector            selector; // get notified about accepts, data ready to read/write etc
    protected final Thread        acceptor; // the thread which calls select() in a loop
    protected final Lock          reg_lock=new ReentrantLock(); // for OP_CONNECT registrations
    protected volatile boolean    registration; // set to true after a registration; the acceptor sets it back to false
    protected int                 max_send_buffers=5; // size of WriteBuffers send buffer array
    protected int                 max_read_batch_size=50;


    public NioServer(IpAddress local_addr) throws Exception {
        this(new DefaultThreadFactory("nio", false), new DefaultSocketFactory(), null, local_addr.getIpAddress(), null, 0,
             local_addr.getPort(), local_addr.getPort() + 50, 0, 0);
    }

    public NioServer(InetAddress bind_addr, int port) throws Exception {
        this(new DefaultThreadFactory("nio", false), new DefaultSocketFactory(), null, bind_addr, null, 0, port, port + 50, 0, 0);
    }

    public NioServer(ThreadFactory f, SocketFactory socket_factory, Receiver<Address> r,
                     InetAddress bind_addr, InetAddress external_addr, int external_port, int srv_port, int end_port,
                     long reaper_interval, long conn_expire_time) throws Exception {
        super(f, socket_factory, r, reaper_interval, conn_expire_time);

        channel=Util.createServerSocketChannel(bind_addr, srv_port, end_port);
        channel.configureBlocking(false);
        selector=Selector.open();

        if(external_addr != null) {
            if(external_port <= 0)
                local_addr=new IpAddress(external_addr, channel.socket().getLocalPort());
            else
                local_addr=new IpAddress(external_addr, external_port);
        }
        else if(bind_addr != null)
            local_addr=new IpAddress(bind_addr, channel.socket().getLocalPort());
        else
            local_addr=new IpAddress(channel.socket().getLocalPort());
        acceptor=f.newThread(new Acceptor(), "NioServer.Acceptor [" + local_addr + "]");

        channel.register(selector, SelectionKey.OP_ACCEPT, null);
    }


    public int       maxSendBuffers()           {return max_send_buffers;}
    public NioServer maxSendBuffers(int num)    {this.max_send_buffers=num; return this;}
    public int       maxReadBatchSize()         {return max_read_batch_size;}
    public NioServer maxReadBatchSize(int size) {max_read_batch_size=size; return this;}


    /** Prints send and receive buffers for all connections */
    public String printBuffers() {
        StringBuilder sb=new StringBuilder("\n");
        synchronized(this) {
            for(Map.Entry<Address,NioConnection> entry: conns.entrySet()) {
                NioConnection val=entry.getValue();
                sb.append(entry.getKey()).append(":\n  ").append("recv_buf: ").append(val.recv_buf)
                  .append("\n  send_buf: ").append(val.send_buf).append("\n");
            }
        }
        return sb.toString();
    }


    @Override
    public synchronized void start() throws Exception {
        if(running.compareAndSet(false, true)) {
            acceptor.start();
            super.start();
        }
    }

    @Override
    public synchronized void stop() {
        super.stop();
        if(running.compareAndSet(true, false))
            Util.close(selector, channel);
    }



    @Override
    protected NioConnection createConnection(Address dest) throws Exception {
        return new NioConnection(dest, this);
    }


    protected SelectionKey register(SelectableChannel ch, int interest_ops, NioConnection conn) throws Exception {
        reg_lock.lock();
        try {
            registration=true;
            selector.wakeup(); // needed because registration will block until selector.select() returns
            return ch.register(selector, interest_ops, conn);
        }
        finally {
            reg_lock.unlock();
        }
    }




    protected class Acceptor implements Runnable {

        public void run() {
            Iterator<SelectionKey> it=null;
            while(running.get() && doSelect()) {
                try {
                    it=selector.selectedKeys().iterator();
                }
                catch(Exception ex) {
                    continue;
                }

                while(it.hasNext()) {
                    SelectionKey key=it.next();
                    NioConnection conn=(NioConnection)key.attachment();
                    try {
                        if(!key.isValid())
                            continue;
                        if(key.isReadable()) {
                            if(max_read_batch_size > 1)
                                conn.receive(max_read_batch_size);
                            else
                                conn.receive();
                        }
                        if(key.isWritable())
                            conn.send();
                        if(key.isAcceptable()) {
                            SocketChannel client_channel=channel.accept();
                            handleAccept(client_channel, key); // handleAccept() handles null client_channel
                        }
                        else if(key.isConnectable()) {
                            SocketChannel ch=(SocketChannel)key.channel();
                            if(ch.finishConnect())
                                key.interestOps(key.interestOps() & ~SelectionKey.OP_CONNECT | SelectionKey.OP_READ);
                        }
                    }
                    catch(Exception ex) {
                        Util.close(conn);
                        removeConnectionIfPresent(conn != null? conn.peerAddress() : null, conn);
                    }
                    finally {
                        it.remove();
                    }
                }
            }
        }


        protected boolean doSelect() {
            try {
                int num=selector.select();
                checkforPendingRegistrations();
                if(num == 0) return true;
            }
            catch(ClosedSelectorException closed_ex) {
                log.trace("selector was closed; acceptor terminating");
                return false;
            }
            catch(Throwable t) {
                log.warn("acceptor failure", t);
            }
            return true;
        }


        protected void handleAccept(final SocketChannel client_channel, SelectionKey key) throws Exception {
            NioConnection conn=null;
            if(client_channel == null) return; // can happen if no connection is available to accept
            try {
                conn=new NioConnection(client_channel, NioServer.this).key(key);
                client_channel.register(selector, SelectionKey.OP_READ, conn);
            }
            catch(Throwable ex) {
                Util.close(conn);
                removeConnectionIfPresent(conn.peerAddress(), conn);
                throw ex;
            }
        }

        protected void checkforPendingRegistrations() {
            if(registration) {
                reg_lock.lock(); // mostly uncontended -> fast
                try {
                    registration=false;
                }
                finally {
                    reg_lock.unlock();
                }
            }
        }
    }


}
