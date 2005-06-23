// $Id: ConnectionTableNIO.java,v 1.1 2005/06/23 13:09:16 belaban Exp $

package org.jgroups.blocks;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Util;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;


/**
 * Manages incoming and outgoing TCP connections. For each outgoing message to destination P, if there
 * is not yet a connection for P, one will be created. Subsequent outgoing messages will use this
 * connection.  For incoming messages, one server socket is created at startup. For each new incoming
 * client connecting, a new thread from a thread pool is allocated and listens for incoming messages
 * until the socket is closed by the peer.<br>Sockets/threads with no activity will be killed
 * after some time.<br> Incoming messages from any of the sockets can be received by setting the
 * message listener.
 *
 * @author Bela Ban
 */
public class ConnectionTableNIO extends ConnectionTable implements Runnable {

    private ServerSocketChannel srv_sock_ch=null;
    private Selector selector=null;
    private ArrayList pendingSocksList=null;

    /**
     * @param srv_port
     * @throws Exception
     */
    public ConnectionTableNIO(int srv_port) throws Exception {
        super(srv_port);
    }

    /**
     * @param srv_port
     * @param reaper_interval
     * @param conn_expire_time
     * @throws Exception
     */
    public ConnectionTableNIO(int srv_port, long reaper_interval,
                              long conn_expire_time) throws Exception {
        super(srv_port, reaper_interval, conn_expire_time);
    }

    /**
     * @param r
     * @param bind_addr
     * @param external_addr
     * @param srv_port
     * @param max_port
     * @throws Exception
     */
    public ConnectionTableNIO(Receiver r, InetAddress bind_addr, InetAddress external_addr, int srv_port, int max_port)
            throws Exception {
        super(r, bind_addr, external_addr, srv_port, max_port);
    }

    /**
     * @param r
     * @param bind_addr
     * @param external_addr
     * @param srv_port
     * @param max_port
     * @param reaper_interval
     * @param conn_expire_time
     * @throws Exception
     */
    public ConnectionTableNIO(Receiver r, InetAddress bind_addr, InetAddress external_addr, int srv_port, int max_port,
                              long reaper_interval, long conn_expire_time) throws Exception {
        super(r, bind_addr, external_addr, srv_port, max_port, reaper_interval, conn_expire_time);
    }

    /**
     * Try to obtain correct Connection (or create one if not yet existent)
     */
    ConnectionTable.Connection getConnection(Address dest) throws Exception {
        Connection conn=null;
        SocketChannel sock_ch;

        synchronized(conns) {
            conn=(Connection)conns.get(dest);
            if(conn == null) {
                InetSocketAddress destAddress=
                        new InetSocketAddress(((IpAddress)dest).getIpAddress(),
                                ((IpAddress)dest).getPort());
                sock_ch=SocketChannel.open(destAddress);
                conn=new Connection(sock_ch, dest);
                conn.sendLocalAddress(local_addr);
                // conns.put(dest, conn);
                addConnection(dest, conn);
                pendingSocksList.add(conn);
                selector.wakeup();
                notifyConnectionOpened(dest);
                if(log.isInfoEnabled()) log.info("created socket to " + dest);
            }
            return conn;
        }
    }

    /**
     * Closes all open sockets, the server socket and all threads waiting for incoming messages
     */
    public void stop() {
        if(srv_sock_ch != null) {
            try {
                ServerSocketChannel temp=srv_sock_ch;
                srv_sock_ch=null;
                temp.close();
            }
            catch(Exception ex) {
            }
        }
        super.stop();
    }

    /**
     * Acceptor thread. Continuously accept new connections. Create a new
     * thread for each new connection and put it in conns. When the thread
     * should stop, it is interrupted by the thread creator.
     */
    public void run() {
        Socket client_sock;
        Connection conn=null;
        Address peer_addr;

        while(srv_sock_ch != null) {
            try {
                if(selector.select() > 0) {
                    Set readyKeys=selector.selectedKeys();
                    for(Iterator i=readyKeys.iterator(); i.hasNext();) {
                        SelectionKey key=(SelectionKey)i.next();
                        i.remove();
                        if((key.readyOps() & SelectionKey.OP_ACCEPT)
                                == SelectionKey.OP_ACCEPT) {
                            ServerSocketChannel readyChannel=
                                    (ServerSocketChannel)key.channel();

                            SocketChannel client_sock_ch=
                                    readyChannel.accept();
                            client_sock=client_sock_ch.socket();

                            if(log.isInfoEnabled())
                                log.info("accepted connection, client_sock="
                                        + client_sock);

                            conn=new Connection(client_sock_ch, null);
                            // will call receive(msg)
                            // get peer's address
                            peer_addr=conn.readPeerAddress(client_sock);

                            conn.setPeerAddress(peer_addr);

                            synchronized(conns) {
                                if(conns.containsKey(peer_addr)) {

                                    if(log.isWarnEnabled())
                                        log.warn(peer_addr
                                                + " is already there, will terminate connection");
                                    conn.destroy();
                                    return;
                                }
                                addConnection(peer_addr, conn);
                            }
                            conn.init();
                            notifyConnectionOpened(peer_addr);
                        }
                        else
                            if(
                                    (key.readyOps() & SelectionKey.OP_READ)
                                    == SelectionKey.OP_READ) {
                                conn=(Connection)key.attachment();
                                ByteBuffer buff=conn.getNIOMsgReader().readCompleteMsgBuffer();
                                if(buff != null) {
                                    receive((Message)Util.objectFromByteBuffer(buff.array()));
                                    conn.getNIOMsgReader().reset();
                                }
                            }
                    }
                }
                else {
                    /*In addition to the accepted Sockets, we must registe
                     * sockets opend by this for OP_READ, because peer may
                     * use the same socket to sends data using the same socket,
                     * instead of opening a new connection. We can not register
                     * with this selectior from a different thread. set pending
                     * and wakeup this selector.
                     */
                    synchronized(conns) {
                        Connection pendingConnection;
                        while((pendingSocksList.size() > 0) && (null != (pendingConnection=(Connection)pendingSocksList.remove(0)))) {
                            pendingConnection.init();
                        }
                    }
                }
            }
            catch(SocketException sock_ex) {
                if(log.isInfoEnabled()) log.info("exception is " + sock_ex);
                if(conn != null)
                    conn.destroy();
                if(srv_sock == null)
                    break; // socket was closed, therefore stop
            }
            catch(Throwable ex) {

                if(log.isWarnEnabled()) log.warn("exception is " + ex);
            }
        }
    }

    /**
     * Finds first available port starting at start_port and returns server socket. Sets srv_port
     */
    protected ServerSocket createServerSocket(int start_port) throws Exception {
        this.selector=Selector.open();
        srv_sock_ch=ServerSocketChannel.open();
        srv_sock_ch.configureBlocking(false);
        while(true) {
            try {
                if(bind_addr == null)
                    srv_sock_ch.socket().bind(new InetSocketAddress(start_port));
                else
                    srv_sock_ch.socket().bind(new InetSocketAddress(bind_addr, start_port), backlog);
            }
            catch(BindException bind_ex) {
                start_port++;
                continue;
            }
            catch(IOException io_ex) {
                if(log.isErrorEnabled()) log.error("exception is " + io_ex);
            }
            srv_port=start_port;
            break;
        }
        pendingSocksList=new ArrayList();
        srv_sock_ch.register(this.selector, SelectionKey.OP_ACCEPT);
        return srv_sock_ch.socket();
    }

    class Connection extends ConnectionTable.Connection {
        private SocketChannel sock_ch=null;
        private static final int HEADER_SIZE=4;
        private static final int DEFAULT_BUFF_SIZE=256;
        final ByteBuffer headerBuffer=ByteBuffer.allocate(HEADER_SIZE);
        NBMessageForm_NIO nioMsgReader=null;

        Connection(SocketChannel s, Address peer_addr) {
            super(s.socket(), peer_addr);
            sock_ch=s;
        }

        void init() {
            in=null;
            out=null;
            try {
                sock_ch.configureBlocking(false);
                nioMsgReader=new NBMessageForm_NIO(DEFAULT_BUFF_SIZE, sock_ch);
                sock_ch.register(selector, SelectionKey.OP_READ, this);
            }
            catch(IOException e) {
            }

            if(log.isInfoEnabled()) log.info("connection was created to " + peer_addr);

        }

        void destroy() {
            closeSocket();
            nioMsgReader=null;
        }

        void doSend(Message msg) throws Exception {
            IpAddress dst_addr=(IpAddress)msg.getDest();
            byte[] buffie=null;

            if(dst_addr == null || dst_addr.getIpAddress() == null) {
                if(log.isErrorEnabled()) log.error("the destination address is null; aborting send");
                return;
            }

            try {
                // set the source address if not yet set
                if(msg.getSrc() == null)
                    msg.setSrc(local_addr);

                buffie=Util.objectToByteBuffer(msg);
                if(buffie.length <= 0) {
                    if(log.isErrorEnabled()) log.error("buffer.length is 0. Will not send message");
                    return;
                }

                headerBuffer.clear();
                headerBuffer.putInt(buffie.length);
                headerBuffer.flip();
                Util.writeFully(headerBuffer, sock_ch);
                ByteBuffer sendBuffer=ByteBuffer.wrap(buffie);
                Util.writeFully(sendBuffer, sock_ch);
            }
            catch(Exception ex) {

                if(log.isErrorEnabled())
                    log.error("to " + dst_addr + ", exception is " + ex + ", stack trace:\n" +
                            Util.printStackTrace(ex));
                remove(dst_addr);
                throw ex;
            }
        }

        void closeSocket() {
            if(sock != null) {
                try {
                    sock_ch.close();
                }
                catch(Exception e) {
                }
                sock=null;
            }
        }

        NBMessageForm_NIO getNIOMsgReader() {
            return nioMsgReader;
        }
    }
}