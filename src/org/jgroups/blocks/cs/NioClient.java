package org.jgroups.blocks.cs;

import org.jgroups.Address;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.*;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.channels.Selector;

/**
 * @author Bela Ban
 * @since  3.6.5
 */
public class NioClient extends NioBaseServer implements Client {
    protected Address       remote_addr; // the address of the server (needs to be set before connecting)
    protected NioConnection conn;        // connection to the server


    /**
     * Creates an instance of an {@link NioClient} that acts as a <em>client</em>: no server channel is created and
     * no acceptor is started to listen for incoming connections. Instead, a channel is created
     * (bound to bind_addr/bind_port) and connected to server_addr/server_port. This is used to send messages to the
     * remote server and receive messages from it. Note that there is only a single TCP connection established between
     * the client and server.
     * @param bind_addr The address to which the local channel should bind to. Can be null, then the OS picks the address
     * @param server_addr The address of the server to connect to
     * @throws Exception If the creation failed
    */
    public NioClient(IpAddress bind_addr, IpAddress server_addr) {
        this(bind_addr != null? bind_addr.getIpAddress() : null, bind_addr != null? bind_addr.getPort() : 0,
             server_addr != null? server_addr.getIpAddress() : null, server_addr != null? server_addr.getPort() : 0);
    }


    /**
     * Creates an instance of an {@link NioClient} that acts as a <em>client</em>: no server channel is created and
     * no acceptor is started to listen for incoming connections. Instead, a channel is created
     * (bound to bind_addr/bind_port) and connected to server_addr/server_port. This is used to send messages to the
     * remote server and receive messages from it. Note that there is only a single TCP connection established between
     * the client and server.
     * @param bind_addr The address to which the local channel should bind to. Can be null, then the OS picks the address
     * @param bind_port The local port. Can be 0, then the OS picks the port.
     * @param server_addr The address of the server to connect to
     * @param server_port The port of the server to connect to.
     * @throws Exception If the creation failed
    */
    public NioClient(InetAddress bind_addr, int bind_port, InetAddress server_addr, int server_port) {
        this(new DefaultThreadFactory("nio", false), new DefaultSocketFactory());
        clientBindAddress(bind_addr).clientBindPort(bind_port);
        this.remote_addr=new IpAddress(server_addr, server_port);
    }

    protected NioClient(ThreadFactory thread_factory, SocketFactory sf) {
        super(thread_factory, sf);
    }



    public Address           remoteAddress()               {return remote_addr;}
    /** Sets the address of the server. Has no effect when already connected. */
    public NioClient         remoteAddress(IpAddress addr) {this.remote_addr=addr; return this;}
    @Override public boolean isOpen()                      {return conn != null && conn.isOpen();}
    @Override public boolean isConnected()                 {return conn != null && conn.isConnected();}

    @Override
    public void start() throws Exception {
        if(running.compareAndSet(false, true)) {
            super.start();
            selector=Selector.open();
            conn=createConnection(remote_addr);
            conn.connect(remote_addr, false);
            local_addr=conn.localAddress();
            if(use_peer_connections)
                conn.sendLocalAddress(local_addr);
            conn.start();
            acceptor=factory.newThread(new Acceptor(), "NioClient.Acceptor [srv=" + remote_addr + "]");
            acceptor.setDaemon(true);
            acceptor.start();
        }
    }

    @Override
    public void stop() {
        super.stop();
        if(running.compareAndSet(true, false))
            Util.close(selector,conn); // closing the selector also stops the acceptor thread
    }

    @Override
    public void send(Address dest, ByteBuffer data) throws Exception {
        send(data);
    }

    @Override
    public void send(Address dest, byte[] data, int offset, int length) throws Exception {
        send(data, offset, length);
    }

    public void send(byte[] data, int offset, int length) throws Exception {
        if(conn == null)
            throw new IllegalStateException("connection to server " + remote_addr + " doesn't exist (has start() been called?)");
        conn.send(data, offset, length);
    }

    public void send(ByteBuffer data) throws Exception {
        if(conn == null)
            throw new IllegalStateException("connection to server " + remote_addr + " doesn't exist (has start() been called?)");
        conn.send(data);
    }


    public String toString() {
        return conn == null? String.format("%s -> %s [not connected]", localAddress(), remoteAddress())
          : String.format("%s", conn);
    }

}
