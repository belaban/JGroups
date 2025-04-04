
package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.PhysicalAddress;
import org.jgroups.annotations.Component;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.blocks.cs.TcpServer;
import org.jgroups.conf.AttributeType;
import org.jgroups.util.SocketFactory;
import org.jgroups.util.TLS;
import org.jgroups.util.Util;

import java.util.Collection;

/**
 * TCP based protocol. Creates a server socket, which gives us the local address
 * of this group member. For each accept() on the server socket, a new thread is
 * created that listens on the socket. For each outgoing message m, if m.dest is
 * in the outgoing hash table, the associated socket will be reused to send
 * message, otherwise a new socket is created and put in the hash table. When a
 * socket connection breaks or a member is removed from the group, the
 * corresponding items in the incoming and outgoing hash tables will be removed
 * as well.
 * <p>
 * 
 * This functionality is in TcpServer, which is used by TCP. TCP sends
 * messages using ct.send() and registers with the connection table to receive
 * all incoming messages.
 * 
 * @author Bela Ban
 */
public class TCP extends BasicTCP {
    protected TcpServer srv;

    public TCP() {}

    @Property(description="Size of the buffer of the BufferedInputStream in TcpConnection. A read always tries to read " +
      "ahead as much data as possible into the buffer. 0: default size",type=AttributeType.BYTES)
    protected int       buffered_input_stream_size=65536;

    @Property(description="Size of the buffer of the BufferedOutputStream in TcpConnection. Smaller messages are " +
      " buffered until this size is exceeded or flush() is called. Bigger messages are sent immediately. 0: default size",
      type=AttributeType.BYTES)
    protected int       buffered_output_stream_size=65536;

    @Property(description="Log a warning (or not) when ServerSocket.accept() throws an exception")
    protected boolean   log_accept_error=true; // https://issues.redhat.com/browse/JGRP-2540

    @Component(name="tls",description="Contains the attributes for TLS (SSL sockets) when enabled=true")
    protected TLS       tls=new TLS();

    @Property(description="use bounded queues for sending (https://issues.redhat.com/browse/JGRP-2759)")
    protected boolean   non_blocking_sends;

    @Property(description="when sending and non_blocking, how many messages to queue max")
    protected int       max_send_queue=128;

    public int getBufferedInputStreamSize() {
        return buffered_input_stream_size;
    }

    public TCP setBufferedInputStreamSize(int buffered_input_stream_size) {
        this.buffered_input_stream_size=buffered_input_stream_size;
        return this;
    }

    public int getBufferedOutputStreamSize() {
        return buffered_output_stream_size;
    }

    public TCP setBufferedOutputStreamSize(int buffered_output_stream_size) {
        this.buffered_output_stream_size=buffered_output_stream_size;
        return this;
    }
    public TLS     tls()                       {return tls;}
    public TCP     tls(TLS t)                  {this.tls=t; return this;}
    public boolean logAcceptError()            {return log_accept_error;}
    public TCP     logAcceptError(boolean l)   {this.log_accept_error=l; if(srv != null) srv.setLogAcceptError(l); return this;}
    public boolean nonBlockingSends()          {return non_blocking_sends;}
    public TCP     nonBlockingSends(boolean b) {this.non_blocking_sends=b; return this;}
    public int     maxSendQueue()              {return max_send_queue;}
    public TCP     maxSendQueue(int s)         {this.max_send_queue=s; return this;}

    @ManagedAttribute
    public int getOpenConnections() {
        return srv.getNumConnections();
    }

    @ManagedOperation
    public String printConnections() {
        return srv.printConnections();
    }

    @ManagedOperation(description="Clears all connections (they will get re-established). For testing only, don't use !")
    public TCP clearConnections() {
        srv.clearConnections(); return this;}

    @Override public void setSocketFactory(SocketFactory factory) {
        super.setSocketFactory(factory);
        if(srv != null)
            srv.socketFactory(factory);
    }

    public void send(Address dest, byte[] data, int offset, int length) throws Exception {
        if(srv != null)
            srv.send(dest, data, offset, length);
    }

    public void retainAll(Collection<Address> members) {
        srv.retainAll(members);
    }

    public void start() throws Exception {
        if(tls.enabled()) {
            SocketFactory factory=tls.createSocketFactory();
            setSocketFactory(factory);
        }
        srv=new TcpServer(getThreadFactory(), getSocketFactory(), bind_addr, bind_port, bind_port+port_range,
                          external_addr, external_port, recv_buf_size).setLogAcceptError(log_accept_error);

        srv.setBufferedInputStreamSize(buffered_input_stream_size).setBufferedOutputStreamSize(buffered_output_stream_size)
          .peerAddressReadTimeout(peer_addr_read_timeout)
          .nonBlockingSends(non_blocking_sends).maxSendQueue(max_send_queue)
          .usePeerConnections(true)
          .useAcks(this.use_acks)
          .socketFactory(getSocketFactory())
          .receiver(this)
          .timeService(time_service)
          .socketConnectionTimeout(sock_conn_timeout)
          .tcpNodelay(tcp_nodelay).linger(linger)
          .clientBindAddress(client_bind_addr).clientBindPort(client_bind_port).deferClientBinding(defer_client_bind_addr)
          .log(this.log).logDetails(this.log_details)
          .addConnectionListener(this);

        if(send_buf_size > 0)
            srv.sendBufferSize(send_buf_size);
        if(recv_buf_size > 0)
            srv.receiveBufferSize(recv_buf_size);

        if(reaper_interval > 0 || conn_expire_time > 0) {
            if(reaper_interval == 0) {
                reaper_interval=5000;
                log.warn("reaper_interval was 0, set it to %d", reaper_interval);
            }
            if(conn_expire_time == 0) {
                conn_expire_time=(long) 1000 * 60 * 5;
                log.warn("conn_expire_time was 0, set it to %d", conn_expire_time);
            }
            srv.connExpireTimeout(conn_expire_time).reaperInterval(reaper_interval);
        }

        if(max_length > 0)
            srv.setMaxLength(max_length);

        // we first start threads in TP (https://issues.redhat.com/browse/JGRP-626)
        super.start();
    }
    
    public void stop() {
        if(log.isDebugEnabled()) log.debug("%s: closing sockets and stopping threads", local_addr);
        super.stop();
        Util.close(srv); //not needed, but just in case
    }


    protected void handleConnect() throws Exception {
        srv.start();
    }

    protected void handleDisconnect() {
        srv.stop();
    }   



    protected PhysicalAddress getPhysicalAddress() {
        return srv != null? (PhysicalAddress)srv.localAddress() : null;
    }
}
