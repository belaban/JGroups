
package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.PhysicalAddress;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.blocks.cs.NioServer;

import java.nio.channels.ClosedChannelException;
import java.util.Collection;

/**
 * Protocol using TCP/IP to send and receive messages. Contrary to {@link TCP}, TCP_NIO uses non-blocking I/O (NIO),
 * which eliminates the thread per connection model. Instead, TCP_NIO uses a single selector to poll for incoming
 * messages and dispatches handling of those to a (configurable) thread pool.
 * <p>
 * Most of the functionality is in {@link NioServer}. TCP_NIO sends
 * messages using {@link NioServer#send(Object,byte[],int,int)} and registers with the server
 * to receive messages.
 * @author Bela Ban
 * @since 3.6.5
 */
public class TCP_NIO2 extends BasicTCP {
    protected NioServer server;

    @Property(description="The max number of outgoing messages that can get queued for a given peer connection " +
      "(before dropping them). Most messages will ge retransmitted; this is mainly used at startup, e.g. to prevent " +
      "dropped discovery requests or responses (sent unreliably, without retransmission).")
    protected int max_send_buffers=5;

    @Property(description="Max number of messages a read will try to read from the socket. Setting this to a higher " +
      "value will increase speed when receiving a lot of messages. However, when the receive message rate is small, " +
      "then every read will create an array of max_read_batch_size messages.")
    protected int max_read_batch_size=10;



    public TCP_NIO2() {}


    @ManagedAttribute
    public int getOpenConnections() {return server.getNumConnections();}

    @ManagedOperation
    public String printConnections() {return server.printConnections();}

    @ManagedOperation(description="Prints send and receive buffers for all connections")
    public String printBuffers() {return server.printBuffers();}

    @ManagedAttribute
    public int maxReadBatchSize() {
        int tmp=server.maxReadBatchSize();
        if(tmp != max_read_batch_size)
            max_read_batch_size=tmp;
        return max_read_batch_size;
    }

    @ManagedAttribute
    public void maxReadBatchSize(int size) {
        this.max_read_batch_size=size;
        server.maxReadBatchSize(size);
    }


    public void send(Address dest, byte[] data, int offset, int length) throws Exception {
        if(server != null) {
            try {
                server.send(dest, data, offset, length);
            }
            catch(ClosedChannelException closed) {}
            catch(Exception ex) {
                log.warn("%s: failed sending message to %s: %s", local_addr, dest, ex);
            }
        }
    }

    public void retainAll(Collection<Address> members) {
        server.retainAll(members);
    }

    public void start() throws Exception {
        server=(NioServer)((NioServer)new NioServer(getThreadFactory(), bind_addr, bind_port, bind_port+port_range, external_addr, external_port)
          .receiver(this)
          .timeService(time_service)
          .receiveBufferSize(recv_buf_size)
          .sendBufferSize(send_buf_size)
          .socketConnectionTimeout(sock_conn_timeout)
          .tcpNodelay(tcp_nodelay).linger(linger)
          .clientBindAddress(client_bind_addr).clientBindPort(client_bind_port).deferClientBinding(defer_client_bind_addr)
          .log(this.log))
          .maxSendBuffers(max_send_buffers).maxReadBatchSize(this.max_read_batch_size)
          .usePeerConnections(true);

        if(reaper_interval > 0 || conn_expire_time > 0) {
            if(reaper_interval == 0) {
                reaper_interval=5000;
                log.warn("reaper_interval was 0, set it to %d", reaper_interval);
            }
            if(conn_expire_time == 0) {
                conn_expire_time=1000 * 60 * 5;
                log.warn("conn_expire_time was 0, set it to %d", conn_expire_time);
            }
            server.connExpireTimeout(conn_expire_time).reaperInterval(reaper_interval);
        }

        // we first start threads in TP (http://jira.jboss.com/jira/browse/JGRP-626)
        super.start();
    }
    
    public void stop() {
        log.debug("closing sockets and stopping threads");
        server.stop(); // not needed, but just in case
        super.stop();
    }


    protected void handleConnect() throws Exception {
        if(isSingleton()) {
            if(connect_count == 0)
                server.start();
            super.handleConnect();
        }
        else
            server.start();
    }

    protected void handleDisconnect() {
        if(isSingleton()) {
            super.handleDisconnect();
            if(connect_count == 0)
                server.stop();
        }
        else
            server.stop();
    }   



    protected PhysicalAddress getPhysicalAddress() {
        return server != null? (PhysicalAddress)server.localAddress() : null;
    }
}
