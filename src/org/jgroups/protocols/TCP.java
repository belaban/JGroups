
package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.PhysicalAddress;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.blocks.cs.TcpServer;
import org.jgroups.util.SocketFactory;

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
    
    private TcpServer server=null;

    public TCP() {}


    @ManagedAttribute
    public int getOpenConnections() {
        return server.getNumConnections();
    }

    @ManagedOperation
    public String printConnections() {
        return server.printConnections();
    }

    @ManagedOperation(description="Clears all connections (they will get re-established). For testing only, don't use !")
    public void clearConnections() {
        server.clearConnections();
    }

    public void setSocketFactory(SocketFactory factory) {
        super.setSocketFactory(factory);
        if(server != null)
            server.socketFactory(factory);
    }

    public void send(Address dest, byte[] data, int offset, int length) throws Exception {
        if(server != null)
            server.send(dest, data, offset, length);
    }

    public void retainAll(Collection<Address> members) {
        server.retainAll(members);
    }

    public void start() throws Exception {
        server=(TcpServer)((TcpServer)new TcpServer(getThreadFactory(), getSocketFactory(), bind_addr, bind_port, bind_port+port_range, external_addr, external_port)
          .receiver(this)
          .timeService(time_service)
          .receiveBufferSize(recv_buf_size)
          .sendBufferSize(send_buf_size)
          .socketConnectionTimeout(sock_conn_timeout)
          .tcpNodelay(tcp_nodelay).linger(linger)
          .clientBindAddress(client_bind_addr).clientBindPort(client_bind_port).deferClientBinding(defer_client_bind_addr)
          .log(this.log))
          .socketFactory(getSocketFactory())
          .peerAddressReadTimeout(peer_addr_read_timeout)
          .useSendQueues(use_send_queues)
          .sendQueueSize(send_queue_size)
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
        if(log.isDebugEnabled()) log.debug("closing sockets and stopping threads");
        server.stop(); //not needed, but just in case
        super.stop();
    }


    protected void handleConnect() throws Exception {
        if(isSingleton()) {
            if(connect_count == 0) {
                server.start();
            }
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
