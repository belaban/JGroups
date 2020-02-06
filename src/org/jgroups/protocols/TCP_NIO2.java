
package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.PhysicalAddress;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.blocks.cs.NioServer;

import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.util.Collection;

/**
 * Protocol using TCP/IP to send and receive messages. Contrary to {@link TCP}, TCP_NIO uses non-blocking I/O (NIO),
 * which eliminates the thread per connection model. Instead, TCP_NIO uses a single selector to poll for incoming
 * messages and dispatches handling of those to a (configurable) thread pool.
 * <p>
 * Most of the functionality is in {@link NioServer}. TCP_NIO sends
 * messages using {@link NioServer#send(Address,byte[],int,int)} and registers with the server
 * to receive messages.
 * @author Bela Ban
 * @since 3.6.5
 */
public class TCP_NIO2 extends BasicTCP {
    protected NioServer server;

    @Property(description="The max number of outgoing messages that can get queued for a given peer connection " +
      "(before dropping them). Most messages will get retransmitted; this is mainly used at startup, e.g. to prevent " +
      "dropped discovery requests or responses (sent unreliably, without retransmission).")
    protected int     max_send_buffers=10;

    @Property(description="If true, a partial write will make a copy of the data so a buffer can be reused")
    protected boolean copy_on_partial_write=true;

    @Property(description="Number of ms a reader thread on a given connection can be idle (not receiving any messages) " +
      "until it terminates. New messages will start a new reader")
    protected long    reader_idle_time=5000;


    public TCP_NIO2() {}


    @ManagedAttribute
    public int getOpenConnections() {return server.getNumConnections();}

    @ManagedOperation
    public String printConnections() {return server.printConnections();}

    @ManagedOperation(description="Prints send and receive buffers for all connections")
    public String printBuffers() {return server.printBuffers();}

    @ManagedOperation(description="Clears all connections (they will get re-established). For testing only, don't use !")
    public void clearConnections() {
        server.clearConnections();
    }


    @ManagedAttribute(description="Is the selector open")
    public boolean isSelectorOpen() {return server != null && server.selectorOpen();}

    @ManagedAttribute(description="Is the acceptor thread (calling select()) running")
    public boolean isAcceptorRunning() {return server != null && server.acceptorRunning();}

    @ManagedAttribute(description="Number of times select() was called")
    public int     numSelects() {return server != null? server.numSelects() : -1;}

    @ManagedAttribute(description="Number of partial writes for all connections (not all bytes were written)")
    public int     numPartialWrites() {return server.numPartialWrites();}

    @ManagedAttribute(description="Number of ms a reader thread on a given connection can be idle (not receiving any messages) " +
      "until it terminates. New messages will start a new reader")
    public void readerIdleTime(long t) {
        this.reader_idle_time=t;
        server.readerIdleTime(t);
    }


    public void send(Address dest, byte[] data, int offset, int length) throws Exception {
        if(server != null) {
            try {
                server.send(dest, data, offset, length);
            }
            catch(ClosedChannelException | CancelledKeyException ignored_exceptions) {}
            catch(Throwable ex) {
                log.warn("%s: failed sending message to %s: %s", local_addr, dest, ex);
            }
        }
    }

    public void retainAll(Collection<Address> members) {
        server.retainAll(members);
    }

    public void start() throws Exception {
        server=new NioServer(getThreadFactory(), getSocketFactory(), bind_addr, bind_port, bind_port+port_range, external_addr, external_port);
        server.receiver(this)
          .timeService(time_service)
          .socketConnectionTimeout(sock_conn_timeout)
          .tcpNodelay(tcp_nodelay).linger(linger)
          .clientBindAddress(client_bind_addr).clientBindPort(client_bind_port).deferClientBinding(defer_client_bind_addr)
          .log(this.log);
        server.maxSendBuffers(max_send_buffers).usePeerConnections(true);
        server.copyOnPartialWrite(this.copy_on_partial_write).readerIdleTime(this.reader_idle_time);

        if(send_buf_size > 0)
            server.sendBufferSize(send_buf_size);
        if(recv_buf_size > 0)
            server.receiveBufferSize(recv_buf_size);

        if(reaper_interval > 0 || conn_expire_time > 0) {
            if(reaper_interval == 0) {
                reaper_interval=5000;
                log.warn("reaper_interval was 0, set it to %d", reaper_interval);
            }
            if(conn_expire_time == 0) {
                conn_expire_time=(long) 1000 * 60 * 5;
                log.warn("conn_expire_time was 0, set it to %d", conn_expire_time);
            }
            server.connExpireTimeout(conn_expire_time).reaperInterval(reaper_interval);
        }

        super.start();
    }
    
    public void stop() {
        log.debug("closing sockets and stopping threads");
        server.stop(); // not needed, but just in case
        super.stop();
    }


    protected void handleConnect() throws Exception {
        server.start();
    }

    protected void handleDisconnect() {
        server.stop();
    }   



    protected PhysicalAddress getPhysicalAddress() {
        return server != null? (PhysicalAddress)server.localAddress() : null;
    }
}
