package org.jgroups.blocks.cs;

import org.jgroups.Address;
import org.jgroups.util.SocketFactory;
import org.jgroups.util.ThreadFactory;

/**
 * Common base class for TCP based clients and servers
 * @author Bela Ban
 * @since  3.6.5
 */
public abstract class TcpBaseServer extends BaseServer {
    protected int               peer_addr_read_timeout=2000; // max time in milliseconds to block on reading peer address

    protected TcpBaseServer(ThreadFactory f, SocketFactory sf, int recv_buf_size) {
        super(f, sf, recv_buf_size);
    }

    @Override
    protected TcpConnection createConnection(Address dest) throws Exception {
        return new TcpConnection(dest, this);
    }


    public int           peerAddressReadTimeout()                {return peer_addr_read_timeout;}
    public TcpBaseServer peerAddressReadTimeout(int timeout)     {this.peer_addr_read_timeout=timeout; return this;}

}
