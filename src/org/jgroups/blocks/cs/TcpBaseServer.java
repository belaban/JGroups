package org.jgroups.blocks.cs;

import org.jgroups.Address;
import org.jgroups.util.DefaultSocketFactory;
import org.jgroups.util.SocketFactory;
import org.jgroups.util.ThreadFactory;

/**
 * Common base class for TCP based clients and servers
 * @author Bela Ban
 * @since  3.6.5
 */
public abstract class TcpBaseServer extends BaseServer {
    protected SocketFactory     socket_factory=new DefaultSocketFactory();
    protected volatile boolean  use_send_queues=true;
    protected int               send_queue_size=2000;
    protected int               peer_addr_read_timeout=2000; // max time in milliseconds to block on reading peer address

    protected TcpBaseServer(ThreadFactory f) {
        super(f);
    }

    @Override
    protected TcpConnection createConnection(Address dest) throws Exception {
        return new TcpConnection(dest, this);
    }


    public int           peerAddressReadTimeout()                {return peer_addr_read_timeout;}
    public TcpBaseServer peerAddressReadTimeout(int timeout)     {this.peer_addr_read_timeout=timeout; return this;}
    public int           sendQueueSize()                         {return send_queue_size;}
    public TcpBaseServer sendQueueSize(int send_queue_size)      {this.send_queue_size=send_queue_size; return this;}
    public boolean       useSendQueues()                         {return use_send_queues;}
    public TcpBaseServer useSendQueues(boolean flag)             {this.use_send_queues=flag; return this;}
    public SocketFactory socketFactory()                         {return socket_factory;}
    public TcpBaseServer socketFactory(SocketFactory factory)    {this.socket_factory=factory; return this;}
}
