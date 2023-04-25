package org.jgroups.stack;

import org.jgroups.Address;
import org.jgroups.PhysicalAddress;
import org.jgroups.annotations.GuardedBy;
import org.jgroups.blocks.cs.*;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.PingData;
import org.jgroups.util.ByteArrayDataInputStream;
import org.jgroups.util.ByteArrayDataOutputStream;
import org.jgroups.util.SocketFactory;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;


/**
 * Client stub that talks to a remote GossipRouter via blocking or non-blocking TCP
 * @author Bela Ban
 */
public class RouterStub extends ReceiverAdapter implements Comparable<RouterStub>, ConnectionListener {
    public interface StubReceiver        {void receive(GossipData data);}
    public interface MembersNotification {void members(List<PingData> mbrs);}
    public interface CloseListener       {void closed(RouterStub stub);}

    protected BaseServer                                  client;
    protected IpAddress                                   local;     // bind address
    protected IpAddress                                   remote;    // address of remote GossipRouter
    protected InetSocketAddress                           remote_sa; // address of remote GossipRouter, not resolved yet
    protected final boolean                               use_nio;
    protected StubReceiver                                receiver;  // external consumer of data, e.g. TUNNEL
    protected CloseListener                               close_listener;
    protected SocketFactory                               socket_factory;
    protected static final Log                            log=LogFactory.getLog(RouterStub.class);

    // max number of ms to wait for socket establishment to GossipRouter
    protected int                                         sock_conn_timeout=3000;
    protected boolean                                     tcp_nodelay=true;

    protected boolean                                     handle_heartbeats;
    // timestamp of last heartbeat (or message from GossipRouter)
    protected volatile long                               last_heartbeat;

    // map to correlate GET_MBRS requests and responses
    protected final Map<String,List<MembersNotification>> get_members_map=new HashMap<>();


    /**
     * Creates a stub to a remote_sa {@link GossipRouter}.
     * @param local_sa The local_sa bind address and port
     * @param remote_sa The address:port of the GossipRouter
     * @param use_nio Whether to use ({@link org.jgroups.protocols.TCP_NIO2}) or {@link org.jgroups.protocols.TCP}
     * @param l The {@link CloseListener}
     * @param sf The {@link SocketFactory} to use to create the client socket
     */
    public RouterStub(InetSocketAddress local_sa, InetSocketAddress remote_sa, boolean use_nio, CloseListener l, SocketFactory sf) {
        this.local=local_sa != null? new IpAddress(local_sa.getAddress(), local_sa.getPort())
          : new IpAddress((InetAddress)null,0);
        this.remote_sa=Objects.requireNonNull(remote_sa);
        this.use_nio=use_nio;
        this.close_listener=l;
        this.socket_factory=sf;
        if(resolveRemoteAddress()) // sets remote
            client=createClient(sf);
    }


    public IpAddress     local()                              {return local;}
    public IpAddress     remote()                             {return remote;}
    public RouterStub    receiver(StubReceiver r)             {receiver=r; return this;}
    public StubReceiver  receiver()                           {return receiver;}
    public boolean       tcpNoDelay()                         {return tcp_nodelay;}
    public RouterStub    tcpNoDelay(boolean tcp_nodelay)      {this.tcp_nodelay=tcp_nodelay; return this;}
    public CloseListener connectionListener()                 {return close_listener;}
    public RouterStub    connectionListener(CloseListener l)  {this.close_listener=l; return this;}
    public int           socketConnectionTimeout()            {return sock_conn_timeout;}
    public RouterStub    socketConnectionTimeout(int timeout) {this.sock_conn_timeout=timeout; return this;}
    public boolean       useNio()                             {return use_nio;}
    public IpAddress     gossipRouterAddress()                {return remote;}
    public boolean       isConnected()                        {return client != null && ((Client)client).isConnected();}
    public RouterStub    handleHeartbeats(boolean f)          {handle_heartbeats=f; return this;}
    public boolean       handleHeartbeats()                   {return handle_heartbeats;}
    public long          lastHeartbeat()                      {return last_heartbeat;}




    /**
     * Registers mbr with the GossipRouter under the given group, with the given logical name and physical address.
     * Establishes a connection to the GossipRouter and sends a CONNECT message.
     * @param group The group cluster) name under which to register the member
     * @param addr The address of the member
     * @param logical_name The logical name of the member
     * @param phys_addr The physical address of the member
     * @throws Exception Thrown when the registration failed
     */
    public void connect(String group, Address addr, String logical_name, PhysicalAddress phys_addr) throws Exception {
        synchronized(this) {
            _doConnect();
        }
        if(handle_heartbeats)
            last_heartbeat=System.currentTimeMillis();
        try {
            writeRequest(new GossipData(GossipType.REGISTER, group, addr, logical_name, phys_addr));
        }
        catch(Exception ex) {
            throw new Exception(String.format("connection to %s failed: %s", group, ex));
        }
    }

    public synchronized void connect() throws Exception {
        _doConnect();
    }

    @GuardedBy("lock")
    protected void _doConnect() throws Exception {
        if(client != null)
            client.start();
        else {
            if(resolveRemoteAddress() && (client=createClient(this.socket_factory)) != null)
                client.start();
            else
                throw new IllegalStateException("client could not be created as remote address has not yet been resolved");
        }
    }


    public void disconnect(String group, Address addr) throws Exception {
        if(isConnected())
            writeRequest(new GossipData(GossipType.UNREGISTER, group, addr));
    }

    public void destroy() {
        Util.close(client);
    }

    /**
     * Fetches a list of {@link PingData} from the GossipRouter, one for each member in the given group. This call
     * returns immediately and when the results are available, the
     * {@link org.jgroups.stack.RouterStub.MembersNotification#members(List)} callback will be invoked.
     * @param group The group for which we need members information
     * @param callback The callback to be invoked.
     */
    public void getMembers(final String group, MembersNotification callback) throws Exception {
        if(callback == null)
            return;
        // if(!isConnected()) throw new Exception ("not connected");
        synchronized(get_members_map) {
            List<MembersNotification> set=get_members_map.computeIfAbsent(group, k -> new ArrayList<>());
            set.add(callback);
        }
        try {
            writeRequest(new GossipData(GossipType.GET_MBRS, group, null));
        }
        catch(Exception ex) {
            removeResponse(group, callback);
            throw new Exception(String.format("connection to %s broken. Could not send %s request: %s",
                                              gossipRouterAddress(), GossipType.GET_MBRS, ex));
        }
    }


    public void sendToAllMembers(String group, Address sender, byte[] data, int offset, int length) throws Exception {
        sendToMember(group, null, sender, data, offset, length); // null destination represents mcast
    }

    public void sendToMember(String group, Address dest, Address sender, byte[] data, int offset, int length) throws Exception {
        try {
            writeRequest(new GossipData(GossipType.MESSAGE, group, dest, data, offset, length).setSender(sender));
        }
        catch(Exception ex) {
            throw new Exception(String.format("connection to %s broken. Could not send message to %s: %s",
                                              gossipRouterAddress(), dest, ex));
        }
    }


    @Override
    public void receive(Address sender, byte[] buf, int offset, int length) {
        ByteArrayDataInputStream in=new ByteArrayDataInputStream(buf, offset, length);
        GossipData data=new GossipData();
        try {
            data.readFrom(in);
            switch(data.getType()) {
                case HEARTBEAT:
                    break;
                case MESSAGE:
                case SUSPECT:
                    if(receiver != null)
                        receiver.receive(data);
                    break;
                case GET_MBRS_RSP:
                    notifyResponse(data.getGroup(), data.getPingData());
                    break;
            }
            if(handle_heartbeats)
                last_heartbeat=System.currentTimeMillis();
        }
        catch(Exception ex) {
            log.error(Util.getMessage("FailedReadingData"), ex);
        }
    }

    public void receive(Address sender, DataInput in) throws Exception {
        GossipData data=new GossipData();
        data.readFrom(in);
        switch(data.getType()) {
            case HEARTBEAT:
                break;
            case MESSAGE:
            case SUSPECT:
                if(receiver != null)
                    receiver.receive(data);
                break;
            case GET_MBRS_RSP:
                notifyResponse(data.getGroup(), data.getPingData());
                break;
        }
        if(handle_heartbeats)
            last_heartbeat=System.currentTimeMillis();
    }

    @Override
    public void connectionClosed(Connection conn) {
        if(close_listener != null)
            close_listener.closed(this);
    }

    @Override
    public void connectionEstablished(Connection conn) {

    }

    @Override public int compareTo(RouterStub o) {
        return remote.compareTo(o.remote);
    }

    public int hashCode() {return remote.hashCode();}

    public boolean equals(Object obj) {
        return compareTo((RouterStub)obj) == 0;
    }

    public String toString() {
        return String.format("RouterStub[local=%s, router_host=%s %s] - age: %s",
                             client != null? client.localAddress() : "n/a", remote,
                             isConnected()? "connected" : "disconnected",
                             Util.printTime(System.currentTimeMillis()-last_heartbeat, TimeUnit.MILLISECONDS));
    }

    /** Creates remote from remote_sa. If the latter is unresolved, tries to resolve it one more time (e.g. via DNS) */
    protected boolean resolveRemoteAddress() {
        if(this.remote != null)
            return true;
        if(this.remote_sa.isUnresolved()) {
            this.remote_sa=new InetSocketAddress(remote_sa.getHostString(), remote_sa.getPort());
            if(this.remote_sa.isUnresolved())
                return false;
        }
        this.remote=new IpAddress(remote_sa.getAddress(), remote_sa.getPort());
        return true;
    }

    protected BaseServer createClient(SocketFactory sf) {
        BaseServer cl=use_nio? new NioClient(local, remote) : new TcpClient(local, remote);
        if(sf != null) cl.socketFactory(sf);
        cl.receiver(this);
        cl.addConnectionListener(this);
        cl.socketConnectionTimeout(sock_conn_timeout).tcpNodelay(tcp_nodelay);
        return cl;
    }

    public synchronized void writeRequest(GossipData req) throws Exception {
        int size=req.serializedSize();
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(size+5);
        req.writeTo(out);
        client.send(remote, out.buffer(), 0, out.position());
    }

    protected void removeResponse(String group, MembersNotification notif) {
        synchronized(get_members_map) {
            List<MembersNotification> set=get_members_map.get(group);
            if(set == null || set.isEmpty()) {
                get_members_map.remove(group);
                return;
            }
            if(set.remove(notif) && set.isEmpty())
                get_members_map.remove(group);
        }
    }

    protected void notifyResponse(String group, List<PingData> list) {
        if(group == null)
            return;
        if(list == null)
            list=Collections.emptyList();
        synchronized(get_members_map) {
            List<MembersNotification> set=get_members_map.get(group);
            while(set != null && !set.isEmpty()) {
                try {
                    MembersNotification rsp=set.remove(0);
                    rsp.members(list);
                }
                catch(Throwable t) {
                    log.error("failed notifying %s: %s", group, t);
                }
            }
            get_members_map.remove(group);
        }
    }


}
