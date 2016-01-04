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
import org.jgroups.util.Util;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;


/**
 * Client stub that talks to a remote GossipRouter via blocking or non-blocking TCP
 * @author Bela Ban
 */
public class RouterStub extends ReceiverAdapter implements Comparable<RouterStub>, ConnectionListener {
    public interface StubReceiver        {void receive(GossipData data);}
    public interface MembersNotification {void members(List<PingData> mbrs);}
    public interface CloseListener       {void closed(RouterStub stub);}

    protected BaseServer                                  client;
    protected final IpAddress                             local;  // bind address
    protected final IpAddress                             remote; // address of remote GossipRouter
    protected final boolean                               use_nio;
    protected StubReceiver                                receiver; // external consumer of data, e.g. TUNNEL
    protected CloseListener                               close_listener;
    protected static final Log                            log=LogFactory.getLog(RouterStub.class);

    // max number of ms to wait for socket establishment to GossipRouter
    protected int                                         sock_conn_timeout=3000;
    protected boolean                                     tcp_nodelay=true;

    // map to correlate GET_MBRS requests and responses
    protected final Map<String,List<MembersNotification>> get_members_map=new HashMap<>();


    /**
     * Creates a stub to a remote GossipRouter
     * @param bind_addr The local address to bind to. If null, one will be picked
     * @param bind_port The local port. If 0, a random port will be used
     * @param router_host The address of the remote {@link GossipRouter}
     * @param router_port The port on which the remote GossipRouter is listening
     * @param use_nio Whether to use blocking or non-blocking IO
     * @param l The {@link org.jgroups.stack.RouterStub.CloseListener}
     */
    public RouterStub(InetAddress bind_addr, int bind_port, InetAddress router_host, int router_port,
                      boolean use_nio, CloseListener l) {
        local=new IpAddress(bind_addr, bind_port);
        this.remote=new IpAddress(router_host, router_port);
        this.use_nio=use_nio;
        this.close_listener=l;
        client=use_nio? new NioClient(bind_addr, bind_port, router_host, router_port)
          : new TcpClient(bind_addr, bind_port, router_host, router_port);
        client.addConnectionListener(this);
        client.receiver(this);
        client.socketConnectionTimeout(sock_conn_timeout).tcpNodelay(tcp_nodelay);
    }


    public RouterStub(IpAddress local, IpAddress remote, boolean use_nio, CloseListener l) {
        this.local=local;
        this.remote=remote;
        this.use_nio=use_nio;
        this.close_listener=l;
        client=use_nio? new NioClient(local, remote) : new TcpClient(local, remote);
        client.receiver(this);
        client.addConnectionListener(this);
        client.socketConnectionTimeout(sock_conn_timeout).tcpNodelay(tcp_nodelay);
    }


    public IpAddress           local()                                  {return local;}
    public IpAddress           remote()                                 {return remote;}
    public RouterStub          receiver(StubReceiver r)                 {receiver=r; return this;}
    public StubReceiver        receiver()                               {return receiver;}
    public boolean             tcpNoDelay()                             {return tcp_nodelay;}
    public RouterStub          tcpNoDelay(boolean tcp_nodelay)          {this.tcp_nodelay=tcp_nodelay; return this;}
    public CloseListener       connectionListener()                     {return close_listener;}
    public RouterStub          connectionListener(CloseListener l)      {this.close_listener=l; return this;}
    public int                 socketConnectionTimeout()                {return sock_conn_timeout;}
    public RouterStub          socketConnectionTimeout(int timeout)     {this.sock_conn_timeout=timeout; return this;}
    public boolean             useNio()                                 {return use_nio;}
    public IpAddress           gossipRouterAddress()                    {return remote;}
    public boolean             isConnected()                            {return client != null && ((Client)client).isConnected();}


    public RouterStub set(String attr, Object val) {
        switch(attr) {
            case "tcp_nodelay":
                tcpNoDelay((Boolean)val);
                break;
            default:
                throw new IllegalArgumentException("Attribute " + attr + " unknown");
        }
        return this;
    }




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
        GossipData request=new GossipData(GossipType.REGISTER, group, addr, logical_name, phys_addr);
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(request.size()+10);
        request.writeTo(out);
        client.send(remote, out.buffer(), 0, out.position());
    }

    public synchronized void connect() throws Exception {
        _doConnect();
    }

    @GuardedBy("lock")
    protected void _doConnect() throws Exception {
        client.start();
    }


    public void disconnect(String group, Address addr) throws Exception {
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
            List<MembersNotification> set=get_members_map.get(group);
            if(set == null)
                get_members_map.put(group, set=new ArrayList<>());
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


    public void sendToAllMembers(String group, byte[] data, int offset, int length) throws Exception {
        sendToMember(group, null, data, offset, length); // null destination represents mcast
    }

    public void sendToMember(String group, Address dest, byte[] data, int offset, int length) throws Exception {
        try {
            writeRequest(new GossipData(GossipType.MESSAGE, group, dest, data, offset, length));
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
                case MESSAGE:
                case SUSPECT:
                    if(receiver != null)
                        receiver.receive(data);
                    break;
                case GET_MBRS_RSP:
                    notifyResponse(data.getGroup(), data.getPingData());
                    break;
            }
        }
        catch(Exception ex) {
            log.error(Util.getMessage("FailedReadingData"), ex);
        }
    }

    @Override
    public void receive(Address sender, ByteBuffer buf) {
        Util.bufferToArray(sender, buf, this);
    }


    @Override
    public void connectionClosed(Connection conn, String reason) {
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
        return String.format("RouterStub[localsocket=%s, router_host=%s]", client.localAddress(), remote);
    }


    protected synchronized void writeRequest(GossipData req) throws Exception {
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(req.size());
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
