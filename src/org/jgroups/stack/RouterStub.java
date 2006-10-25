
package org.jgroups.stack;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.protocols.TunnelHeader;
import org.jgroups.util.Buffer;
import org.jgroups.util.ExposedByteArrayOutputStream;
import org.jgroups.util.Util;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.net.InetAddress;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.List;


/**
 * Client stub that talks to a remote GossipRouter
 * @author Bela Ban
 * @version $Id: RouterStub.java,v 1.22 2006/10/25 08:23:58 belaban Exp $
 */
public class RouterStub {
    String router_host=null;       // name of the router host
    int router_port=0;          // port on which router listens on router_host
    Socket sock=null;              // socket connecting to the router
    private ExposedByteArrayOutputStream out_stream=new ExposedByteArrayOutputStream(512);
    DataOutputStream output=null;            // output stream associated with sock
    DataInputStream input=null;             // input stream associated with sock
    DatagramSocket my_sock=null;    // needed to generate an ID that's unqique across this host
    Address local_addr=null;        // addr of group mbr. Once assigned, remains the same
    static final long RECONNECT_TIMEOUT=5000; // msecs to wait until next connection retry attempt
    private volatile boolean connected=false;
    private volatile boolean reconnect=false;   // controls reconnect() loop
    protected static final Log log=LogFactory.getLog(RouterStub.class);
    protected ConnectionListener conn_listener;
    private String groupname=null;
    private InetAddress bind_addr=null;





    public interface ConnectionListener {
        void connectionStatusChange(boolean connected);
    }

    public RouterStub() {
    }

    /**
     Creates a stub for a remote Router object.
     @param router_host The name of the router's host
     @param router_port The router's port
     */
    public RouterStub(String router_host, int router_port) {
        this.router_host=router_host != null? router_host : "localhost";
        this.router_port=router_port;
    }


    public InetAddress getBindAddress() {
        return bind_addr;
    }

    public void setBindAddress(InetAddress bind_addr) {
        this.bind_addr=bind_addr;
    }

    public String getRouterHost() {
        return router_host;
    }

    public void setRouterHost(String router_host) {
        this.router_host=router_host;
    }

    public int getRouterPort() {
        return router_port;
    }

    public void setRouterPort(int router_port) {
        this.router_port=router_port;
    }


    public boolean isConnected() {
        return connected;
    }

    public void setConnectionListener(ConnectionListener conn_listener) {
        this.conn_listener=conn_listener;
    }

    public synchronized Address getLocalAddress() throws SocketException {
        if(local_addr == null)
            local_addr=generateLocalAddress();
        return local_addr;
    }


    private synchronized Address generateLocalAddress() throws SocketException {
        my_sock=new DatagramSocket(0, bind_addr);
        local_addr=new IpAddress(bind_addr, my_sock.getLocalPort());
        return local_addr;
    }

//    private synchronized long generateUniquePort() {
//        long ret=System.currentTimeMillis();
//        if(ret <= last_port) {
//            ret=++last_port;
//        }
//        else {
//            last_port=ret;
//        }
//        return ret;
//    }

    /**
     Register this process with the router under <code>groupname</code>.
     @param groupname The name of the group under which to register
     */
    public synchronized void connect(String groupname) throws Exception {
        if(groupname == null || groupname.length() == 0)
            throw new Exception("groupname is null");

        this.groupname=groupname;

        if(local_addr == null)
            local_addr=generateLocalAddress();

        try {
            sock=new Socket(router_host, router_port, bind_addr, 0);
            sock.setSoLinger(true, 500);
            output=new DataOutputStream(sock.getOutputStream());
            GossipData req=new GossipData(GossipRouter.CONNECT, groupname, local_addr, null);
            req.writeTo(output);
            output.flush();
            input=new DataInputStream(sock.getInputStream()); // retrieve our own address by reading it from the socket
            setConnected(true);
        }
        catch(Exception e) {
            if(log.isWarnEnabled())
                log.warn("failed connecting to " + router_host + ":" + router_port);
            Util.close(sock); Util.close(input); Util.close(output);
            setConnected(false);
            throw e;
        }
    }


   public void connect(String groupname, String router_host, int router_port) throws Exception {
        setRouterHost(router_host);
        setRouterPort(router_port);
        connect(groupname);
    }


    /** Closes the socket and the input and output streams associated with it */
    public synchronized void disconnect() {
        disconnect(false);
    }



    public synchronized void disconnect(boolean is_reconnect) {
        try {
            if(sock == null || output == null || input == null) {
                setConnected(false);
                return;
            }

            if(groupname == null || groupname.length() == 0) {
                if(log.isErrorEnabled()) log.error("groupname is null");
                return;
            }

            if(local_addr == null) {
                if(log.isErrorEnabled()) log.error("local_addr is null");
                return;
            }

            GossipData req=new GossipData(GossipRouter.DISCONNECT, groupname, local_addr, null);
            req.writeTo(output);
            setConnected(false);
        }
        catch(Exception e) {
            // if(log.isErrorEnabled()) log.error("failed unregistering " + local_addr, e);
        }
        finally {
            Util.close(output);
            Util.close(input);
            Util.close(sock); sock=null;
            setConnected(false);
            // stop the TUNNEL receiver thread
            reconnect=false;
            if(is_reconnect) {
                Util.close(my_sock);
                local_addr=null;
            }
        }
    }






    /**
     Retrieves the membership (list of Addresses) for a given group. This is mainly used by the PING
     protocol to obtain its initial membership. This is used infrequently, so don't maintain socket
     for the entire time, but create/delete it on demand.
     */
    public List get(String groupname) {
        List ret=null;
        Socket tmpsock=null;
        DataOutputStream tmpOutput=null;
        DataInputStream tmpInput=null;

        if(groupname == null || groupname.length() == 0) {
            if(log.isErrorEnabled()) log.error("groupname is null");
            return null;
        }

        try {
            tmpsock=new Socket(router_host, router_port);
            tmpsock.setSoLinger(true, 500);

            // request membership for groupname
            tmpOutput=new DataOutputStream(tmpsock.getOutputStream());
            GossipData request=new GossipData(GossipRouter.ROUTER_GET, groupname, null, null);
            request.writeTo(tmpOutput);

            tmpInput=new DataInputStream(tmpsock.getInputStream());
            GossipData response=new GossipData();
            response.readFrom(tmpInput);
            return response.getMembers();
        }
        catch(Exception e) {
            if(log.isErrorEnabled()) log.error("exception=" + e);
        }
        finally {
            Util.close(tmpOutput);
            Util.close(tmpInput);
            Util.close(tmpsock);
        }
        return ret;
    }


    /** Sends a message to the router. Returns false if message cannot be sent (e.g. no connection to
     router, true otherwise. */
    public boolean send(Message msg, String groupname) {
        Address dst_addr=null;

        if(sock == null || output == null || input == null) {
            if(log.isErrorEnabled()) log.error("no connection to router (groupname=" + groupname + ')');
            setConnected(false);
            return false;
        }

        if(msg == null) {
            if(log.isErrorEnabled()) log.error("message is null");
            return false;
        }

        try {
            dst_addr=msg.getDest(); // could be null in case of mcast
            try {
                out_stream.reset();
            }
            catch (Exception ex) {
                out_stream=new ExposedByteArrayOutputStream(512);
            }
            // at this point out_stream is always valid and non-null

            DataOutputStream tmp=new DataOutputStream(out_stream);
            msg.writeTo(tmp);
            tmp.close();
            Buffer buf=new Buffer(out_stream.getRawBuffer(), 0, out_stream.size());

            // 1. Group name
            output.writeUTF(groupname);

            // 2. Destination address
            Util.writeAddress(dst_addr, output);

            // 3. Length of byte buffer
            output.writeInt(buf.getLength());

            // 4. Byte buffer
            output.write(buf.getBuf(), 0, buf.getLength());
        }
        catch(Exception e) {
            if(log.isErrorEnabled()) log.error("failed sending message to " + dst_addr, e);
            setConnected(false);
            return false;
        }
        return true;
    }


    /** Receives a message from the router (blocking mode). If the connection is down,
     false is returned, otherwise true */
    public Message receive() throws Exception {
        Message ret=null;
        byte[] buf=null;
        int len;

        if(sock == null || output == null || input == null) {
            // if(log.isErrorEnabled()) log.error("no connection to router");
            setConnected(false);
            return null;
        }
        Address dest;
        try {
            dest=Util.readAddress(input);
            len=input.readInt();
            if(len == 0) {
                ret=null;
            }
            else {
                buf=new byte[len];
                input.readFully(buf, 0, len);
                ret=new Message(false);
                ByteArrayInputStream tmp=new ByteArrayInputStream(buf);
                DataInputStream in=new DataInputStream(tmp);
                ret.readFrom(in);
                ret.setDest(dest);
                in.close();
            }
            if(log.isTraceEnabled()) log.trace("received "+ret);
            return ret;
        }
        catch(Exception e) {
            setConnected(false);
            throw e;
        }
    }


    /** Tries to establish connection to router. Tries until router is up again. */
    public void reconnect(int max_attempts) throws Exception {
        int num_atttempts=0;

        if(connected) return;
        disconnect();
        reconnect=true;
        while(reconnect && (num_atttempts++ < max_attempts || max_attempts == -1)) {
            try {
                connect(groupname);
                break;
            }
            catch(Exception ex) { // this is a normal case
                if(log.isTraceEnabled()) log.trace("failed reconnecting", ex);
            }
            if(max_attempts == -1)
                Util.sleep(RECONNECT_TIMEOUT);
        }
        if(!connected)
            throw new Exception("reconnect failed");
        if(log.isTraceEnabled()) log.trace("client reconnected");
    }


    public void reconnect() throws Exception {
        reconnect(-1);
    }


    private void notifyConnectionListener(boolean connected) {
        if(conn_listener != null) {
            conn_listener.connectionStatusChange(connected);
        }
    }

    private void setConnected(boolean connected) {
        boolean notify=this.connected != connected;
        this.connected=connected;
        if(notify) {
            try {
                notifyConnectionListener(this.connected);
            }
            catch(Throwable t) {
                log.error("failed notifying ConnectionListener " + conn_listener, t);
            }
        }
    }

    public static void main(String[] args) {
        if(args.length != 2) {
            System.out.println("RouterStub <host> <port>");
            return;
        }
        RouterStub stub=new RouterStub(args[0], Integer.parseInt(args[1])),
                   stub2=new RouterStub(args[0], Integer.parseInt(args[1]));
        Address my_addr;
        boolean rc;
        final String groupname="BelaGroup";
        Message msg;
        List mbrs;

        try {
            System.out.println("Registering under " + groupname);
            stub.connect(groupname);
            System.out.println("My address is " + stub.getLocalAddress());
            my_addr=stub2.getLocalAddress();
            stub2.connect(groupname);
            System.out.println("Getting members of " + groupname + ": ");
            mbrs=stub.get(groupname);
            System.out.println("Done, mbrs are " + mbrs);

            for(int i=1; i <= 10; i++) {
                msg=new Message(null, my_addr, "Bela #" + i);
                msg.putHeader("TUNNEL", new TunnelHeader(groupname));
                rc=stub2.send(msg, groupname);
                System.out.println("Sent msg #" + i + ", rc=" + rc);
            }

            for(int i=0; i < 10; i++) {
                msg=stub.receive();
                System.out.println("Received msg " + msg.getObject());
            }
        }
        catch(Exception ex) {
            log.error(ex);
        }
        finally {
            stub.disconnect();
        }
    }


}
