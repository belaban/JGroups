// $Id: RouterStub.java,v 1.3 2003/10/29 16:03:44 ovidiuf Exp $

package org.jgroups.stack;


import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;

import org.jgroups.*;
import org.jgroups.util.*;
import org.jgroups.log.Trace;
import org.jgroups.protocols.TunnelHeader;




public class RouterStub {
    String router_host=null;       // name of the router host
    int router_port=0;          // port on which router listens on router_host
    Socket sock=null;              // socket connecting to the router
    DataOutputStream output=null;            // output stream associated with sock
    DataInputStream input=null;             // input stream associated with sock
    Address local_addr=null;        // addr of group mbr. Once assigned, remains the same
    final long RECONNECT_TIMEOUT=5000; // msecs to wait until next connection retry attempt
    private volatile boolean connected=false;

    private volatile boolean reconnect=false;   // controls reconnect() loop


    /**
     Creates a stub for a remote Router object.
     @param router_host The name of the router's host
     @param router_port The router's port
     */
    public RouterStub(String router_host, int router_port) {
        this.router_host=router_host != null? router_host : "localhost";
        this.router_port=router_port;
    }


    /**
     * Establishes a connection to the router. The router will send my address (its peer address) back
     * as an Address, which is subsequently returned to the caller. The reason for not using
     * InetAddress.getLocalHost() or sock.getLocalAddress() is that this may not be permitted
     * with certain security managers (e.g if this code runs in an applet). Also, some network
     * address translation (NAT) (e.g IP Masquerading) may return the wrong address.
     */
    public Address connect() throws Exception {
        Address ret=null;
        int len=0;
        byte[] buf;

        try {
            sock=new Socket(router_host, router_port);
            sock.setSoLinger(true, 500);

            // Retrieve our own address by reading it from the socket
            input=new DataInputStream(sock.getInputStream());
            len=input.readInt();
            buf=new byte[len];
            input.readFully(buf);
            ret=(Address)Util.objectFromByteBuffer(buf);
            output=new DataOutputStream(sock.getOutputStream());
            connected=true;
        }
        catch(Exception e) {
            connected=false;
            throw e;
        }

        // IpAddress uses InetAddress.getLocalHost() to find the host. May not be permitted in applets !
        if(ret == null && sock != null)
            ret=new org.jgroups.stack.IpAddress(sock.getLocalPort());

        // set local address; this is the one that we will use from now on !
        if(local_addr == null)
            local_addr=ret;

        return ret;
    }


    /** Closes the socket and the input and output streams associated with it */
    public void disconnect() {
        if(output != null) {
            try {
                output.close();
                output=null;
            }
            catch(Exception e) {
            }
        }

        if(input != null) {
            try {
                input.close();
                input=null;
            }
            catch(Exception e) {
            }
        }

        if(sock != null) {
            try {
                sock.close();
                sock=null;
            }
            catch(Exception e) {
            }
        }
        connected=false;
        // stop the TUNNEL receiver thread
        reconnect=false;
    }


    /**
     Register this process with the router under <code>groupname</code>.
     @param groupname The name of the group under which to register
     @return boolean False if connection down, true if registration successful.
     */
    public boolean register(String groupname) {
        byte[] buf=null;

        if(sock == null || output == null || input == null) {
            Trace.error("RouterStub", ".register(): No connection to router (groupname=" + groupname + ")");
            connected=false;
            return false;
        }

        if(groupname == null || groupname.length() == 0) {
            Trace.error("RouterStub", "register(): groupname is null");
            return false;
        }

        if(local_addr == null) {
            Trace.error("RouterStub", "register(): local_addr is null");
            return false;
        }

        try {
            buf=Util.objectToByteBuffer(local_addr);
            output.writeInt(Router.REGISTER);
            output.writeUTF(groupname);
            output.writeInt(buf.length);
            output.write(buf, 0, buf.length);  // local_addr
            output.flush();
        }
        catch(Exception e) {
            Trace.error("RouterStub", "register(): "+Trace.getStackTrace(e));
            connected=false;
            return false;
        }
        return true;
    }


    /**
     Retrieves the membership (list of Addresses) for a given group. This is mainly used by the PING
     protocol to obtain its initial membership. This is used infrequently, so don't maintain socket
     for the entire time, but create/delete it on demand.
     */
    public List get(String groupname) {
        List ret=null;
        Socket tmpsock=null;
        DataOutputStream output=null;
        DataInputStream input=null;
        int len;
        byte[] buf;


        if(groupname == null || groupname.length() == 0) {
            Trace.error("RouterStub", "get(): groupname is null");
            return null;
        }

        try {
            tmpsock=new Socket(router_host, router_port);
            tmpsock.setSoLinger(true, 500);
            input=new DataInputStream(tmpsock.getInputStream());

            len=input.readInt();     // discard my own address
            buf=new byte[len];       // (first thing returned by router on acept())
            input.readFully(buf);
            output=new DataOutputStream(tmpsock.getOutputStream());

            // request membership for groupname
            output.writeInt(Router.GET);
            output.writeUTF(groupname);

            // wait for response (List)
            len=input.readInt();
            if(len == 0)
                return null;

            buf=new byte[len];
            input.readFully(buf);
            ret=(List)Util.objectFromByteBuffer(buf);
        }
        catch(Exception e) {
            Trace.error("RouterStub", "get(): exception=" + e);
        }
        finally {
            try {
                if(output != null) output.close();
            }
            catch(Exception e) {
            }
            try {
                if(input != null) input.close();
            }
            catch(Exception e) {
            }
            try {
                if(tmpsock != null) tmpsock.close();
            }
            catch(Exception e) {
            }
        }
        return ret;
    }


    /** Sends a message to the router. Returns false if message cannot be sent (e.g. no connection to
     router, true otherwise. */
    public boolean send(Message msg, String groupname) {
        byte[] msg_buf=null;
        byte[] dst_buf=null;
        Object dst_addr=null;

        if(sock == null || output == null || input == null) {
            Trace.error("RouterStub", "send(): No connection to router (groupname=" + groupname + ")");
            connected=false;
            return false;
        }

        if(msg == null) {
            Trace.error("RouterStub", "send(): Message is null");
            return false;
        }

        try {
            dst_addr=msg.getDest(); // could be null in case of mcast
            if(dst_addr != null)
                dst_buf=Util.objectToByteBuffer(dst_addr);

            msg_buf=Util.objectToByteBuffer(msg);

            output.writeUTF(groupname);

            if(dst_buf != null && dst_buf.length > 0) {
                output.writeInt(dst_buf.length);
                output.write(dst_buf, 0, dst_buf.length);
            }
            else
                output.writeInt(0);

            output.writeInt(msg_buf.length);
            output.write(msg_buf, 0, msg_buf.length);
        }
        catch(Exception e) {
            Trace.error("RouterStub", "send(): "+Trace.getStackTrace(e));
            connected=false;
            return false;
        }
        return true;
    }


    /** Receives a message from the router (blocking mode). If the connection is down,
     false is returned, otherwise true */
    public Message receive() {
        Message ret=null;
        byte[] buf=null;
        int len;

        if(sock == null || output == null || input == null) {
            Trace.error("RouterStub", "receive(): No connection to router");
            connected=false;
            return null;
        }
        try {
            len=input.readInt();
            if(len == 0) {
                ret=null;
            }
            else {
                buf=new byte[len];
                input.readFully(buf, 0, len);
                ret=(Message)Util.objectFromByteBuffer(buf);
            }
        }
        catch(Exception e) {
            if (connected) {
                Trace.error("RouterStub", "receive(): "+Trace.getStackTrace(e));                
            }
            connected=false;
            return null;
        }

        if (Trace.debug) {
            Trace.debug("RouterStub", "received "+ret);
        }
        return ret;
    }


    /** Tries to establish connection to router. Tries until router is up again. */
    public synchronized boolean reconnect() {
        Address new_addr=null;

        if(connected) return false;

        disconnect();
        reconnect=true;
        while(reconnect) {
            try {
                if((new_addr=connect()) != null)
                    break;
            }
            catch(Exception ex) {
                Trace.warn("RouterStub", "reconnect(): exception is " + ex);
            }
            Util.sleep(RECONNECT_TIMEOUT);
        }
        if(new_addr == null) {
            return false;
        }
        Trace.warn("RouterStub", "reconnect(): Client reconnected, new addess is " + new_addr);
        return true;
    }


    public static void main(String[] args) {
        if(args.length != 2) {
            System.out.println("RouterStub <host> <port>");
            return;
        }
        RouterStub stub=new RouterStub(args[0], new Integer(args[1]).intValue());
        Address my_addr;
        boolean rc;
        final String groupname="BelaGroup";
        Message msg;
        List mbrs;

        try {
            my_addr=stub.connect();
            System.out.println("My address is " + my_addr);

            System.out.println("Registering under " + groupname);
            rc=stub.register(groupname);
            System.out.println("Done, rc=" + rc);


            System.out.println("Getting members of " + groupname + ": ");
            mbrs=stub.get(groupname);
            System.out.println("Done, mbrs are " + mbrs);


            for(int i=0; i < 10; i++) {
                msg=new Message(null, my_addr, "Bela #" + i);
                msg.putHeader("TUNNEL", new TunnelHeader(groupname));
                rc=stub.send(msg, groupname);
                System.out.println("Sent msg, rc=" + rc);
            }

            for(int i=0; i < 10; i++) {
                System.out.println("stub.receive():");
                msg=stub.receive();
                System.out.println("Received msg");
            }

        }
        catch(Exception ex) {
            System.err.println(ex);
        }
        finally {
            stub.disconnect();
        }
    }


}
