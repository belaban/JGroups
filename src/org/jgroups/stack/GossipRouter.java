// $Id: GossipRouter.java,v 1.1 2003/10/15 20:13:03 ovidiuf Exp $

package org.jgroups.stack;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Vector;
import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.log.Trace;
import org.jgroups.util.Promise;
import org.jgroups.util.Util;

/**
 * Router for TCP based group comunication (using layer TCP instead of UDP). 
 * Instead of the TCP layer sending packets point-to-point to each other 
 * member, it sends the packet to the router which - depending on the target 
 * address - multicasts or unicasts it to the group / or single member.<p>
 * This class is especially interesting for applets which cannot directly make
 * connections (neither UDP nor TCP) to a host different from the one they were
 * loaded from. Therefore, an applet would create a normal channel plus 
 * protocol stack, but the bottom layer would have to be the TCP layer which
 * sends all packets point-to-point (over a TCP connection) to the router,
 * which in turn forwards them to their end location(s) (also over TCP). A
 * centralized router would therefore have to be running on the host the applet
 * was loaded from.<p>
 * An alternative for running JGroups in an applet (IP multicast is not allows
 * in applets as of 1.2), is to use point-to-point UDP communication via the
 * gossip server. However, then the appplet has to be signed which involves
 * additional administrative effort on the part of the user.<p>
 * Since 2.1.1 the GossipRouter is also able to answer Gossip requests. Instead
 * of running different Router and GossipServer processes, is enough just to
 * run a single GossipRouter. This should simplify the administration of a
 * JG realm that has needs gossip and routing services. 
 *
 * @since 2.1.1
 *
 * @author Bela Ban
 * @author Ovidiu Feodorov <ovidiuf@users.sourceforge.net>
 */
public class GossipRouter {


    private static final Object GOSSIP_REQUEST = new Object();
    private static final Object GOSSIP_FAILURE = new Object();

    Hashtable    groups=new Hashtable();  // groupname - vector of AddressEntry's

    // gossip membership information
    Map gossipTable = new HashMap();

    int          port=8080;
    ServerSocket srv_sock=null;
    InetAddress  bind_address;

    // number of millisecs the main thread waits to receive a gossip request
    // after connection was established; upon expiration, the router initiates
    // the routing protocol on the connection. Don't set the interval too big, 
    // otherwise the router will appear slow in answering routing requests.
    private long gossipRequestTimeout;

    // default value for gossipRequestTimeout
    public static final long GOSSIP_REQUEST_TIMEOUT = 1000;

    // time (in ms) main thread waits for a router client to send the routing 
    // request type and the group afiliation before it declares the request
    // failed.
    private long routingClientReplyTimeout;

    // default value for routingClientReplyTimeout
    public static final long ROUTING_CLIENT_REPLY_TIMEOUT = 120000;

    // time (in msecs) until a cached 'gossip' member entry expires
    private long expiryTime;

    // default value for expiryTime
    public static final long EXPIRY_TIME = 30000;

    // BufferedInputStream mark buffer size
    private int MARK_BUFFER_SIZE = 2048;

    public static final int GET=-10;
    public static final int REGISTER=-11;
    public static final int DUMP=-21;   

    // The cache sweeper. Starts as daemon thread, so we won't block on it
    // upon termination
    Timer timer = new Timer(true);   

    public GossipRouter(int port) throws Exception {
        this(port, null, 
             EXPIRY_TIME,
             GOSSIP_REQUEST_TIMEOUT,
             ROUTING_CLIENT_REPLY_TIMEOUT);    
    }

    public GossipRouter(int port, InetAddress bind_address) throws Exception {
        this(port, bind_address, 
             EXPIRY_TIME,
             GOSSIP_REQUEST_TIMEOUT, 
             ROUTING_CLIENT_REPLY_TIMEOUT);
    }

    public GossipRouter(int port, InetAddress bind_address, long expiryTime) 
        throws Exception {
        this(port, bind_address, 
             expiryTime,
             GOSSIP_REQUEST_TIMEOUT, 
             ROUTING_CLIENT_REPLY_TIMEOUT);
    }

    public GossipRouter(int port, InetAddress bind_address, long expiryTime,
                  long gossipRequestTimeout, long routingClientReplyTimeout)
        throws Exception {

        this.port=port;
        this.bind_address=bind_address;
        if (bind_address!=null) {
            srv_sock=new ServerSocket(port, 50, bind_address);  // backlog of 50 connections
        }
        else {
            srv_sock=new ServerSocket(port, 50);  // backlog of 50 connections
        }
        this.expiryTime = expiryTime;
        this.gossipRequestTimeout = gossipRequestTimeout;
        this.routingClientReplyTimeout = routingClientReplyTimeout;

        // start the sweeper
        timer.schedule(new TimerTask() {
                public void run() {
                    sweep();
                }
            }, expiryTime, expiryTime);

//         Trace.info("GossipRouter", "GossipRouter(port="+port+", bindaddress="+bind_address+
//                    ", expiry="+expiryTime+", gossipRequestTimeout="+
//                    gossipRequestTimeout+", routingClientReplyTimeout="+
//                    routingClientReplyTimeout);
    }




    public void start() {
        Socket sock = null;
        DataInputStream input = null;
        DataOutputStream output = null;
        Address peer_addr = null;
        byte[] buf;
        int len, type = -1;
        String gname = null;
        Date d;

        if(bind_address == null) bind_address=srv_sock.getInetAddress();
        d=new Date();
        if(Trace.trace) {
            Trace.info("GossipRouter", "GossipRouter started at " + d);
            Trace.info("GossipRouter", "Listening on port " + port + " bound on address " + bind_address + "\n");
        }
        d=null;

        while(true) {

            try {
                sock=srv_sock.accept();
                sock.setSoLinger(true, 500);

                if(Trace.debug) {
                    Trace.debug("GossipRouter", "router accepted connection from "+sock);
                }

                final BufferedInputStream bis = new BufferedInputStream(sock.getInputStream());
                final Promise waitArea = new Promise();
                final Socket s = sock;

                // @since 2.2.1
                // Handling of gossip requests on a different thread allows 
                // the GossipRouter to serve both routing and gossip requests.
                // The GossipRouter stays backward compatible, old clients 
                // shouldn't be aware they talk to a merged Router/GossipServer.

               Thread t = new Thread(new Runnable() {
                        public void run() {
                            ObjectInputStream ois = null;
                            try {
                                bis.mark(MARK_BUFFER_SIZE);
                                // blocks until gossip request or 'forever'
                                ois = new ObjectInputStream(bis);
                                GossipData gossip_req = (GossipData)ois.readObject();

                                // it is a gossip request, set the main thread free
                                waitArea.setResult(GOSSIP_REQUEST);
                                GossipData gresp = processGossip(gossip_req);
                                if (gresp!=null) {
                                    ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
                                    oos.writeObject(gresp);
                                    oos.close();
                                }
                                bis.close();
                                s.close();
                            }
                            catch(Exception e) {
                                Trace.debug("GossipRouter","Gossip Thread exception :"+e);
                                waitArea.setResult(GOSSIP_FAILURE);
                            }
                            finally {
                                try {
                                    ois.close();
                                }
                                catch(Exception e) {
                                    // OK
                                }
                            }
                        }
                    }, "Gossip Request Thread");

                t.start();

                Object waitResult = waitArea.getResult(gossipRequestTimeout);
                waitArea.reset();

                if (waitResult!=null) {
                    // gossip request, let the gossip thread deal with it
                    continue;
                }

                // timeout, this is a routing request
                peer_addr = new IpAddress(sock.getInetAddress(), sock.getPort());
                output=new DataOutputStream(sock.getOutputStream());
                
                // return the address of the peer so it can set it
                buf=Util.objectToByteBuffer(peer_addr);
                output.writeInt(buf.length);
                output.write(buf, 0, buf.length);

                // The gossip thread still waits for a serialized object, so
                // wait that read to fail. If it actually gets a GossipData,
                // that's an error condition we should handle here
                waitResult = waitArea.getResult(routingClientReplyTimeout);

                if (waitResult==null) {
                    // timeout
                    throw new Exception("Timeout waiting for router client answer");
                }
                else if (waitResult==GOSSIP_REQUEST) {
                    // lazy gossip client, let it handle its business, it will
                    // fail anyway
                    output.close();
                    continue;
                }

                bis.reset();
                input=new DataInputStream(bis);
                type=input.readInt();
                gname=input.readUTF();

                // We can have 2 kinds of messages at this point: GET requests or REGISTER requests.
                // GET requests are processed right here, REGISTRATION requests cause the spawning of
                // a separate thread handling it (long running thread as it will do the message routing
                // on behalf of that client for the duration of the client's lifetime).

                switch(type) {
                    case GossipRouter.GET:
                        processGetRequest(sock, output, gname); // closes sock after processing
                        break;
                    case GossipRouter.DUMP:
                        processDumpRequest(peer_addr, sock, output); // closes sock after processing
                        break;
                    case GossipRouter.REGISTER:
                        Address addr;
                        len=input.readInt();
                        buf=new byte[len];
                        input.readFully(buf, 0, buf.length); // read Address
                        addr=(Address)Util.objectFromByteBuffer(buf);
                        addEntry(gname, new AddressEntry(addr, sock, output));
                        new SocketThread(sock, input).start();
                        break;
                    default:
                        Trace.error("GossipRouter", "request of type " + type + " not recognized");
                        continue;
                }
            }
            catch(Exception e) {
                Trace.error("GossipRouter", "Failure handling a client connection: " + e);
		e.printStackTrace();
                try {
                    sock.close();
                }
                catch(IOException e2) {
                    Trace.warn("GossipRouter", "Failed to close socket "+sock);
                }
                continue;
            }
        }
    }

    public void stop() {
        // TO_DO
    }


    //
    // GOSSIPING
    //

    /**
     * @since 2.2.1
     **/
    private GossipData processGossip(GossipData gossip) {

        if (Trace.debug) {
            Trace.debug("GossipRouter", "processGossip("+gossip+")");
        }

        if (gossip==null) {
            Trace.warn("Route", "null gossip request");
            return null;
        }

        String group = gossip.getGroup();
        Address mbr = null;

        synchronized(gossipTable) {

            switch(gossip.getType()) {
                
            case GossipData.REGISTER_REQ: 
                mbr=gossip.getMbr();
                if(group == null || mbr == null) {
                    Trace.error("GossipRouter", "processGossip(): group or member is null, cannot register member");
                    return null;
                }
                addGossipEntry(group, new AddressEntry(mbr));
                return null;

            case GossipData.GET_REQ:
                if(group == null) {
                    Trace.error("GossipRouter", "group is null, cannot get membership");
                    return null;
                }
                Vector mbrs = null;
                List l = (List)gossipTable.get(group);
                if (l != null) {
                    mbrs = new Vector();
                    for(Iterator i = l.iterator(); i.hasNext(); ) {
                        AddressEntry e = (AddressEntry)i.next();
                        mbrs.add(e.addr);
                    }
                }
                return new GossipData(GossipData.GET_RSP, group, null, mbrs);

            case GossipData.GET_RSP: 
                Trace.warn("GossipRouter", "received a GET_RSP. Should not be received by server");
                return null;

            default:
                Trace.warn("GossipRouter", "received unkown gossip request (gossip=" + gossip + ")");
                return null;
            }
        }
    }


    /**
     * Adds a new member to the group in the gossip table or renews the 
     * membership where is the case.
     *
     * @since 2.2.1
     **/
    private void addGossipEntry(String groupname, AddressEntry e) {

        List val;

        if(groupname == null) {
            Trace.error("GossipRouter", "groupname was null, not added !");
            return;
        }

        synchronized(gossipTable) {

            val=(List)gossipTable.get(groupname);
            if(val == null) {
                val=Collections.synchronizedList(new ArrayList());
                gossipTable.put(groupname, val);
            }
            int index = val.indexOf(e);
            if (index==-1) {
                val.add(e);
                return;
            }
            ((AddressEntry)val.get(index)).update();
        }
    }


    /**   
     * Removes expired gossip entries (entries older than EXPIRY_TIME msec).
     * @since 2.2.1
     */
    private void sweep() {

        long diff, currentTime=System.currentTimeMillis();
        int num_entries_removed=0;
        String key=null;
        List val;

        if (Trace.debug) {
            Trace.debug("GossipRouter", "running sweep");
        }

        synchronized(gossipTable) {
            for(Iterator i=gossipTable.keySet().iterator(); i.hasNext();) {
                key=(String)i.next();
                val=(List)gossipTable.get(key);
                if(val != null) {
                    for(Iterator j=val.iterator(); j.hasNext();) {
                        AddressEntry ae = (AddressEntry)j.next();
                        diff=currentTime - ae.timestamp;
                        if(diff > expiryTime) {
                            j.remove();
                            if(Trace.trace)
                                Trace.info("GossipRouter", "removed member " + ae +
                                           " from group " + key + "(" + diff + " msecs old)");
                            num_entries_removed++;
                        }
                    }
                }
            }
        }
        
        if(Trace.trace && num_entries_removed > 0) {
            Trace.info("GossipRouter", "done (removed " + num_entries_removed + " entries)");
        }
    }

    //
    // ROUTING
    //


    /**
     Gets the members of group 'groupname'. Returns them as a List of Addresses.
     */
    private void processGetRequest(Socket sock, DataOutputStream output, String groupname) {

        List grpmbrs=(List)groups.get(groupname);
        org.jgroups.util.List ret=null;
        AddressEntry entry;
        byte[] buf;

        if(Trace.debug) {
            Trace.debug("GossipRouter.processGetRequest()", "groupname=" + groupname + ", result=" + grpmbrs);
        }

        if(grpmbrs != null && grpmbrs.size() > 0) {
            ret=new org.jgroups.util.List();
            for(Iterator i=grpmbrs.iterator(); i.hasNext(); ) {
                entry=(AddressEntry)i.next();
                ret.add(entry.addr);
            }
        }
        try {
            if(ret == null || ret.size() == 0) {
                output.writeInt(0);
            }
            else {
                buf=Util.objectToByteBuffer(ret);
                output.writeInt(buf.length);
                output.write(buf, 0, buf.length);
            }
        }
        catch(Exception e) {
            Trace.error("GossipRouter.processGetRequest()", "exception=" + e);
        }
        finally {
            try {
                if(output != null)
                    output.close();
                sock.close();
            }
            catch(Exception e) {
            }
        }
    }


    /**
     * Dumps the routing table as String to the socket's OutputStream.
     **/
    private void processDumpRequest(Address peerAddress, Socket sock, DataOutputStream output) {

        StringBuffer sb=new StringBuffer();
        synchronized(groups) {
            if(groups.size() == 0) {
                sb.append("empty routing table");
            }
            else {
                for(Iterator i=groups.keySet().iterator(); i.hasNext();) {
                    String gname=(String)i.next();
                    sb.append("GROUP: '" + gname + "'\n");
                    List l=(List)groups.get(gname);
                    if(l == null) {
                        sb.append("\tnull list of addresses\n");
                    }
                    else
                        if(l.size() == 0) {
                            sb.append("\tempty list of addresses\n");
                        }
                        else {
                            for(Iterator j=l.iterator(); j.hasNext();) {
                                AddressEntry ae=(AddressEntry)j.next();
                                sb.append("\t");
                                sb.append(ae.toString());
                                sb.append("\n");
                            }
                        }
                }
            }
        }
        try {
            output.writeUTF(sb.toString());
        }
        catch(Exception e) {
            Trace.error("GossipRouter",
                        "Error sending the answer back to the client: " + e);
        }
        finally {
            try {
                if(output != null) {
                    output.close();
                }
            }
            catch(Exception e) {
                Trace.error("GossipRouter",
                            "Error closing the output stream: " + e);
            }
            try {
                sock.close();
            }
            catch(Exception e) {
                Trace.error("GossipRouter",
                            "Error closing the socket: " + e);
            }
        }
    }



    private synchronized void route(Address dest, String dest_group, byte[] msg) {

	Message message = null;
	try {
	    message = (Message)Util.objectFromByteBuffer(msg);
	}
	catch(Exception e) {
	    Trace.error("GossipRouter", "error when deserializing message: "+e.getMessage());
	}

        if (Trace.debug) {
            Trace.debug("GossipRouter", "routing request for "+dest_group+", destination "+
                        (dest==null?"ALL":dest.toString())+", message="+message);
        }


        if(dest == null) { 
            // send to all members in group dest.getChannelName()
            if(dest_group == null) {
                Trace.error("GossipRouter", "both dest address and group are null");
                return;
            }
            else {
                sendToAllMembersInGroup(dest_group, msg);
            }
        }
        else {                  
            // send to destination address
            DataOutputStream out=findSocket(dest);
            if(out != null) {
                sendToMember(out, msg);
            }
            else {
                Trace.error("GossipRouter", "routing of message to " + dest + " failed; outstream is null !");
            }
        }
    }


    /**
     * Adds a new member to the routing group.
     **/
    private void addEntry(String groupname, AddressEntry e) {

        List val;

        if(groupname == null) {
            Trace.error("GossipRouter.addEntry()", "groupname was null, not added !");
            return;
        }

        synchronized(groups) {
            val=(List)groups.get(groupname);

            if(val == null) {
                val=Collections.synchronizedList(new ArrayList());
                groups.put(groupname, val);
            }
            int index = val.indexOf(e);
            if (index==-1) {
                val.add(e);
                return;
            }
            // new connection for an existing member
            ((AddressEntry)val.remove(index)).destroy();
            val.add(e);
        }
    }


    private void removeEntry(Socket sock) {

        List val;
        AddressEntry entry;
        synchronized(groups) {
            for(Enumeration e=groups.keys(); e.hasMoreElements();) {
                val=(List)groups.get(e.nextElement());
                for(Iterator i=val.iterator(); i.hasNext();) {
                    entry=(AddressEntry)i.next();
                    if(entry.sock == sock) {
                        entry.destroy();
                        //Util.print("Removing entry " + entry);
                        i.remove();
                        return;
                    }
                }
            }
        }
    }


    private void removeEntry(OutputStream out) {

        List val;
        AddressEntry entry;
        synchronized(groups) {
            for(Enumeration e=groups.keys(); e.hasMoreElements();) {
                val=(List)groups.get(e.nextElement());
                for(Iterator i=val.iterator(); i.hasNext();) {
                    entry=(AddressEntry)i.next();
                    if(entry.output == out) {
                        entry.destroy();
                        //Util.print("Removing entry " + entry);
                        i.remove();
                        return;
                    }
                }
            }
        }
    }


    private void removeEntry(String groupname, Address addr) {

        List val;
        AddressEntry entry;
        synchronized(groups) {
            val=(List)groups.get(groupname);
            if(val == null || val.size() == 0) {
                return;
            }
            for(Iterator i=val.iterator(); i.hasNext();) {
                entry=(AddressEntry)i.next();
                if(entry.addr.equals(addr)) {
                    entry.destroy();
                    //Util.print("Removing entry " + entry);
                    i.remove();
                    return;
                }
            }
        }
    }


    /**
     * Returns null for 'gossip' members or if the address is not found.
     **/
    private DataOutputStream findSocket(Address addr) {
        
        List val;
        AddressEntry entry;
        synchronized(groups) {
            for(Enumeration e=groups.keys(); e.hasMoreElements();) {
                val=(List)groups.get(e.nextElement());
                for(Iterator i=val.iterator(); i.hasNext();) {
                    entry=(AddressEntry)i.next();
                    if(addr.equals(entry.addr)) {
                        return entry.output;
                    }
                }
            }
            return null;
        }
    }




    private void sendToAllMembersInGroup(String groupname, byte[] msg) {

        List val;
        synchronized(groups) {
            val=(List)groups.get(groupname);
            if(val == null || val.size() == 0) {
                return;
            }
            for(Iterator i=val.iterator(); i.hasNext();) {
                DataOutputStream dos = ((AddressEntry)i.next()).output;
                if (dos!=null) {
                    // send only to 'connected' members
                    sendToMember(dos, msg);
                }
            }
        }
    }


    private void sendToMember(DataOutputStream out, byte[] msg) {

        if (out==null) {
            return;
        }
        try {
            out.writeInt(msg.length);
            out.write(msg, 0, msg.length);
        }
        catch(Exception e) {
            Trace.error("GossipRouter", "sendToMember: exception=" + e);
            removeEntry(out); // closes socket
        }
    }



    /**
     * Class used to store Addresses in both routing and gossip tables. 
     * If it is used for routing, sock and output have valid values, otherwise
     * they're null and only the timestamp counts.
     **/
    class AddressEntry {
        
        Address addr=null;
        Socket sock=null;
        DataOutputStream output=null;
        long timestamp=0;

        /**
         * AddressEntry for a 'gossip' membership.
         **/
        public AddressEntry(Address addr) {
            this(addr, null, null);
        }

        public AddressEntry(Address addr, Socket sock, DataOutputStream output) {
            this.addr=addr;
            this.sock=sock;
            this.output=output;
            this.timestamp = System.currentTimeMillis();
        }

        void destroy() {
            if(output != null) {
                try {
                    output.close();
                }
                catch(Exception e) {
                }
                output=null;
            }
            if(sock != null) {
                try {
                    sock.close();
                }
                catch(Exception e) {
                }
                sock=null;
            }
            timestamp = 0;
        }

        public void update() {
            timestamp = System.currentTimeMillis();
        }

        public boolean equals(Object other) {
            return addr.equals(((AddressEntry)other).addr);
        }

        public String toString() {
            StringBuffer sb = new StringBuffer("addr=");
            sb.append(addr);
            if (sock==null) {
                sb.append(", timestamp=");
                sb.append(timestamp);
            }
            else {
                sb.append(", sock=");
                sb.append(sock);
            }
            return sb.toString();
        }
    }

    /** A SocketThread manages one connection to a client. Its main task is message routing. */
    class SocketThread extends Thread {

        Socket sock=null;
        DataInputStream input=null;

        public SocketThread(Socket sock, DataInputStream ois) {
            this.sock=sock;
            input=ois;
        }

        void closeSocket() {
            try {
                if(input != null)
                    input.close();
                if(sock != null)
                    sock.close();
            }
            catch(Exception e) {
            }
        }


        public void run() {

            byte[] buf;
            int len;
            Address dst_addr=null;
            String gname;

            while(true) {
                try {
                    gname=input.readUTF(); // group name
                    len=input.readInt();
                    if(len == 0)
                        dst_addr=null;
                    else {
                        buf=new byte[len];
                        input.readFully(buf, 0, buf.length);  // dest address
                        dst_addr=(Address)Util.objectFromByteBuffer(buf);
                    }

                    len=input.readInt();
                    if(len == 0) {
                        Trace.warn("GossipRouter.SocketThread.run()", "received null message");
                        continue;
                    }
                    buf=new byte[len];
                    input.readFully(buf, 0, buf.length);  // message
                    route(dst_addr, gname, buf);
                }
                catch(EOFException io_ex) {
                    if(Trace.trace)
                        Trace.info("GossipRouter", "client " +sock.getInetAddress().getHostName() + ":" + sock.getPort() +
                                   " closed connection; removing it from routing table");
                    removeEntry(sock); // will close socket
                    return;
                }
                catch(Exception e) {
                    Trace.error("GossipRouter","SocketThread.run() exception=" + e);
                    break;
                }
            }
            closeSocket();
        }

    }


    public static void main(String[] args) throws Exception {
        String arg;
        int port=8080;
        long expiry = GossipRouter.EXPIRY_TIME;
        long timeout = GossipRouter.GOSSIP_REQUEST_TIMEOUT;
        long routingTimeout = GossipRouter.ROUTING_CLIENT_REPLY_TIMEOUT;
        GossipRouter router=null;
        InetAddress address=null;

        for(int i=0; i < args.length; i++) {
            arg=args[i];
            if(arg.equals("-help")) {
                System.out.println();
                System.out.println("GossipRouter [-port <port>] [-bindaddress <address>] [options]");
                System.out.println("Options: ");
                System.out.println("        -expiry <msecs>   - Time until a gossip cache entry expires.");
                System.out.println("        -timeout <msecs>  - Number of millisecs the router waits to receive");
                System.out.println("                            a gossip request after connection was established;");
                System.out.println("                            upon expiration, the router initiates the routing");
                System.out.println("                            protocol on the connection.");
                return;
            }
            else if(arg.equals("-port")) {
                    port=new Integer(args[++i]).intValue();
            }
            else if(arg.equals("-bindaddress")) {
                address=InetAddress.getByName(args[++i]);
            }
            else if(arg.equals("-expiry")) {
                expiry=new Long(args[++i]).longValue();
            }
            else if(arg.equals("-timeout")) {
                timeout=new Long(args[++i]).longValue();
            }
            else if(arg.equals("-rtimeout")) {
                routingTimeout=new Long(args[++i]).longValue();
            }
        }
        System.out.println("GossipRouter is starting...");
        Trace.init();

        try {
            router= new GossipRouter(port, address, expiry, timeout, routingTimeout);
            router.start();
            System.out.println("GossipRouter was created at " + new Date());
            System.out.println("Listening on port " + port + " and bound to address " + address);
        }
        catch(Exception e) {
            System.err.println(e);
        }
    }


}
