// $Id: GossipRouter.java,v 1.20 2006/10/11 14:37:06 belaban Exp $

package org.jgroups.stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.Address;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.util.Util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

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
 * @author Bela Ban
 * @author Ovidiu Feodorov <ovidiuf@users.sourceforge.net>
 * @since 2.1.1
 */
public class GossipRouter {
    public static final byte CONNECT=1; // CONNECT(group, addr) --> local address
    public static final byte DISCONNECT=2; // DISCONNECT(group, addr)
    public static final byte REGISTER=3; // REGISTER(group, addr)
    public static final byte GOSSIP_GET=4; // GET(group) --> List<addr> (members)
    public static final byte ROUTER_GET=5; // GET(group) --> List<addr> (members)
    public static final byte GET_RSP=6; // GET_RSP(List<addr>)
    public static final byte UNREGISTER=7; // UNREGISTER(group, addr)
    public static final byte DUMP=8; // DUMP
    public static final byte SHUTDOWN=9;

    public static final int PORT=8980;
    public static final long EXPIRY_TIME=30000;
    public static final long GOSSIP_REQUEST_TIMEOUT=1000;
    public static final long ROUTING_CLIENT_REPLY_TIMEOUT=120000;

    private int port;
    private String bindAddressString;

    // time (in msecs) until a cached 'gossip' member entry expires
    private long expiryTime;

    // number of millisecs the main thread waits to receive a gossip request
    // after connection was established; upon expiration, the router initiates
    // the routing protocol on the connection. Don't set the interval too big, 
    // otherwise the router will appear slow in answering routing requests.
    private long gossipRequestTimeout;

    // time (in ms) main thread waits for a router client to send the routing 
    // request type and the group afiliation before it declares the request
    // failed.
    private long routingClientReplyTimeout;

    // HashMap<String,List<AddressEntry>. Maintains associations between groups and their members
    private final Hashtable routingTable=new Hashtable();

    // (groupname - vector of AddressEntry's)
    private final Map gossipTable=new HashMap();

    private ServerSocket srvSock=null;
    private InetAddress bindAddress=null;

    private boolean up=true;

    /** whether to discard message sent to self */
    private boolean discard_loopbacks=false;


    // the cache sweeper
    Timer timer=null;

    protected final Log log=LogFactory.getLog(this.getClass());

    //
    // JMX INSTRUMENTATION - MANAGEMENT INTERFACE
    //

    public GossipRouter() {
        this(PORT);
    }

    public GossipRouter(int port) {
        this(port, null);
    }

    public GossipRouter(int port, String bindAddressString) {
        this(port, bindAddressString, EXPIRY_TIME);
    }

    public GossipRouter(int port, String bindAddressString,
                        long expiryTime) {
        this(port, bindAddressString, expiryTime,
             GOSSIP_REQUEST_TIMEOUT,
             ROUTING_CLIENT_REPLY_TIMEOUT);
    }

    public GossipRouter(int port, String bindAddressString,
                        long expiryTime, long gossipRequestTimeout,
                        long routingClientReplyTimeout) {
        this.port=port;
        this.bindAddressString=bindAddressString;
        this.expiryTime=expiryTime;
        this.gossipRequestTimeout=gossipRequestTimeout;
        this.routingClientReplyTimeout=routingClientReplyTimeout;
    }

    //
    // MANAGED ATTRIBUTES
    //

    public void setPort(int port) {
        this.port=port;
    }

    public int getPort() {
        return port;
    }

    public void setBindAddress(String bindAddress) {
        bindAddressString=bindAddress;
    }

    public String getBindAddress() {
        return bindAddressString;
    }

    public void setExpiryTime(long expiryTime) {
        this.expiryTime=expiryTime;
    }

    public long getExpiryTime() {
        return expiryTime;
    }

    public void setGossipRequestTimeout(long gossipRequestTimeout) {
        this.gossipRequestTimeout=gossipRequestTimeout;
    }

    public long getGossipRequestTimeout() {
        return gossipRequestTimeout;
    }

    public void setRoutingClientReplyTimeout(long routingClientReplyTimeout) {
        this.routingClientReplyTimeout=routingClientReplyTimeout;
    }

    public long getRoutingClientReplyTimeout() {
        return routingClientReplyTimeout;
    }

    public boolean isStarted() {
        return srvSock != null;
    }

    public boolean isDiscardLoopbacks() {
        return discard_loopbacks;
    }

    public void setDiscardLoopbacks(boolean discard_loopbacks) {
        this.discard_loopbacks=discard_loopbacks;
    }


    public static String type2String(int type) {
        switch(type) {
            case CONNECT:
                return "CONNECT";
            case DISCONNECT:
                return "DISCONNECT";
            case REGISTER:
                return "REGISTER";
            case GOSSIP_GET:
                return "GOSSIP_GET";
            case ROUTER_GET:
                return "ROUTER_GET";
            case GET_RSP:
                return "GET_RSP";
            case UNREGISTER:
                return "UNREGISTER";
            case DUMP:
                return "DUMP";
            case SHUTDOWN:
                return "SHUTDOWN";
            default:
                return "unknown";
        }
    }

    //
    // JBoss MBean LIFECYCLE OPERATIONS
    //


    /**
     * JBoss MBean lifecycle operation.
     */
    public void create() throws Exception {
        // not used
    }

    /**
     * JBoss MBean lifecycle operation. Called after create(). When this method
     * is called, the managed attributes have already been set.<br>
     * Brings the Router in fully functional state.
     */
    public void start() throws Exception {
        if(srvSock != null) {
            throw new Exception("Router already started.");
        }

        if(bindAddressString != null) {
            bindAddress=InetAddress.getByName(bindAddressString);
            srvSock=new ServerSocket(port, 50, bindAddress);
        }
        else {
            srvSock=new ServerSocket(port, 50);
        }

        up=true;

        // start the main server thread
        new Thread(new Runnable() {
            public void run() {
                mainLoop();
                cleanup();
            }
        }, "GossipRouter").start();

        // starts the cache sweeper as daemon thread, so we won't block on it
        // upon termination
        timer=new Timer(true);
        timer.schedule(new TimerTask() {
            public void run() {
                sweep();
            }
        }, expiryTime, expiryTime);
    }

    /**
     * JBoss MBean lifecycle operation. The JMX agent allways calls this method
     * before destroy(). Close connections and frees resources.
     */
    public void stop() {
        up=false;

        if(srvSock == null) {
            if(log.isWarnEnabled()) log.warn("router already stopped");
            return;
        }

        timer.cancel();
        shutdown();
        try {
            srvSock.close();
        }
        catch(Exception e) {
            if(log.isErrorEnabled()) log.error("Failed to close server socket: " + e);
        }
        // exiting the mainLoop will clean the tables
        srvSock=null;
        if(log.isInfoEnabled()) log.info("router stopped");
    }

    /**
     * JBoss MBean lifecycle operation.
     */
    public void destroy() {
        // not used
    }

    //
    // ORDINARY OPERATIONS
    //

    public String dumpRoutingTable() {
        return dumpTable(routingTable);
    }

    public String dumpGossipTable() {
        return dumpTable(gossipTable);
    }


    /**
     * The main server loop. Runs on the JGroups Router Main Thread.
     */
    private void mainLoop() {
        Socket sock=null;
        DataInputStream input=null;
        DataOutputStream output=null;
        Address peer_addr=null, mbr;

        if(bindAddress == null) {
            bindAddress=srvSock.getInetAddress();
        }
        System.out.println("GossipRouter started at " + new Date() +
                "\nListening on port " + port + " bound on address " + bindAddress + '\n');

        GossipData req;
        String group;

        while(up && srvSock != null) {
            try {
                sock=srvSock.accept();
                sock.setSoLinger(true, 500);
                input=new DataInputStream(sock.getInputStream());
                // if(log.isTraceEnabled())
                   // log.trace("accepted connection from " + sock);

                req=new GossipData();
                req.readFrom(input);

                switch(req.getType()) {
                    case GossipRouter.REGISTER:
                        mbr=req.getAddress();
                        group=req.getGroup();
                        if(log.isTraceEnabled())
                            log.trace("REGISTER(" + group + ", " + mbr + ")");
                        if(group == null || mbr == null) {
                            if(log.isErrorEnabled()) log.error("group or member is null, cannot register member");
                        }
                        else
                            addGossipEntry(group, new AddressEntry(mbr));
                        Util.close(input);
                        Util.close(sock);
                        break;

                    case GossipRouter.UNREGISTER:
                        mbr=req.getAddress();
                        group=req.getGroup();
                        if(log.isTraceEnabled())
                            log.trace("UNREGISTER(" + group + ", " + mbr + ")");
                        if(group == null || mbr == null) {
                            if(log.isErrorEnabled()) log.error("group or member is null, cannot unregister member");
                        }
                        else
                            removeGossipEntry(group, mbr);
                        Util.close(input);
                        Util.close(output);
                        Util.close(sock);
                        break;

                    case GossipRouter.GOSSIP_GET:
                        group=req.getGroup();
                        List mbrs=null;
                        List l=(List)gossipTable.get(group);
                        if(l != null) {
                            mbrs=new LinkedList();
                            for(Iterator i=l.iterator(); i.hasNext();) {
                                AddressEntry e=(AddressEntry)i.next();
                                mbrs.add(e.addr);
                            }
                        }
                        if(log.isTraceEnabled())
                            log.trace("GOSSIP_GET(" + group + ") --> " + mbrs);
                        output=new DataOutputStream(sock.getOutputStream());
                        GossipData rsp=new GossipData(GossipRouter.GET_RSP, group, null, mbrs);
                        rsp.writeTo(output);
                        Util.close(input);
                        Util.close(output);
                        Util.close(sock);
                        break;

                    case GossipRouter.ROUTER_GET:
                        group=req.getGroup();
                        output=new DataOutputStream(sock.getOutputStream());
                        List ret=getMembers(group);
                        if(log.isTraceEnabled())
                            log.trace("ROUTER_GET(" + group + ") --> " + ret);
                        rsp=new GossipData(GossipRouter.GET_RSP, group, null, ret);
                        rsp.writeTo(output);
                        Util.close(input);
                        Util.close(output);
                        Util.close(sock);
                        break;

                    case GossipRouter.DUMP:
                        output=new DataOutputStream(sock.getOutputStream());
                        output.writeUTF(dumpRoutingTable());
                        Util.close(input);
                        Util.close(output);
                        Util.close(sock);
                        break;

                    case GossipRouter.CONNECT:
                        output=new DataOutputStream(sock.getOutputStream());
                        peer_addr=new IpAddress(sock.getInetAddress(), sock.getPort());
                        output=new DataOutputStream(sock.getOutputStream());
                        // return the address of the peer so it can set it
                        Util.writeAddress(peer_addr, output);
                        String group_name=req.getGroup();
                        if(log.isTraceEnabled())
                            log.trace("CONNECT(" + group_name + ", " + peer_addr + ")");
                        SocketThread st=new SocketThread(sock, input, peer_addr);
                        addEntry(group_name, new AddressEntry(peer_addr, sock, st, output));
                        st.start();
                        break;

                    case GossipRouter.DISCONNECT:
                        Address addr=req.getAddress();
                        group_name=req.getGroup();
                        removeEntry(group_name, addr);
                        if(log.isTraceEnabled())
                            log.trace("DISCONNECT(" + group_name + ", " + addr + ")");
                        Util.close(input);
                        Util.close(output);
                        Util.close(sock);
                        break;

                    case GossipRouter.SHUTDOWN:
                        if(log.isInfoEnabled()) log.info("router shutting down");
                        Util.close(input);
                        Util.close(output);
                        Util.close(sock);
                        up=false;
                        break;
                    default:
                        if(log.isWarnEnabled())
                            log.warn("received unkown gossip request (gossip=" + req + ')');
                        break;
                }
            }
            catch(Exception e) {
                if(up)
                    if(log.isErrorEnabled()) log.error("failure handling a client connection", e);
                Util.close(input);
                Util.close(output);
                Util.close(sock);
            }
        }
    }


    /**
     * Cleans the routing tables while the Router is going down.
     */
    private void cleanup() {

        // shutdown the routing threads and cleanup the tables
        synchronized(routingTable) {
            for(Iterator i=routingTable.keySet().iterator(); i.hasNext();) {
                String gname=(String)i.next();
                List l=(List)routingTable.get(gname);
                if(l != null) {
                    for(Iterator j=l.iterator(); j.hasNext();) {
                        AddressEntry e=(AddressEntry)j.next();
                        e.destroy();
                    }
                }
            }
            routingTable.clear();
        }
        synchronized(gossipTable) {
            gossipTable.clear();
        }

    }

    /**
     * Connects to the ServerSocket and sends the shutdown header.
     */
    private void shutdown() {
        Socket s=null;
        DataOutputStream dos=null;
        try {
            s=new Socket(srvSock.getInetAddress(),srvSock.getLocalPort());
            dos=new DataOutputStream(s.getOutputStream());
            dos.writeInt(SHUTDOWN);
            dos.writeUTF("");
        }
        catch(Exception e) {
            if(log.isErrorEnabled()) log.error("shutdown failed: " + e);
        }
        finally {
            Util.close(s);
            Util.close(dos);
        }
    }


    /**
     * Adds a new member to the group in the gossip table or renews the
     * membership where is the case.
     * @since 2.2.1
     */
    private void addGossipEntry(String groupname, AddressEntry e) {
        List val;
        if(groupname == null) {
            if(log.isErrorEnabled()) log.error("groupname was null, not added !");
            return;
        }

        synchronized(gossipTable) {
            val=(List)gossipTable.get(groupname);
            if(val == null) {
                val=Collections.synchronizedList(new ArrayList());
                gossipTable.put(groupname, val);
            }
            int index=val.indexOf(e);
            if(index == -1) {
                val.add(e);
                return;
            }
            ((AddressEntry)val.get(index)).update();
        }
    }


    private void removeGossipEntry(String groupname, Address mbr) {
        List val=null;
        if(groupname == null || mbr == null) {
            if(log.isErrorEnabled()) log.error("groupname or mbr was null, not removed");
            return;
        }

        synchronized(gossipTable) {
            val=(List)gossipTable.get(groupname);
            if(val == null)
                return;
        }
        AddressEntry entry;
        for(Iterator it=val.iterator(); it.hasNext();) {
            entry=(GossipRouter.AddressEntry)it.next();
            if(mbr.equals(entry.addr)) {
                entry.destroy();
                it.remove();
                break;
            }
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

        synchronized(gossipTable) {
            for(Iterator i=gossipTable.keySet().iterator(); i.hasNext();) {
                key=(String)i.next();
                val=(List)gossipTable.get(key);
                if(val != null) {
                    for(Iterator j=val.iterator(); j.hasNext();) {
                        AddressEntry ae=(AddressEntry)j.next();
                        diff=currentTime - ae.timestamp;
                        if(diff > expiryTime) {
                            j.remove();
                            if(log.isTraceEnabled())
                                log.trace("removed " + ae + " from " + key + " (" + diff + " msecs old)");
                            num_entries_removed++;
                        }
                    }
                }
                if(val.size() == 0)
                    i.remove();
            }
        }

        if(num_entries_removed > 0) {
            if(log.isTraceEnabled()) log.trace("done (removed " + num_entries_removed + " entries)");
        }
    }


    /**
     * Gets the members of group 'groupname'. Returns them as a List of Addresses.
     */
    private List getMembers(String groupname) {
        List grpmbrs=null;
        List ret=new LinkedList();
        AddressEntry entry;

        synchronized(routingTable) {
            grpmbrs=(List)routingTable.get(groupname);
            if(grpmbrs != null && grpmbrs.size() > 0) {
                for(Iterator i=grpmbrs.iterator(); i.hasNext();) {
                    entry=(AddressEntry)i.next();
                    ret.add(entry.addr);
                }
            }
        }
        return ret;
    }


    private String dumpTable(Map map) {
        String label=(map instanceof Hashtable)? "routing" : "gossip";
        StringBuffer sb=new StringBuffer();
        synchronized(map) {
            if(map.size() == 0) {
                sb.append("empty ");
                sb.append(label);
                sb.append(" table");
            }
            else {
                for(Iterator i=map.keySet().iterator(); i.hasNext();) {
                    String gname=(String)i.next();
                    sb.append("GROUP: '" + gname + "'\n");
                    List l=(List)map.get(gname);
                    if(l == null) {
                        sb.append("\tnull list of addresses\n");
                    }
                    else if(l.size() == 0) {
                        sb.append("\tempty list of addresses\n");
                    }
                    else {
                        for(Iterator j=l.iterator(); j.hasNext();) {
                            AddressEntry ae=(AddressEntry)j.next();
                            sb.append('\t');
                            sb.append(ae.toString());
                            sb.append('\n');
                        }
                    }
                }
            }
        }
        return sb.toString();
    }


    private void route(Address dest, String dest_group, byte[] msg, Address sender) {
        //if(log.isTraceEnabled()) {
          //  int len=msg != null? msg.length : 0;
            //log.trace("routing request from " + sender + " for " + dest_group + " to " +
              //      (dest == null? "ALL" : dest.toString()) + ", " + len + " bytes");
        //}

        if(dest == null) {
            // send to all members in group dest.getChannelName()
            if(dest_group == null) {
                if(log.isErrorEnabled()) log.error("both dest address and group are null");
            }
            else {
                sendToAllMembersInGroup(dest_group, msg, sender);
            }
        }
        else {
            // send to destination address
            AddressEntry ae=findAddressEntry(dest);
            if(ae == null) {
                if(log.isErrorEnabled())
                    log.error("cannot find " + dest + " in the routing table, routing table=\n" + dumpRoutingTable() +
                            "\ngossip table=\n" + dumpGossipTable());
                return;
            }
            if(ae.output == null) {
                if(log.isErrorEnabled()) log.error(dest + " is associated with a null output stream");
                return;
            }
            try {
                sendToMember(dest, ae.output, msg, sender);
            }
            catch(Exception e) {
                if(log.isErrorEnabled()) log.error("failed sending message to " + dest + ": " + e.getMessage());
                removeEntry(ae.sock); // will close socket
            }
        }
    }


    /**
     * Adds a new member to the routing group.
     */
    private void addEntry(String groupname, AddressEntry e) {
        List val;

        if(groupname == null) {
            if(log.isErrorEnabled()) log.error("groupname was null, not added !");
            return;
        }

        synchronized(routingTable) {
            val=(List)routingTable.get(groupname);
            if(val == null) {
                val=Collections.synchronizedList(new ArrayList());
                routingTable.put(groupname, val);
            }
            int index=val.indexOf(e);
            if(index == -1) {
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
        synchronized(routingTable) {
            for(Enumeration e=routingTable.keys(); e.hasMoreElements();) {
                val=(List)routingTable.get(e.nextElement());
                for(Iterator i=val.iterator(); i.hasNext();) {
                    entry=(AddressEntry)i.next();
                    if(entry.sock == sock) {
                        entry.destroy();
                        i.remove();
                        return;
                    }
                }
            }
        }
    }


    private void removeEntry(String groupname, Address member) {
        List val;
        synchronized(routingTable) {
            val=(List)routingTable.get(groupname);
            if(val == null)
                return;
        }
        synchronized(val) {
            for(Iterator it=val.iterator(); it.hasNext();) {
                AddressEntry entry=(AddressEntry)it.next();
                if(entry.addr != null && entry.addr.equals(member)) {
                    it.remove();
                    return;
                }
            }
        }
    }


    /**
     * @return null if not found
     */
    private AddressEntry findAddressEntry(Address addr) {
        List val;
        AddressEntry entry;
        synchronized(routingTable) {
            for(Enumeration e=routingTable.keys(); e.hasMoreElements();) {
                val=(List)routingTable.get(e.nextElement());
                for(Iterator i=val.iterator(); i.hasNext();) {
                    entry=(AddressEntry)i.next();
                    if(addr.equals(entry.addr)) {
                        return entry;
                    }
                }
            }
            return null;
        }
    }


    private void sendToAllMembersInGroup(String groupname, byte[] msg, Address sender) {
        List val;
        val=(List)routingTable.get(groupname);
        if(val == null || val.size() == 0) {
            return;
        }

        synchronized(val) {
            for(Iterator i=val.iterator(); i.hasNext();) {
                AddressEntry ae=(AddressEntry)i.next();
                DataOutputStream dos=ae.output;

                if(dos != null) {
                    // send only to 'connected' members
                    try {
                        sendToMember(null, dos, msg, sender);
                    }
                    catch(Exception e) {
                        if(log.isWarnEnabled()) log.warn("cannot send to " + ae.addr + ": " + e.getMessage());
                        ae.destroy(); // this closes the socket
                        i.remove();
                    }
                }
            }
        }
    }


    /**
     * @throws IOException
     */
    private void sendToMember(Address dest, DataOutputStream out, byte[] msg, Address sender) throws IOException {
        if(out == null)
            return;

        if(discard_loopbacks && dest != null && dest.equals(sender)) {
            return;
        }

        synchronized(out) {
            Util.writeAddress(dest, out);
            out.writeInt(msg.length);
            out.write(msg, 0, msg.length);
        }
    }


    /**
     * Class used to store Addresses in both routing and gossip tables.
     * If it is used for routing, sock and output have valid values, otherwise
     * they're null and only the timestamp counts.
     */
    class AddressEntry {
        Address addr=null;
        Socket sock=null;
        DataOutputStream output=null;
        long timestamp=0;
        final SocketThread thread;

        /**
         * AddressEntry for a 'gossip' membership.
         */
        public AddressEntry(Address addr) {
            this(addr, null, null, null);
        }

        public AddressEntry(Address addr, Socket sock, SocketThread thread, DataOutputStream output) {
            this.addr=addr;
            this.sock=sock;
            this.thread=thread;
            this.output=output;
            this.timestamp=System.currentTimeMillis();
        }

        void destroy() {
            if(thread != null) {
                thread.finish();
            }
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
            timestamp=0;
        }

        public void update() {
            timestamp=System.currentTimeMillis();
        }

        public boolean equals(Object other) {
            return addr.equals(((AddressEntry)other).addr);
        }

        public String toString() {
            StringBuffer sb=new StringBuffer("addr=");
            sb.append(addr);
            if(sock == null) {
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


    private static int threadCounter=0;


    /**
     * A SocketThread manages one connection to a client. Its main task is message routing.
     */
    class SocketThread extends Thread {
        private volatile boolean active=true;
        Socket sock=null;
        DataInputStream input=null;
        Address addr=null;

        public SocketThread(Socket sock, DataInputStream ois, Address addr) {
            super(Util.getGlobalThreadGroup(), "SocketThread " + (threadCounter++));
            this.sock=sock;
            input=ois;
            this.addr=addr;
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

        void finish() {
            if(log.isTraceEnabled()) log.trace("terminating the SocketThread for " + sock);
            active=false;
        }


        public void run() {
            byte[] buf;
            int len;
            Address dst_addr=null;
            String gname;

            while(active) {
                try {
                    // 1. Group name is first
                    gname=input.readUTF();

                    // 2. Second is the destination address
                    dst_addr=Util.readAddress(input);

                    //if(log.isTraceEnabled()) {
                      //  log.trace("group " + gname + ", routing request to " + (dst_addr == null? "all" : dst_addr.toString()));
                    //}

                    // 3. Then the length of the byte buffer representing the message
                    len=input.readInt();
                    if(len == 0) {
                        if(log.isWarnEnabled()) log.warn("received null message");
                        continue;
                    }

                    // 4. Finally the message itself, as a byte buffer
                    buf=new byte[len];
                    input.readFully(buf, 0, buf.length);  // message
                    route(dst_addr, gname, buf, addr);
                }
                catch(EOFException io_ex) {
                    if(log.isTraceEnabled())
                        log.trace("client " + sock.getInetAddress().getHostName() + ':' + sock.getPort() +
                                " closed connection; removing it from routing table");
                    removeEntry(sock); // will close socket
                    return;
                }
                catch(Exception e) {
                    if(log.isErrorEnabled()) log.error("exception=" + e);
                    break;
                }
            }
            closeSocket();
        }

    }


    public static void main(String[] args) throws Exception {
        String arg;
        int port=8080;
        long expiry=GossipRouter.EXPIRY_TIME;
        long timeout=GossipRouter.GOSSIP_REQUEST_TIMEOUT;
        long routingTimeout=GossipRouter.ROUTING_CLIENT_REPLY_TIMEOUT;
        GossipRouter router=null;
        String address=null;

        for(int i=0; i < args.length; i++) {
            arg=args[i];
            if("-port".equals(arg)) {
                port=Integer.parseInt(args[++i]);
                continue;
            }
            if("-bindaddress".equals(arg)) {
                address=args[++i];
                continue;
            }
            if("-expiry".equals(arg)) {
                expiry=Long.parseLong(args[++i]);
                continue;
            }
            if("-timeout".equals(arg)) {
                timeout=Long.parseLong(args[++i]);
                continue;
            }
            if("-rtimeout".equals(arg)) {
                routingTimeout=Long.parseLong(args[++i]);
                continue;
            }
            help();
            return;
        }
        System.out.println("GossipRouter is starting...");

        try {
            ClassConfigurator.getInstance(true);
            router=new GossipRouter(port, address, expiry, timeout, routingTimeout);
            router.start();
        }
        catch(Exception e) {
            System.err.println(e);
        }
    }

    static void help() {
        System.out.println();
        System.out.println("GossipRouter [-port <port>] [-bindaddress <address>] [options]");
        System.out.println("Options: ");
        System.out.println("        -expiry <msecs>   - Time until a gossip cache entry expires.");
        System.out.println("        -timeout <msecs>  - Number of millisecs the router waits to receive");
        System.out.println("                            a gossip request after connection was established;");
        System.out.println("                            upon expiration, the router initiates the routing");
        System.out.println("                            protocol on the connection.");
    }


}
