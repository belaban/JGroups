// $Id: GossipRouter.java,v 1.9 2004/12/13 15:30:06 belaban Exp $

package org.jgroups.stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.Address;
import org.jgroups.util.Promise;
import org.jgroups.util.Util;

import java.io.*;
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

    public static final int GET = -10;
    public static final int REGISTER = -11;
    public static final int DUMP = -21;   
    public static final int SHUTDOWN = -1;
    public static final int SHUTDOWN_OK = -2;

    public static final int PORT = 8980;
    public static final long EXPIRY_TIME = 30000;
    public static final long GOSSIP_REQUEST_TIMEOUT = 1000;
    public static final long ROUTING_CLIENT_REPLY_TIMEOUT = 120000;

    // BufferedInputStream mark buffer size
    private final int MARK_BUFFER_SIZE = 2048;

    private static final Object GOSSIP_REQUEST = new Object();
    private static final Object GOSSIP_FAILURE = new Object();

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

    // HashMap<String,List<Address>. Maintains associations between groups and their members
    private final Hashtable routingTable=new Hashtable();

    // (groupname - vector of AddressEntry's)
    private final Map gossipTable = new HashMap();

    private ServerSocket srvSock = null;
    private InetAddress bindAddress = null;

    // the cache sweeper
    Timer timer = null;

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
        this.expiryTime = expiryTime;
        this.gossipRequestTimeout = gossipRequestTimeout;
        this.routingClientReplyTimeout = routingClientReplyTimeout;
    }


    //
    // MANAGED ATTRIBUTES
    //

    public void setPort(int port) {
        this.port = port;
    }

    public int getPort() {
         return port;
    }

    public void setBindAddress(String bindAddress) {
        bindAddressString = bindAddress;
    }

    public String getBindAddress() {
         return bindAddressString;
    }

    public void setExpiryTime(long expiryTime) {
        this.expiryTime = expiryTime;
    }

    public long getExpiryTime() {
         return expiryTime;
    }

    public void setGossipRequestTimeout(long gossipRequestTimeout) {
        this.gossipRequestTimeout = gossipRequestTimeout;
    }

    public long getGossipRequestTimeout() {
         return gossipRequestTimeout;
    }

    public void setRoutingClientReplyTimeout(long routingClientReplyTimeout) {
        this.routingClientReplyTimeout = routingClientReplyTimeout;
    }

    public long getRoutingClientReplyTimeout() {
         return routingClientReplyTimeout;
    }

    public boolean isStarted() {
        return srvSock!=null;
    }

    //
    // JBoss MBean LIFECYCLE OPERATIONS
    //


    /**
     * JBoss MBean lifecycle operation.
     **/
    public void create() throws Exception {
        // not used
    }

    /**
     * JBoss MBean lifecycle operation. Called after create(). When this method
     * is called, the managed attributes have already been set.<br>
     * Brings the Router in fully functional state.
     **/
    public void start() throws Exception {

        if (srvSock!=null) {
            throw new Exception("Router already started.");
        }

        if (bindAddressString!=null) {
            bindAddress = InetAddress.getByName(bindAddressString);
            srvSock = new ServerSocket(port, 50, bindAddress);  
        }
        else {
            srvSock = new ServerSocket(port, 50);  
        }

        // start the main server thread
        new Thread(new Runnable() {
                public void run() {
                    mainLoop();
                    cleanup();
                }
            }, "JGroups Router Main Thread").start();

        // starts the cache sweeper as daemon thread, so we won't block on it
        // upon termination
        timer = new Timer(true);   
        timer.schedule(new TimerTask() {
                public void run() {
                    sweep();
                }
            }, expiryTime, expiryTime);
    }

    /**
     * JBoss MBean lifecycle operation. The JMX agent allways calls this method
     * before destroy(). Close connections and frees resources.
     **/
    public void stop() {

        if (srvSock==null) {
            if(log.isWarnEnabled()) log.warn("Router already stopped");
            return;
        }

        timer.cancel();
        shutdown();
        try {
            srvSock.close();
        }
        catch(Exception e) {
            if(log.isErrorEnabled()) log.error("Failed to close server socket: "+e);
        }
        // exiting the mainLoop will clean the tables
        srvSock = null;
        if(log.isInfoEnabled()) log.info("Router stopped");
    }

    /**
     * JBoss MBean lifecycle operation.
     **/
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



    //
    // END OF MANAGEMENT INTERFACE
    //

    public static String requestTypeToString(int type) {
        return
            type == GET ? "GET" :
                (type == REGISTER ? "REGISTER" :
                    (type == DUMP ? "DUMP" :
                        (type == SHUTDOWN ? "SHUTDOWN" : "UNKNOWN REQUEST: "+type)));
    }


    /**
     * The main server loop. Runs on the JGroups Router Main Thread.
     **/
    private void mainLoop() {
        Socket sock = null;
        DataInputStream input = null;
        DataOutputStream output = null;
        Address peer_addr = null;
        byte[] buf;
        int len, type = -1;
        String gname = null;
        Date d;
        boolean up = true;

        if(bindAddress == null) {
            bindAddress=srvSock.getInetAddress();
        }
        d=new Date();
        System.out.println("GossipRouter started at " + d +
                           "\nListening on port " + port + " bound on address " + bindAddress + '\n');
        d=null;

        while(up) {

            try {
                sock=srvSock.accept();
                sock.setSoLinger(true, 500);

                if(log.isTraceEnabled()) {
                    log.trace("router accepted connection from "+sock);
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
                                if (gresp != null) {
                                    ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
                                    oos.writeObject(gresp);
                                    oos.close();
                                }
                                bis.close();
                                s.close();
                            }
                            catch(Exception e) {
                                if(log.isDebugEnabled()) log.debug("gossip thread exception :"+e);
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

                if (waitResult != null) {
                    // gossip request, let the gossip thread deal with it
                    continue;
                }

                // timeout, this is a routing request

                peer_addr = new IpAddress(sock.getInetAddress(), sock.getPort());
                output = new DataOutputStream(sock.getOutputStream());
                
                // return the address of the peer so it can set it
                buf = Util.objectToByteBuffer(peer_addr);
                output.writeInt(buf.length);
                output.write(buf, 0, buf.length);

                // The gossip thread still waits for a serialized object, so
                // wait that read to fail. If it actually gets a GossipData,
                // that's an error condition we should handle here
                waitResult = waitArea.getResult(routingClientReplyTimeout);

                if (waitResult == null) {
                    // timeout
                    throw new Exception("Timeout waiting for router client answer");
                }
                else if (waitResult == GOSSIP_REQUEST) {
                    // lazy gossip client, let it handle its business, it will
                    // fail anyway
                    output.close();
                    continue;
                }

                bis.reset();
                input=new DataInputStream(bis);

                type=input.readInt();
                if(log.isTraceEnabled()) {
                    log.trace("request of type "+requestTypeToString(type));
                }

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
                    processDumpRequest(sock, output); // closes sock after processing
                    break;
                case GossipRouter.REGISTER:
                    Address addr;
                    len=input.readInt();
                    buf=new byte[len];
                    input.readFully(buf, 0, buf.length); // read Address
                    addr=(Address)Util.objectFromByteBuffer(buf);
                    SocketThread st = new SocketThread(sock, input, addr);
                    addEntry(gname, new AddressEntry(addr, sock, st, output));
                    st.start();
                    break;
                case GossipRouter.SHUTDOWN:
                    if(log.isInfoEnabled()) log.info("router shutting down");
                    output.writeInt(SHUTDOWN_OK);
                    output.flush();
                    try {
                        sock.close();
                    }
                    catch(Exception e) {
                        // OK, going down anyway
                    }
                    up = false;
                    continue;
                default:
                    if(log.isErrorEnabled()) log.error("request of type " + type + " not recognized");
                    continue;
                }
            }
            catch(Exception e) {
                if(log.isErrorEnabled()) log.error("failure handling a client connection: " + e.getMessage(), e);
                try {
                    sock.close();
                }
                catch(IOException e2) {
                    if(log.isWarnEnabled()) log.warn("failed to close socket "+sock);
                }
                continue;
            }
        }
    }


    /**
     * Cleans the routing tables while the Router is going down.
     **/
    private void cleanup() {

        // shutdown the routing threads and cleanup the tables
        synchronized(routingTable) {
            for(Iterator i=routingTable.keySet().iterator(); i.hasNext();) {
                String gname=(String)i.next();
                List l=(List)routingTable.get(gname);
                if (l!=null) {
                    for(Iterator j=l.iterator(); j.hasNext(); ) {
                        AddressEntry e = (AddressEntry)j.next();
                        e.destroy();
                    }
                }
            }
            routingTable.clear();
            if(log.isInfoEnabled()) log.info("routing table cleared");
        }
        synchronized(gossipTable) {
            gossipTable.clear();
            if(log.isInfoEnabled()) log.info("gossip table cleared");
        }

    }

    /**
     * Connects to the ServerSocket and sends the shutdown header.
     **/
    private void shutdown() {       
        try {
            Socket s = new Socket(srvSock.getInetAddress(), 
                                  srvSock.getLocalPort());
            DataInputStream dis = new DataInputStream(s.getInputStream());
            int len = dis.readInt();
            byte[] buf = new byte[len];
            dis.readFully(buf, 0, buf.length);
            DataOutputStream dos = new DataOutputStream(s.getOutputStream());
            dos.writeInt(SHUTDOWN);
            dos.writeUTF("");
            // waits until the server replies
            dis.readInt();
            dos.flush();
            dos.close();
            s.close();
        }
        catch(Exception e) {
            if(log.isErrorEnabled()) log.error("shutdown failed: "+e);
        }
        
    }

    //
    // GOSSIPING
    //

    /**
     * @since 2.2.1
     **/
    private GossipData processGossip(GossipData gossip) {

         {
            if(log.isDebugEnabled()) log.debug("gossip is "+gossip);
        }

        if (gossip==null) {
            if(log.isWarnEnabled()) log.warn("null gossip request");
            return null;
        }

        String group = gossip.getGroup();
        Address mbr = null;

        synchronized(gossipTable) {

            switch(gossip.getType()) {
                
            case GossipData.REGISTER_REQ: 
                mbr=gossip.getMbr();
                if(group == null || mbr == null) {
                    if(log.isErrorEnabled()) log.error("group or member is null, cannot register member");
                    return null;
                }
                addGossipEntry(group, new AddressEntry(mbr));
                return null;

            case GossipData.GET_REQ:
                if(group == null) {
                    if(log.isErrorEnabled()) log.error("group is null, cannot get membership");
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
                if(log.isWarnEnabled()) log.warn("received a GET_RSP. Should not be received by server");
                return null;

            default:
                if(log.isWarnEnabled()) log.warn("received unkown gossip request (gossip=" + gossip + ')');
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
            if(log.isErrorEnabled()) log.error("groupname was null, not added !");
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

         {
            if(log.isDebugEnabled()) log.debug("running sweep");
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

                               if(log.isInfoEnabled()) log.info("Removed member " + ae +
                                       " from group " + key + '(' + diff + " msecs old)");
                            num_entries_removed++;
                        }
                    }
                }
            }
        }
        
        if(num_entries_removed > 0) {
            if(log.isInfoEnabled()) log.info("done (removed " + num_entries_removed + " entries)");
        }
    }

    //
    // ROUTING
    //

    /**
     Gets the members of group 'groupname'. Returns them as a List of Addresses.
     */
    private void processGetRequest(Socket sock, DataOutputStream output, String groupname) {

        List grpmbrs=(List)routingTable.get(groupname);
        org.jgroups.util.List ret=null;
        AddressEntry entry;
        byte[] buf;

        if(log.isTraceEnabled()) {
            log.trace("groupname=" + groupname + ", result=" + grpmbrs);
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
            if(log.isErrorEnabled()) log.error("exception=" + e);
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
    private void processDumpRequest(Socket sock, DataOutputStream output) {

        try {
            output.writeUTF(dumpRoutingTable());
        }
        catch(Exception e) {
            if(log.isErrorEnabled()) log.error("error sending the answer back to the client: " + e);
        }
        finally {
            try {
                if(output != null) {
                    output.close();
                }
            }
            catch(Exception e) {
                if(log.isErrorEnabled()) log.error("error closing the output stream: " + e);
            }
            try {
                sock.close();
            }
            catch(Exception e) {
                if(log.isErrorEnabled()) log.error("error closing the socket: " + e);
            }
        }
    }


    private String dumpTable(Map map) {

        String label = (map instanceof Hashtable)?"routing":"gossip";
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
                    else
                        if(l.size() == 0) {
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
        if(log.isTraceEnabled()) {
            int len=msg != null? msg.length : 0;
            log.trace("routing request from " + sender + " for "+dest_group+" to " +
                      (dest==null?"ALL":dest.toString())+", " + len + " bytes");
        }

        if(dest == null) {
            // send to all members in group dest.getChannelName()
            if(dest_group == null) {
                if(log.isErrorEnabled()) log.error("both dest address and group are null");
                return;
            }
            else {
                sendToAllMembersInGroup(dest_group, msg, sender);
            }
        }
        else {                  
            // send to destination address
            AddressEntry ae = findAddressEntry(dest);
            if (ae == null) {
                if(log.isErrorEnabled()) log.error("cannot find address "+dest+" in the routing table");
                return;
            }
            if (ae.output==null) {
                if(log.isErrorEnabled()) log.error("address "+dest+" is associated with a null output stream");
                return;
            }
            try {
                sendToMember(ae.output, msg);
            }
            catch(Exception e) {
                if(log.isErrorEnabled()) log.error("failed sending message to "+dest+": "+e.getMessage());
                removeEntry(ae.sock); // will close socket
            }
        }
    }


    /**
     * Adds a new member to the routing group.
     **/
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
        synchronized(routingTable) {
            for(Enumeration e=routingTable.keys(); e.hasMoreElements();) {
                val=(List)routingTable.get(e.nextElement());
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

    /**
     * @return null if not found
     **/
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
                AddressEntry ae = (AddressEntry)i.next();
                if(ae.addr != null && ae.addr.equals(sender)) {
                    // if(log.isTraceEnabled())
                       // log.trace("dropped message to sender of multicast (" + ae.addr + ")");
                    continue;
                }
                DataOutputStream dos = ae.output;

                if (dos!=null) {
                    // send only to 'connected' members
                    try {
                        sendToMember(dos, msg);
                    }
                    catch(Exception e) {
                        if(log.isWarnEnabled()) log.warn("cannot send to "+ae.addr+": "+e.getMessage());
                        ae.destroy(); // this closes the socket
                        i.remove();
                    }
                }
            }
        }
    }


    /**
     * @exception IOException 
     **/
    private void sendToMember(DataOutputStream out, byte[] msg) throws IOException {
        if (out==null) {
            return;
        }

        synchronized(out) {
            out.writeInt(msg.length);
            out.write(msg, 0, msg.length);
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
        final SocketThread thread;

        /**
         * AddressEntry for a 'gossip' membership.
         **/
        public AddressEntry(Address addr) {
            this(addr, null, null, null);
        }

        public AddressEntry(Address addr, Socket sock, SocketThread thread, DataOutputStream output) {
            this.addr=addr;
            this.sock=sock;
            this.thread = thread;
            this.output=output;
            this.timestamp = System.currentTimeMillis();
        }

        void destroy() {
            if (thread != null) {
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


    private static int threadCounter = 0;


    /** A SocketThread manages one connection to a client. Its main task is message routing. */
    class SocketThread extends Thread {
        private volatile boolean active = true;
        Socket sock=null;
        DataInputStream input=null;
        Address addr=null;

        public SocketThread(Socket sock, DataInputStream ois, Address addr) {
            super("SocketThread "+(threadCounter++));
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
            if(log.isDebugEnabled()) log.debug("terminating the SocketThread for "+sock);
            active = false;
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

                    // 3. Then the length of the byte buffer representing the message
                    len=input.readInt();
                    if(len == 0) {
                        if(log.isWarnEnabled()) log.warn("received null message");
                        continue;
                    }

                    // 4. Finally the message itself, as a byte buffer
                    buf=new byte[len];
                    input.readFully(buf, 0, buf.length);  // message

                    // Then route the message to everyone else except me
                    route(dst_addr, gname, buf, addr);
                }
                catch(EOFException io_ex) {
                    if(log.isInfoEnabled())
                        log.info("client " +sock.getInetAddress().getHostName() + ':' + sock.getPort() +
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
        long expiry = GossipRouter.EXPIRY_TIME;
        long timeout = GossipRouter.GOSSIP_REQUEST_TIMEOUT;
        long routingTimeout = GossipRouter.ROUTING_CLIENT_REPLY_TIMEOUT;
        GossipRouter router=null;
        String address=null;

        for(int i=0; i < args.length; i++) {
            arg=args[i];
            if("-help".equals(arg)) {
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
            else if("-port".equals(arg)) {
                    port=Integer.parseInt(args[++i]);
            }
            else if("-bindaddress".equals(arg)) {
                address=args[++i];
            }
            else if("-expiry".equals(arg)) {
                expiry=Long.parseLong(args[++i]);
            }
            else if("-timeout".equals(arg)) {
                timeout=Long.parseLong(args[++i]);
            }
            else if("-rtimeout".equals(arg)) {
                routingTimeout=Long.parseLong(args[++i]);
            }
        }
        System.out.println("GossipRouter is starting...");

        try {
            router= new GossipRouter(port, address, expiry, timeout, routingTimeout);
            router.start();
        }
        catch(Exception e) {
            System.err.println(e);
        }
    }


}
