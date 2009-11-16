package org.jgroups.stack;

import org.jgroups.Address;
import org.jgroups.PhysicalAddress;
import org.jgroups.protocols.PingData;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.util.*;
import org.jgroups.util.UUID;

import javax.management.MBeanServer;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Router for TCP based group comunication (using layer TCP instead of UDP). Instead of the TCP
 * layer sending packets point-to-point to each other member, it sends the packet to the router
 * which - depending on the target address - multicasts or unicasts it to the group / or single member.
 * <p/>
 * This class is especially interesting for applets which cannot directly make connections (neither
 * UDP nor TCP) to a host different from the one they were loaded from. Therefore, an applet would
 * create a normal channel plus protocol stack, but the bottom layer would have to be the TCP layer
 * which sends all packets point-to-point (over a TCP connection) to the router, which in turn
 * forwards them to their end location(s) (also over TCP). A centralized router would therefore have
 * to be running on the host the applet was loaded from.
 * <p/>
 * An alternative for running JGroups in an applet (IP multicast is not allows in applets as of
 * 1.2), is to use point-to-point UDP communication via the gossip server. However, then the appplet
 * has to be signed which involves additional administrative effort on the part of the user.
 * <p/>
 * Note that a GossipRouter is also a good way of running JGroups in Amazon's EC2 environment which (as of summer 09)
 * doesn't support IP multicasting.
 * @author Bela Ban
 * @author Vladimir Blagojevic
 * @author Ovidiu Feodorov <ovidiuf@users.sourceforge.net>
 * @version $Id: GossipRouter.java,v 1.68 2009/11/16 08:59:31 belaban Exp $
 * @since 2.1.1
 */
public class GossipRouter {
    public static final byte CONNECT=1;    // CONNECT(group, addr) --> local address
    public static final byte DISCONNECT=2; // DISCONNECT(group, addr)
    public static final byte GOSSIP_GET=4; // GET(group) --> List<addr> (members)
    public static final byte SHUTDOWN=9;
    public static final byte MESSAGE=10;
    public static final byte SUSPECT=11;
    public static final byte PING=12;
    public static final byte CLOSE=13;

    public static final int PORT=12001;

    @ManagedAttribute(description="server port on which the GossipRouter accepts client connections", writable=true)
    private int port;

    @ManagedAttribute(description="address to which the GossipRouter should bind", writable=true, name="bindAddress")
    private String bindAddressString;

    // Maintains associations between groups and their members
    private final ConcurrentMap<String, ConcurrentMap<Address, ConnectionHandler>> routingTable=new ConcurrentHashMap<String, ConcurrentMap<Address, ConnectionHandler>>();

    /**
     * Store physical address(es) associated with a logical address. Used mainly by TCPGOSSIP
     */
    private final Map<Address, Set<PhysicalAddress>> address_mappings=new ConcurrentHashMap<Address,Set<PhysicalAddress>>();

    private ServerSocket srvSock=null;
    private InetAddress bindAddress=null;

    @Property(description="Time (in ms) for setting SO_LINGER on sockets returned from accept(). 0 means do not set SO_LINGER")
    private long linger_timeout=2000L;

    @Property(description="Time (in ms) for SO_TIMEOUT on sockets returned from accept(). 0 means don't set SO_TIMEOUT")
    private long sock_read_timeout=0L;

    @Property(description="The max queue size of backlogged connections")
    private int backlog=1000;

    private final AtomicBoolean running = new AtomicBoolean(false);

    @ManagedAttribute(description="whether to discard message sent to self", writable=true)
    private boolean discard_loopbacks=false;

    protected List<ConnectionTearListener> connectionTearListeners=new CopyOnWriteArrayList<ConnectionTearListener>();

    protected ThreadFactory default_thread_factory=new DefaultThreadFactory(Util.getGlobalThreadGroup(), "gossip-handlers", true, true);

    protected final Log log=LogFactory.getLog(this.getClass());

    private boolean jmx=false;

    private boolean registered=false;

    public GossipRouter() {
        this(PORT);
    }

    public GossipRouter(int port) {
        this(port, null);
    }

    public GossipRouter(int port, String bindAddressString) {
        this.port=port;
        this.bindAddressString=bindAddressString;
        connectionTearListeners.add(new FailureDetectionListener());
    }

    public GossipRouter(int port, String bindAddressString, boolean jmx) {
        this(port, bindAddressString);
        this.jmx=jmx;
    }

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

    public int getBacklog() {
        return backlog;
    }

    public void setBacklog(int backlog) {
        this.backlog=backlog;
    }

    @Deprecated
    public void setExpiryTime(long expiryTime) {

    }

    @Deprecated
    public static long getExpiryTime() {
        return 0;
    }

    @Deprecated
    public void setGossipRequestTimeout(long gossipRequestTimeout) {
    }

    @Deprecated
    public static long getGossipRequestTimeout() {
        return 0;
    }

    @Deprecated
    public void setRoutingClientReplyTimeout(long routingClientReplyTimeout) {
    }

    @Deprecated
    public static long getRoutingClientReplyTimeout() {
        return 0;
    }

    @ManagedAttribute(description="status")
    public boolean isStarted() {
        return isRunning();
    }

    public boolean isDiscardLoopbacks() {
        return discard_loopbacks;
    }

    public void setDiscardLoopbacks(boolean discard_loopbacks) {
        this.discard_loopbacks=discard_loopbacks;
    }

    public long getLingerTimeout() {
        return linger_timeout;
    }

    public void setLingerTimeout(long linger_timeout) {
        this.linger_timeout=linger_timeout;
    }

    public long getSocketReadTimeout() {
        return sock_read_timeout;
    }

    public void setSocketReadTimeout(long sock_read_timeout) {
        this.sock_read_timeout=sock_read_timeout;
    }

    public ThreadFactory getDefaultThreadPoolThreadFactory() {
        return default_thread_factory;
    }

    public static String type2String(int type) {
        switch(type) {
            case CONNECT:     return "CONNECT";
            case DISCONNECT:  return "DISCONNECT";
            case GOSSIP_GET:  return "GOSSIP_GET";
            case SHUTDOWN:    return "SHUTDOWN";
            case MESSAGE:     return "MESSAGE";
            case SUSPECT:     return "SUSPECT";
            case PING:        return "PING";
            case CLOSE:       return "CLOSE";
            default:
                return "unknown (" + type + ")";
        }
    }


    /**
     * Lifecycle operation. Called after create(). When this method is called, the managed attributes
     * have already been set.<br>
     * Brings the Router into a fully functional state.
     */
    @ManagedOperation(description="Lifecycle operation. Called after create(). When this method is called, "
            + "the managed attributes have already been set. Brings the Router into a fully functional state.")
    public void start() throws Exception {
        if(running.compareAndSet(false, true)) {           
            if(jmx && !registered) {
                MBeanServer server=Util.getMBeanServer();
                JmxConfigurator.register(this, server, "jgroups:name=GossipRouter");
                registered=true;
            }
    
            if(bindAddressString != null) {
                bindAddress=InetAddress.getByName(bindAddressString);
                srvSock=new ServerSocket(port, backlog, bindAddress);
            }
            else {
                srvSock=new ServerSocket(port, backlog);
            }       
    
            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    GossipRouter.this.stop();
                }
            });
    
            // start the main server thread
            new Thread(new Runnable() {
                public void run() {
                    mainLoop();
                }
            }, "GossipRouter").start();
        } else {
            throw new Exception("Router already started.");
        }
    }

    /**
     * Always called before destroy(). Close connections and frees resources.
     */
    @ManagedOperation(description="Always called before destroy(). Closes connections and frees resources")
    public void stop() {
        if(running.compareAndSet(true, false)){
            Util.close(srvSock);                
            for(ConcurrentMap<Address,ConnectionHandler> map: routingTable.values()) {
                for(ConnectionHandler ce: map.values())
                    ce.close();
            }
            routingTable.clear();
    
            if(log.isInfoEnabled())
                log.info("router stopped");            
        }
    }    

    public void destroy() {
    }
    
    @ManagedAttribute(description="operational status", name="running")
    public boolean isRunning() {
        return running.get();
    }

    @ManagedOperation(description="dumps the contents of the routing table")
    public String dumpRoutingTable() {
        String label="routing";
        StringBuilder sb=new StringBuilder();

        if(routingTable.isEmpty()) {
            sb.append("empty ").append(label).append(" table");
        }
        else {
            boolean first=true;
            for(Map.Entry<String, ConcurrentMap<Address, ConnectionHandler>> entry : routingTable.entrySet()) {
                String gname=entry.getKey();
                if(!first)
                    sb.append("\n");
                else
                    first=false;
                sb.append(gname + ": ");
                Map<Address,ConnectionHandler> map=entry.getValue();
                if(map == null || map.isEmpty()) {
                    sb.append("null");
                }
                else {
                    sb.append(Util.printListWithDelimiter(map.keySet(), ", "));
                }
            }
        }
        return sb.toString();
    }

    @ManagedOperation(description="dumps the contents of the routing table")
    public String dumpRoutingTableDetailed() {
        String label="routing";
        StringBuilder sb=new StringBuilder();

        if(routingTable.isEmpty()) {
            sb.append("empty ").append(label).append(" table");
        }
        else {
            boolean first=true;
            for(Map.Entry<String, ConcurrentMap<Address, ConnectionHandler>> entry : routingTable.entrySet()) {
                String gname=entry.getKey();
                if(!first)
                    sb.append("\n");
                else
                    first=false;
                sb.append(gname + ":\n");
                Map<Address,ConnectionHandler> map=entry.getValue();
                if(map == null || map.isEmpty()) {
                    sb.append("null");
                }
                else {
                    for(Map.Entry<Address,ConnectionHandler> en: map.entrySet()) {
                        sb.append(en.getKey() + ": ");
                        ConnectionHandler handler=en.getValue();
                        sb.append("sock=" +handler.sock).append("\n");
                    }
                }
                sb.append("\n");
            }
        }
        return sb.toString();
    }

    @ManagedOperation(description="dumps the mappings between logical and physical addresses")
    public String dumpAddresssMappings() {
        StringBuilder sb=new StringBuilder();
        for(Map.Entry<Address,Set<PhysicalAddress>> entry: address_mappings.entrySet()) {
            sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
        }
        return sb.toString();
    }

    private void mainLoop() {
        if(bindAddress == null)
            bindAddress=srvSock.getInetAddress();

        printStartupInfo();

        while(isRunning()) {
            Socket sock=null;
            try {
                sock=srvSock.accept();
                if(linger_timeout > 0) {
                    int linger=Math.max(1, (int)(linger_timeout / 1000));
                    sock.setSoLinger(true, linger);
                }
                if(sock_read_timeout > 0)
                    sock.setSoTimeout((int)sock_read_timeout);

                ConnectionHandler ch=new ConnectionHandler(sock);
                getDefaultThreadPoolThreadFactory().newThread(ch).start();
            }
            catch(IOException e) {
                //only consider this exception if GR is not shutdown
                if(isRunning()) {
                    log.error("failure handling connection from " + sock, e);
                    Util.close(sock);
                }
            }
        }
    }
    
    private void route(Address dest, String group, byte[] msg) {
        if(dest == null) { // send to all members in group
            if(group == null) {
                if(log.isErrorEnabled())
                    log.error("group is null");
            }
            else {
                sendToAllMembersInGroup(group, msg);
            }
        }
        else { // send unicast

            ConnectionHandler handler=findAddressEntry(group, dest);
            if(handler == null) {
                if(log.isTraceEnabled())
                    log.trace("cannot find " + dest + " in the routing table, \nrouting table=\n" + dumpRoutingTable());
                return;
            }
            if(handler.output == null) {
                if(log.isErrorEnabled())
                    log.error(dest + " is associated with a null output stream");
                return;
            }
            try {
                sendToMember(dest, handler.output, msg);
            }
            catch(Exception e) {
                if(log.isErrorEnabled())
                    log.error("failed sending message to " + dest + ": " + e.getMessage());
                removeEntry(group, dest); // will close socket
            }
        }
    }



    private void removeEntry(String group, Address addr) {
        // Remove from routing table
        ConcurrentMap<Address, ConnectionHandler> map;
        if(group != null) {
            map=routingTable.get(group);
            if(map != null && map.remove(addr) != null && map.isEmpty())
                routingTable.remove(group);
        }
        else {
            for(Map.Entry<String,ConcurrentMap<Address,ConnectionHandler>> entry: routingTable.entrySet()) {
                map=entry.getValue();
                if(map != null && map.remove(addr) != null && map.isEmpty())
                    routingTable.remove(entry.getKey());
            }
        }

        address_mappings.remove(addr);

        if(addr instanceof UUID)
            UUID.remove((UUID)addr);
    }

    /**
     * @return null if not found
     */
    private ConnectionHandler findAddressEntry(String group, Address addr) {
        if(group == null || addr == null)
            return null;
        ConcurrentMap<Address,ConnectionHandler> map=routingTable.get(group);
        if(map == null)
            return null;
        return map.get(addr);
    }

    private void sendToAllMembersInGroup(String group, byte[] msg) {
        final ConcurrentMap<Address,ConnectionHandler> map=routingTable.get(group);
        if(map == null || map.isEmpty()) {
            if(log.isWarnEnabled())
            log.warn("didn't find any members for group " + group);
            return;
        }

        synchronized(map) {
            for(Map.Entry<Address,ConnectionHandler> entry: map.entrySet()) {
                ConnectionHandler handler=entry.getValue();
                DataOutputStream dos=handler.output;

                if(dos != null) {
                    try {
                        sendToMember(null, dos, msg);
                    }
                    catch(Exception e) {
                        if(log.isWarnEnabled())
                            log.warn("cannot send to " + entry.getKey() + ": " + e.getMessage());
                    }
                }
            }
        }
    }

    private static void sendToMember(Address dest, final DataOutputStream out, byte[] msg) throws IOException {
        if(out == null)
            return;
        synchronized(out) {
            GossipData request=new GossipData(GossipRouter.MESSAGE, null, dest, msg);
            request.writeTo(out);
            out.flush();
        }
    }

    private void notifyAbnormalConnectionTear(final ConnectionHandler ch, final Exception e) {
        for (ConnectionTearListener l : connectionTearListeners) {
            l.connectionTorn(ch, e);
        }
    }

    public interface ConnectionTearListener {
        public void connectionTorn(ConnectionHandler ch, Exception e);
    }

    /*
    * https://jira.jboss.org/jira/browse/JGRP-902
    */
    class FailureDetectionListener implements ConnectionTearListener {

        public void connectionTorn(ConnectionHandler ch, Exception e) {
            Set<String> groups = ch.known_groups;
            for (String group : groups) {
                if(group == null)
                    continue;
                Map<Address, ConnectionHandler> map = routingTable.get(group);
                if (map != null && !map.isEmpty()) {
                    for (Iterator<Entry<Address, ConnectionHandler>> i = map.entrySet().iterator(); i.hasNext();) {
                        ConnectionHandler entry = i.next().getValue();
                        DataOutputStream stream = entry.output;
                        try {
                            for (Address a : ch.logical_addrs) {
                                GossipData suspect = new GossipData(GossipRouter.SUSPECT);
                                suspect.writeTo(stream);
                                Util.writeAddress(a, stream);
                                stream.flush();
                            }
                        } catch (Exception ioe) {
                            // intentionally ignored
                        }
                    }
                }
            }
        }
    }

    /**
     * Prints startup information.
     */
    private void printStartupInfo() {
        System.out.println("GossipRouter started at " + new Date());

        System.out.print("Listening on port " + port);
        System.out.println(" bound on address " + bindAddress);

        System.out.print("Backlog is " + backlog);
        System.out.print(", linger timeout is " + linger_timeout);
        System.out.println(", and read timeout is " + sock_read_timeout);
    }


    /**
     * Handles the requests from a client (RouterStub)
     */
    class ConnectionHandler implements Runnable {
        private final AtomicBoolean active = new AtomicBoolean(false);
        private final Socket sock;
        private final DataOutputStream output;
        private final DataInputStream input;
        private final List<Address> logical_addrs=new ArrayList<Address>();
        Set<String> known_groups = new HashSet<String>();

        public ConnectionHandler(Socket sock) throws IOException {
            this.sock=sock;
            this.input=new DataInputStream(sock.getInputStream());
            this.output=new DataOutputStream(sock.getOutputStream());
        }

        void close() {
            if(active.compareAndSet(true, false)) {
                Util.close(input);
                Util.close(output);
                Util.close(sock);
                for(Address addr: logical_addrs) {
                    removeEntry(null, addr);
                }
            }
        }

        public void run() {
            if(active.compareAndSet(false, true)) {
                try {
                    readLoop();
                }
                finally {
                    close();
                }
            }
        }
        
        public boolean isRunning() {
            return active.get();
        }

        private void readLoop() {
            while(isRunning()) {
                GossipData request;
                Address addr;
                String group;
                try {                   
                    request=new GossipData();
                    request.readFrom(input);
                    byte command=request.getType();
                    addr=request.getAddress();
                    group=request.getGroup();
                    known_groups.add(group);
                    ConcurrentMap<Address,ConnectionHandler> map;

                    switch(command) {

                        case GossipRouter.CONNECT:
                            String logical_name=request.getLogicalName();
                            if(logical_name != null && addr instanceof org.jgroups.util.UUID)
                                org.jgroups.util.UUID.add((org.jgroups.util.UUID)addr, logical_name);
                            
                            // group name, logical address, logical name, physical addresses (could be null)
                            logical_addrs.add(addr); // allows us to remove the entries for this connection on socket close

                            map=routingTable.get(group);
                            if(map == null) {
                                map=new ConcurrentHashMap<Address,ConnectionHandler>();
                                routingTable.put(group, map); // no concurrent requests on the same connection
                            }
                            map.put(addr, this);

                            Set<PhysicalAddress> physical_addrs;
                            if(request.getPhysicalAddresses() != null) {
                                physical_addrs=address_mappings.get(addr);
                                if(physical_addrs == null) {
                                    physical_addrs=new HashSet<PhysicalAddress>();
                                    address_mappings.put(addr, physical_addrs);
                                }
                                physical_addrs.addAll(request.getPhysicalAddresses());
                            }
                            break;

                        case GossipRouter.PING:
                            // do nothing here - client doesn't expect response data
                            break;

                        case GossipRouter.MESSAGE:
                            if(request.buffer == null || request.buffer.length == 0) {
                                if(log.isWarnEnabled())
                                    log.warn("received null message");
                                break;
                            }

                            try {
                                route(addr, request.getGroup(), request.getBuffer());
                            }
                            catch(Exception e) {
                                if(log.isErrorEnabled())
                                    log.error("failed routing request to " + addr, e);
                            }
                            break;

                        case GossipRouter.GOSSIP_GET:
                            List<PingData> mbrs=new ArrayList<PingData>();
                            map=routingTable.get(group);
                            if(map != null) {
                                for(Address logical_addr: map.keySet()) {
                                    physical_addrs=address_mappings.get(logical_addr);
                                    PingData rsp=new PingData(logical_addr, null, true, UUID.get(logical_addr),
                                                              physical_addrs != null? new ArrayList<PhysicalAddress>(physical_addrs) : null);
                                    mbrs.add(rsp);
                                }
                            }
                            output.writeShort(mbrs.size());
                            for(PingData data: mbrs)
                                data.writeTo(output);
                            output.flush();
                            break;

                        case GossipRouter.DISCONNECT:
                            removeEntry(request.getGroup(), request.getAddress());
                            break;
                            
                        case GossipRouter.CLOSE:
                            close();
                            break;
                            
                        case -1: // EOF
                            notifyAbnormalConnectionTear(this, new EOFException("Connection broken"));
                            break;
                    }
                }
                catch(SocketTimeoutException ste) {
                }                
                catch(IOException ioex) {
                    notifyAbnormalConnectionTear(this, ioex);
                    break;
                }                
                catch(Exception ex) {
                    if (active.get()) {
                        if (log.isWarnEnabled())
                            log.warn("Exception in ConnectionHandler thread", ex);
                    }
                    break;
                }
            }
        }

        public String toString() {
            StringBuilder sb=new StringBuilder();
            sb.append("peer: " + sock.getInetAddress());
            if(!logical_addrs.isEmpty())
                sb.append(", logical_addrs: " + Util.printListWithDelimiter(logical_addrs, ", "));
            return sb.toString();
        }
    }

    public static void main(String[] args) throws Exception {
        int port=12001;
        int backlog=0;
        long soLinger=-1;
        long soTimeout=-1;

        GossipRouter router=null;
        String bind_addr=null;
        boolean jmx=false;

        for(int i=0; i < args.length; i++) {
            String arg=args[i];
            if("-port".equals(arg)) {
                port=Integer.parseInt(args[++i]);
                continue;
            }
            if("-bindaddress".equals(arg) || "-bind_addr".equals(arg)) {
                bind_addr=args[++i];
                continue;
            }
            if("-backlog".equals(arg)) {
                backlog=Integer.parseInt(args[++i]);
                continue;
            }
            if("-expiry".equals(arg)) {
                System.err.println("-expiry has been deprecated and will be ignored");
                continue;
            }
            if("-jmx".equals(arg)) {
                jmx=true;
                continue;
            }
            // this option is not used and should be deprecated/removed in a future release
            if("-timeout".equals(arg)) {
                System.out.println("    -timeout is deprecated and will be ignored");
                ++i;
                continue;
            }
            // this option is not used and should be deprecated/removed in a future release
            if("-rtimeout".equals(arg)) {
                System.out.println("    -rtimeout is deprecated and will be ignored");
                ++i;
                continue;
            }
            if("-solinger".equals(arg)) {
                soLinger=Long.parseLong(args[++i]);
                continue;
            }
            if("-sotimeout".equals(arg)) {
                soTimeout=Long.parseLong(args[++i]);
                continue;
            }
            help();
            return;
        }
        System.out.println("GossipRouter is starting. CTRL-C to exit JVM");

        try {
            router=new GossipRouter(port, bind_addr, jmx);

            if(backlog > 0)
                router.setBacklog(backlog);

            if(soTimeout >= 0)
                router.setSocketReadTimeout(soTimeout);

            if(soLinger >= 0)
                router.setLingerTimeout(soLinger);

            router.start();
        }
        catch(Exception e) {
            System.err.println(e);
        }
    }

    static void help() {
        System.out.println();
        System.out.println("GossipRouter [-port <port>] [-bind_addr <address>] [options]");
        System.out.println();
        System.out.println("Options:");
        System.out.println();

        System.out.println("    -backlog <backlog>    - Max queue size of backlogged connections. Must be");
        System.out.println("                            greater than zero or the default of 1000 will be");
        System.out.println("                            used.");
        System.out.println();
        System.out.println("    -jmx                  - Expose attributes and operations via JMX.");
        System.out.println();
        System.out.println("    -solinger <msecs>     - Time for setting SO_LINGER on connections. 0");
        System.out.println("                            means do not set SO_LINGER. Must be greater than");
        System.out.println("                            or equal to zero or the default of 2000 will be");
        System.out.println("                            used.");
        System.out.println();
        System.out.println("    -sotimeout <msecs>    - Time for setting SO_TIMEOUT on connections. 0");
        System.out.println("                            means don't set SO_TIMEOUT. Must be greater than");
        System.out.println("                            or equal to zero or the default of 3000 will be");
        System.out.println("                            used.");
        System.out.println();
    }
}