package org.jgroups.stack;

import org.jgroups.Address;
import org.jgroups.PhysicalAddress;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.util.DefaultThreadFactory;
import org.jgroups.util.ThreadFactory;
import org.jgroups.util.Util;

import javax.management.MBeanServer;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.*;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Router for TCP based group comunication (using layer TCP instead of UDP). Instead of the TCP
 * layer sending packets point-to-point to each other member, it sends the packet to the router
 * which - depending on the target address - multicasts or unicasts it to the group / or single member.
 * <p>
 * This class is especially interesting for applets which cannot directly make connections (neither
 * UDP nor TCP) to a host different from the one they were loaded from. Therefore, an applet would
 * create a normal channel plus protocol stack, but the bottom layer would have to be the TCP layer
 * which sends all packets point-to-point (over a TCP connection) to the router, which in turn
 * forwards them to their end location(s) (also over TCP). A centralized router would therefore have
 * to be running on the host the applet was loaded from.
 * <p>
 * An alternative for running JGroups in an applet (IP multicast is not allows in applets as of
 * 1.2), is to use point-to-point UDP communication via the gossip server. However, then the appplet
 * has to be signed which involves additional administrative effort on the part of the user.
 * <p>
 * Note that a GossipRouter is also a good way of running JGroups in Amazon's EC2 environment which (as of summer 09)
 * doesn't support IP multicasting.
 * 
 * @author Bela Ban
 * @author Vladimir Blagojevic
 * @author Ovidiu Feodorov <ovidiuf@users.sourceforge.net>
 * @version $Id: GossipRouter.java,v 1.55 2009/07/03 14:36:46 belaban Exp $
 * @since 2.1.1
 */
public class GossipRouter {
   public static final byte CONNECT = 1; // CONNECT(group, addr) --> local address
   public static final byte DISCONNECT = 2; // DISCONNECT(group, addr)
   public static final byte GOSSIP_GET = 4; // GET(group) --> List<addr> (members)
   public static final byte SHUTDOWN = 9;
   public static final byte MESSAGE = 10;
   public static final byte SUSPECT=11;

   public static final int  PORT = 12001;
   public static final long GOSSIP_REQUEST_TIMEOUT = 1000;
   public static final long ROUTING_CLIENT_REPLY_TIMEOUT = 120000;

   @ManagedAttribute(description = "server port on which the GossipRouter accepts client connections", writable = true)
   private int port;

   @ManagedAttribute(description = "address to which the GossipRouter should bind", writable = true, name = "bindAddress")
   private String bindAddressString;

   @ManagedAttribute(description = "number of millisecs the main thread waits to receive a gossip request "
            + "after connection was established; upon expiration, the router initiates "
            + "the routing protocol on the connection. Don't set the interval too big, "
            + "otherwise the router will appear slow in answering routing requests.", writable = true)
   private long gossipRequestTimeout;

   @ManagedAttribute(description = "time (in ms) main thread waits for a router client to send the routing "
            + "request type and the group afiliation before it declares the request " + "failed.", writable = true)
   private long routingClientReplyTimeout;

   // Maintains associations between groups and their members
   private final ConcurrentMap<String, ConcurrentMap<Address, RoutingEntry>> routingTable = new ConcurrentHashMap<String, ConcurrentMap<Address, RoutingEntry>>();

   /** Store physical address(es) associated with a logical address. Used mainly by TCPGOSSIP */
   private final Map<Address,List<PhysicalAddress>> address_mappings=new ConcurrentHashMap<Address,List<PhysicalAddress>>();

   private ServerSocket srvSock = null;
   private InetAddress  bindAddress = null;

   @Property(description = "Time (in ms) for setting SO_LINGER on sockets returned from accept(). 0 means do not set SO_LINGER")
   private long linger_timeout = 2000L;

   @Property(description = "Time (in ms) for SO_TIMEOUT on sockets returned from accept(). 0 means don't set SO_TIMEOUT")
   private long sock_read_timeout = 5000L;

   @Property(description = "The max queue size of backlogged connections")
   private int backlog = 1000;

   @ManagedAttribute(description = "operational status", name = "running")
   private boolean up = false;

   @ManagedAttribute(description = "whether to discard message sent to self", writable = true)
   private boolean discard_loopbacks = false;
   
   protected List<ConnectionTearListener> connectionTearListeners = new CopyOnWriteArrayList<ConnectionTearListener>();

   protected ThreadFactory default_thread_factory = new DefaultThreadFactory(Util.getGlobalThreadGroup(), "gossip-handlers", true, true);

   protected final Log log = LogFactory.getLog(this.getClass());

   private boolean jmx = false;
   
   private boolean registered = false;

   public GossipRouter() {
      this(PORT);
   }

   public GossipRouter(int port) {
      this(port, null);
   }

   public GossipRouter(int port, String bindAddressString) {
      this(port, bindAddressString, GOSSIP_REQUEST_TIMEOUT, ROUTING_CLIENT_REPLY_TIMEOUT);
   }

   public GossipRouter(int port, String bindAddressString, long gossipRequestTimeout, long routingClientReplyTimeout) {
      this.port = port;
      this.bindAddressString = bindAddressString;
      this.gossipRequestTimeout = gossipRequestTimeout;
      this.routingClientReplyTimeout = routingClientReplyTimeout;
      connectionTearListeners.add(new FailureDetectionListener());
   }

   public GossipRouter(int port, String bindAddressString,
            long gossipRequestTimeout, long routingClientReplyTimeout, boolean jmx) {
      this(port, bindAddressString, gossipRequestTimeout, routingClientReplyTimeout);
      this.jmx = jmx;
   }

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

   public int getBacklog() {
      return backlog;
   }

   public void setBacklog(int backlog) {
      this.backlog = backlog;
   }

    @Deprecated
   public void setExpiryTime(long expiryTime) {

   }

    @Deprecated
   public static long getExpiryTime() {
        return 0;
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

   @ManagedAttribute(description = "status")
   public boolean isStarted() {
      return srvSock != null;
   }

   public boolean isDiscardLoopbacks() {
      return discard_loopbacks;
   }

   public void setDiscardLoopbacks(boolean discard_loopbacks) {
      this.discard_loopbacks = discard_loopbacks;
   }

   public long getLingerTimeout() {
      return linger_timeout;
   }

   public void setLingerTimeout(long linger_timeout) {
      this.linger_timeout = linger_timeout;
   }

   public long getSocketReadTimeout() {
      return sock_read_timeout;
   }

   public void setSocketReadTimeout(long sock_read_timeout) {
      this.sock_read_timeout = sock_read_timeout;
   }

   public ThreadFactory getDefaultThreadPoolThreadFactory() {
      return default_thread_factory;
   }

   public static String type2String(int type) {
      switch (type) {
         case CONNECT:
            return "CONNECT";
         case DISCONNECT:
            return "DISCONNECT";
         case GOSSIP_GET:
            return "GOSSIP_GET";
         case SHUTDOWN:
            return "SHUTDOWN";
         default:
            return "unknown";
      }
   }


   /**
    * Lifecycle operation. Called after create(). When this method is called, the managed attributes
    * have already been set.<br>
    * Brings the Router into a fully functional state.
    */
   @ManagedOperation(description = "Lifecycle operation. Called after create(). When this method is called, "
            + "the managed attributes have already been set. Brings the Router into a fully functional state.")
   public void start() throws Exception {
      if (srvSock != null)
         throw new Exception("Router already started.");

      if (jmx && !registered) {
         MBeanServer server = Util.getMBeanServer();
         JmxConfigurator.register(this, server, "jgroups:name=GossipRouter");
         registered = true;
      }

      if (bindAddressString != null) {
         bindAddress = InetAddress.getByName(bindAddressString);
         srvSock = new ServerSocket(port, backlog, bindAddress);
      } else {
         srvSock = new ServerSocket(port, backlog);
      }

      up = true;

      Runtime.getRuntime().addShutdownHook(new Thread() {
         public void run() {
            cleanup();
            GossipRouter.this.stop();
         }
      });

       try {
           mainLoop();
       }
       finally {
           cleanup();
       }
   }

   /**
    * Always called before destroy(). Close connections and frees resources.
    */
   @ManagedOperation(description = "Always called before destroy(). Closes connections and frees resources")
   public void stop() {
      up = false;
      if (srvSock != null) {
         shutdown();
         Util.close(srvSock);
         // exiting the mainLoop will clean the tables
         srvSock = null;
      }
      if (log.isInfoEnabled())
         log.info("router stopped");
   }

   public void destroy() {
   }

   @ManagedOperation(description = "dumps the contents of the routing table")
   public String dumpRoutingTable() {
      String label = "routing";
      StringBuilder sb = new StringBuilder();

      if (routingTable.isEmpty()) {
         sb.append("empty ").append(label).append(" table");
      }
      else {
          for(Map.Entry<String,ConcurrentMap<Address,RoutingEntry>> entry: routingTable.entrySet()) {
              String gname = entry.getKey();
              sb.append("GROUP: '" + gname + "'\n");
              Map<Address, RoutingEntry> map = entry.getValue();
              if (map == null || map.isEmpty()) {
                  sb.append("\tnull\n");
              }
              else {
                  for (RoutingEntry re: map.values()) {
                      sb.append('\t').append(re).append('\n');
                  }
              }
          }
      }
       return sb.toString();
   }

   private void mainLoop() {

      if (bindAddress == null) {
         bindAddress = srvSock.getInetAddress();
      }

      printStartupInfo();

      while (up && srvSock != null) {
         try {
            final Socket sock = srvSock.accept();
            if (linger_timeout > 0) {
               int linger = Math.max(1, (int) (linger_timeout / 1000));
               sock.setSoLinger(true, linger);
            }
            if (sock_read_timeout > 0) {
               sock.setSoTimeout((int) sock_read_timeout);
            }

            final DataInputStream input = new DataInputStream(sock.getInputStream());
            DataOutputStream output = null;
            Address peer_addr = null, logical_addr;
            GossipData req = new GossipData();
            try {
               req.readFrom(input);
               switch (req.getType()) {
                  case GossipRouter.CONNECT:
                     peer_addr = new IpAddress(sock.getInetAddress(), sock.getPort());
                     logical_addr = req.getAddress();
                     String group_name = req.getGroup();

                     if (log.isTraceEnabled())
                        log.trace("CONNECT(" + group_name + ", " + logical_addr + ")");
                     ConnectionHandler ch = new ConnectionHandler(sock, group_name, logical_addr);
                     addEntry(group_name, logical_addr, new RoutingEntry(logical_addr, peer_addr, ch));
                     getDefaultThreadPoolThreadFactory().newThread(ch).start();
                     break;

                  case GossipRouter.SHUTDOWN:
                     if (log.isInfoEnabled())
                        log.info("router shutting down");
                     Util.close(input);
                     Util.close(output);
                     Util.close(sock);
                     up = false;
                     break;
                  default:
                     if (log.isWarnEnabled())
                        log.warn("received unkown gossip request (gossip=" + req + ')');
                     break;
               }
            } catch (Exception e) {
               if (up)
                  if (log.isErrorEnabled())
                     log.error("failure handling a client request", e);
               Util.close(input);
               Util.close(output);
               Util.close(sock);
            }
         } catch (SocketException se) {
            if (srvSock != null && srvSock.isClosed()) {
               log.warn("Server socket closing");
            }
         } catch (Exception exc) {
            if (log.isErrorEnabled())
               log.error("failure receiving and setting up a client request", exc);
         }
      }
   }

   /**
    * Cleans the routing tables while the Router is going down.
    */
   private void cleanup() {
      // shutdown the routing threads and cleanup the tables
      for (Map<Address, RoutingEntry> map : routingTable.values()) {
         if (map != null) {
            for (RoutingEntry entry : map.values()) {
               entry.destroy();
            }
         }
      }
      routingTable.clear();
   }

   /**
    * Connects to the ServerSocket and sends the shutdown header.
    */
   private void shutdown() {
      Socket s = null;
      DataOutputStream dos = null;
      try {
         s = new Socket(srvSock.getInetAddress(), srvSock.getLocalPort());
         dos = new DataOutputStream(s.getOutputStream());
         dos.writeInt(SHUTDOWN);
         dos.writeUTF("");
      } catch (Exception e) {
         if (log.isErrorEnabled())
            log.error("shutdown failed: " + e);
      } finally {
         Util.close(s);
         Util.close(dos);
      }
   }



   private void route(Address dest, String dest_group, byte[] msg, Address sender) {
      if (dest == null) { // send to all members in group dest.getChannelName()
         if (dest_group == null) {
            if (log.isErrorEnabled())
               log.error("both dest address and group are null");
         } else {
            sendToAllMembersInGroup(dest_group, msg, sender);
         }
      } else {
         // send to destination address
         RoutingEntry ae = findAddressEntry(dest_group, dest);
         if (ae == null) {
            if (log.isTraceEnabled())
               log.trace("cannot find " + dest + " in the routing table, \nrouting table=\n"
                        + dumpRoutingTable());
            return;
         }
         if (ae.getOutputStream() == null) {
            if (log.isErrorEnabled())
               log.error(dest + " is associated with a null output stream");
            return;
         }
         try {
            sendToMember(dest, ae.getOutputStream(), msg, sender);
         } catch (Exception e) {
            if (log.isErrorEnabled())
               log.error("failed sending message to " + dest + ": " + e.getMessage());
            removeEntry(dest_group, dest); // will close socket
         }
      }
   }

   private void addEntry(String groupname, Address logical_addr, RoutingEntry entry) {
      addEntry(groupname, logical_addr, entry, false);
   }

   /**
    * Adds a new member to the routing group.
    */
   private void addEntry(String groupname, Address logical_addr, RoutingEntry entry,
            boolean update_only) {
      if (groupname == null || logical_addr == null) {
         if (log.isErrorEnabled())
            log.error("groupname or logical_addr was null, entry was not added");
         return;
      }

      ConcurrentMap<Address, RoutingEntry> mbrs = routingTable.get(groupname);
      if (mbrs == null) {
         mbrs = new ConcurrentHashMap<Address, RoutingEntry>();
         mbrs.put(logical_addr, entry);
         routingTable.putIfAbsent(groupname, mbrs);
      } else {
         RoutingEntry tmp = mbrs.get(logical_addr);
         if (tmp != null) { // already present
            if (update_only) {
               tmp.update();
               return;
            }
            tmp.destroy();
         }
         mbrs.put(logical_addr, entry);
      }
   }

   private void removeEntry(String groupname, Address logical_addr) {
      final Map<Address, RoutingEntry> val = routingTable.get(groupname);
      if (val == null)
         return;
      synchronized (val) {
         RoutingEntry entry = val.get(logical_addr);
         if (entry != null) {
            entry.destroy();
            val.remove(logical_addr);
         }
      }
   }

   /**
    * @return null if not found
    */
   private RoutingEntry findAddressEntry(String group_name, Address logical_addr) {
      if (group_name == null || logical_addr == null)
         return null;
      Map<Address, RoutingEntry> val = routingTable.get(group_name);
      if (val == null)
         return null;
      return val.get(logical_addr);
   }

   private void sendToAllMembersInGroup(String groupname, byte[] msg, Address sender) {
      Map<Address, RoutingEntry> val = routingTable.get(groupname);
      if (val == null || val.isEmpty())
         return;

      synchronized (val) {
         for (Iterator<Entry<Address, RoutingEntry>> i = val.entrySet().iterator(); i.hasNext();) {
            Entry<Address, RoutingEntry> tmp = i.next();
            RoutingEntry entry = tmp.getValue();
            DataOutputStream dos = entry.getOutputStream();

            if (dos != null) {
               // send only to 'connected' members
               try {
                  sendToMember(null, dos, msg, sender);
               } catch (Exception e) {
                  if (log.isWarnEnabled())
                     log.warn("cannot send to " + entry.logical_addr + ": " + e.getMessage());
                  entry.destroy(); // this closes the socket
                  i.remove();
               }
            }
         }
      }
   }

   private void sendToMember(Address dest, DataOutputStream out, byte[] msg, Address sender)
            throws IOException {
      if (out == null)
         return;

      if (discard_loopbacks && dest != null && dest.equals(sender)) {
         return;
      }

      synchronized (out) {
         out.writeByte(MESSAGE);
         Util.writeAddress(dest, out);
         out.writeInt(msg.length);
         out.write(msg, 0, msg.length);
         out.flush();
      }
   }
   
   private void notifyAbnormalConnectionTear(final ConnectionHandler ch, final Exception e) {
      Thread thread = getDefaultThreadPoolThreadFactory().newThread(new Runnable() {
         public void run() {
            for (ConnectionTearListener l : connectionTearListeners) {
               l.connectionTorn(ch, e);
            }
         }
      }, "notifyAbnormalConnectionTear");
      thread.start();
   }
   
   public interface ConnectionTearListener{
      public void connectionTorn(ConnectionHandler ch,Exception e);
   }
   
   /*
    * https://jira.jboss.org/jira/browse/JGRP-902
    */
   class FailureDetectionListener implements ConnectionTearListener {

      public void connectionTorn(ConnectionHandler ch, Exception e) {
         final Map<Address, RoutingEntry> map = routingTable.get(ch.group_name);
         if (map != null && !map.isEmpty()) {
            for (final Iterator<Entry<Address, RoutingEntry>> i = map.entrySet().iterator(); i.hasNext();) {
               final RoutingEntry entry = i.next().getValue();
               Address logical_addr = entry.logical_addr;
               Address broken = ch.logical_addr;
               if ((logical_addr != null && broken != null && !logical_addr.equals(broken))) {
                  DataOutputStream stream = entry.getOutputStream();
                  if (stream != null) {
                     try {
                        stream.writeByte(SUSPECT);
                        Util.writeAddress(ch.logical_addr, stream);
                        stream.flush();
                        if(log.isDebugEnabled()){
                           log.debug("Notified entry " + logical_addr + " about suspect " + ch.logical_addr);   
                        }                        
                     } catch (IOException ioe) {}
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
    * 
    */
   class RoutingEntry {
      private final Address logical_addr, physical_addr;
      private final ConnectionHandler handler;
      private long timestamp = 0;

      public RoutingEntry(Address logical_addr, Address physical_addr,ConnectionHandler ch) throws IOException {
         this.logical_addr = logical_addr;
         this.physical_addr = physical_addr;
         this.handler = ch;
         this.timestamp = System.currentTimeMillis();
      }

      void destroy() {
         handler.close();
         timestamp = 0;
      }
      
      DataOutputStream getOutputStream(){
         update();
         return handler.output;
      }

      public void update() {
         timestamp = System.currentTimeMillis();
      }

      public boolean equals(Object other) {
         return other instanceof RoutingEntry
                  && logical_addr.equals(((RoutingEntry) other).logical_addr);
      }

      public String toString() {
         StringBuilder sb = new StringBuilder("logical addr=");
         sb.append(logical_addr).append(" (").append(physical_addr).append(")");
         if (timestamp > 0) {
            long diff = System.currentTimeMillis() - timestamp;
            sb.append(", ").append(diff).append(" ms old");
         }
         return sb.toString();
      }
   }

   /**
    * Handles the requests from a client (RouterStub)
    */
   class ConnectionHandler implements Runnable {
      private volatile boolean active = true;
      private final Socket sock;
      private final DataOutputStream output;
      private final DataInputStream input;
      private final Address logical_addr;
      private final String group_name;

      public ConnectionHandler(Socket sock, String group_name, Address logical_addr) throws IOException {
         this.sock = sock;
         this.input = new DataInputStream(sock.getInputStream());
         this.output = new DataOutputStream(sock.getOutputStream());
         this.group_name = group_name;
         this.logical_addr = logical_addr;
      }

      void close() {
         active = false;
         Util.close(input);
         Util.close(output);
         Util.close(sock);
      }
      
      public void run() {
         try {
            //ack connection establishment
            output.writeBoolean(true);
            readLoop();
         } catch (IOException e1) {
            try {
               output.writeBoolean(false);
            } catch (IOException e) {
            }
         } finally {
            close();
         }
      }
      
      private void readLoop() {
         while (active) {
            try {
               byte command = input.readByte();
               switch (command) {
                  case GossipRouter.MESSAGE:
                     // 1. Group name is first
                     String gname = input.readUTF();

                     // 2. Second is the destination address
                     Address dst_addr = Util.readAddress(input);

                     // 3. Then the length of the byte buffer representing the message
                     int len = input.readInt();
                     if (len == 0) {
                        if (log.isWarnEnabled())
                           log.warn("received null message");
                        continue;
                     }

                     // 4. Finally the message itself, as a byte buffer
                     byte[] buf = new byte[len];
                     input.readFully(buf, 0, buf.length); // message

                     try {
                        route(dst_addr, gname, buf, logical_addr);
                     } catch (Exception e) {
                        if (log.isErrorEnabled())
                           log.error("failed routing request to " + dst_addr, e);
                        break;
                     }
                     break;
                  case GossipRouter.GOSSIP_GET:
                     String group = input.readUTF();
                     List<Address> mbrs = null;
                     Map<Address, RoutingEntry> map = routingTable.get(group);
                     if (map != null) {
                        mbrs = new LinkedList<Address>(map.keySet());
                     }
                     DataOutputStream rsp = new DataOutputStream(sock.getOutputStream());
                     Util.writeAddresses(mbrs, rsp);
                     break;
                  case GossipRouter.DISCONNECT:
                     removeEntry(group_name, logical_addr);
                     close();
                     break;
                  case -1: // EOF
                     notifyAbnormalConnectionTear(this,null);
                     removeEntry(group_name, logical_addr); 
                     break;
               }
            } catch (SocketTimeoutException ste) {
               continue; // do nothing - blocking read timeout caused it             
            } catch (IOException ioex) {               
               notifyAbnormalConnectionTear(this, ioex);
               removeEntry(group_name, logical_addr);
               break;
            } catch (Exception ex) {
               if (log.isWarnEnabled())
                  log.warn("Exception in TUNNEL receiver thread", ex);
               
               removeEntry(group_name, logical_addr);
               break;
            }
         }
      }
   }
   
   public static void main(String[] args) throws Exception {
      String arg;
      int port = 12001;
      long timeout = GossipRouter.GOSSIP_REQUEST_TIMEOUT;
      long routingTimeout = GossipRouter.ROUTING_CLIENT_REPLY_TIMEOUT;

      int backlog = 0;
      long soLinger = -1;
      long soTimeout = -1;

      GossipRouter router = null;
      String bind_addr = null;
      boolean jmx = false;

      for (int i = 0; i < args.length; i++) {
         arg = args[i];
         if ("-port".equals(arg)) {
            port = Integer.parseInt(args[++i]);
            continue;
         }
         if ("-bindaddress".equals(arg) || "-bind_addr".equals(arg)) {
            bind_addr = args[++i];
            continue;
         }
         if ("-backlog".equals(arg)) {
            backlog = Integer.parseInt(args[++i]);
            continue;
         }
         if ("-expiry".equals(arg)) {
             System.err.println("-expiry has been deprecated and will be ignored");
            continue;
         }
         if ("-jmx".equals(arg)) {
            jmx = true;
            continue;
         }
         // this option is not used and should be deprecated/removed in a future release
         if ("-timeout".equals(arg)) {
            System.out.println("    -timeout is deprecated and will be ignored");
            ++i;
            continue;
         }
         // this option is not used and should be deprecated/removed in a future release
         if ("-rtimeout".equals(arg)) {
            System.out.println("    -rtimeout is deprecated and will be ignored");
            ++i;
            continue;
         }
         if ("-solinger".equals(arg)) {
            soLinger = Long.parseLong(args[++i]);
            continue;
         }
         if ("-sotimeout".equals(arg)) {
            soTimeout = Long.parseLong(args[++i]);
            continue;
         }
         help();
         return;
      }
      System.out.println("GossipRouter is starting. CTRL-C to exit JVM");

      try {
         router = new GossipRouter(port, bind_addr, timeout, routingTimeout, jmx);

         if (backlog > 0)
            router.setBacklog(backlog);

         if (soTimeout >= 0)
            router.setSocketReadTimeout(soTimeout);

         if (soLinger >= 0)
            router.setLingerTimeout(soLinger);

         router.start();
      } catch (Exception e) {
         System.err.println(e);
      }
       finally {
          router.stop();
          router.cleanup();
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