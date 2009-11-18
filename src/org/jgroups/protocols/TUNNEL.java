
package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.PhysicalAddress;
import org.jgroups.annotations.Experimental;
import org.jgroups.annotations.GuardedBy;
import org.jgroups.annotations.Property;
import org.jgroups.stack.*;
import org.jgroups.util.*;
import org.jgroups.util.UUID;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.*;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Replacement for UDP. Instead of sending packets via UDP, a TCP connection is opened to a Router
 * (using the RouterStub client-side stub), the IP address/port of which was given using channel
 * properties <code>router_host</code> and <code>router_port</code>. All outgoing traffic is sent
 * via this TCP socket to the Router which distributes it to all connected TUNNELs in this group.
 * Incoming traffic received from Router will simply be passed up the stack.
 * 
 * <p>
 * A TUNNEL layer can be used to penetrate a firewall, most firewalls allow creating TCP connections
 * to the outside world, however, they do not permit outside hosts to initiate a TCP connection to a
 * host inside the firewall. Therefore, the connection created by the inside host is reused by
 * Router to send traffic from an outside host to a host inside the firewall.
 * 
 * @author Bela Ban
 * @author Vladimir Blagojevic
 * @version $Id: TUNNEL.java,v 1.91 2009/11/18 16:48:44 vlada Exp $
 */
@Experimental
public class TUNNEL extends TP {

   /*
    * ----------------------------------------- Properties
    * --------------------------------------------------
    */

   @Deprecated
   @Property(name = "router_host", deprecatedMessage = "router_host is deprecated. Specify target GRs using gossip_router_hosts", description = "Router host address")
   private String router_host = null;

   @Deprecated
   @Property(name = "router_port", deprecatedMessage = "router_port is deprecated. Specify target GRs using gossip_router_hosts", description = "Router port")
   private int router_port = 0;

   @Property(description = "Interval in msec to attempt connecting back to router in case of torn connection. Default is 5000 msec")
   private long reconnect_interval = 5000;

   /*
    * --------------------------------------------- Fields
    * ------------------------------------------------------
    */

   private final List<InetSocketAddress> gossip_router_hosts = new ArrayList<InetSocketAddress>();

   private final List<RouterStub> stubs = new ArrayList<RouterStub>();

   private TUNNELPolicy tunnel_policy = new DefaultTUNNELPolicy();

   private DatagramSocket sock;

   /** time to wait in ms between reconnect attempts */

   @GuardedBy("reconnectorLock")
   private final Map<InetSocketAddress, Future<?>> reconnectFutures = new HashMap<InetSocketAddress, Future<?>>();

   private final Lock reconnectorLock = new ReentrantLock();

   public TUNNEL() {
   }

   @Property
   public void setGossipRouterHosts(String hosts) throws UnknownHostException {
      gossip_router_hosts.clear();
      // if we get passed value of List<SocketAddress>#toString() we have to strip []
      if (hosts.startsWith("[") && hosts.endsWith("]")) {
         hosts = hosts.substring(1, hosts.length() - 1);
      }
      gossip_router_hosts.addAll(Util.parseCommaDelimitedHosts2(hosts, 1));
   }

   public String toString() {
      return "TUNNEL";
   }

   @Deprecated
   public String getRouterHost() {
      return router_host;
   }

   @Deprecated
   public void setRouterHost(String router_host) {
      this.router_host = router_host;
   }

   @Deprecated
   public int getRouterPort() {
      return router_port;
   }

   @Deprecated
   public void setRouterPort(int router_port) {
      this.router_port = router_port;
   }

   public long getReconnectInterval() {
      return reconnect_interval;
   }

   public void setReconnectInterval(long reconnect_interval) {
      this.reconnect_interval = reconnect_interval;
   }

   /*------------------------------ Protocol interface ------------------------------ */

   public synchronized void setTUNNELPolicy(TUNNELPolicy policy) {
      if (policy == null)
         throw new IllegalArgumentException("Tunnel policy has to be non null");
      tunnel_policy = policy;
   }

    public void init() throws Exception {
        super.init();

        if(enable_bundling) {
            log.warn("bundling is currently not supported by TUNNEL; bundling is disabled");
            enable_bundling=false;
        }

        if (timer == null)
            throw new Exception("timer cannot be retrieved from protocol stack");

        if ((router_host == null || router_port == 0) && gossip_router_hosts.isEmpty()) {
            throw new Exception("either router_host and router_port have to be set or a list of gossip routers");
        }

        if (router_host != null && router_port != 0 && !gossip_router_hosts.isEmpty()) {
            throw new Exception("cannot specify both router host and port along with gossip_router_hosts");
        }

        if (router_host != null && router_port != 0 && gossip_router_hosts.isEmpty()) {
            gossip_router_hosts.add(new InetSocketAddress(router_host, router_port));
        }

        if (log.isDebugEnabled()) {
            log.debug("GossipRouters are:" + gossip_router_hosts.toString());
        }
                        
    }

    public void start() throws Exception {
      // loopback turned on is mandatory
      loopback = true;

      sock = new DatagramSocket(0, bind_addr);

      super.start();

      for (InetSocketAddress gr : gossip_router_hosts) {
         RouterStub stub = new RouterStub(gr.getHostName(), gr.getPort(), bind_addr);
         stub.setConnectionListener(new StubConnectionListener(stub));
         stubs.add(stub);
      }
   }

    public void destroy() {        
        for (RouterStub stub : stubs) {
            stopReconnecting(stub);
            stub.destroy();
        }
        super.destroy();
    }

   private void disconnectStub(String group, Address addr) {
      for (RouterStub stub : stubs) {         
         stub.disconnect(group,addr);
      }
   }

   public Object handleDownEvent(Event evt) {
      Object retEvent = super.handleDownEvent(evt);
      switch (evt.getType()) {
         case Event.CONNECT:
         case Event.CONNECT_WITH_STATE_TRANSFER:
         case Event.CONNECT_USE_FLUSH:
         case Event.CONNECT_WITH_STATE_TRANSFER_USE_FLUSH:
             String group=(String)evt.getArg();
             Address local= null;
             if(!isSingleton()) {
                 local = local_addr;                 
             } else {
                 ProtocolAdapter adapter = ProtocolAdapter.thread_local.get();
                 local = adapter.local_addr;
             }
             PhysicalAddress physical_addr=(PhysicalAddress)down(new Event(Event.GET_PHYSICAL_ADDRESS, local));
             List<PhysicalAddress> physical_addrs=Arrays.asList(physical_addr);
             String logical_name=org.jgroups.util.UUID.get(local);
             tunnel_policy.connect(stubs, group, local, logical_name, physical_addrs);
            break;

         case Event.DISCONNECT:
             if(!isSingleton()) {
                 local = local_addr;        
                 group = channel_name;
             } else {
                 ProtocolAdapter adapter = ProtocolAdapter.thread_local.get();
                 local = adapter.local_addr;
                 group = adapter.cluster_name;
             }
             disconnectStub(group,local);
            break;
      }
      return retEvent;
   }

   private void startReconnecting(final RouterStub stub) {
      reconnectorLock.lock();
      try {
         Future<?> reconnectorFuture = reconnectFutures.get(stub.getGossipRouterAddress());
         if (reconnectorFuture == null || reconnectorFuture.isDone()) {
            final Runnable reconnector = new Runnable() {
               public void run() {
                  try {
                      if (log.isInfoEnabled()) {
                          log.info("Reconnecting " + stub);
                      }

                      if(!isSingleton()){
                          PhysicalAddress physical_addr=(PhysicalAddress)down(new Event(Event.GET_PHYSICAL_ADDRESS, local_addr));
                          List<PhysicalAddress> physical_addrs=Arrays.asList(physical_addr);
                          stub.connect(channel_name, local_addr, UUID.get(local_addr), physical_addrs);
                          if (log.isInfoEnabled()) {
                              log.info("Reconnected " + stub);
                          }
                      }
                      else {
                          for(Protocol p: up_prots.values()) {
                              if(p instanceof ProtocolAdapter) {
                                  Address local=((ProtocolAdapter)p).local_addr;
                                  String cluster_name=((ProtocolAdapter)p).cluster_name;
                                  PhysicalAddress physical_addr=(PhysicalAddress)down(new Event(Event.GET_PHYSICAL_ADDRESS, local));
                                  List<PhysicalAddress> physical_addrs=Arrays.asList(physical_addr);
                                  stub.connect(cluster_name, local, UUID.get(local), physical_addrs);
                                  if (log.isInfoEnabled()) {
                                      log.info("Reconnected " + stub);
                                  }
                              }
                          }
                      }
                  } catch(Throwable ex) {
                     if (log.isWarnEnabled())
                         log.warn("failed reconnecting stub to GR at " + stub.getGossipRouterAddress() + ": " + ex);
                  }
               }
            };
            reconnectorFuture = timer.scheduleWithFixedDelay(reconnector, 0, reconnect_interval,
                     TimeUnit.MILLISECONDS);
            reconnectFutures.put(stub.getGossipRouterAddress(), reconnectorFuture);
         }
      } finally {
         reconnectorLock.unlock();
      }
   }

   private void stopReconnecting(final RouterStub stub) {
      reconnectorLock.lock();
      InetSocketAddress address = stub.getGossipRouterAddress();
      try {
         Future<?> reconnectorFuture = reconnectFutures.get(address);
         if (reconnectorFuture != null) {
            reconnectorFuture.cancel(true);
            reconnectFutures.remove(address);
         }
      } finally {
         reconnectorLock.unlock();
      }
   }

   private class StubConnectionListener implements RouterStub.ConnectionListener {

      private volatile RouterStub.ConnectionStatus currentState = RouterStub.ConnectionStatus.INITIAL;
      private final RouterStub stub;

      public StubConnectionListener(RouterStub stub) {
         super();
         this.stub = stub;
      }

      public void connectionStatusChange(RouterStub.ConnectionStatus newState) {
         if (newState == RouterStub.ConnectionStatus.DISCONNECTED) {
             stub.interrupt();
             startReconnecting(stub);
         }
         else if (currentState != RouterStub.ConnectionStatus.CONNECTED
                 && newState == RouterStub.ConnectionStatus.CONNECTED) {
             stopReconnecting(stub);             
             StubReceiver stubReceiver = new StubReceiver(stub.getInputStream());
             stub.setReceiver(stubReceiver);
             Thread t = global_thread_factory.newThread(stubReceiver, "TUNNEL receiver for " + stub.toString());
             stubReceiver.setThread(t);
             t.setDaemon(true);
             t.start();
         }
          currentState = newState;
      }
   }

    public class StubReceiver implements Runnable {

        private Thread runner;
        private final DataInputStream input;
        
        public StubReceiver(DataInputStream input) {
            this.input = input;
        }

        public synchronized void setThread(Thread t) {
            runner = t;
        }

        public synchronized Thread getThread() {
            return runner;
        }

        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                try {                                        
                    GossipData msg = new GossipData();
                    msg.readFrom(input);
                    switch (msg.getType()) {
                        case GossipRouter.MESSAGE:
                            byte[] data = msg.getBuffer();
                            receive(null/* src will be read from data */, data, 0, data.length);
                            break;
                        case GossipRouter.SUSPECT:
                            final Address suspect = Util.readAddress(input);
                            log.debug("Firing suspect event " + suspect + " at " + local_addr);
                            if(suspect != null) {
                                // https://jira.jboss.org/jira/browse/JGRP-902                               
                                Thread thread = getThreadFactory().newThread(new Runnable() {
                                    public void run() {
                                        fireSuspectEvent(suspect);
                                    }
                                }, "StubReceiver-suspect");
                                thread.start();
                            }
                            break;
                    }
                } catch (SocketTimeoutException ste) {
                    // do nothing - blocking read timeout caused it
                    continue;
                } catch (SocketException se) {
                    break;
                } catch (IOException ioe) {
                    /*
                     * This is normal course of operation Thread should not die
                     */
                    continue;
                } catch (Exception e) {
                    if (log.isWarnEnabled())
                        log.warn("failure in TUNNEL receiver thread", e);
                    break;
                }
            }
        }

        private void fireSuspectEvent(Address suspect) {
            up(new Event(Event.SUSPECT, suspect));            
        }
    }


    protected void send(Message msg, Address dest, boolean multicast) throws Exception {

        // we don't currently support message bundling in TUNNEL
        TpHeader hdr=(TpHeader)msg.getHeader(getName());
        if(hdr == null)
            throw new Exception("message " + msg + " doesn't have a transport header, cannot route it");
        String group=hdr.channel_name;

        ExposedByteArrayOutputStream out_stream=new ExposedByteArrayOutputStream((int)(msg.size() + 50));
        ExposedDataOutputStream dos=new ExposedDataOutputStream(out_stream);

        writeMessage(msg, dos, multicast);
        Buffer buf=new Buffer(out_stream.getRawBuffer(), 0, out_stream.size());

        if(stats) {
            num_msgs_sent++;
            num_bytes_sent+=buf.getLength();
        }
        if(multicast) {
            tunnel_policy.sendToAllMembers(stubs, group, buf.getBuf(), buf.getOffset(), buf.getLength());
        }
        else {
            tunnel_policy.sendToSingleMember(stubs, group, dest, buf.getBuf(), buf.getOffset(), buf.getLength());
        }
    }


    public void sendMulticast(byte[] data, int offset, int length) throws Exception {
        throw new UnsupportedOperationException("sendMulticast() should not get called on TUNNEL");
    }

   public void sendUnicast(PhysicalAddress dest, byte[] data, int offset, int length) throws Exception {
       throw new UnsupportedOperationException("sendUnicast() should not get called on TUNNEL");
   }

   public String getInfo() {
      if (stubs.isEmpty())
         return stubs.toString();
      else
         return "RouterStubs not yet initialized";
   }

   protected PhysicalAddress getPhysicalAddress() {
      return sock != null ? new IpAddress(bind_addr, sock.getLocalPort()) : null;
   }

   public interface TUNNELPolicy {
      public void connect(List<RouterStub> stubs, String group, Address addr, String logical_name, List<PhysicalAddress> phys_addrs);

      public void sendToAllMembers(List<RouterStub> stubs, String group, byte[] data, int offset, int length) throws Exception;

       public void sendToSingleMember(List<RouterStub> stubs, String group, Address dest, byte[] data, int offset,
               int length) throws Exception;
   }

   private class DefaultTUNNELPolicy implements TUNNELPolicy {

      public void sendToAllMembers(List<RouterStub> stubs, String group, byte[] data, int offset, int length)
               throws Exception {
         boolean sent = false;
          if(stubs.size() > 1)
              Collections.shuffle(stubs);  // todo: why is this needed ?
         for (RouterStub stub : stubs) {
            try {
               stub.sendToAllMembers(group, data, offset, length);
               if (log.isTraceEnabled())
                  log.trace("sent a message to all members, GR used " + stub.getGossipRouterAddress());
               sent = true;
               break;
            } catch (Exception e) {
                if (log.isWarnEnabled())
                    log.warn("failed sending a message to all members, GR used " + stub.getGossipRouterAddress());
            }
         }
         if (!sent)
            throw new Exception("None of the available stubs " + stubs + " accepted a multicast message");
      }

      public void sendToSingleMember(List<RouterStub> stubs, String group, Address dest, byte[] data, int offset,
               int length) throws Exception {
         boolean sent = false;
          if(stubs.size() > 1)
              Collections.shuffle(stubs); // todo: why is this needed ?
         for (RouterStub stub : stubs) {
            try {
               stub.sendToMember(group, dest, data, offset, length);
               if (log.isDebugEnabled())
                  log.debug("sent a message to " + dest + ", GR used " + stub.getGossipRouterAddress());
               sent = true;
               break;
            } catch (Exception e) {
                if (log.isWarnEnabled()) {
                    log.warn("failed sending a message to " + dest + ", GR used " + stub.getGossipRouterAddress());
                }
            }
         }
         if (!sent)
            throw new Exception("None of the available stubs " + stubs
                     + " accepted a message for dest " + dest);
      }

       public void connect(List<RouterStub> stubs, String group, Address addr, String logical_name, List<PhysicalAddress> phys_addrs) {
           for (RouterStub stub : stubs) {
               try {
                   stub.connect(group, addr, logical_name, phys_addrs);
               }
               catch (Exception e) {
                   if (log.isWarnEnabled())
                       log.warn("Failed connecting to GossipRouter at " + stub.getGossipRouterAddress());
                   startReconnecting(stub);
               }
           }
       }
   }
}
