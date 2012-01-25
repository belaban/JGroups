
package org.jgroups.protocols;

import java.io.DataInputStream;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Global;
import org.jgroups.Message;
import org.jgroups.PhysicalAddress;
import org.jgroups.annotations.Experimental;
import org.jgroups.annotations.Property;
import org.jgroups.stack.GossipData;
import org.jgroups.stack.GossipRouter;
import org.jgroups.stack.RouterStubManager;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.RouterStub;
import org.jgroups.util.Buffer;
import org.jgroups.util.ExposedByteArrayOutputStream;
import org.jgroups.util.ExposedDataOutputStream;
import org.jgroups.util.Util;

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
 */
@Experimental
public class TUNNEL extends TP {

    /*
    * ----------------------------------------- Properties
    * --------------------------------------------------
    */

    @Property(description = "Interval in msec to attempt connecting back to router in case of torn connection. Default is 5000 msec")
    private long reconnect_interval = 5000;

    @Property(description="Should TCP no delay flag be turned on")
    boolean tcp_nodelay=false;

    /*
    * --------------------------------------------- Fields
    * ------------------------------------------------------
    */

   private final List<InetSocketAddress> gossip_router_hosts = new ArrayList<InetSocketAddress>();

   private TUNNELPolicy tunnel_policy = new DefaultTUNNELPolicy();

   private DatagramSocket sock;
   
   private volatile RouterStubManager stubManager;

   public TUNNEL() {
   }

    public boolean supportsMulticasting() {
        return false;
    }

    @Property(description="A comma-separated list of GossipRouter hosts, e.g. HostA[12001],HostB[12001]")
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
      
        // Postpone TUNNEL and shared transport until 3.0 timeframe
        // TODO [JGRP-1194] - Revisit implementation of TUNNEL and shared transport
        if (isSingleton()) {
            throw new Exception("TUNNEL and shared transport mode are not supported!");
        }

        if (log.isDebugEnabled())
            log.debug("GossipRouters are:" + gossip_router_hosts.toString());
        
        stubManager = RouterStubManager.emptyGossipClientStubManager(this);
        sock = getSocketFactory().createDatagramSocket("jgroups.tunnel.ucast_sock", bind_port, bind_addr);
        
        // loopback turned on is mandatory
        loopback = true;
    }
    
    public void destroy() {        
        stubManager.destroyStubs();
        super.destroy();
    }

   private void disconnectStub(String group, Address addr) {
      stubManager.disconnectStubs();
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
                 // TODO [JGRP-1194] - Revisit implementation of TUNNEL and shared transport   
                 ProtocolAdapter adapter = ProtocolAdapter.thread_local.get();
                 local = adapter.local_addr;
             }
             
             if(stubManager != null) {
                stubManager.destroyStubs();
             }
             stubManager = new TUNNELStubManager(this,group,local,getReconnectInterval());
             for (InetSocketAddress gr : gossip_router_hosts) {
                 RouterStub stub = stubManager.createAndRegisterStub(gr.getHostName(), gr.getPort(), bind_addr);
                 stub.setTcpNoDelay(tcp_nodelay);           
              }  
             PhysicalAddress physical_addr=(PhysicalAddress)down(new Event(Event.GET_PHYSICAL_ADDRESS, local));
             List<PhysicalAddress> physical_addrs=Arrays.asList(physical_addr);
             String logical_name=org.jgroups.util.UUID.get(local);
             List<RouterStub> stubs = stubManager.getStubs();
             tunnel_policy.connect(stubs, group, local, logical_name, physical_addrs);
            break;

         case Event.DISCONNECT:
             if(!isSingleton()) {
                 local = local_addr;        
                 group = channel_name;
             } else {
                 // TODO [JGRP-1194] - Revisit implementation of TUNNEL and shared transport
                 ProtocolAdapter adapter = ProtocolAdapter.thread_local.get();
                 local = adapter.local_addr;
                 group = adapter.cluster_name;
             }
             disconnectStub(group,local);
            break;
      }
      return retEvent;
   }
    private class TUNNELStubManager extends RouterStubManager {

        TUNNELStubManager(Protocol owner, String channelName, Address logicalAddress, long interval) {            
            super(owner, channelName, logicalAddress, interval);
        }

        public void connectionStatusChange(RouterStub stub, RouterStub.ConnectionStatus newState) {
            super.connectionStatusChange(stub, newState);
            if (newState == RouterStub.ConnectionStatus.CONNECTED) {               
                StubReceiver stubReceiver = new StubReceiver(stub);
                stub.setReceiver(stubReceiver);
                Thread t = global_thread_factory.newThread(stubReceiver, "TUNNEL receiver for " + stub.toString());
                stubReceiver.setThread(t);
                t.setDaemon(true);
                t.start();
            } 
        }
    }

    public class StubReceiver implements Runnable {

        private Thread runner;        
        private final RouterStub stub;
       
        public StubReceiver(RouterStub stub) {
            this.stub = stub;          
        }

        public synchronized void setThread(Thread t) {
            runner = t;
        }

        public synchronized Thread getThread() {
            return runner;
        }

        public void run() {
            final DataInputStream input = stub.getInputStream();
            mainloop:
            while (!Thread.currentThread().isInterrupted()) {
                try {                                        
                    GossipData msg = new GossipData();
                    msg.readFrom(input);
                    switch (msg.getType()) {
                        case GossipRouter.DISCONNECT_OK:                            
                            break mainloop;
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
                }catch (Exception ioe) {     
                    if(stub.isConnected())
                        continue mainloop;
                    else 
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
        TpHeader hdr=(TpHeader)msg.getHeader(this.id);
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
        List<RouterStub> stubs = stubManager.getStubs();
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
       List<RouterStub> stubs = stubManager.getStubs();
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
                if(!stub.isConnected())
                    continue;
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

      public void sendToSingleMember(List<RouterStub> stubs, String group, Address dest, byte[] data, int offset, int length) throws Exception {
         boolean sent = false;
          if(stubs.size() > 1)
              Collections.shuffle(stubs); 
         for (RouterStub stub : stubs) {
            try {
                if(!stub.isConnected())
                    continue;
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
                   stubManager.startReconnecting(stub);
               }
           }
       }
   }
}
