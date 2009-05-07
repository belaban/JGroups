package org.jgroups.stack;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.Address;
import org.jgroups.protocols.TUNNEL;
import org.jgroups.util.Util;

/**
 * Client stub that talks to a remote GossipRouter
 * 
 * @author Bela Ban
 * @version $Id: RouterStub.java,v 1.43 2009/05/07 07:51:57 vlada Exp $
 */
public class RouterStub {

   public final static int STATUS_CONNECTED = 0;

   public final static int STATUS_DISCONNECTED = 1;

   private final String router_host; // name of the router host

   private final int router_port; // port on which router listens on

   private Socket sock = null; // socket connecting to the router

   private DataOutputStream output = null;

   private DataInputStream input = null;

   private final Address local_addr;

   private volatile int connectionState = STATUS_DISCONNECTED;

   private static final Log log = LogFactory.getLog(TUNNEL.class);

   private ConnectionListener conn_listener;

   private String groupname = null;

   private final InetAddress bind_addr;

   private int sock_conn_timeout = 3000; // max number of ms to wait for socket establishment to
                                         // GossipRouter

   private int sock_read_timeout = 3000; // max number of ms to wait for socket reads (0 means block
                                         // forever, or until the sock is closed)

   private volatile boolean intentionallyDisconnected = false;

   public interface ConnectionListener {
      void connectionStatusChange(int state);
   }

   /**
    * Creates a stub for a remote Router object.
    * 
    * @param routerHost
    *           The name of the router's host
    * @param routerPort
    *           The router's port
    * @throws SocketException
    */
   public RouterStub(String routerHost, int routerPort, InetAddress bindAddress,
            Address localAddress) {
      if (localAddress == null) {
         throw new IllegalArgumentException("localAddress cannot be null");
      }
      router_host = routerHost != null ? routerHost : "localhost";
      router_port = routerPort;
      bind_addr = bindAddress;
      local_addr = localAddress;
   }

   public int getSocketConnectionTimeout() {
      return sock_conn_timeout;
   }

   public void setSocketConnectionTimeout(int sock_conn_timeout) {
      this.sock_conn_timeout = sock_conn_timeout;
   }

   public int getSocketReadTimeout() {
      return sock_read_timeout;
   }

   public void setSocketReadTimeout(int sock_read_timeout) {
      this.sock_read_timeout = sock_read_timeout;
   }

   public boolean isConnected() {
      return connectionState == STATUS_CONNECTED;
   }

   public void setConnectionListener(ConnectionListener conn_listener) {
      this.conn_listener = conn_listener;
   }

   public Address getLocalAddress() throws SocketException {
      return local_addr;
   }

   /**
    * Register this process with the router under <code>groupname</code>.
    * 
    * @param groupname
    *           The name of the group under which to register
    */
   public synchronized void connect(String groupname) throws Exception {
      if (groupname == null || groupname.length() == 0)
         throw new Exception("groupname is null");

      if (!isConnected()) {
         this.groupname = groupname;
         try {
            sock = new Socket();
            sock.bind(new InetSocketAddress(bind_addr, 0));
            sock.setSoTimeout(sock_read_timeout);
            sock.setSoLinger(true, 2);
            sock.connect(new InetSocketAddress(router_host, router_port), sock_conn_timeout);
            output = new DataOutputStream(sock.getOutputStream());

            GossipData req = new GossipData(GossipRouter.CONNECT, groupname, local_addr, null);
            req.writeTo(output);
            output.flush();
            input = new DataInputStream(sock.getInputStream());
            boolean connectedOk = input.readBoolean();
            intentionallyDisconnected = false;
            if (connectedOk) {
               connectionStateChanged(STATUS_CONNECTED);
               if (log.isDebugEnabled()) {
                  log.debug("Connected " + this + ", groupname=" + groupname);
               }
            } else
               throw new Exception("Failed to get connection ack from gossip router");
         } catch (Exception e) {
            if (log.isWarnEnabled())
               log.warn(this + " failed connecting to " + router_host + ":" + router_port);
            Util.close(sock);
            Util.close(input);
            Util.close(output);
            connectionStateChanged(STATUS_DISCONNECTED);
            throw e;
         }
      }
   }

   public synchronized void disconnect() {
      try {
         output.writeByte(GossipRouter.DISCONNECT);
         output.flush();
      } catch (Exception e) {
      } finally {
         Util.close(output);
         Util.close(input);
         Util.close(sock);
         sock = null;
         intentionallyDisconnected = true;
         connectionStateChanged(STATUS_DISCONNECTED);
      }
   }

   public boolean isIntentionallyDisconnected() {
      return intentionallyDisconnected;
   }

   public synchronized List<Address> getMembers(final String group, long timeout) throws Exception {
      List<Address> mbrs = new LinkedList<Address>();
      try {
         output.writeByte(GossipRouter.GOSSIP_GET);
         // 1. Group name
         output.writeUTF(groupname);
         output.flush();

         mbrs = (List<Address>) Util.readAddresses(input, LinkedList.class);

      } catch (SocketException se) {
         if (log.isWarnEnabled())
            log.warn("Router stub " + this + " did not send message", se);
         connectionStateChanged(STATUS_DISCONNECTED);
      } catch (Exception e) {
         if (log.isErrorEnabled())
            log.error("Router stub " + this + " failed sending message to router");
         connectionStateChanged(STATUS_DISCONNECTED);
         throw new Exception("Connection broken", e);
      }
      return mbrs;
   }

   public InetSocketAddress getGossipRouterAddress() {
      return new InetSocketAddress(router_host, router_port);
   }

   public String toString() {
      return "RouterStub[local_address=" + local_addr + ",router_host=" + router_host
               + ",router_port=" + router_port + ",connected=" + isConnected() + "]";
   }

   public void sendToAllMembers(byte[] data, int offset, int length) throws Exception {
      // null destination represents mcast
      sendToSingleMember(null, data, offset, length);
   }

   public synchronized void sendToSingleMember(Address dest, byte[] data, int offset, int length)
            throws Exception {
      try {
         output.writeByte(GossipRouter.MESSAGE);
         // 1. Group name
         output.writeUTF(groupname);

         // 2. Destination address (null in case of mcast)
         Util.writeAddress(dest, output);

         // 3. Length of byte buffer
         output.writeInt(data.length);

         // 4. Byte buffer
         output.write(data, 0, data.length);

         output.flush();

      } catch (SocketException se) {
         if (log.isWarnEnabled())
            log.warn("Router stub " + this + " did not send message to "
                     + (dest == null ? "mcast" : dest + " since underlying socket is closed"), se);
         connectionStateChanged(STATUS_DISCONNECTED);
         throw new Exception("dest=" + dest + " (" + length + " bytes)", se);
      } catch (Exception e) {
         if (log.isErrorEnabled())
            log.error("Router stub " + this + " failed sending message to router");
         connectionStateChanged(STATUS_DISCONNECTED);
         throw new Exception("dest=" + dest + " (" + length + " bytes)", e);
      }
   }

   public DataInputStream getInputStream() {
      return input;
   }

   private void connectionStateChanged(int newState) {
      boolean notify = connectionState != newState;
      connectionState = newState;
      if (notify && conn_listener != null) {
         try {
            conn_listener.connectionStatusChange(newState);
         } catch (Throwable t) {
            log.error("failed notifying ConnectionListener " + conn_listener, t);
         }
      }
   }
}
