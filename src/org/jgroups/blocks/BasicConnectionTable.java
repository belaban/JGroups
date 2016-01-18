package org.jgroups.blocks;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.Version;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.DefaultSocketFactory;
import org.jgroups.util.DefaultThreadFactory;
import org.jgroups.util.SocketFactory;
import org.jgroups.util.ThreadFactory;
import org.jgroups.util.Util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Shared class for TCP connection tables.
 * @author Scott Marlow
 */
public abstract class BasicConnectionTable {
    private ThreadFactory factory;
    final Map<Address,Connection>  conns=new HashMap<>();         // keys: Addresses (peer address), values: Connection
    Receiver              receiver=null;
    boolean               use_send_queues=false;       // max number of messages in a send queue
    int                   send_queue_size=10000;
    InetAddress           bind_addr=null;
    Address               local_addr=null;             // bind_addr + port of srv_sock
    int                   srv_port=7800;
    int                   recv_buf_size=120000;
    int                   send_buf_size=60000;
    final List<ConnectionListener>        conn_listeners=new ArrayList<>(); // listeners to be notified when a conn is established/torn down
    Reaper                reaper=null;                 // closes conns that have been idle for more than n secs
    long                  reaper_interval=60000;       // reap unused conns once a minute
    long                  conn_expire_time=300000;     // connections can be idle for 5 minutes before they are reaped
    int                   sock_conn_timeout=1000;      // max time in millis to wait for Socket.connect() to return
    int                   peer_addr_read_timeout=2000; // max time in milliseconds to block on reading peer address
    protected final Log   log= LogFactory.getLog(getClass());
    final byte[]          cookie={'b', 'e', 'l', 'a'};
    boolean               use_reaper=false;            // by default we don't reap idle conns
    static final int      backlog=20;                  // 20 conn requests are queued by ServerSocket (addtl will be discarded)
    volatile ServerSocket srv_sock=null;
    boolean               tcp_nodelay=false;
    int                   linger=-1;
    protected SocketFactory socket_factory=new DefaultSocketFactory();

   /**
    * The address which will be broadcast to the group (the externally visible address which this host should
    * be contacted on). If external_addr is null, it will default to the same address that the server socket is bound to.
    */
    InetAddress		    external_addr=null;
    int                 external_port=0;
    int                 max_port=0;                   // maximum port to bind to (if < srv_port, no limit)
    Thread              acceptor=null;               // continuously calls srv_sock.accept()
    boolean             running=false;
    /** Total number of Connections created for this connection table */
    static AtomicInteger conn_creations=new AtomicInteger(0);

    final static long   MAX_JOIN_TIMEOUT=Global.THREAD_SHUTDOWN_WAIT_TIME;



    protected BasicConnectionTable() {        
        factory = new DefaultThreadFactory("Connection Table", false);
    }

    public final void setReceiver(Receiver r) {
        receiver=r;
    }

    public void addConnectionListener(ConnectionListener l) {
        if(l != null && !conn_listeners.contains(l))
            conn_listeners.add(l);
    }

    public void removeConnectionListener(ConnectionListener l) {
        if(l != null) conn_listeners.remove(l);
    }

    public Address getLocalAddress() {
        if(local_addr == null)
            local_addr=bind_addr != null ? new IpAddress(bind_addr, srv_port) : null;
        return local_addr;
    }

    public int getSendBufferSize() {
        return send_buf_size;
    }

    public void setSendBufferSize(int send_buf_size) {
        this.send_buf_size=send_buf_size;
    }

    public int getReceiveBufferSize() {
        return recv_buf_size;
    }

    public void setReceiveBufferSize(int recv_buf_size) {
        this.recv_buf_size=recv_buf_size;
    }

    public int getSocketConnectionTimeout() {
        return sock_conn_timeout;
    }

    public void setSocketConnectionTimeout(int sock_conn_timeout) {
        this.sock_conn_timeout=sock_conn_timeout;
    }

    public int getPeerAddressReadTimeout() {
        return peer_addr_read_timeout;
    }

    public void setPeerAddressReadTimeout(int peer_addr_read_timeout) {
        this.peer_addr_read_timeout=peer_addr_read_timeout;
    }

    public int getNumConnections() {
        return conns.size();
    }

    public static int getNumberOfConnectionCreations() {
        return conn_creations.intValue();
    }

    public boolean getTcpNodelay() {
        return tcp_nodelay;
    }

    public void setTcpNodelay(boolean tcp_nodelay) {
        this.tcp_nodelay=tcp_nodelay;
    }

    public int getLinger() {
        return linger;
    }

    public void setLinger(int linger) {
        this.linger=linger;
    }
    
    public void setThreadFactory(ThreadFactory factory){
        this.factory = factory;
    }
    
    public ThreadFactory getThreadFactory(){
        return factory;
    }

    public SocketFactory getSocketFactory() {
        return socket_factory;
    }

    public void setSocketFactory(SocketFactory socket_factory) {
        this.socket_factory=socket_factory;
    }

    public boolean getUseSendQueues() {return use_send_queues;}

    public void setUseSendQueues(boolean flag) {this.use_send_queues=flag;}

    public int getSendQueueSize() {
        return send_queue_size;
    }

    public void setSendQueueSize(int send_queue_size) {
        this.send_queue_size=send_queue_size;
    }

    public void start() throws Exception {
        running=true;
    }

    public void stop() {
        running=false;

        // 1. Stop the reaper
        if(reaper != null)
            reaper.stop();

        // 2. close the server socket (this also stops the acceptor thread)
        if(srv_sock != null) {
            try {
                ServerSocket tmp=srv_sock;
                srv_sock=null;
                socket_factory.close(tmp);
                if(acceptor != null)
                    Util.interruptAndWaitToDie(acceptor);
            }
            catch(Exception e) {
            }
        }

        // 3. then close the connections
        Collection<Connection> connsCopy=null;
        synchronized(conns) {
            connsCopy=new LinkedList<>(conns.values());
            conns.clear();
        }
        for(Connection conn:connsCopy) {
            conn.destroy();
        }
        connsCopy.clear();
        local_addr=null;
    }

    /**
     Remove <code>addr</code>from connection table. This is typically triggered when a member is suspected.
     */
    public void removeConnection(Address addr) {
        Connection conn;

       synchronized(conns) {
           conn=conns.remove(addr);
       }

       if(conn != null) {
           try {
               conn.destroy();  // won't do anything if already destroyed
           }
           catch(Exception e) {
           }
       }
       if(log.isTraceEnabled()) log.trace("removed " + addr + ", connections are " + toString());
   }

   /**
    * Calls the receiver callback. We do not serialize access to this method, and it may be called concurrently
    * by several Connection handler threads. Therefore the receiver needs to be reentrant.
    */
   public void receive(Address sender, byte[] data, int offset, int length) {
       if(receiver != null)
           receiver.receive(sender, data, offset, length);
   }

   public String toString() {
       StringBuilder ret=new StringBuilder();
       Address key;
       Connection val;
       Entry<Address,Connection> entry;
       HashMap<Address,Connection> copy;

       synchronized(conns) {
           copy=new HashMap<>(conns);
       }
       ret.append("local_addr=" + local_addr).append("\n");
       ret.append("connections (" + copy.size() + "):\n");
       for(Iterator<Entry<Address,Connection>> it=copy.entrySet().iterator(); it.hasNext();) {
           entry=it.next();
           key=entry.getKey();
           val=entry.getValue();
           ret.append(key + ": " + val + '\n');
       }
       ret.append('\n');
       return ret.toString();
   }

   void notifyConnectionOpened(Address peer) {
       if(peer == null) return;
       for(int i=0; i < conn_listeners.size(); i++)
           conn_listeners.get(i).connectionOpened(peer);
   }

   void notifyConnectionClosed(Address peer) {
       if(peer == null) return;
       for(int i=0; i < conn_listeners.size(); i++)
           conn_listeners.get(i).connectionClosed(peer);
   }

   void addConnection(Address peer, Connection c) {
       synchronized (conns) {
           conns.put(peer, c); 
       }       
       if(reaper != null && !reaper.isRunning())
           reaper.start();
   }

   public void send(Address dest, byte[] data, int offset, int length) throws Exception {
       Connection conn;
       if(dest == null) {
           if(log.isErrorEnabled())
               log.error(Util.getMessage("DestinationIsNull"));
           return;
       }

       if(data == null) {
           log.warn("data is null; discarding packet");
           return;
       }

       if(!running) {
           if(log.isWarnEnabled())
               log.warn("connection table is not running, discarding message to " + dest);
           return;
       }

       if(dest.equals(local_addr)) {
           receive(local_addr, data, offset, length);
           return;
       }

       // 1. Try to obtain correct Connection (or create one if not yet existent)
       try {
           conn=getConnection(dest);
           if(conn == null) return;
       }
       catch(Throwable ex) {
           throw new Exception("connection to " + dest + " could not be established", ex);
       }

       // 2. Send the message using that connection
       try {
           conn.send(data, offset, length);
       }
       catch(Throwable ex) {
           log.trace("sending msg to " + dest + " failed (" + ex.getClass().getName() + "); removing from connection table", ex);
           removeConnection(dest);
       }
   }

   abstract Connection getConnection(Address dest) throws Exception;

      /**
       * Removes all connections from ConnectionTable which are not in current_mbrs
       * @param current_mbrs
       */
      public void retainAll(Collection<Address> current_mbrs) {
          if(current_mbrs == null) return;
          HashMap<Address,Connection> copy;
          synchronized(conns) {
              copy=new HashMap<>(conns);
              conns.keySet().retainAll(current_mbrs);
          }
          copy.keySet().removeAll(current_mbrs);
                    
          //destroy orphaned connection i.e. connections
          //to members that are not in current view
          for(Connection orphanConnection:copy.values()){                         
              if (log.isTraceEnabled())
                log.trace("At " + local_addr + " destroying orphan to "
                        + orphanConnection.getPeerAddress());
              orphanConnection.destroy();             
          }     
          copy.clear();
      }



    /** Used for message reception. */
    public interface Receiver {
       void receive(Address sender, byte[] data, int offset, int length);
   }

   /** Used to be notified about connection establishment and teardown. */
   public interface ConnectionListener {
       void connectionOpened(Address peer_addr);
       void connectionClosed(Address peer_addr);
   }

   class Connection implements Runnable {
       Socket           sock=null;                // socket to/from peer (result of srv_sock.accept() or new Socket())
       String           sock_addr=null;           // used for Thread.getName()
       DataOutputStream out=null;                 // for sending messages
       DataInputStream  in=null;                  // for receiving messages
       Thread           receiverThread=null;      // thread for receiving messages
       Address          peer_addr=null;           // address of the 'other end' of the connection
       final Lock       send_lock=new ReentrantLock();  // serialize send()
       long             last_access=System.currentTimeMillis(); // last time a message was sent or received

       /** Bounded queue of data to be sent to the peer of this connection */
       BlockingQueue<byte[]> send_queue=null;
       Sender                sender=null;
       boolean               is_running=false;


       private String getSockAddress() {
           if(sock_addr != null)
               return sock_addr;
           if(sock != null) {
               StringBuilder sb;
               sb=new StringBuilder();
               sb.append(sock.getLocalAddress().getHostAddress()).append(':').append(sock.getLocalPort());
               sb.append(" - ").append(sock.getInetAddress().getHostAddress()).append(':').append(sock.getPort());
               sock_addr=sb.toString();
           }
           return sock_addr;
       }




       Connection(Socket s, Address peer_addr) {
           sock=s;
           this.peer_addr=peer_addr;

           if(use_send_queues) {
               send_queue=new LinkedBlockingQueue<>(send_queue_size);
               sender=new Sender();
           }

           try {
               // out=new DataOutputStream(sock.getOutputStream());
               // in=new DataInputStream(sock.getInputStream());

               // The change to buffered input and output stream yielded a 400% performance gain !
               // bela Sept 7 2006
               out=new DataOutputStream(new BufferedOutputStream(sock.getOutputStream()));
               in=new DataInputStream(new BufferedInputStream(sock.getInputStream()));
               if(sender != null)
                   sender.start();
               conn_creations.incrementAndGet();
           }
           catch(Exception ex) {
               if(log.isErrorEnabled()) log.error(Util.getMessage("ExceptionIs") + ex);
           }
       }


       boolean established() {
           return receiverThread != null;
       }


       void setPeerAddress(Address peer_addr) {
           this.peer_addr=peer_addr;
       }

       Address getPeerAddress() {return peer_addr;}

       void updateLastAccessed() {
           last_access=System.currentTimeMillis();
       }

       void init() {
           is_running=true;
           if(receiverThread == null || !receiverThread.isAlive()) {
               // Roland Kurmann 4/7/2003, put in thread_group - removed bela Nov 2012
               receiverThread=getThreadFactory().newThread(this, "ConnectionTable.Connection.Receiver [" + getSockAddress() + "]");
               receiverThread.start();
               if(log.isTraceEnabled())
                   log.trace("receiver started: " + receiverThread);
           }

       }
       
       /**
         * Returns true if underlying socket to peer is closed
         *  
         * @return
         */
        boolean isSocketClosed() {
            return !(sock != null && sock.isConnected());
        }


       void destroy() {
       	   if(log.isTraceEnabled()) log.trace("destroyed " + this);
           is_running=false;
           closeSocket(); // should terminate handler as well
           if(sender != null)
               sender.stop();
           Thread tmp=receiverThread;
           receiverThread=null;
           if(tmp != null) {
               Util.interruptAndWaitToDie(tmp);
           }

           conn_creations.decrementAndGet();
       }


       /**
        *
        * @param data Guaranteed to be non null
        * @param offset
        * @param length
        */
       void send(byte[] data, int offset, int length) {
           if(!is_running) {
               if(log.isWarnEnabled())
                   log.warn("Connection is not running, discarding message");
               return;
           }
           if(use_send_queues) {
               try {
                   // we need to copy the byte[] buffer here because the original buffer might get changed meanwhile
                   byte[] tmp=new byte[length];
                   System.arraycopy(data, offset, tmp, 0, length);
                   send_queue.put(tmp);
               }
               catch(InterruptedException e) {
                   Thread.currentThread().interrupt();
               }
           }
           else
               _send(data, offset, length, true);
       }


       /**
        * Sends data using the 'out' output stream of the socket
        * @param data
        * @param offset
        * @param length
        * @param acquire_lock
        */
       private void _send(byte[] data, int offset, int length, boolean acquire_lock) {
           if(acquire_lock)
               send_lock.lock();

           try {
               doSend(data, offset, length);
               updateLastAccessed();
           }
           catch(InterruptedException iex) {
               Thread.currentThread().interrupt(); // set interrupt flag again
           }
           catch(Throwable ex) {
               if(log.isErrorEnabled()) log.error(Util.getMessage("FailedSendingDataTo") + peer_addr + ": " + ex);
           }
           finally {
               if(acquire_lock)
                   send_lock.unlock();
           }
       }


       void doSend(byte[] data, int offset, int length) throws Exception {
           try {
               if(out != null) {
                   out.writeInt(length); // write the length of the data buffer first
                   out.write(data, offset, length);
                   out.flush();  // may not be very efficient (but safe)
               }
           }
           catch(Exception ex) {
               removeConnection(peer_addr);
               throw ex;
           }
       }


       /**
        * Reads the peer's address. First a cookie has to be sent which has to match my own cookie, otherwise
        * the connection will be refused
        */
       Address readPeerAddress(Socket client_sock) throws Exception {
           Address     client_peer_addr=null;
           byte[]      input_cookie=new byte[cookie.length];
           int         client_port=client_sock != null? client_sock.getPort() : 0;
           short       version;
           InetAddress client_addr=client_sock != null? client_sock.getInetAddress() : null;

           int timeout=client_sock.getSoTimeout();
           client_sock.setSoTimeout(peer_addr_read_timeout);

           try {

               if(in != null) {
                   initCookie(input_cookie);

                   // read the cookie first
                   in.readFully(input_cookie, 0, input_cookie.length);
                   if(!matchCookie(input_cookie))
                       throw new SocketException("ConnectionTable.Connection.readPeerAddress(): cookie sent by " +
                               client_peer_addr + " does not match own cookie; terminating connection");
                   // then read the version
                   version=in.readShort();

                   if(Version.isBinaryCompatible(version) == false) {
                       if(log.isWarnEnabled())
                           log.warn(new StringBuilder("packet from ").append(client_addr).append(':').append(client_port).
                                   append(" has different version (").append(Version.print(version)).append(") from ours (").
                                   append(Version.printVersion()).append("). This may cause problems").toString());
                   }
                   client_peer_addr=new IpAddress();
                   client_peer_addr.readFrom(in);

                   updateLastAccessed();
               }
               return client_peer_addr;
           }
           finally {
               client_sock.setSoTimeout(timeout);
           }
       }


       /**
        * Send the cookie first, then the our port number. If the cookie doesn't match the receiver's cookie,
        * the receiver will reject the connection and close it.
        */
       void sendLocalAddress(Address local_addr) {
           if(local_addr == null) {
               if(log.isWarnEnabled()) log.warn("local_addr is null");
               return;
           }
           if(out != null) {
               try {
                   // write the cookie
                   out.write(cookie, 0, cookie.length);

                   // write the version
                   out.writeShort(Version.version);
                   local_addr.writeTo(out);
                   out.flush(); // needed ?
                   updateLastAccessed();
               }
               catch(Throwable t) {
                   if(log.isErrorEnabled()) log.error(Util.getMessage("ExceptionIs") + t);
               }
           }
       }


       void initCookie(byte[] c) {
           if(c != null)
               for(int i=0; i < c.length; i++)
                   c[i]=0;
       }

       boolean matchCookie(byte[] input) {
           if(input == null || input.length < cookie.length) return false;
           for(int i=0; i < cookie.length; i++)
               if(cookie[i] != input[i]) return false;
           return true;
       }


       String printCookie(byte[] c) {
           if(c == null) return "";
           return new String(c);
       }


       public void run() {
           while(receiverThread != null && receiverThread.equals(Thread.currentThread()) && is_running) {
               try {
                   if(in == null) {
                       if(log.isErrorEnabled()) log.error(Util.getMessage("InputStreamIsNull"));
                       break;
                   }
                   int len=in.readInt();
                   byte[] buf=new byte[len];
                   in.readFully(buf, 0, len);
                   updateLastAccessed();
                   receive(peer_addr, buf, 0, len); // calls receiver.receive(msg)
               }
               catch(OutOfMemoryError mem_ex) {
                   if(log.isWarnEnabled()) log.warn("dropped invalid message, closing connection");
                   break; // continue;
               }               
               catch(IOException io_ex) {
                   //this is very common occurrence, hence log under trace level
                   if(log.isTraceEnabled()) log.trace("Exception while read blocked for data from peer ", io_ex);
                   notifyConnectionClosed(peer_addr);
                   break;
               }
               catch(Throwable e) {
                   if(log.isWarnEnabled()) log.warn("Problem encountered while receiving message from peer " + peer_addr, e);
               }
           }
           if(log.isTraceEnabled())
               log.trace("ConnectionTable.Connection.Receiver terminated");
           receiverThread=null;
           closeSocket();
           // remove(peer_addr);
       }


       public String toString() {
           StringBuilder ret=new StringBuilder();
           InetAddress local=null, remote=null;
           String local_str, remote_str;

           Socket tmp_sock=sock;
           if(tmp_sock == null)
               ret.append("<null socket>");
           else {
               //since the sock variable gets set to null we want to make
               //make sure we make it through here without a nullpointer exception               
               local=tmp_sock.getLocalAddress();
               remote=tmp_sock.getInetAddress();
               local_str=local != null ? Util.shortName(local) : "<null>";
               remote_str=remote != null ? Util.shortName(remote) : "<null>";
               ret.append('<' + local_str + ':' + tmp_sock.getLocalPort() +
                          " --> " + remote_str + ':' + tmp_sock.getPort() + "> (" +
                          ((System.currentTimeMillis() - last_access) / 1000) + " secs old)");               
           }
           tmp_sock=null;

           return ret.toString();
       }


       void closeSocket() {
           Util.close(sock); // should actually close in/out (so we don't need to close them explicitly)
           sock=null;
           Util.close(out);  // flushes data
           // removed 4/22/2003 (request by Roland Kurmann)
           // out=null;
           Util.close(in);
       }


       class Sender implements Runnable {
           AtomicReference<Thread> senderThread = new AtomicReference<>();
           private final AtomicBoolean is_it_running=new AtomicBoolean();

           void start() {
               Thread localThread=senderThread.getAndSet(getThreadFactory()
                                                           .newThread(this,
                                                                      "ConnectionTable.Connection.Sender local_addr=" + local_addr + " [" + getSockAddress() + "]"));
               if(localThread == null ) {
                   is_it_running.set(true);
                   senderThread.get().setDaemon(true);
                   senderThread.get().start();
                   if(log.isTraceEnabled()) log.trace("sender thread started: " + senderThread);
               }
           }



           void stop() {
               is_it_running.set(false);
               if(send_queue != null)
                   send_queue.clear();
               Thread localThread = senderThread.getAndSet(null);
               if(localThread != null) {
                   Util.interruptAndWaitToDie(localThread);
               }
           }

           boolean isRunning() {
               return is_it_running.get() && senderThread.get() != null;
           }

           public void run() {
               byte[] data;
               while(senderThread.get() != null && senderThread.get().equals(Thread.currentThread()) && is_it_running.get()) {
                   try {
                       data=send_queue.take();
                       if(data == null)
                           continue;
                       // we don't need to serialize access to 'out' as we're the only thread sending messages
                       _send(data, 0, data.length, false);
                   }
                   catch(InterruptedException e) {
                       ;
                   }
               }
               is_it_running.set(false);
               if(log.isTraceEnabled())
                   log.trace("ConnectionTable.Connection.Sender thread terminated");
           }
       }


   }

   class Reaper implements Runnable {
       Thread t=null;

       Reaper() {
           ;
       }

       // return true if we have zero connections
       private boolean haveZeroConnections() {
           synchronized(conns) {
               return conns.isEmpty();
           }
       }

       public void start() {

           if(haveZeroConnections())
               return;
           if(t != null && !t.isAlive())
               t=null;
           if(t == null) {
               // RKU 7.4.2003, put in threadgroup -- removed bela Nov 2012
               t=getThreadFactory().newThread(this, "ConnectionTable.ReaperThread");
               t.setDaemon(true); // will allow us to terminate if all remaining threads are daemons
               t.start();
           }
       }

       public void stop() {
           Thread tmp=t;
           if(t != null)
               t=null;
           if(tmp != null) {
               Util.interruptAndWaitToDie(tmp);
           }
       }


       public boolean isRunning() {
           return t != null;
       }

       public void run() {
           Connection connection;
           Entry<Address,Connection> entry;
           long curr_time;

           if(log.isDebugEnabled()) log.debug("connection reaper thread was started. Number of connections=" +
                   conns.size() + ", reaper_interval=" + reaper_interval + ", conn_expire_time=" +
                   conn_expire_time);

           while(!haveZeroConnections() && t != null && t.equals(Thread.currentThread())) {
               Util.sleep(reaper_interval);
               if(t == null || !Thread.currentThread().equals(t))
                   break;
               synchronized(conns) {
                   curr_time=System.currentTimeMillis();
                   for(Iterator<Entry<Address,Connection>> it=conns.entrySet().iterator(); it.hasNext();) {
                       entry=it.next();
                       connection=entry.getValue();
                       if(log.isTraceEnabled()) log.trace("connection is " +
                                                        ((curr_time - connection.last_access) / 1000) + " seconds old (curr-time=" +
                                                        curr_time + ", last_access=" + connection.last_access + ')');
                       if(connection.last_access + conn_expire_time < curr_time) {
                           if(log.isTraceEnabled()) log.trace("connection " + connection +
                                                            " has been idle for too long (conn_expire_time=" + conn_expire_time +
                                                            "), will be removed");
                           connection.destroy();
                           it.remove();
                       }
                   }
               }
           }
           if(log.isDebugEnabled()) log.debug("reaper terminated");
           t=null;
       }
   }
}
