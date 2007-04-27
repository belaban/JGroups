package org.jgroups.protocols.pbcast;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.Event;
import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.Message;
import org.jgroups.TimeoutException;
import org.jgroups.View;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.StateTransferInfo;
import org.jgroups.util.Promise;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import EDU.oswego.cs.dl.util.concurrent.PooledExecutor;
import EDU.oswego.cs.dl.util.concurrent.ThreadFactory;

/**
 * <code>STREAMING_STATE_TRANSFER</code>, as its name implies, allows a streaming 
 * state transfer between two channel instances. 
 * 
 * <p>
 * 
 * Major advantage of this approach is that transfering application state to a 
 * joining member of a group does not entail loading of the complete application 
 * state into memory. Application state, for example, might be located entirely 
 * on some form of disk based storage. The default <code>STATE_TRANSFER</code> 
 * requires this state to be loaded entirely into memory before being transferred 
 * to a group member while <code>STREAMING_STATE_TRANSFER</code> does not. 
 * Thus <code>STREAMING_STATE_TRANSFER</code> protocol is able to transfer 
 * application state that is very large (>1Gb) without a likelihood of such transfer 
 * resulting in OutOfMemoryException.
 * 
 * <p>
 * 
 * Channel instance can be configured with either <code>STREAMING_STATE_TRANSFER</code> 
 * or <code>STATE_TRANSFER</code> but not both protocols at the same time. 
 * 
 * <p>
 * 
 * In order to process streaming state transfer an application has to implement 
 * <code>ExtendedMessageListener</code> if it is using channel in a push style 
 * mode or it has to process <code>StreamingSetStateEvent</code> and 
 * <code>StreamingGetStateEvent</code> if it is using channel in a pull style mode.    
 * 
 * 
 * @author Vladimir Blagojevic
 * @see org.jgroups.ExtendedMessageListener
 * @see org.jgroups.StreamingGetStateEvent
 * @see org.jgroups.StreamingSetStateEvent
 * @see org.jgroups.protocols.pbcast.STATE_TRANSFER
 * @since 2.4
 * 
 * @version $Id$
 * 
 */
public class STREAMING_STATE_TRANSFER extends Protocol
{
   private Address local_addr = null;

   private final Vector members = new Vector();

   private final Map state_requesters = new HashMap();   

   private Digest digest = null;

   private final HashMap map = new HashMap(); // to store configuration information

   private int num_state_reqs = 0;

   private long num_bytes_sent = 0;

   private double avg_state_size = 0;

   private final static String NAME = "STREAMING_STATE_TRANSFER";

   private InetAddress bind_addr;

   private int bind_port = 0;

   private StateProviderThreadSpawner spawner;

   private int max_pool = 5;

   private long pool_thread_keep_alive;

   private int socket_buffer_size = 8 * 1024;

   private boolean use_reading_thread;

   private final Promise flush_promise = new Promise();;

   private volatile boolean use_flush;

   private long flush_timeout = 4000;

   private final Object poolLock = new Object();

   private int threadCounter;

   private volatile boolean flushProtocolInStack = false;   

   public final String getName()
   {
      return NAME;
   }

   public int getNumberOfStateRequests()
   {
      return num_state_reqs;
   }

   public long getNumberOfStateBytesSent()
   {
      return num_bytes_sent;
   }

   public double getAverageStateSize()
   {
      return avg_state_size;
   }

   public Vector requiredDownServices()
   {
      Vector retval = new Vector();
      retval.addElement(new Integer(Event.GET_DIGEST_STATE));
      retval.addElement(new Integer(Event.SET_DIGEST));
      return retval;
   }

   public void resetStats()
   {
      super.resetStats();
      num_state_reqs = 0;
      num_bytes_sent = 0;
      avg_state_size = 0;
   }

   public boolean setProperties(Properties props)
   {
      super.setProperties(props);
      use_flush = Util.parseBoolean(props, "use_flush", false);            
      flush_timeout = Util.parseLong(props, "flush_timeout", flush_timeout);
      
      try
      {
         bind_addr = Util.parseBindAddress(props, "bind_addr");
      }
      catch (UnknownHostException e)
      {
         log.error("(bind_addr): host " + e.getLocalizedMessage() + " not known");
         return false;
      }
      bind_port = Util.parseInt(props, "start_port", 0);
      socket_buffer_size = Util.parseInt(props, "socket_buffer_size", 8 * 1024); //8K
      max_pool = Util.parseInt(props, "max_pool", 5);
      pool_thread_keep_alive = Util.parseLong(props, "pool_thread_keep_alive", 1000 * 30); //30 sec
      use_reading_thread = Util.parseBoolean(props, "use_reading_thread", false);
      if (props.size() > 0)
      {
         log.error("the following properties are not recognized: " + props);

         return false;
      }
      return true;
   }

   public void init() throws Exception
   {
      map.put("state_transfer", Boolean.TRUE);
      map.put("protocol_class", getClass().getName());
   }

   public void start() throws Exception
   {
      passUp(new Event(Event.CONFIG, map));
      if(!flushProtocolInStack && use_flush)
      {
         log.warn("use_flush is true, however, FLUSH protocol not found in stack.");
         use_flush = false;
      }
   }

   public void stop()
   {
      super.stop();     
      if (spawner != null)
      {
         spawner.stop();
      }
   }

   public void up(Event evt)
   {
      switch (evt.getType())
      {
         case Event.BECOME_SERVER :
            break;

         case Event.SET_LOCAL_ADDRESS :
            local_addr = (Address) evt.getArg();
            break;

         case Event.TMP_VIEW :
         case Event.VIEW_CHANGE :
            handleViewChange((View) evt.getArg());
            break;

         case Event.GET_DIGEST_STATE_OK :
            synchronized (state_requesters)
            {
               digest = (Digest) evt.getArg();
               if (log.isDebugEnabled())
                  log.debug("GET_DIGEST_STATE_OK: digest is " + digest);
            }
            respondToStateRequester();
            return;

         case Event.MSG :
            Message msg = (Message) evt.getArg();
            StateHeader hdr = (StateHeader) msg.removeHeader(getName());
            if (hdr != null)
            {
               switch (hdr.type)
               {
                  case StateHeader.STATE_REQ :
                     handleStateReq(hdr);
                     break;
                  case StateHeader.STATE_RSP :
                     handleStateRsp(hdr);
                     break;
                  case StateHeader.STATE_REMOVE_REQUESTER :
                     removeFromStateRequesters(hdr.sender, hdr.state_id);
                     break;
                  default :
                     if (log.isErrorEnabled())
                        log.error("type " + hdr.type + " not known in StateHeader");
                     break;
               }
               return;
            }
            break;
         case Event.CONFIG :
            Map config = (Map) evt.getArg();
            if (bind_addr == null && (config != null && config.containsKey("bind_addr")))
            {               
               bind_addr = (InetAddress) config.get("bind_addr");
               if (log.isDebugEnabled())
                  log.debug("using bind_addr from CONFIG event " + bind_addr);
            }    
            break;
      }
      passUp(evt);
   }

   public void down(Event evt)
   {
      Address target;
      StateTransferInfo info;

      switch (evt.getType())
      {

         case Event.TMP_VIEW :
         case Event.VIEW_CHANGE :
            handleViewChange((View) evt.getArg());
            break;

         case Event.GET_STATE :
            info = (StateTransferInfo) evt.getArg();
            if (info.target == null)
            {
               target = determineCoordinator();
            }
            else
            {
               target = info.target;
               if (target.equals(local_addr))
               {
                  if (log.isErrorEnabled())
                     log.error("GET_STATE: cannot fetch state from myself !");
                  target = null;
               }
            }
            if (target == null)
            {
               if (log.isDebugEnabled())
                  log.debug("GET_STATE: first member (no state)");
               passUp(new Event(Event.GET_STATE_OK, new StateTransferInfo()));
            }
            else
            {
               boolean successfulFlush = false;
               if(use_flush) {
                  successfulFlush = startFlush(flush_timeout, 5);
               }
               if (successfulFlush)
               {
                  log.debug("Successful flush at " + local_addr);
               }
               Message state_req = new Message(target, null, null);
               state_req.putHeader(NAME, new StateHeader(StateHeader.STATE_REQ, local_addr, info.state_id));
               String stateRequested = info.state_id==null?"full":info.state_id;
               if (log.isDebugEnabled())
                  log.debug("Member " + local_addr + " asking " + target + " for " + stateRequested + " state");

               // suspend sending and handling of mesage garbage collection gossip messages,
               // fixes bugs #943480 and #938584). Wake up when state has been received
               if (log.isTraceEnabled())
                  log.trace("passing down a SUSPEND_STABLE event");
               passDown(new Event(Event.SUSPEND_STABLE, new Long(info.timeout)));                
               passDown(new Event(Event.MSG, state_req));
            }
            return; // don't pass down any further !

         case Event.STATE_TRANSFER_INPUTSTREAM_CLOSED :
            if (use_flush)
            {
               stopFlush();
            }

            if (log.isTraceEnabled())
               log.trace("STATE_TRANSFER_INPUTSTREAM_CLOSED received");
            //resume sending and handling of message garbage collection gossip messages,
            // fixes bugs #943480 and #938584). Wakes up a previously suspended message garbage
            // collection protocol (e.g. STABLE)
            if (log.isTraceEnabled())
               log.trace("passing down a RESUME_STABLE event");
            passDown(new Event(Event.RESUME_STABLE));
            return;
         case Event.SUSPEND_OK :
            if (use_flush)
            {
               flush_promise.setResult(Boolean.TRUE);
            }
            break;
         case Event.SUSPEND_FAILED :
            if (use_flush)
            {                  
               flush_promise.setResult(Boolean.FALSE);
            }
            break;      
         case Event.CONFIG :
            Map config = (Map) evt.getArg();           
            if(config != null && config.containsKey("flush_timeout"))
            {
               Long ftimeout = (Long) config.get("flush_timeout");
               use_flush = true;             
               flush_timeout = ftimeout.longValue();                             
            }
            if((config != null && !config.containsKey("flush_suported")))
            {                             
               flushProtocolInStack = true;                              
            }
            break;   
            
      }

      passDown(evt); // pass on to the layer below us
   }

   /* --------------------------- Private Methods -------------------------------- */

   /**
    * When FLUSH is used we do not need to pass digests between members
    *
    * see JGroups/doc/design/PArtialStateTransfer.txt
    * see JGroups/doc/design/FLUSH.txt
    *
    * @return true if use of digests is required, false otherwise
    */
   private boolean isDigestNeeded()
   {
      return !use_flush;
   }

   private void respondToStateRequester()
   {

      // setup the plumbing if needed
      if (spawner == null)
      {
         ServerSocket serverSocket = Util.createServerSocket(bind_addr, bind_port);
         spawner = new StateProviderThreadSpawner(setupThreadPool(), serverSocket);
         new Thread(Util.getGlobalThreadGroup(), spawner, "StateProviderThreadSpawner").start();
      }

      synchronized (state_requesters)
      {
         if (state_requesters.isEmpty())
         {
            if(log.isWarnEnabled())
               log.warn("Should be responding to state requester, but there are no requesters !");
            return;
         }

         if (digest == null && isDigestNeeded())
         {
            if(log.isWarnEnabled())
            {
               log.warn("Should be responding to state requester, but there is no digest !");
            }
         }
         else if (digest != null && isDigestNeeded())
         {
            digest = digest.copy();
         }

         if (log.isTraceEnabled())
            log.trace("Iterating state requesters " + state_requesters);

         for (Iterator it = state_requesters.keySet().iterator(); it.hasNext();)
         {
            String tmp_state_id = (String) it.next();
            Set requesters = (Set) state_requesters.get(tmp_state_id);
            for (Iterator iter = requesters.iterator(); iter.hasNext();)
            {
               Address requester = (Address) iter.next();
               Message state_rsp = new Message(requester);
               StateHeader hdr = new StateHeader(StateHeader.STATE_RSP, local_addr, spawner.getServerSocketAddress(),
                                     isDigestNeeded()?digest:null, tmp_state_id);
               state_rsp.putHeader(NAME, hdr);

               if (log.isTraceEnabled())
                  log.trace("Responding to state requester " + requester + " with address "
                        + spawner.getServerSocketAddress() + " and digest " + digest);
               passDown(new Event(Event.MSG, state_rsp));
               if (stats)
               {
                  num_state_reqs++;
               }
            }
         }
      }
   }

   private boolean startFlush(long timeout, int numberOfAttempts)
   {
      boolean successfulFlush = false;
      flush_promise.reset();
      passUp(new Event(Event.SUSPEND));
      try
      {         
         Boolean r = (Boolean) flush_promise.getResultWithTimeout(timeout);
         successfulFlush = r.booleanValue();
      }
      catch (TimeoutException e)
      {
         log.warn("Initiator of flush and state requesting member " + local_addr
               + " timed out waiting for flush responses after " + flush_timeout + " msec");
      }

      if (!successfulFlush && numberOfAttempts > 0)
      {
         long backOffSleepTime = Util.random(5000);
         if(log.isInfoEnabled())               
            log.info("Flush in progress detected at " + local_addr + ". Backing off for "
                  + backOffSleepTime + " ms. Attempts left " + numberOfAttempts);
         
         Util.sleepRandom(backOffSleepTime);      
         successfulFlush = startFlush(flush_timeout, --numberOfAttempts);
      }     
      return successfulFlush;
   }

   private void stopFlush()
   {
      passUp(new Event(Event.RESUME));
   }

   private PooledExecutor setupThreadPool()
   {
      PooledExecutor threadPool = new PooledExecutor(max_pool);
      threadPool.waitWhenBlocked();
      threadPool.setMinimumPoolSize(1);
      threadPool.setKeepAliveTime(pool_thread_keep_alive);
      threadPool.setThreadFactory(new ThreadFactory()
      {

         public Thread newThread(final Runnable command)
         {
            synchronized (poolLock)
            {
               threadCounter++;
            }
            return new Thread(Util.getGlobalThreadGroup(), "STREAMING_STATE_TRANSFER.poolid=" + threadCounter)
            {
               public void run()
               {
                  if (log.isTraceEnabled())
                  {
                     log.trace(Thread.currentThread() + " started.");
                  }
                  command.run();
                  if (log.isTraceEnabled())
                  {
                     log.trace(Thread.currentThread() + " stopped.");
                  }
               }
            };
         }

      });
      return threadPool;
   }

   private Address determineCoordinator()
   {
      Address ret = null;
      synchronized (members)
      {
         if (!members.isEmpty())
         {
            for (int i = 0; i < members.size(); i++)
               if (!local_addr.equals(members.elementAt(i)))
                  return (Address) members.elementAt(i);
         }
      }
      return ret;
   }

   private void handleViewChange(View v)
   {                 
      synchronized (members)
      {         
         members.clear();
         members.addAll(v.getMembers());      
      }
   }

   private void handleStateReq(StateHeader hdr)
   {
      Object sender = hdr.sender;
      if (sender == null)
      {
         if (log.isErrorEnabled())
            log.error("sender is null !");
         return;
      }
      String id = hdr.state_id;
      synchronized (state_requesters)
      {
         boolean empty = state_requesters.isEmpty();
         Set requesters = (Set) state_requesters.get(id);
         if (requesters == null)
         {
            requesters = new HashSet();
         }
         requesters.add(sender);
         state_requesters.put(id, requesters);

         if (!isDigestNeeded())
         {
            respondToStateRequester();
         }
         else if (empty)
         {
            digest = null;
            if (log.isTraceEnabled())
               log.trace("passing down GET_DIGEST_STATE");
            passDown(new Event(Event.GET_DIGEST_STATE));
         }
      }
   }

   void handleStateRsp(StateHeader hdr)
   {
      Digest tmp_digest = hdr.my_digest;      
      if (isDigestNeeded())
      {
         if (tmp_digest == null)
         {
            if(log.isWarnEnabled())
               log.warn("digest received from " + hdr.sender + " is null, skipping setting digest !");
         }
         else
         {
            // set the digest (e.g.in NAKACK)
            passDown(new Event(Event.SET_DIGEST, tmp_digest));
         }
      }      
      connectToStateProvider(hdr);
   }

   void removeFromStateRequesters(Address address, String state_id)
   {
      synchronized (state_requesters)
      {
         Set requesters = (Set) state_requesters.get(state_id);
         if (requesters != null && !requesters.isEmpty())
         {
            boolean removed = requesters.remove(address);
            if (log.isTraceEnabled())
            {
               log.trace("Attempted to clear " + address + " from requesters, successful=" + removed);
            }
            if (requesters.isEmpty())
            {
               state_requesters.remove(state_id);
               if (log.isTraceEnabled())
               {
                  log.trace("Cleared all requesters for state " + state_id + ",state_requesters=" + state_requesters);
               }
            }
         }        
      }
   }

   private void connectToStateProvider(StateHeader hdr)
   {      
      IpAddress address = hdr.bind_addr;
      String tmp_state_id = hdr.getStateId();
      StreamingInputStreamWrapper wrapper = null;
      StateTransferInfo sti = null;
      final Socket socket = new Socket();
      try
      {
         socket.bind(new InetSocketAddress(bind_addr, 0));
         int bufferSize = socket.getReceiveBufferSize();
         socket.setReceiveBufferSize(socket_buffer_size);
         if (log.isDebugEnabled())
            log.debug("Connecting to state provider " + address.getIpAddress() + ":" + address.getPort()
                  + ", original buffer size was " + bufferSize + " and was reset to " + socket.getReceiveBufferSize());
         socket.connect(new InetSocketAddress(address.getIpAddress(), address.getPort()));
         if (log.isDebugEnabled())
            log.debug("Connected to state provider, my end of the socket is " + socket.getLocalAddress() + ":"
                  + socket.getLocalPort() + " passing inputstream up...");

         //write out our state_id and address so state provider can clear this request
         ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
         out.writeObject(tmp_state_id);
         out.writeObject(local_addr);

         wrapper = new StreamingInputStreamWrapper(socket);
         sti = new StateTransferInfo(hdr.sender, wrapper, tmp_state_id);                  
      }
      catch (IOException e)
      {
         if(log.isWarnEnabled())
         {
            log.warn("State reader socket thread spawned abnormaly", e);
         }
         
         //pass null stream up so that JChannel.getState() returns false 
         InputStream is = null;
         sti = new StateTransferInfo(hdr.sender, is, tmp_state_id);         
      }
      finally
      {
         if (!socket.isConnected())
         {
            if(log.isWarnEnabled())
               log.warn("Could not connect to state provider. Closing socket...");
            try
            {
               if (wrapper != null)
               {
                  wrapper.close();
               }
               else
               {
                  socket.close();
               }

            }
            catch (IOException e)
            {
            }
            //since socket did not connect properly we have to
            //clear our entry in state providers hashmap "manually"
            Message m = new Message(hdr.sender);
            StateHeader mhdr = new StateHeader(StateHeader.STATE_REMOVE_REQUESTER, local_addr, tmp_state_id);
            m.putHeader(NAME, mhdr);
            passDown(new Event(Event.MSG, m));
         }
         passStreamUp(sti);
      }
   }

   private void passStreamUp(final StateTransferInfo sti)
   {
      Runnable readingThread = new Runnable()
      {
         public void run()
         {
            passUp(new Event(Event.STATE_TRANSFER_INPUTSTREAM, sti));
         }
      };
      if (use_reading_thread)
      {
         new Thread(Util.getGlobalThreadGroup(), readingThread, "STREAMING_STATE_TRANSFER.reader").start();

      }
      else
      {
         readingThread.run();
      }
   }

   /*
    * ------------------------ End of Private Methods
    * ------------------------------
    */

   private class StateProviderThreadSpawner implements Runnable
   {
      PooledExecutor pool;

      ServerSocket serverSocket;

      IpAddress address;
      
      Thread runner;

      volatile boolean running = true;

      public StateProviderThreadSpawner(PooledExecutor pool, ServerSocket stateServingSocket)
      {
         if(pool == null || stateServingSocket == null)
         {
            throw new IllegalArgumentException("Cannot create thread pool");
         }
         this.pool = pool;
         this.serverSocket = stateServingSocket;
         this.address = new IpAddress(STREAMING_STATE_TRANSFER.this.bind_addr, serverSocket.getLocalPort());
      }

      public void run()
      {
         runner = Thread.currentThread();
         for (; running;)
         {
            try
            {
               if (log.isDebugEnabled())
                  log.debug("StateProviderThreadSpawner listening at " + getServerSocketAddress() + "...");
               if (log.isTraceEnabled())
                  log.trace("Pool has " + pool.getPoolSize() + " active threads");
               final Socket socket = serverSocket.accept();
               pool.execute(new Runnable()
               {
                  public void run()
                  {
                     if (log.isDebugEnabled())
                        log.debug("Accepted request for state transfer from " + socket.getInetAddress() + ":"
                              + socket.getPort() + " handing of to PooledExecutor thread");
                     new StateProviderHandler().process(socket);
                  }
               });

            }
            catch (IOException e)
            {
               if(log.isWarnEnabled())
               {
                  //we get this exception when we close server socket
                  //exclude that case
                  if (!serverSocket.isClosed())
                  {
                     log.warn("Spawning socket from server socket finished abnormaly", e);
                  }
               }
            }
            catch (InterruptedException e)
            {
               // should not happen
            }
         }
      }

      public IpAddress getServerSocketAddress()
      {
         return address;
      }

      public void stop()
      {
         running = false;
         try
         {
            if (!serverSocket.isClosed())
            {
               serverSocket.close();
            }
         }
         catch (IOException e)
         {
         }
         finally
         {
            if (log.isDebugEnabled())
               log.debug("Waiting for StateProviderThreadSpawner to die ... ");

            try
            {
               runner.join(3000);
            }
            catch (InterruptedException ignored)
            {
            }
           
            if (log.isDebugEnabled())
               log.debug("Shutting the thread pool down... ");

            pool.shutdownNow();
            try
            {
               //TODO use some system wide timeout eventually
               pool.awaitTerminationAfterShutdown(5000);
            }
            catch (InterruptedException ignored)
            {
            }
         }
         if (log.isDebugEnabled())
            log.debug("Thread pool is shutdown. All pool threads are cleaned up.");
      }
   }

   private class StateProviderHandler
   {
      public void process(Socket socket)
      {
         StreamingOutputStreamWrapper wrapper = null;
         ObjectInputStream ois = null;
         try
         {
            int bufferSize = socket.getSendBufferSize();
            socket.setSendBufferSize(socket_buffer_size);
            if (log.isDebugEnabled())
               log.debug("Running on " + Thread.currentThread() + ". Accepted request for state transfer from "
                     + socket.getInetAddress() + ":" + socket.getPort() + ", original buffer size was " + bufferSize
                     + " and was reset to " + socket.getSendBufferSize() + ", passing outputstream up... ");

            //read out state requesters state_id and address and clear this request
            ois = new ObjectInputStream(socket.getInputStream());
            String state_id = (String) ois.readObject();
            Address stateRequester = (Address) ois.readObject();
            removeFromStateRequesters(stateRequester, state_id);

            wrapper = new StreamingOutputStreamWrapper(socket);
            StateTransferInfo sti = new StateTransferInfo(stateRequester, wrapper, state_id);
            passUp(new Event(Event.STATE_TRANSFER_OUTPUTSTREAM, sti));
         }
         catch (IOException e)
         {
            if(log.isWarnEnabled())
            {
               log.warn("State writer socket thread spawned abnormaly", e);
            }
         }
         catch (ClassNotFoundException e)
         {
            //thrown by ois.readObject()
            //should never happen since String/Address are core classes
         }
         finally
         {
            if (socket != null && !socket.isConnected())
            {
               if(log.isWarnEnabled())
                  log.warn("Accepted request for state transfer but socket " + socket
                        + " not connected properly. Closing it...");
               try
               {
                  if (wrapper != null)
                  {
                     wrapper.close();
                  }
                  else
                  {
                     socket.close();
                  }
               }
               catch (IOException e)
               {
               }
            }
         }
      }
   }

   private class StreamingInputStreamWrapper extends InputStream
   {

      private Socket inputStreamOwner;

      private InputStream delegate;

      private Channel channelOwner;

      public StreamingInputStreamWrapper(Socket inputStreamOwner) throws IOException
      {
         super();
         this.inputStreamOwner = inputStreamOwner;
         this.delegate = new BufferedInputStream(inputStreamOwner.getInputStream());
         this.channelOwner = stack.getChannel();
      }

      public int available() throws IOException
      {
         return delegate.available();
      }

      public void close() throws IOException
      {
         if (log.isDebugEnabled())
         {
            log.debug("State reader " + inputStreamOwner + " is closing the socket ");
         }
         if (channelOwner != null && channelOwner.isConnected())
         {
            channelOwner.down(new Event(Event.STATE_TRANSFER_INPUTSTREAM_CLOSED));
         }
         inputStreamOwner.close();
      }

      public synchronized void mark(int readlimit)
      {
         delegate.mark(readlimit);
      }

      public boolean markSupported()
      {
         return delegate.markSupported();
      }

      public int read() throws IOException
      {
         return delegate.read();
      }

      public int read(byte[] b, int off, int len) throws IOException
      {
         return delegate.read(b, off, len);
      }

      public int read(byte[] b) throws IOException
      {
         return delegate.read(b);
      }

      public synchronized void reset() throws IOException
      {
         delegate.reset();
      }

      public long skip(long n) throws IOException
      {
         return delegate.skip(n);
      }
   }

   private class StreamingOutputStreamWrapper extends OutputStream
   {
      private Socket outputStreamOwner;

      private OutputStream delegate;

      private long bytesWrittenCounter = 0;
      
      private Channel channelOwner;

      public StreamingOutputStreamWrapper(Socket outputStreamOwner) throws IOException
      {
         super();
         this.outputStreamOwner = outputStreamOwner;
         this.delegate = new BufferedOutputStream(outputStreamOwner.getOutputStream());
         this.channelOwner = stack.getChannel();
      }

      public void close() throws IOException
      {
         if (log.isDebugEnabled())
         {
            log.debug("State writer " + outputStreamOwner + " is closing the socket ");
         }
         try
         {
            if (channelOwner != null && channelOwner.isConnected())
            {
               channelOwner.down(new Event(Event.STATE_TRANSFER_OUTPUTSTREAM_CLOSED));
            }           
            outputStreamOwner.close();
         }
         catch (IOException e)
         {
            throw e;
         }
         finally
         {
            if (stats)
            {
               synchronized (state_requesters)
               {
                  num_bytes_sent += bytesWrittenCounter;
                  avg_state_size = num_bytes_sent / (double) num_state_reqs;
               }
            }
         }
      }

      public void flush() throws IOException
      {
         delegate.flush();
      }

      public void write(byte[] b, int off, int len) throws IOException
      {
         delegate.write(b, off, len);
         bytesWrittenCounter += len;
      }

      public void write(byte[] b) throws IOException
      {
         delegate.write(b);
         if (b != null)
         {
            bytesWrittenCounter += b.length;
         }
      }

      public void write(int b) throws IOException
      {
         delegate.write(b);
         bytesWrittenCounter += 1;
      }
   }

   public static class StateHeader extends Header implements Streamable
   {
      public static final byte STATE_REQ = 1;

      public static final byte STATE_RSP = 2;

      public static final byte STATE_REMOVE_REQUESTER = 3;

      long id = 0; // state transfer ID (to separate multiple state transfers at the same time)

      byte type = 0;

      Address sender; // sender of state STATE_REQ or STATE_RSP

      Digest my_digest = null; // digest of sender (if type is STATE_RSP)

      IpAddress bind_addr = null;

      String state_id = null; // for partial state transfer

      public StateHeader()
      { // for externalization
      }

      public StateHeader(byte type, Address sender, String state_id)
      {
         this.type = type;
         this.sender = sender;
         this.state_id = state_id;
      }

      public StateHeader(byte type, Address sender, long id, Digest digest)
      {
         this.type = type;
         this.sender = sender;
         this.id = id;
         this.my_digest = digest;
      }

      public StateHeader(byte type, Address sender, IpAddress bind_addr, Digest digest, String state_id)
      {
         this.type = type;
         this.sender = sender;
         this.my_digest = digest;
         this.bind_addr = bind_addr;
         this.state_id = state_id;
      }

      public int getType()
      {
         return type;
      }

      public Digest getDigest()
      {
         return my_digest;
      }

      public String getStateId()
      {
         return state_id;
      }

      public boolean equals(Object o)
      {
         StateHeader other;

         if (sender != null && o != null)
         {
            if (!(o instanceof StateHeader))
               return false;
            other = (StateHeader) o;
            return sender.equals(other.sender) && id == other.id;
         }
         return false;
      }

      public int hashCode()
      {
         if (sender != null)
            return sender.hashCode() + (int) id;
         else
            return (int) id;
      }

      public String toString()
      {
         StringBuffer sb = new StringBuffer();
         sb.append("type=").append(type2Str(type));
         if (sender != null)
            sb.append(", sender=").append(sender).append(" id=").append(id);
         if (my_digest != null)
            sb.append(", digest=").append(my_digest);
         return sb.toString();
      }

      static String type2Str(int t)
      {
         switch (t)
         {
            case STATE_REQ :
               return "STATE_REQ";
            case STATE_RSP :
               return "STATE_RSP";
            case STATE_REMOVE_REQUESTER :
               return "STATE_REMOVE_REQUESTER";
            default :
               return "<unknown>";
         }
      }

      public void writeExternal(ObjectOutput out) throws IOException
      {
         out.writeObject(sender);
         out.writeLong(id);
         out.writeByte(type);
         out.writeObject(my_digest);
         out.writeObject(bind_addr);
         out.writeUTF(state_id);
      }

      public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
      {
         sender = (Address) in.readObject();
         id = in.readLong();
         type = in.readByte();
         my_digest = (Digest) in.readObject();
         bind_addr = (IpAddress) in.readObject();
         state_id = in.readUTF();
      }

      public void writeTo(DataOutputStream out) throws IOException
      {
         out.writeByte(type);
         out.writeLong(id);
         Util.writeAddress(sender, out);
         Util.writeStreamable(my_digest, out);
         Util.writeStreamable(bind_addr, out);
         Util.writeString(state_id, out);
      }

      public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException
      {
         type = in.readByte();
         id = in.readLong();
         sender = Util.readAddress(in);
         my_digest = (Digest) Util.readStreamable(Digest.class, in);
         bind_addr = (IpAddress) Util.readStreamable(IpAddress.class, in);
         state_id = Util.readString(in);
      }

      public long size()
      {
         long retval = Global.LONG_SIZE + Global.BYTE_SIZE; // id and type

         retval += Util.size(sender);

         retval += Global.BYTE_SIZE; // presence byte for my_digest
         if (my_digest != null)
            retval += my_digest.serializedSize();

         retval += Global.BYTE_SIZE; // presence byte for state_id
         if (state_id != null)
            retval += state_id.length() + 2;
         return retval;
      }

   }

}
