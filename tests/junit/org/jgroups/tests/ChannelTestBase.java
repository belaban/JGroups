package org.jgroups.tests;

import java.io.InputStream;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.ChannelException;
import org.jgroups.ExtendedReceiver;
import org.jgroups.JChannel;
import org.jgroups.JChannelFactory;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.util.Util;

import EDU.oswego.cs.dl.util.concurrent.Semaphore;

/**
 * 
 * @author Bela Ban
 * @author Vladimir Blagojevic
 * @author <a href="mailto://brian.stansberry@jboss.com">Brian Stansberry</a>
 * @version $Revision$
 */
public class ChannelTestBase extends TestCase
{
   private static Random random = new Random();

   static String CHANNEL_CONFIG = "udp.xml";  
   
   static String MUX_CHANNEL_CONFIG = "stacks.xml"; 
   
   static String MUX_CHANNEL_CONFIG_STACK_NAME ="udp";

   protected int active_threads = 0;
   
   protected JChannelFactory muxFactory[] = null;  

   protected String thread_dump = null;    

   protected final Log log = LogFactory.getLog(this.getClass());

   public void setUp() throws Exception
   {
      super.setUp();          
      if (isMuxChannelUsed())
      {
         int factoryCount = Integer.parseInt(System.getProperty("mux.factorycount", "4"));         
         muxFactory = new JChannelFactory[factoryCount];
         
         for (int i = 0; i < muxFactory.length; i++)
         {
            muxFactory[i] = new JChannelFactory();
            muxFactory[i].setMultiplexerConfig(System.getProperty("mux.config", MUX_CHANNEL_CONFIG));
         }        
         MUX_CHANNEL_CONFIG_STACK_NAME = System.getProperty("mux.config.stackname", MUX_CHANNEL_CONFIG_STACK_NAME);
      }
      
      if (shouldCompareThreadCount())
      {
         active_threads = Thread.activeCount();
         thread_dump = "active threads before (" + active_threads + "):\n" + Util.activeThreads();
      }
      
      CHANNEL_CONFIG = System.getProperty("channel.config", CHANNEL_CONFIG);     
   }

   protected void tearDown() throws Exception
   {
      super.tearDown();

      if (isMuxChannelUsed())
      {
         for (int i = 0; i < muxFactory.length; i++)
         {
            muxFactory[i].destroy();
         }
      }
      
      Util.sleep(500); // remove this in 2.5 !

      if (shouldCompareThreadCount())
      {
         int current_active_threads = Thread.activeCount();

         String msg = "";
         if (active_threads != current_active_threads)
         {
            System.out.println(thread_dump);
            System.out.println("active threads after (" + current_active_threads + "):\n" + Util.activeThreads());
            msg = "active threads:\n" + dumpThreads();
         }
         assertEquals(msg, active_threads, current_active_threads);
      }
   }
   
   protected boolean isMuxChannelUsed()
   {
      return Boolean.valueOf(System.getProperty("mux.on", "false")).booleanValue();
   }
   
   protected boolean shouldCompareThreadCount()
   {
      return Boolean.valueOf(System.getProperty("threadcount", "false")).booleanValue();
   }

   /**
    * Default channel factory used in junit tests
    *
    */
   protected class DefaultChannelTestFactory implements ChannelTestFactory
   {      
      public Channel createChannel(Object id) throws Exception
      {
         return createChannel(CHANNEL_CONFIG, true);
      }

      protected Channel createChannel(String configFile, boolean useBlocking) throws Exception
      {
         HashMap channelOptions = new HashMap();
         channelOptions.put(new Integer(Channel.BLOCK), Boolean.TRUE);
         return createChannel(configFile, channelOptions);
      }

      protected Channel createChannel(String configFile, Map channelOptions) throws Exception
      {
         Channel ch = null;
         log.info("Using configuration file " + configFile);
         ch = new JChannel(configFile);
         for (Iterator iter = channelOptions.keySet().iterator(); iter.hasNext();)
         {
            Integer key = (Integer) iter.next();
            Object value = channelOptions.get(key);
            ch.setOpt(key.intValue(), value);
         }
         return ch;
      }
   }    
   
   /**
    * Default channel factory used in junit tests
    *
    */
   public class DefaultMuxChannelTestFactory implements ChannelTestFactory
   {
      JChannelFactory f = null;
      
      public DefaultMuxChannelTestFactory(JChannelFactory f)
      {
         this.f = f;         
      }

      public Channel createChannel(Object id) throws Exception
      {
         log.info("Factory " + f.toString() + " creating mux channel using "
               + MUX_CHANNEL_CONFIG + " and stack "
               + MUX_CHANNEL_CONFIG_STACK_NAME);
         return f.createMultiplexerChannel(MUX_CHANNEL_CONFIG_STACK_NAME, id.toString());
      }         
   }
   /**
    * Decouples channel creation for junit tests
    *
    */
   protected interface ChannelTestFactory
   {
      public Channel createChannel(Object id) throws Exception;
   }

   /**
    * Base class for all aplications using channel
    *   
    *
    */
   protected abstract class ChannelApplication implements Runnable, MemberRetrievable
   {
      protected Channel channel;

      protected Thread thread;

      protected Throwable exception;

      protected String name;          

      public ChannelApplication(String name,JChannelFactory f) throws Exception
      {
         if(f==null)
         {
            createChannel(name, new DefaultChannelTestFactory());
         }
         else
         {
            createChannel(name, new DefaultMuxChannelTestFactory(f));
         }
      }

      /**
       * Creates a unconnected channel and assigns a name to it.
       * 
       * @param name name of this channel
       * @param factory factory to create Channel
       * @throws ChannelException
       */
      public ChannelApplication(String name, ChannelTestFactory factory) throws Exception
      {
         createChannel(name, factory);
      }
      
      private void createChannel(String name, ChannelTestFactory factory) throws Exception
      {
         this.name = name;
         channel = factory.createChannel(name);
      }

      /**
       * Method allowing implementation of specific test application level logic      
       * @throws Exception
       */
      protected abstract void useChannel() throws Exception;

      public void run()
      {
         try
         {
            useChannel();
         }
         catch (Exception e)
         {
            log.error(name + ": " + e.getLocalizedMessage(), e);

            // Save it for the test to check
            exception = e;
         }
      }

      public List getMembers()
      {
         List result = null;
         View v = channel.getView();
         if (v != null)
         {
            result = v.getMembers();
         }
         return result;
      }

      public Address getLocalAddress()
      {
         return channel.getLocalAddress();
      }

      public void start()
      {
         thread = new Thread(this, getName());
         thread.start();
         Address a = getLocalAddress();
         boolean connected = a != null ? true : false;
         if (connected)
         {
            log.info("Thread for channel " + a + "[" + getName() + "] started");
         }
         else
         {
            log.info("Thread for channel [" + getName() + "] started");
         }
      }

      public void setChannel(Channel ch)
      {
         this.channel = ch;
      }

      public Channel getChannel()
      {
         return channel;
      }

      public String getName()
      {
         return name;
      }

      public void cleanup()
      {
         if (thread != null && thread.isAlive())
         {
            thread.interrupt();
         }
         Address a = getLocalAddress();
         boolean connected = a != null ? true : false;
         if (connected)
         {
            log.info("Closing channel " + a + "[" + getName() + "]");
         }
         else
         {
            log.info("Closing channel [" + getName() + "]");
         }
         channel.close();
      }
   }
   
   protected abstract class PushChannelApplication extends ChannelApplication implements ExtendedReceiver
   {
      RpcDispatcher dispatcher;

      public PushChannelApplication(String name) throws Exception
      {
         this(name, new DefaultChannelTestFactory(), false);
      }
      
      public PushChannelApplication(String name, JChannelFactory f) throws Exception
      {
         this(name, new DefaultMuxChannelTestFactory(f), false);
      }

      public PushChannelApplication(String name, boolean useDispatcher) throws Exception
      {
         this(name, new DefaultChannelTestFactory(), useDispatcher);
      }

      public PushChannelApplication(String name, ChannelTestFactory factory, boolean useDispatcher)
            throws Exception
      {
         super(name, factory);
         if (useDispatcher)
         {
            dispatcher = new RpcDispatcher(channel, this, this, this);
         }
         else
         {
            channel.setReceiver(this);
         }
      }

      public RpcDispatcher getDispatcher()
      {
         return dispatcher;
      }

      public boolean hasDispatcher()
      {
         return dispatcher != null;
      }

      public void block()
      {
         log.debug("Channel " + getLocalAddress() + "[" + getName() + "] in blocking");
      }

      public byte[] getState()
      {
         log.debug("Channel " + getLocalAddress() + "[" + getName() + "] ");
         return null;
      }

      public void getState(OutputStream ostream)
      {
         log.debug("Channel " + getLocalAddress() + "[" + getName() + "]");
      }

      public byte[] getState(String state_id)
      {
         log.debug("Channel " + getLocalAddress() + "[" + getName() + " state id =" + state_id);
         return null;
      }

      public void getState(String state_id, OutputStream ostream)
      {
         log.debug("Channel " + getLocalAddress() + "[" + getName() + "] state id =" + state_id);
      }

      public void receive(Message msg)
      {
      }

      public void setState(byte[] state)
      {
         log.debug("Channel " + getLocalAddress() + "[" + getName() + "] ");
      }

      public void setState(InputStream istream)
      {
         log.debug("Channel " + getLocalAddress() + "[" + getName() + "]");
      }

      public void setState(String state_id, byte[] state)
      {
         log.debug("Channel " + getLocalAddress() + "[" + getName() + "] state id =" + state_id + ", state size is "
               + state.length);
      }

      public void setState(String state_id, InputStream istream)
      {
         log.debug("Channel " + getLocalAddress() + "[" + getName() + "] state id " + state_id);
      }

      public void suspect(Address suspected_mbr)
      {
         log.debug("Channel " + getLocalAddress() + "[" + getName() + "] suspecting " + suspected_mbr);
      }

      public void unblock()
      {
         log.debug("Channel " + getLocalAddress() + "[" + getName() + "] unblocking");
      }

      public void viewAccepted(View new_view)
      {
         log.debug("Channel " + getLocalAddress() + "[" + getName() + "] accepted view " + new_view);
      }
   }

   /**
    * Channel with semaphore allows application to go through fine-grained synchronous step control.
    * <p> 
    * PushChannelApplicationWithSemaphore application will not proceed to useChannel() 
    * until it acquires permit from semphore. After useChannel() completes the acquired 
    * permit will be released.  Test driver should control how semaphore tickets are given 
    * and acquired.
    *
    */
   protected abstract class PushChannelApplicationWithSemaphore extends PushChannelApplication
   {
      protected Semaphore semaphore;

      public PushChannelApplicationWithSemaphore(String name, ChannelTestFactory factory, Semaphore semaphore,
            boolean useDispatcher) throws Exception
      {
         super(name, factory, useDispatcher);
         this.semaphore = semaphore;
      }

      protected PushChannelApplicationWithSemaphore(String name, Semaphore semaphore) throws Exception
      {
         this(name, new DefaultChannelTestFactory(), semaphore, false);
      }
      
      protected PushChannelApplicationWithSemaphore(String name, JChannelFactory f,Semaphore semaphore) throws Exception
      {
         this(name, new DefaultMuxChannelTestFactory(f), semaphore, false);
      }

      protected PushChannelApplicationWithSemaphore(String name, Semaphore semaphore, boolean useDispatcher)
            throws Exception
      {
         this(name, new DefaultChannelTestFactory(), semaphore, useDispatcher);
      }

      public void run()
      {
         boolean acquired = false;
         try
         {
            acquired = semaphore.attempt(60000);
            if (!acquired)
            {
               throw new Exception(name + " cannot acquire semaphore");
            }

            useChannel();
         }
         catch (Exception e)
         {
            log.error(name + ": " + e.getLocalizedMessage(), e);
            // Save it for the test to check
            exception = e;
         }
         finally
         {
            if (acquired)
            {
               semaphore.release();
            }
         }
      }
   }     

   protected interface MemberRetrievable
   {
      public List getMembers();

      public Address getLocalAddress();
   }

   /**
    * Checks each channel in the parameter array to see if it has the 
    * exact same view as other channels in an array.    
    */
   public static boolean areViewsComplete(MemberRetrievable[] channels)
   {
      int memberCount = channels.length;

      for (int i = 0; i < memberCount; i++)
      {
         if (!isViewComplete(channels[i], memberCount))
         {
            return false;
         }
      }

      return true;
   }

   /**
    * Loops, continually calling {@link #areViewsComplete(MemberRetrievable[])}
    * until it either returns true or <code>timeout</code> ms have elapsed.
    *
    * @param channels  channels which must all have consistent views
    * @param timeout max number of ms to loop
    * @throws RuntimeException if <code>timeout</code> ms have elapse without
    *                          all channels having the same number of members.
    */
   public static void blockUntilViewsReceived(MemberRetrievable[] channels, long timeout)
   {
      long failTime = System.currentTimeMillis() + timeout;

      while (System.currentTimeMillis() < failTime)
      {
         sleepThread(100);
         if (areViewsComplete(channels))
         {
            return;
         }
      }

      throw new RuntimeException("timed out before caches had complete views");
   }

   public static boolean isViewComplete(MemberRetrievable channel, int memberCount)
   {

      List members = channel.getMembers();
      if (members == null || memberCount > members.size())
      {
         return false;
      }
      else if (memberCount < members.size())
      {
         // This is an exceptional condition
         StringBuffer sb = new StringBuffer("Channel at address ");
         sb.append(channel.getLocalAddress());
         sb.append(" had ");
         sb.append(members.size());
         sb.append(" members; expecting ");
         sb.append(memberCount);
         sb.append(". Members were (");
         for (int j = 0; j < members.size(); j++)
         {
            if (j > 0)
            {
               sb.append(", ");
            }
            sb.append(members.get(j));
         }
         sb.append(')');

         throw new IllegalStateException(sb.toString());
      }

      return true;
   }

   public static void takeAllPermits(Semaphore semaphore, int count)
   {
      for (int i = 0; i < count; i++)
      {
         try
         {
            semaphore.acquire();
         }
         catch (InterruptedException e)
         {
            //not interested
            e.printStackTrace();
         }
      }
   }

   public static void acquireSemaphore(Semaphore semaphore, long timeout, int count) throws Exception
   {
      for (int i = 0; i < count; i++)
      {
         boolean acquired = false;
         try
         {
            acquired = semaphore.attempt(timeout);
         }
         catch (InterruptedException e)
         {
            //not interested but print it
            e.printStackTrace();
         }
         if (!acquired)
            throw new Exception("Failed to acquire semaphore");
      }
   }

   public static void sleepRandom(int maxTime)
   {
      sleepThread(random.nextInt(maxTime));
   }

   /**
    * Puts the current thread to sleep for the desired number of ms, suppressing
    * any exceptions.
    *
    * @param sleeptime number of ms to sleep
    */
   public static void sleepThread(long sleeptime)
   {
      try
      {
         Thread.sleep(sleeptime);
      }
      catch (InterruptedException ie)
      {
      }
   }

   /* CAUTION: JDK 5 specific code */
   private String dumpThreads()
   {
      StringBuffer sb = new StringBuffer();
      ThreadMXBean bean = ManagementFactory.getThreadMXBean();
      long[] ids = bean.getAllThreadIds();
      ThreadInfo[] threads = bean.getThreadInfo(ids, 20);
      for (int i = 0; i < threads.length; i++)
      {
         ThreadInfo info = threads[i];
         if (info == null)
            continue;
         sb.append(info.getThreadName()).append(":\n");
         StackTraceElement[] stack_trace = info.getStackTrace();
         for (int j = 0; j < stack_trace.length; j++)
         {
            StackTraceElement el = stack_trace[j];
            sb.append("at ").append(el.getClassName()).append(".").append(el.getMethodName());
            sb.append("(").append(el.getFileName()).append(":").append(el.getLineNumber()).append(")");
            sb.append("\n");
         }
         sb.append("\n\n");
      }
      return sb.toString();
   }
}
