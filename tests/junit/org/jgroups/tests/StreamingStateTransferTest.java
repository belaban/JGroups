package org.jgroups.tests;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.JChannelFactory;
import org.jgroups.Message;
import org.jgroups.util.Util;

import EDU.oswego.cs.dl.util.concurrent.Semaphore;

/**
 * Tests streaming state transfer.
 * 
 * 
 * @author Vladimir Blagojevic
 * @version $Id$ 
 *
 */
public class StreamingStateTransferTest extends ChannelTestBase
{
   
   
   public void setUp() throws Exception
   {
      super.setUp();         
      CHANNEL_CONFIG = System.getProperty("channel.conf.flush", "flush-udp.xml");      
   }
   
   public boolean useBlocking()
   {
      return true;
   }
   
   public void testTransfer()
   {
      String channelNames [] = null;
      //mux applications on top of same channel have to have unique name
      if(isMuxChannelUsed())
      {
         channelNames = createMuxApplicationNames(1);         
      }
      else
      {
         channelNames = new String[]{"A", "B", "C", "D"};        
      }
      transferHelper(channelNames,false);
   }
   
   public void testRpcChannelTransfer()
   {
      //do this test for regular channels only
      if(!isMuxChannelUsed())
      {
         String channelNames []= new String[]{"A", "B", "C", "D"};   
         transferHelper(channelNames,true);
      }
   }
   
   public void testMultipleServiceMuxChannel()
   {
      String channelNames [] = null;
      //mux applications on top of same channel have to have unique name
      if(isMuxChannelUsed())
      {
         channelNames = createMuxApplicationNames(2);  
         transferHelper(channelNames,false);
      }           
   }

   public void transferHelper(String channelNames[], boolean useDispatcher)
   {                 
      int channelCount = channelNames.length;
      StreamingStateTransferApplication[] channels = null;

      channels = new StreamingStateTransferApplication[channelCount];

      //Create a semaphore and take all its tickets
      Semaphore semaphore = new Semaphore(channelCount);
      
      try
      {

         takeAllPermits(semaphore, channelCount);
         
         // Create activation threads that will block on the semaphore        
         for (int i = 0; i < channelCount; i++)
         {

            if(isMuxChannelUsed())
            {
               channels[i] = new StreamingStateTransferApplication(channelNames[i],muxFactory[i%getMuxFactoryCount()],semaphore);  
            }
            else
            {
               channels[i] = new StreamingStateTransferApplication(channelNames[i],semaphore,useDispatcher);
            }

            // Start threads and let them join the channel
            semaphore.release(1);
            channels[i].start();            
            sleepThread(2000);            
         }               
         
         if(isMuxChannelUsed())
         {
            blockUntilViewsReceived(channels,getMuxFactoryCount(), 60000);
         }
         else
         {
            blockUntilViewsReceived(channels, 60000);
         }
         

         //Reacquire the semaphore tickets; when we have them all
         // we know the threads are done         
         acquireSemaphore(semaphore, 60000, channelCount);          
         
         int getStateInvokedCount = 0;
         int setStateInvokedCount = 0;
         int partialGetStateInvokedCount = 0;
         int partialSetStateInvokedCount = 0;
         
         sleepThread(3000);
         for (int i = 0; i < channels.length; i++)
         {
            if(channels[i].getStateInvoked)
            {
               getStateInvokedCount++;
            }
            if(channels[i].setStateInvoked)
            {
               setStateInvokedCount++;
            }  
            if(channels[i].partialGetStateInvoked)
            {
               partialGetStateInvokedCount++;
            } 
            if(channels[i].partialSetStateInvoked)
            {
               partialSetStateInvokedCount++;
            }
            Map map = channels[i].getMap();
            for (int j = 0; j < channels.length; j++)
            {
               List l = (List) map.get(channels[j].getLocalAddress());
               int size = l!=null?l.size():0;
               assertEquals("Correct element count in map ",StreamingStateTransferApplication.COUNT,size);              
            }
         }
         if(isMuxChannelUsed())
         {
            int factor = channelCount/getMuxFactoryCount();
            assertEquals("Correct invocation count of getState ",1*factor, getStateInvokedCount);
            assertEquals("Correct invocation count of setState ",(channelCount/factor)-1,setStateInvokedCount/factor);
            assertEquals("Correct invocation count of partial getState ",1*factor, partialGetStateInvokedCount);
            assertEquals("Correct invocation count of partial setState ",(channelCount/factor)-1,partialSetStateInvokedCount/factor);
         }
         else
         {
            assertEquals("Correct invocation count of getState ",1, getStateInvokedCount);
            assertEquals("Correct invocation count of setState ",channelCount-1,setStateInvokedCount);
            assertEquals("Correct invocation count of partial getState ",1, partialGetStateInvokedCount);
            assertEquals("Correct invocation count of partial setState ",channelCount-1,partialSetStateInvokedCount);
         }
               
      }
      catch (Exception ex)
      {
         log.warn(ex);
      }
      finally
      {
         for (int i = 0; i < channelCount; i++)
         {
            sleepThread(500);
            channels[i].cleanup();
         }
      }
   }
   
   protected class StreamingChannelTestFactory extends DefaultChannelTestFactory
   {
      public Channel createChannel(Object id) throws Exception
      {
         return createChannel(CHANNEL_CONFIG, true);
      }
   }

   protected class StreamingStateTransferApplication extends PushChannelApplicationWithSemaphore
   {            
      private Map stateMap = new HashMap();
      
      public static final int COUNT = 25;

      private Object partialTransferObject = new String("partial");
      
      boolean partialSetStateInvoked = false;

      boolean partialGetStateInvoked = false;

      boolean setStateInvoked = false;

      boolean getStateInvoked = false;

      public StreamingStateTransferApplication(String name, Semaphore s,boolean useDispatcher) throws Exception
      {
         super(name,new StreamingChannelTestFactory(),s,useDispatcher);
         channel.connect("test");
      }    
      
      public StreamingStateTransferApplication(String name, JChannelFactory factory,Semaphore s) throws Exception
      {
         super(name,factory,s);
         channel.connect("test");
      } 

      public void receive(Message msg)
      {
         Address sender = msg.getSrc();
         synchronized(stateMap)
         {
            List list = (List) stateMap.get(sender);
            if(list == null)
            {
               list = new ArrayList();
               stateMap.put(sender, list);
            }
            list.add(msg.getObject());
         }
      }
      
      public Map getMap()
      {
         return stateMap;
      }

      public void useChannel() throws Exception
      {        
         for(int i = 0;i < COUNT;i++)
         {
            channel.send(null,null,new Integer(i));
         }
         channel.getState(null, 25000);                         
         channel.getState(null, name, 25000);
      }

      public void getState(OutputStream ostream)
      {
         super.getState(ostream);
         ObjectOutputStream oos = null;
         try
         {
            oos = new ObjectOutputStream(ostream);
            HashMap copy = null;
            synchronized (stateMap)
            {
               copy = new HashMap(stateMap);
            }              
            oos.writeObject(copy);                         
            oos.flush();
         }
         catch (IOException e)
         {
            e.printStackTrace();
         }
         finally
         {
            getStateInvoked = true;
            Util.close(oos);
         }
      }

      public void setState(InputStream istream)
      {
         super.setState(istream);
         ObjectInputStream ois = null;
         try
         {
            ois = new ObjectInputStream(istream);
            Map map = (Map) ois.readObject();
            synchronized (stateMap)
            {
               stateMap.clear();
               stateMap.putAll(map);
            }           
         }
         catch (Exception e)
         {
            e.printStackTrace();
         }
         finally
         {
            setStateInvoked = true;
            Util.close(ois);
         }
      }

      public void setState(String state_id, InputStream istream)
      {
         super.setState(state_id, istream);
         ObjectInputStream ois = null;
         try
         {
            ois = new ObjectInputStream(istream);            
            TestCase.assertEquals("Got partial state requested ", ois.readObject(), name);           
            TestCase.assertEquals("Got partial state ", partialTransferObject, ois.readObject());
         }
         catch (Exception e)
         {
            e.printStackTrace();
         }
         finally
         {
            partialSetStateInvoked = true;
            Util.close(ois);
         }
      }

      public void getState(String state_id, OutputStream ostream)
      {
         super.getState(state_id, ostream);
         ObjectOutputStream oos = null;
         try
         {
            oos = new ObjectOutputStream(ostream);
            oos.writeObject(state_id);
            oos.writeObject(partialTransferObject);
            oos.flush();
         }
         catch (IOException e)
         {
            e.printStackTrace();
         }
         finally
         {
            partialGetStateInvoked  = true;
            Util.close(oos);
         }
      }        
   }

   public static Test suite()
   {
      return new TestSuite(StreamingStateTransferTest.class);
   }

   public static void main(String[] args)
   {
      String[] testCaseName = {StreamingStateTransferTest.class.getName()};
      junit.textui.TestRunner.main(testCaseName);
   }
}
