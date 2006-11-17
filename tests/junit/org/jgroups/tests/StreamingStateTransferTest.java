package org.jgroups.tests;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.jgroups.Channel;
import org.jgroups.JChannelFactory;
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
      CHANNEL_CONFIG = System.getProperty("channel.config.streaming", "conf/flush-udp.xml");      
   }  
   
   public void testStreamingStateTransfer()
   {
      testTransfer(false);
   }
   
   public void testRpcStreamingStateTransfer()
   {
      testTransfer(true);
   }

   public void testTransfer(boolean useDispatcher)
   {
      String[] channelNames = null;
      
      //for mux all names are used as app ids and need to be the same
      if(isMuxChannelUsed())
      {
         channelNames = new String[]{"A", "A", "A", "A"}; 
      }
      else
      {
         channelNames = new String[]{"A", "B", "C", "D"};
      }
      
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
               channels[i] = new StreamingStateTransferApplication(channelNames[i],muxFactory[i],semaphore);  
            }
            else
            {
               channels[i] = new StreamingStateTransferApplication(channelNames[i],semaphore,useDispatcher);
            }

            // Start threads and let them join the channel                           
            channels[i].start();
            semaphore.release(1);
            sleepThread(2000);
         }

         // Make sure everyone is in sync
         blockUntilViewsReceived(channels, 60000);

         //Reacquire the semaphore tickets; when we have them all
         // we know the threads are done         
         acquireSemaphore(semaphore, 60000, channelCount);         
         
         int getStateInvokedCount = 0;
         int setStateInvokedCount = 0;
         int partialGetStateInvokedCount = 0;
         int partialSetStateInvokedCount = 0;
         
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
         }
         
         assertEquals("Correct invocation count of getState ",1, getStateInvokedCount);
         assertEquals("Correct invocation count of setState ",channelCount-1,setStateInvokedCount);
         assertEquals("Correct invocation count of partial getState ",1, partialGetStateInvokedCount);
         assertEquals("Correct invocation count of partial setState ",channelCount-1,partialSetStateInvokedCount);
               
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
      private Object transferObject = new String("JGroups");

      private Object partialTransferObject = new String("partial");
      
      boolean partialSetStateInvoked = false;

      boolean partialGetStateInvoked = false;

      boolean setStateInvoked = false;

      boolean getStateInvoked = false;

      public StreamingStateTransferApplication(String name, Semaphore s,boolean useDispatcher) throws Exception
      {
         super(name,new StreamingChannelTestFactory(),s,useDispatcher);
      }    
      
      public StreamingStateTransferApplication(String name, JChannelFactory factory,Semaphore s) throws Exception
      {
         super(name,factory,s);
      } 

      public void useChannel() throws Exception
      {
         channel.connect("test");
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
            oos.writeObject(transferObject);
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
            TestCase.assertEquals("Got full state requested ", transferObject,ois.readObject());
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
