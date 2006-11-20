package org.jgroups.tests;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.jgroups.Address;
import org.jgroups.BlockEvent;
import org.jgroups.Channel;
import org.jgroups.ChannelException;
import org.jgroups.Event;
import org.jgroups.ExtendedReceiverAdapter;
import org.jgroups.GetStateEvent;
import org.jgroups.JChannel;
import org.jgroups.JChannelFactory;
import org.jgroups.Message;
import org.jgroups.SetStateEvent;
import org.jgroups.UnblockEvent;
import org.jgroups.View;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;

import EDU.oswego.cs.dl.util.concurrent.Semaphore;

/**
 * Tests the FLUSH protocol, requires flush-udp.xml in ./conf to be present and configured to use FLUSH
 * @author Bela Ban
 * @version $Id: FlushTest.java,v 1.19 2006/11/20 22:22:28 vlada Exp $
 */
public class FlushTest extends ChannelTestBase
{
   Channel c1, c2, c3;

   static final String CONFIG = "flush-udp.xml";

   public void setUp() throws Exception
   {
      super.setUp();
      CHANNEL_CONFIG = System.getProperty("channel.config.flush", "flush-udp.xml");
   }

   public void tearDown() throws Exception
   {
      if (c3 != null)
      {
         c3.close();
         assertFalse(c3.isOpen());
         assertFalse(c3.isConnected());
         c3 = null;
      }

      if (c2 != null)
      {
         c2.close();
         assertFalse(c2.isOpen());
         assertFalse(c2.isConnected());
         c2 = null;
      }

      if (c1 != null)
      {
         c1.close();
         assertFalse(c1.isOpen());
         assertFalse(c1.isConnected());
         c1 = null;
      }

      Util.sleep(1000);
      super.tearDown();
   }

   public void testSingleChannel() throws Exception
   {
      Semaphore s = new Semaphore(1);
      MyReceiver receivers[] = new MyReceiver[]{new MyReceiver("c1", s, false)};
      receivers[0].start();
      s.release(1);

      //Make sure everyone is in sync
      blockUntilViewsReceived(receivers, 60000);

      // Sleep to ensure the threads get all the semaphore tickets
      sleepThread(1000);

      // Reacquire the semaphore tickets; when we have them all
      // we know the threads are done         
      try
      {
         acquireSemaphore(s, 60000, 1);
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
      finally
      {
         receivers[0].cleanup();
      }

      checkEventSequence(receivers[0]);

   }

   /**
    * Tests issue #1 in http://jira.jboss.com/jira/browse/JGRP-335
    */
   public void testJoinFollowedByUnicast() throws ChannelException
   {
      c1 = createChannel();
      c1.setReceiver(new MySimpleReplier(c1, true));
      c1.connect("test");

      Address target = c1.getLocalAddress();
      Message unicast_msg = new Message(target);

      c2 = createChannel();
      c2.setReceiver(new MySimpleReplier(c2, false));
      c2.connect("test");

      // now send unicast, this might block as described in the case
      c2.send(unicast_msg);
      // if we don't get here this means we'd time out
   }

   /**
    * Tests issue #2 in http://jira.jboss.com/jira/browse/JGRP-335
    */
   public void testStateTransferFollowedByUnicast() throws ChannelException
   {
      c1 = createChannel();
      c1.setReceiver(new MySimpleReplier(c1, true));
      c1.connect("test");

      Address target = c1.getLocalAddress();
      Message unicast_msg = new Message(target);

      c2 = createChannel();
      c2.setReceiver(new MySimpleReplier(c2, false));
      c2.connect("test");

      // Util.sleep(100);
      System.out.println("\n** Getting the state **");
      c2.getState(null, 10000);
      // now send unicast, this might block as described in the case
      c2.send(unicast_msg);
   }

   public void testChannelAfterConnect()
   {
      String[] names = null;
      if(isMuxChannelUsed())
      {
         names = createMuxApplicationNames(1); 
      }
      else
      {
         names = new String[]{"A", "B", "C", "D"};
      }      
      testChannels(names,false);
   }

   public void testChannelsWithStateTransfer()
   {
      String[] names = null;
      if(isMuxChannelUsed())
      {
         names = createMuxApplicationNames(1); 
      }
      else
      {
         names = new String[]{"A", "B", "C", "D"};
      }
      testChannels(names,true);
   }
   
   public void testMultipleServiceMuxChannelWithStateTransfer()
   {
      String[] names = null;
      if(isMuxChannelUsed())
      {
         names = createMuxApplicationNames(2);
         testChannels(names,true);
      }         
   }

   public void testChannels(String names[], boolean useTransfer)
   {     
      int count = names.length;

      MyReceiver[] channels = new MyReceiver[count];
      try
      {
         // Create a semaphore and take all its permits
         Semaphore semaphore = new Semaphore(count);
         takeAllPermits(semaphore, count);

         // Create activation threads that will block on the semaphore        
         for (int i = 0; i < count; i++)
         {
            if(isMuxChannelUsed())
            {
               channels[i] = new MyReceiver(names[i],muxFactory[i%getMuxFactoryCount()], semaphore, useTransfer);
            }
            else
            {
               channels[i] = new MyReceiver(names[i], semaphore, useTransfer);
            }

            // Release one ticket at a time to allow the thread to start working                           
            channels[i].start();
            
            
            if (!useTransfer)
            {
               semaphore.release(1);
            }
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
        

         //if state transfer is used release all at once
         //clear all channels of view events
         if (useTransfer)
         {            
            for (int i = 0; i < count; i++)
            {
               channels[i].clear();
            }
            semaphore.release(count);
         }

         // Sleep to ensure the threads get all the semaphore tickets
         sleepThread(1000);

         // Reacquire the semaphore tickets; when we have them all
         // we know the threads are done         
         acquireSemaphore(semaphore, 60000, count);

         //Sleep to ensure async message arrive
         sleepThread(3000);

         for (int i = 0; i < count; i++)
         {
            MyReceiver receiver = channels[i];
            log.info("Events for " + channels[i].getLocalAddress()+channels[i].getName() + " are " + channels[i].getEvents());
            if (useTransfer)
            {               
               checkEventStateTransferSequence(receiver);
            }
            else
            {
               checkEventSequence(receiver);
            }
         }
      }
      catch (Exception ex)
      {
         log.warn("Exception encountered during test", ex);
      }
      finally
      {
         for (int i = 0; i < count; i++)
         {
            sleepThread(500);
            channels[i].cleanup();
         }
      }
   }
   
   private void checkEventSequence(MyReceiver receiver)
   {
      List events = receiver.getEvents();
      String eventString = "[" + receiver.getName() + ",events:" + events;
      assertNotNull(events);
      int size = events.size();
      for (int i = 0; i < size; i++)
      {
         Object event = events.get(i);
         if (event instanceof BlockEvent)
         {
            if (i + 1 < size)
            {
               assertTrue("After Block should be View " + eventString, events.get(i + 1) instanceof View);
            }
            if (i != 0)
            {
               assertTrue("Before Block should be Unblock " + eventString, events.get(i - 1) instanceof UnblockEvent);
            }
         }
         if (event instanceof View)
         {
            if (i + 1 < size)
            {
               assertTrue("After View should be Unblock " + eventString, events.get(i + 1) instanceof UnblockEvent);
            }
            assertTrue("Before View should be Block " + eventString, events.get(i - 1) instanceof BlockEvent);
         }
         if (event instanceof UnblockEvent)
         {
            if (i + 1 < size)
            {
               assertTrue("After UnBlock should be Block " + eventString, events.get(i + 1) instanceof BlockEvent);
            }
            assertTrue("Before UnBlock should be View " + eventString, events.get(i - 1) instanceof View);
         }

      }
      receiver.clear();
   }

   private void checkEventStateTransferSequence(MyReceiver receiver)
   {
      List events = receiver.getEvents();
      String eventString = "[" + receiver.getName() + ",events:" + events;
      assertNotNull(events);
      int size = events.size();
      for (int i = 0; i < size; i++)
      {
         Object event = events.get(i);
         if (event instanceof BlockEvent)
         {            
            if (i + 1 < size)
            {
               Object o = events.get(i + 1);
               assertTrue("After Block should be state or unblock " + eventString, o instanceof SetStateEvent
                     || o instanceof GetStateEvent || o instanceof UnblockEvent);
            }
            else if (i != 0)
            {
               Object o = events.get(i + 1);
               assertTrue("Before Block should be state or Unblock " + eventString, o instanceof SetStateEvent
                     || o instanceof GetStateEvent || o instanceof UnblockEvent);
            }
         }
         if (event instanceof SetStateEvent || event instanceof GetStateEvent)
         {
            if (i + 1 < size)
            {
               assertTrue("After state should be Unblock " + eventString, events.get(i + 1) instanceof UnblockEvent);
            }
            assertTrue("Before state should be Block " + eventString, events.get(i - 1) instanceof BlockEvent);
         }

         if (event instanceof UnblockEvent)
         {
            if (i + 1 < size)
            {
               assertTrue("After UnBlock should be Block " + eventString, events.get(i + 1) instanceof BlockEvent);
            }
            else
            {
               Object o = events.get(size - 2);
               assertTrue("Before UnBlock should be block or state  " + eventString, o instanceof SetStateEvent
                     || o instanceof GetStateEvent || o instanceof BlockEvent);
            }
         }

      }
      receiver.clear();
   }  

   private Channel createChannel() throws ChannelException
   {
      Channel ret = new JChannel(CHANNEL_CONFIG);
      ret.setOpt(Channel.BLOCK, Boolean.TRUE);
      Protocol flush = ((JChannel) ret).getProtocolStack().findProtocol("FLUSH");
      if (flush != null)
      {
         Properties p = new Properties();
         p.setProperty("timeout", "0");
         flush.setProperties(p);

         // send timeout up and down the stack, so other protocols can use the same value too
         Map map = new HashMap();
         map.put("flush_timeout", new Long(0));
         flush.passUp(new Event(Event.CONFIG, map));
         flush.passDown(new Event(Event.CONFIG, map));
      }
      return ret;
   }

   private class MyReceiver extends PushChannelApplicationWithSemaphore
   {
      List events;

      boolean shouldFetchState;

      protected MyReceiver(String name, Semaphore semaphore, boolean shouldFetchState) throws Exception
      {
         super(name, semaphore);
         this.shouldFetchState = shouldFetchState;
         events = Collections.synchronizedList(new LinkedList());
         channel.connect("test");
      }
      
      protected MyReceiver(String name, JChannelFactory factory,Semaphore semaphore, boolean shouldFetchState) throws Exception
      {
         super(name, factory,semaphore);
         this.shouldFetchState = shouldFetchState;
         events = Collections.synchronizedList(new LinkedList());
         channel.connect("test");
      }

      public void clear()
      {
         events.clear();
      }

      public List getEvents()
      {
         return new LinkedList(events);
      }

      public void block()
      {
         events.add(new BlockEvent());
      }

      public void unblock()
      {
         events.add(new UnblockEvent());
      }

      public void viewAccepted(View new_view)
      {
         events.add(new_view);
      }

      public byte[] getState()
      {
         events.add(new GetStateEvent(null, null));
         return new byte[]
         {'b', 'e', 'l', 'a'};
      }

      public void setState(byte[] state)
      {
         events.add(new SetStateEvent(null, null));
      }

      public void getState(OutputStream ostream)
      {
         events.add(new GetStateEvent(null, null));
         byte[] payload = new byte[]
         {'b', 'e', 'l', 'a'};
         try
         {
            ostream.write(payload);
         }
         catch (IOException e)
         {
            e.printStackTrace();
         }
         finally
         {
            Util.close(ostream);
         }
      }

      public void setState(InputStream istream)
      {
         events.add(new SetStateEvent(null, null));
         byte[] payload = new byte[4];
         try
         {
            istream.read(payload);
         }
         catch (IOException e)
         {
            e.printStackTrace();
         }
         finally
         {
            Util.close(istream);
         }
      }

      protected void useChannel() throws Exception
      {
         if (shouldFetchState)
         {
            channel.getState(null, 25000);
         }
      }
   }

   private static class MySimpleReplier extends ExtendedReceiverAdapter
   {
      Channel channel;

      boolean handle_requests = false;

      public MySimpleReplier(Channel channel, boolean handle_requests)
      {
         this.channel = channel;
         this.handle_requests = handle_requests;
      }

      public void receive(Message msg)
      {
         Message reply = new Message(msg.getSrc());
         try
         {
            System.out.print("-- MySimpleReplier[" + channel.getLocalAddress() + "]: received message from "
                  + msg.getSrc());
            if (handle_requests)
            {
               System.out.println(", sending reply");
               channel.send(reply);
            }
            else
               System.out.println("\n");
         }
         catch (Exception e)
         {
            e.printStackTrace();
         }
      }

      public void viewAccepted(View new_view)
      {
         System.out.println("-- MySimpleReplier[" + channel.getLocalAddress() + "]: viewAccepted(" + new_view + ")");
      }

      public void block()
      {
         System.out.println("-- MySimpleReplier[" + channel.getLocalAddress() + "]: block()");
      }

      public void unblock()
      {
         System.out.println("-- MySimpleReplier[" + channel.getLocalAddress() + "]: unblock()");
      }
   }
   
   public static Test suite()
   {
      return new TestSuite(FlushTest.class);
   }

   public static void main(String[] args)
   {
      junit.textui.TestRunner.run(FlushTest.suite());
   }
}
