// $Id: CausalDemo.java,v 1.8 2009/04/09 09:11:27 belaban Exp $
package org.jgroups.demos;

import org.jgroups.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.Serializable;
import java.util.Random;
import java.util.Vector;



/**
 * Simple causal demo where each member bcast a consecutive letter from the
 * alphabet and picks the next member to transmit the next letter. Start a
 * few instances of CausalDemo and pass a paramter "-start" to a CausalDemo
 * that initiates transmission of a letter A. All participanting members should
 * have correct alphabet. DISCARD layer has been added to simulate lost messages,
 * thus forcing delaying of delivery of a certain alphabet letter until the causally
 * prior one has been received.  Remove CAUSAL from the stack and witness how FIFO
 * alone doesn't provide this guarantee.
 *
 * @author Vladimir Blagojevic
 */
public class CausalDemo implements Runnable
{
   private Channel channel;
   private final Vector alphabet = new Vector();
   private boolean starter = false;
   private int doneCount=0;
   private Log log=LogFactory.getLog(getClass());

   private final String props = "causal.xml";

   public CausalDemo(boolean start)
   {
      starter = start;
   }

   public String getNext(String c)
   {
      char letter = c.charAt(0);
      return new String(new char[]{++letter});
   }

   public void listAlphabet()
   {
      System.out.println(alphabet);
   }

   public void run()
   {
      Object obj;
      Message msg;
      Random r = new Random();

      try
      {
         channel = new JChannel(props);
         channel.connect("CausalGroup");
         System.out.println("View:" + channel.getView());
         if (starter)
            channel.send(new Message(null, null, new CausalMessage("A", (Address) channel.getView().getMembers().get(0))));

      }
      catch (Exception e)
      {
         System.out.println("Could not conec to channel");
      }

      try
      {
         Runtime.getRuntime().addShutdownHook(
               new Thread("Shutdown cleanup thread")
               {
                  public void run()
                  {

                     listAlphabet();
                     channel.disconnect();
                     channel.close();
                  }
               }
         );
      }
      catch (Exception e)
      {
         System.out.println("Exception while shutting down" + e);
      }

      while (true)
      {
         try
         {
            CausalMessage cm = null;
            obj = channel.receive(0); // no timeout
            if (obj instanceof Message)
            {
               msg = (Message) obj;
               cm = (CausalMessage) msg.getObject();
               Vector members = channel.getView().getMembers();
               String receivedLetter = cm.message;

               if("Z".equals(receivedLetter))
               {
                  channel.send(new Message(null, null, new CausalMessage("done", null)));
               }
               if("done".equals(receivedLetter))
               {
                  if(++doneCount >= members.size())
                  {
                     System.exit(0);
                  }
                  continue;
               }

               alphabet.add(receivedLetter);
               listAlphabet();

               //am I chosen to transmit next letter?
               if (cm.member.equals(channel.getAddress()))
               {
                  int nextTarget = r.nextInt(members.size());

                  //chose someone other than yourself
                  while (nextTarget == members.indexOf(channel.getAddress()))
                  {
                     nextTarget = r.nextInt(members.size());
                  }
                  Address next = (Address) members.get(nextTarget);
                  String nextChar = getNext(receivedLetter);
                  if (nextChar.compareTo("Z") < 1)
                  {
                     System.out.println("Sending " + nextChar);
                     channel.send(new Message(null, null, new CausalMessage(nextChar, next)));
                  }
               }
            }
         }
         catch (ChannelNotConnectedException conn)
         {
            break;
         }
         catch (Exception e)
         {
            log.error(e);
         }
      }

   }


   public static void main(String args[])
   {
      CausalDemo test = null;
      boolean    start=false;

      for(int i=0; i < args.length; i++) {
	  if("-help".equals(args[i])) {
	      System.out.println("CausalDemo [-help] [-start]");
	      return;
	  }
	  if("-start".equals(args[i])) {
	      start=true;
	      continue;
	  }
      }

      //if parameter start is passed , start the demo
      test = new CausalDemo(start);
      try
      {
         new Thread(test).start();
      }
      catch (Exception e)
      {
         System.err.println(e);
      }

   }

}

class CausalMessage implements Serializable
{
   public final String message;
   public final Address member;

   public CausalMessage(String message, Address member)
   {
      this.message = message;
      this.member = member;
   }

   public String toString()
   {
      return "CausalMessage[" + message + '=' + message + "member=" + member + ']';
   }

}
