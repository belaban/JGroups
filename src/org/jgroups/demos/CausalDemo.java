// $Id: CausalDemo.java,v 1.2 2004/03/30 06:47:16 belaban Exp $
package org.jgroups.demos;

import org.jgroups.*;

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
   private Thread mythread;
   private Vector alphabet = new Vector();
   private boolean starter = false;
   private int doneCount=0;

   private String props = "UDP(mcast_addr=228.8.8.8;mcast_port=45566;ip_ttl=32;" +
           "mcast_send_buf_size=150000;mcast_recv_buf_size=80000):" +
           "PING(timeout=2000;num_initial_members=5):" +
           "DISCARD(up=0.05;excludeitself=true):" +
           "FD_SOCK:" +
           "VERIFY_SUSPECT(timeout=1500):" +
           "pbcast.NAKACK(gc_lag=50;retransmit_timeout=300,600,1200,2400,4800,9600):" +
           "UNICAST(timeout=5000):" +
           "pbcast.STABLE(desired_avg_gossip=2000):" +
           "FRAG(frag_size=4096;down_thread=false;up_thread=false):" +
           "pbcast.GMS(join_timeout=5000;join_retry_timeout=2000;" +
           "shun=false;print_local_addr=true):CAUSAL";

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
               String receivedLetter = new String(cm.message);

               if(receivedLetter.equals("Z"))
               {
                  channel.send(new Message(null, null, new CausalMessage("done", null)));
               }
               if(receivedLetter.equals("done"))
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
               if (cm.member.equals(channel.getLocalAddress()))
               {
                  int nextTarget = r.nextInt(members.size());

                  //chose someone other than yourself
                  while (nextTarget == members.indexOf(channel.getLocalAddress()))
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
            System.err.println(e);
         }
      }

   }


   public static void main(String args[])
   {
      CausalDemo test = null;
      boolean    start=false;

      for(int i=0; i < args.length; i++) {
	  if(args[i].equals("-help")) {
	      System.out.println("CausalDemo [-help] [-start]");
	      return;
	  }
	  if(args[i].equals("-start")) {
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
   public String message;
   public Address member;

   public CausalMessage(String message, Address member)
   {
      this.message = message;
      this.member = member;
   }

   public String toString()
   {
      return "CausalMessage[" + message + "=" + message + "member=" + member + "]";
   }

}
