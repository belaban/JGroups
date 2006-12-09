package org.jgroups.blocks;

import org.jgroups.MessageListener;
import org.jgroups.MembershipListener;
import org.jgroups.Channel;
import org.jgroups.ChannelException;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.Address;

import java.util.Vector;

public class RpcDispatcherAnycastTestServerObject implements MessageListener, MembershipListener
{
   int i = 0;
   private Channel c;
   private RpcDispatcher d;

   public RpcDispatcherAnycastTestServerObject() throws ChannelException
   {
      c = new JChannel();
      c.connect("TEST");
      d = new RpcDispatcher(c, this, this, this);
   }

   public void doSomething()
   {
      System.out.println("doSomething invoked on " + c.getLocalAddress() + ".  i = " + i);
      i++;
      System.out.println("Now i = " + i);
   }

   public void callRemote(boolean useAnycast, boolean excludeSelf)
   {
      Vector v = c.getView().getMembers();
      if (excludeSelf) v.remove(c.getLocalAddress());
      d.callRemoteMethods(v, "doSomething", new Object[]{}, new Class[]{}, GroupRequest.GET_ALL, 10000, useAnycast);
   }

   public void shutdown()
   {
      c.close();
   }


   public void receive(Message msg)
   {
   }

   public byte[] getState()
   {
      return new byte[0];
   }

   public void setState(byte[] state)
   {
   }

   public void viewAccepted(View new_view)
   {
   }

   public void suspect(Address suspected_mbr)
   {
   }

   public void block()
   {
   }
}