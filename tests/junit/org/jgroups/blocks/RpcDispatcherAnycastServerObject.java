package org.jgroups.blocks;

import org.jgroups.MessageListener;
import org.jgroups.tests.ChannelTestBase.ChannelRetrievable;
import org.jgroups.MembershipListener;
import org.jgroups.Channel;
import org.jgroups.ChannelException;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.Address;
import org.jgroups.util.RspList;
import org.jgroups.util.Rsp;

import java.util.Vector;
import java.util.Map;
import java.util.Iterator;

public class RpcDispatcherAnycastServerObject implements MessageListener, ChannelRetrievable, MembershipListener
{
   int i = 0;
   private Channel c;
   private RpcDispatcher d;

   public RpcDispatcherAnycastServerObject(Channel channel) throws ChannelException
   {
      c = channel;      
      d = new RpcDispatcher(c, this, this, this);
      c.connect("test");
   }
   
   public Channel getChannel() {
       return c;
   }

   public void doSomething()
   {
      System.out.println("doSomething invoked on " + c.getLocalAddress() + ".  i = " + i);
      i++;
      System.out.println("Now i = " + i);
   }

   public void callRemote(boolean useAnycast, boolean excludeSelf)
   {
       // we need to copy the vector, otherwise the modification below will throw an exception because the underlying
       // vector is unmodifiable
      Vector v = new Vector(c.getView().getMembers());
      if (excludeSelf) v.remove(c.getLocalAddress());
      RspList rsps=d.callRemoteMethods(v, "doSomething", new Object[]{}, new Class[]{}, GroupRequest.GET_ALL, 10000, useAnycast);
      Map.Entry entry;
       for(Iterator it=rsps.entrySet().iterator(); it.hasNext();) {
           entry=(Map.Entry)it.next();
           Address member=(Address)entry.getKey();
           Rsp rsp=(Rsp)entry.getValue();
           if(!rsp.wasReceived())
               throw new RuntimeException("response from " + member + " was not received, rsp=" + rsp);
       }

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