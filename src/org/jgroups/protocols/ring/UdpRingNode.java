//$Id: UdpRingNode.java,v 1.2 2004/01/16 07:45:36 belaban Exp $

package org.jgroups.protocols.ring;

import java.io.Serializable;
import java.io.IOException;
import java.util.Vector;
import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.protocols.TOTAL_TOKEN;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.RpcProtocol;


public class UdpRingNode implements RingNode
{

   Address thisNode,nextNode;
   RpcProtocol rpcProtocol;
   Object token;
   Object mutex = new Object();
   TOTAL_TOKEN.RingTokenHeader tokenHeader;
   boolean tokenInStack = false;


   public UdpRingNode(RpcProtocol owner, Address memberAddress)
   {
      rpcProtocol = owner;
      thisNode = memberAddress;
      nextNode = null;
      tokenHeader = new TOTAL_TOKEN.RingTokenHeader();
   }

   public IpAddress getTokenReceiverAddress()
   {
      return (IpAddress) thisNode;
   }

   public synchronized void tokenArrived(Object token)
   {
      tokenInStack = true;
      this.token = token;
      this.notifyAll();
   }

   public Object receiveToken(int timeout) throws TokenLostException
   {
      Address wasNext = nextNode;

      try
      {

         synchronized (this)
         {
            while (!tokenInStack)
            {
               wait(timeout);
               break;
            }

            //we haven't received token for the time of timeout
            if (!tokenInStack)
            {
               throw new TokenLostException("Token wait timout expired", null,
                                            wasNext, TokenLostException.WHILE_RECEIVING);
            }
         }
      }
      catch (InterruptedException ie)
      {
         throw new TokenLostException("Token thread interrupted", ie,
                                      wasNext, TokenLostException.WHILE_RECEIVING);
      }
      return token;
   }

   public Object receiveToken() throws TokenLostException
   {
      return receiveToken(0);
   }

   public synchronized void passToken(Object token)
   {
       Message t = null;
       try {
           t=new Message(nextNode, thisNode, (Serializable) token);
           t.putHeader(TOTAL_TOKEN.prot_name, tokenHeader);
           rpcProtocol.passDown(new Event(Event.MSG, t));
           tokenInStack = false;
       }
       catch(IOException e) {
           e.printStackTrace();
       }
   }


   public synchronized void reconfigure(Vector newMembers)
   {
      if (isNextNeighbourChanged(newMembers))
      {
         nextNode = getNextNode(newMembers);
      }
   }

   private boolean isNextNeighbourChanged(Vector newMembers)
   {
      Address oldNeighbour = nextNode;
      Address newNeighbour = getNextNode(newMembers);
      return !(newNeighbour.equals(oldNeighbour));
   }

   private Address getNextNode(Vector otherNodes)
   {
      int myIndex = otherNodes.indexOf(thisNode);
      return (myIndex == otherNodes.size() - 1)?
              (Address) otherNodes.firstElement():
              (Address) otherNodes.elementAt(myIndex + 1);

   }
}
