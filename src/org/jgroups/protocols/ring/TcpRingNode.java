//$Id: TcpRingNode.java,v 1.1.1.1 2003/09/09 01:24:11 belaban Exp $

package org.jgroups.protocols.ring;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Vector;
import org.jgroups.Address;
import org.jgroups.SuspectedException;
import org.jgroups.TimeoutException;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.log.Trace;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.RpcProtocol;
import org.jgroups.util.Util;


public class TcpRingNode implements RingNode
{

   ServerSocket tokenReceiver;
   Socket previous,next;
   Address thisNode,nextNode;
   ObjectInputStream ios;
   ObjectOutputStream oos;
   RpcProtocol rpcProtocol;
   boolean failedOnTokenLostException = false;

   Object socketMutex = new Object();

   public TcpRingNode(RpcProtocol owner, Address memberAddress)
   {
      tokenReceiver = Util.createServerSocket(12000);
      rpcProtocol = owner;
      thisNode = memberAddress;
      nextNode = null;
   }

   public IpAddress getTokenReceiverAddress()
   {
      return new IpAddress(tokenReceiver.getLocalPort());
   }

   public Object receiveToken(int timeout) throws TokenLostException
   {
      RingToken token = null;
      Address wasNextNode = nextNode;
      try
      {
         if (previous == null)
         {
            previous = tokenReceiver.accept();
            ios = new ObjectInputStream((previous.getInputStream()));

         }
         previous.setSoTimeout(timeout);
         token = new RingToken();
         token.readExternal(ios);
      }
      catch (InterruptedIOException io)
      {
         //read was blocked for more than a timeout, assume token lost
         throw new TokenLostException(io.getMessage(), io, wasNextNode, TokenLostException.WHILE_RECEIVING);
      }
      catch (ClassNotFoundException cantHappen)
      {
      }
      catch (IOException ioe)
      {
         closeSocket(previous);
         previous = null;
         if (ios != null)
         {
            try
            {
               ios.close();
            }
            catch (IOException ignored)
            {
            }
         }

         token = (RingToken) receiveToken(timeout);
      }
      return token;
   }

   public Object receiveToken() throws TokenLostException
   {
      return receiveToken(0);
   }

   public void passToken(Object token) throws TokenLostException
   {
      synchronized (socketMutex)
      {
         try
         {
            ((RingToken) token).writeExternal(oos);
            oos.flush();
            oos.reset();
         }
         catch (IOException e)
         {
            e.printStackTrace();
            //something went wrong with the next neighbour while it was receiving
            //token, assume token lost
            throw new TokenLostException(e.getMessage(), e, nextNode, TokenLostException.WHILE_SENDING);
         }

      }
   }

   public void tokenArrived(Object token)
   {
      //not needed , callback for udp ring
   }

   public void reconfigureAll(Vector newMembers)
   {

   }


   public void reconfigure(Vector newMembers)
   {

      if (isNextNeighbourChanged(newMembers))
      {
         IpAddress tokenRecieverAddress = null;
         synchronized (socketMutex)
         {
            nextNode = getNextNode(newMembers);
            if (Trace.trace)
               Trace.info("TcpRingNode.reconfigure()", " next node " + nextNode);

            try
            {
               tokenRecieverAddress = (IpAddress) rpcProtocol.callRemoteMethod(nextNode, "getTokenReceiverAddress", GroupRequest.GET_FIRST, 0);
            }
            catch (TimeoutException tim)
            {
               Trace.error("TcpRingNode.reconfigure()", " timeouted while doing rpc call getTokenReceiverAddress" + tim);
               tim.printStackTrace();
            }
            catch (SuspectedException sus)
            {
               Trace.error("TcpRingNode.reconfigure()", " suspected node while doing rpc call getTokenReceiverAddress" + sus);
               sus.printStackTrace();
            }
            try
            {
               closeSocket(next);
               next = new Socket(tokenRecieverAddress.getIpAddress(), tokenRecieverAddress.getPort());
               next.setTcpNoDelay(true);
               oos = new ObjectOutputStream(next.getOutputStream());
            }
            catch (IOException ioe)
            {
               Trace.error("TcpRingNode.reconfigure()", "could not connect to next node " + ioe);
               ioe.printStackTrace();
            }
         }
      }
   }

   private void closeSocket(Socket socket)
   {
      if (socket == null) return;
      try
      {
         socket.close();
      }
      catch (IOException ioe)
      {
         ioe.printStackTrace();
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
