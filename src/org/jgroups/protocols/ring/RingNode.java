//$Id: RingNode.java,v 1.2 2004/03/30 06:47:20 belaban Exp $

package org.jgroups.protocols.ring;

import org.jgroups.stack.IpAddress;

import java.util.Vector;

public interface RingNode
{
   Object receiveToken(int timeout) throws TokenLostException;

   Object receiveToken() throws TokenLostException;

   void passToken(Object token) throws TokenLostException;

   IpAddress getTokenReceiverAddress();

   void reconfigure(Vector newMembers);

   void tokenArrived(Object token);
}
