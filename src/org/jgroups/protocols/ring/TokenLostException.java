//$Id: TokenLostException.java,v 1.2 2004/03/30 06:47:20 belaban Exp $

package org.jgroups.protocols.ring;

import org.jgroups.Address;

import java.io.InterruptedIOException;

public class TokenLostException extends InterruptedIOException
{

   public static int UNDEFINED = 0;
   public static int WHILE_RECEIVING = 1;
   public static int WHILE_SENDING =2;

   protected Address failedNode;
   protected Throwable cause;
   protected int mode = UNDEFINED;

   public TokenLostException()
   {
      super();
   }
   public TokenLostException(String message)
   {
      super(message);
   }

   public TokenLostException(String message,Throwable cause,Address failedNode, int mode)
   {
      super();
      this.failedNode = failedNode;
      this.mode = mode;
   }

   public int getFailureMode()
   {
      return mode;
   }

   public Address getFailedNode()
   {
      return failedNode;
   }

   public String toString()
   {
      StringBuffer buf = new StringBuffer();
      buf.append("TokenLostException[");
      buf.append("cause=").append(cause);
      buf.append(",failedNode=").append(failedNode);

      buf.append(']');
      return buf.toString();
   }

}
