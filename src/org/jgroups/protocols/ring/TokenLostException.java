//$Id: TokenLostException.java,v 1.3 2004/09/15 17:40:59 belaban Exp $

package org.jgroups.protocols.ring;

import org.jgroups.Address;

import java.io.InterruptedIOException;

public class TokenLostException extends InterruptedIOException
{

   public static final int UNDEFINED = 0;
   public static final int WHILE_RECEIVING = 1;
   public static final int WHILE_SENDING =2;

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
