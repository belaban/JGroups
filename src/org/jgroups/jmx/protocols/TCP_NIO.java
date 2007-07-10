package org.jgroups.jmx.protocols;

/**
 * @author Scott Marlow
 * @version $Id: TCP_NIO.java,v 1.3 2007/07/10 13:02:50 belaban Exp $
 */
public class TCP_NIO  extends TCP implements TCP_NIOMBean {

   org.jgroups.protocols.TCP_NIO my_p;
   public TCP_NIO() {
   }

   public TCP_NIO(org.jgroups.stack.Protocol p) {
       super(p);
       this.my_p=(org.jgroups.protocols.TCP_NIO)p;
   }

   public void attachProtocol(org.jgroups.stack.Protocol p) {
       this.my_p=(org.jgroups.protocols.TCP_NIO)p;
   }

   public int getReaderThreads() {
      return my_p.getReaderThreads();
   }

   public int getWriterThreads() {
      return my_p.getWriterThreads();
   }

   public int getProcessorThreads() {
      return my_p.getProcessorThreads();
   }

   public int getProcessorMinThreads() {
      return my_p.getProcessorMinThreads();
   }

   public int getProcessorMaxThreads() {
      return my_p.getProcessorMaxThreads();
   }

   public int getProcessorQueueSize() {
      return my_p.getProcessorQueueSize();
   }

   public long getProcessorKeepAliveTime() {
      return my_p.getProcessorKeepAliveTime();
   }

}
