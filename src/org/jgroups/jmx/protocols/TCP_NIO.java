package org.jgroups.jmx.protocols;

/**
 * @author Scott Marlow
 * @version $Id: TCP_NIO.java,v 1.1 2005/11/22 13:58:18 smarlownovell Exp $
 */
public class TCP_NIO  extends TCP implements TCP_NIOMBean {

   org.jgroups.protocols.TCP_NIO p;
   public TCP_NIO() {
   }

   public TCP_NIO(org.jgroups.stack.Protocol p) {
       super(p);
       this.p=(org.jgroups.protocols.TCP_NIO)p;
   }

   public void attachProtocol(org.jgroups.stack.Protocol p) {
       super.attachProtocol(p);
       this.p=(org.jgroups.protocols.TCP_NIO)p;
   }

   public int getReaderThreads() {
      return p.getReaderThreads();
   }

   public int getWriterThreads() {
      return p.getWriterThreads();
   }

   public int getProcessorThreads() {
      return p.getProcessorThreads();
   }

   public int getProcessorMinThreads() {
      return p.getProcessorMinThreads();
   }

   public int getProcessorMaxThreads() {
      return p.getProcessorMaxThreads();
   }

   public int getProcessorQueueSize() {
      return p.getProcessorQueueSize();
   }

   public int getProcessorKeepAliveTime() {
      return p.getProcessorKeepAliveTime();
   }

}
