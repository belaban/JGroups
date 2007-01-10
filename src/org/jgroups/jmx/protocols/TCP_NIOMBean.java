package org.jgroups.jmx.protocols;

/**
 * @author Scott Marlow
 * @version $Id: TCP_NIOMBean.java,v 1.2 2007/01/10 19:38:18 smarlownovell Exp $
 */
public interface TCP_NIOMBean extends TCPMBean {

   int getReaderThreads();
   int getWriterThreads();
   int getProcessorThreads();
   int getProcessorMinThreads();
   int getProcessorMaxThreads();
   int getProcessorQueueSize();
   long getProcessorKeepAliveTime();
}
