package org.jgroups.jmx.protocols;

/**
 * @author Scott Marlow
 * @version $Id: TCP_NIOMBean.java,v 1.1 2005/11/22 13:58:18 smarlownovell Exp $
 */
public interface TCP_NIOMBean extends TCPMBean {

   int getReaderThreads();
   int getWriterThreads();
   int getProcessorThreads();
   int getProcessorMinThreads();
   int getProcessorMaxThreads();
   int getProcessorQueueSize();
   int getProcessorKeepAliveTime();
}
