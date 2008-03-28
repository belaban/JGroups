package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.blocks.BasicConnectionTable;
import org.jgroups.blocks.ConnectionTableNIO;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.PortsManager;

import java.net.InetAddress;
import java.util.Collection;
import java.util.Properties;

/**
 * Transport using NIO
 * @author Scott Marlow
 * @author Alex Fu
 * @author Bela Ban
 * @version $Id: TCP_NIO.java,v 1.21 2008/03/28 02:36:23 vlada Exp $
 */
public class TCP_NIO extends BasicTCP implements BasicConnectionTable.Receiver
{

   /*
   * (non-Javadoc)
   *
   * @see org.jgroups.protocols.TCP#getConnectionTable(long, long)
   */
   protected ConnectionTableNIO getConnectionTable(long ri, long cet,
                                                   InetAddress b_addr, InetAddress bc_addr,
                                                   int s_port, int e_port, PortsManager pm) throws Exception {
       ConnectionTableNIO retval=null;
       if (ri == 0 && cet == 0) {
           retval = new ConnectionTableNIO(this, b_addr, bc_addr, s_port, e_port, pm, false );
       }
       else {
           if (ri == 0) {
               ri = 5000;
               if(log.isWarnEnabled()) log.warn("reaper_interval was 0, set it to " + ri);
           }
           if (cet == 0) {
               cet = 1000 * 60 * 5;
               if(log.isWarnEnabled()) log.warn("conn_expire_time was 0, set it to " + cet);
           }
           retval = new ConnectionTableNIO(this, b_addr, bc_addr, s_port, e_port, pm, ri, cet, false);
       }
       retval.setThreadFactory(getProtocolStack().getThreadFactory());
       retval.setProcessorMaxThreads(getProcessorMaxThreads());
       retval.setProcessorQueueSize(getProcessorQueueSize());
       retval.setProcessorMinThreads(getProcessorMinThreads());
       retval.setProcessorKeepAliveTime(getProcessorKeepAliveTime());
       retval.setProcessorThreads(getProcessorThreads());
       retval.start();
       return retval;
   }

    public String printConnections()     {return ct.toString();}

   public void send(Address dest, byte[] data, int offset, int length) throws Exception {
      ct.send(dest, data, offset, length);
   }

   public void start() throws Exception {
       ct=getConnectionTable(reaper_interval,conn_expire_time,bind_addr,external_addr,bind_port,port_range,pm);
       ct.setUseSendQueues(use_send_queues);
       // ct.addConnectionListener(this);
       ct.setReceiveBufferSize(recv_buf_size);
       ct.setSendBufferSize(send_buf_size);
       ct.setSocketConnectionTimeout(sock_conn_timeout);
       ct.setPeerAddressReadTimeout(peer_addr_read_timeout);
       ct.setTcpNodelay(tcp_nodelay);
       ct.setLinger(linger);
       local_addr=ct.getLocalAddress();
       if(additional_data != null && local_addr instanceof IpAddress)
           ((IpAddress)local_addr).setAdditionalData(additional_data);
       super.start();
   }

   public void retainAll(Collection<Address> members) {
      ct.retainAll(members);
   }

   public void stop() {
       ct.stop();
       super.stop();
   }

   public String getName() {
        return "TCP_NIO";
    }

   @ManagedAttribute
   public int getReaderThreads() { return m_reader_threads; }
   @ManagedAttribute
   public int getWriterThreads() { return m_writer_threads; }
   @ManagedAttribute
   public int getProcessorThreads() { return m_processor_threads; }
   @ManagedAttribute
   public int getProcessorMinThreads() { return m_processor_minThreads;}
   @ManagedAttribute
   public int getProcessorMaxThreads() { return m_processor_maxThreads;}
   @ManagedAttribute
   public int getProcessorQueueSize() { return m_processor_queueSize; }
   @ManagedAttribute
   public long getProcessorKeepAliveTime() { return m_processor_keepAliveTime; }
   @ManagedAttribute
   public int getOpenConnections()      {return ct.getNumConnections();}

    

   /** Setup the Protocol instance acording to the configuration string */
   public boolean setProperties(Properties props) {
       String str;

       str=props.getProperty("reader_threads");
       if(str != null) {
          m_reader_threads=Integer.parseInt(str);
          props.remove("reader_threads");
       }

       str=props.getProperty("writer_threads");
       if(str != null) {
          m_writer_threads=Integer.parseInt(str);
          props.remove("writer_threads");
       }

       str=props.getProperty("processor_threads");
       if(str != null) {
          m_processor_threads=Integer.parseInt(str);
          props.remove("processor_threads");
       }

      str=props.getProperty("processor_minThreads");
      if(str != null) {
         m_processor_minThreads=Integer.parseInt(str);
         props.remove("processor_minThreads");
      }

      str=props.getProperty("processor_maxThreads");
      if(str != null) {
         m_processor_maxThreads =Integer.parseInt(str);
         props.remove("processor_maxThreads");
      }

      str=props.getProperty("processor_queueSize");
      if(str != null) {
         m_processor_queueSize=Integer.parseInt(str);
         props.remove("processor_queueSize");
      }

      str=props.getProperty("processor_keepAliveTime");
      if(str != null) {
         m_processor_keepAliveTime=Long.parseLong(str);
         props.remove("processor_keepAliveTime");
      }

      return super.setProperties(props);
   }

   private int m_reader_threads = 3;

   private int m_writer_threads = 3;

   private int m_processor_threads = 5;                    // PooledExecutor.createThreads()
   private int m_processor_minThreads = 5;                 // PooledExecutor.setMinimumPoolSize()
   private int m_processor_maxThreads = 5;                 // PooledExecutor.setMaxThreads()
   private int m_processor_queueSize=100;                   // Number of queued requests that can be pending waiting
                                                            // for a background thread to run the request.
   private long m_processor_keepAliveTime = Long.MAX_VALUE; // PooledExecutor.setKeepAliveTime( milliseconds);
                                                            // negative value used to mean (before 2.5 release) to wait forever,
                                                            // instead set to Long.MAX_VALUE to keep alive forever
   private ConnectionTableNIO ct;
}