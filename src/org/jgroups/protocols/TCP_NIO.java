package org.jgroups.protocols;

import org.jgroups.blocks.ConnectionTableNIO;
import org.jgroups.Address;
import org.jgroups.stack.IpAddress;

import java.net.InetAddress;
import java.util.Properties;
import java.util.Collection;

/**
 * Transport using NIO
 * @author Scott Marlow
 * @author Alex Fu
 * @author Bela Ban
 * @version $Id: TCP_NIO.java,v 1.7 2006/06/24 13:17:32 smarlownovell Exp $
 */
public class TCP_NIO extends BasicTCP implements ConnectionTableNIO.Receiver
{

   /*
   * (non-Javadoc)
   *
   * @see org.jgroups.protocols.TCP#getConnectionTable(long, long)
   */
   protected ConnectionTableNIO getConnectionTable(long ri, long cet,
                                                   InetAddress b_addr, InetAddress bc_addr, int s_port, int e_port) throws Exception {

      if (ri == 0 && cet == 0) {
         ct = new ConnectionTableNIO(this, b_addr, bc_addr, s_port, e_port );
      } else {
         if (ri == 0) {
            ri = 5000;
            if(warn) log.warn("reaper_interval was 0, set it to "
                  + ri);
         }
         if (cet == 0) {
            cet = 1000 * 60 * 5;
            if(warn) log.warn("conn_expire_time was 0, set it to "
                  + cet);
         }
         ct = new ConnectionTableNIO(this, b_addr, bc_addr, s_port, e_port, ri, cet);
      }
      return ct;
   }

   public String printConnections()     {return ct.toString();}

   public void send(Address dest, byte[] data, int offset, int length) throws Exception {
      ct.send(dest, data, offset, length);
   }

   public void start() throws Exception {
       ct=getConnectionTable(reaper_interval,conn_expire_time,bind_addr,external_addr,start_port,end_port);
       ct.setUseSendQueues(use_send_queues);
       // ct.addConnectionListener(this);
       ct.setReceiveBufferSize(recv_buf_size);
       ct.setSendBufferSize(send_buf_size);
       ct.setSocketConnectionTimeout(sock_conn_timeout);
       local_addr=ct.getLocalAddress();
       if(additional_data != null && local_addr instanceof IpAddress)
           ((IpAddress)local_addr).setAdditionalData(additional_data);
       super.start();
   }

   public void retainAll(Collection members) {
      ct.retainAll(members);
   }

   public void stop() {
       ct.stop();
       super.stop();
   }

   public String getName() {
        return "TCP_NIO";
    }

   public int getReaderThreads() { return m_reader_threads; }
   public int getWriterThreads() { return m_writer_threads; }
   public int getProcessorThreads() { return m_processor_threads; }
   public int getProcessorMinThreads() { return m_processor_minThreads;}
   public int getProcessorMaxThreads() { return m_processor_maxThreads;}
   public int getProcessorQueueSize() { return m_processor_queueSize; }
   public int getProcessorKeepAliveTime() { return m_processor_keepAliveTime; }

   public int getOpenConnections()      {return ct.getNumConnections();}
   public InetAddress getBindAddr() {return bind_addr;}
   public void setBindAddr(InetAddress bind_addr) {this.bind_addr=bind_addr;}
   public int getStartPort() {return start_port;}
   public void setStartPort(int start_port) {this.start_port=start_port;}
   public int getEndPort() {return end_port;}
   public void setEndPort(int end_port) {this.end_port=end_port;}
   public long getReaperInterval() {return reaper_interval;}
   public void setReaperInterval(long reaper_interval) {this.reaper_interval=reaper_interval;}
   public long getConnExpireTime() {return conn_expire_time;}
   public void setConnExpireTime(long conn_expire_time) {this.conn_expire_time=conn_expire_time;}
   public boolean isLoopback() {return loopback;}
   public void setLoopback(boolean loopback) {this.loopback=loopback;}

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
         m_processor_keepAliveTime=Integer.parseInt(str);
         props.remove("processor_keepAliveTime");
      }

      return super.setProperties(props);
   }

   private int m_reader_threads = 8;

   private int m_writer_threads = 8;

   private int m_processor_threads = 10;                    // PooledExecutor.createThreads()
   private int m_processor_minThreads = 10;                 // PooledExecutor.setMinimumPoolSize()
   private int m_processor_maxThreads = 10;                 // PooledExecutor.setMaxThreads()
   private int m_processor_queueSize=100;                   // Number of queued requests that can be pending waiting
                                                            // for a background thread to run the request.
   private int m_processor_keepAliveTime = -1;              // PooledExecutor.setKeepAliveTime( milliseconds);
                                                            // A negative value means to wait forever
   private ConnectionTableNIO ct;
}