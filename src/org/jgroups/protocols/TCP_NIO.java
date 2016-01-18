package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.PhysicalAddress;
import org.jgroups.annotations.Experimental;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.annotations.Unsupported;
import org.jgroups.blocks.AbstractBasicConnectionTable;
import org.jgroups.blocks.ConnectionTableNIO;

import java.net.InetAddress;
import java.util.Collection;

/**
 * Transport using NIO
 * @author Scott Marlow
 * @author Alex Fu
 * @author Bela Ban
 */
@Experimental @Unsupported @Deprecated
public class TCP_NIO extends AbstractBasicTCP implements AbstractBasicConnectionTable.Receiver
{

    /*
    * (non-Javadoc)
    *
    * @see org.jgroups.protocols.TCP#getConnectionTable(long, long)
    */
   protected ConnectionTableNIO getConnectionTable(long ri, long cet,
                                                   InetAddress b_addr, InetAddress bc_addr,
                                                   int external_port, int s_port, int e_port) throws Exception {
       ConnectionTableNIO retval=null;
       if (ri == 0 && cet == 0) {
           retval = new ConnectionTableNIO(this, b_addr, bc_addr, external_port, s_port, e_port, false );
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
           retval = new ConnectionTableNIO(this, b_addr, bc_addr, external_port, s_port, e_port, ri, cet, false);
       }
       retval.setThreadFactory(getThreadFactory());
       retval.setProcessorMaxThreads(getProcessorMaxThreads());
       retval.setProcessorQueueSize(getProcessorQueueSize());
       retval.setProcessorMinThreads(getProcessorMinThreads());
       retval.setProcessorKeepAliveTime(getProcessorKeepAliveTime());
       retval.setProcessorThreads(getProcessorThreads());
       retval.start();
       return retval;
   }

    public String printConnections()     {return ct.toString();}

    protected PhysicalAddress getPhysicalAddress() {
        return ct != null? (PhysicalAddress)ct.getLocalAddress() : null;
    }

   public void send(Address dest, byte[] data, int offset, int length) throws Exception {
      ct.send(dest, data, offset, length);
   }

   public void start() throws Exception {
       ct=getConnectionTable(reaper_interval,conn_expire_time,bind_addr,external_addr,external_port,bind_port,bind_port+port_range);
       ct.setUseSendQueues(use_send_queues);
       // ct.addConnectionListener(this);
       ct.setReceiveBufferSize(recv_buf_size);
       ct.setSendBufferSize(send_buf_size);
       ct.setSocketConnectionTimeout(sock_conn_timeout);
       ct.setPeerAddressReadTimeout(peer_addr_read_timeout);
       ct.setTcpNodelay(tcp_nodelay);
       ct.setLinger(linger);
       super.start();
   }

   public void retainAll(Collection<Address> members) {
      ct.retainAll(members);
   }

   public void stop() {
       ct.stop();
       super.stop();
   }

   public int getReaderThreads() { return reader_threads; }
   public int getWriterThreads() { return writer_threads; }
   public int getProcessorThreads() { return processor_threads; }
   public int getProcessorMinThreads() { return processor_minThreads;}
   public int getProcessorMaxThreads() { return processor_maxThreads;}
   public int getProcessorQueueSize() { return processor_queueSize; }
   public long getProcessorKeepAliveTime() { return processor_keepAliveTime; }
   @ManagedAttribute
   public int getOpenConnections()      {return ct.getNumConnections();}


   @Property
   private int reader_threads= 3;

   @Property
   private int writer_threads= 3;

   @Property
   private int processor_threads= 5;                    // PooledExecutor.createThreads()
   @Property
   private int processor_minThreads= 5;                 // PooledExecutor.setMinimumPoolSize()
   @Property
   private int processor_maxThreads= 5;                 // PooledExecutor.setMaxThreads()
   @Property
   private int processor_queueSize=100;                   // Number of queued requests that can be pending waiting
                                                            // for a background thread to run the request.
   @Property
   private long processor_keepAliveTime= Long.MAX_VALUE; // PooledExecutor.setKeepAliveTime( milliseconds);
                                                            // negative value used to mean (before 2.5 release) to wait forever,
                                                            // instead set to Long.MAX_VALUE to keep alive forever
   private ConnectionTableNIO ct;
}