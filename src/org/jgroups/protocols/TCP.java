// $Id: TCP.java,v 1.33 2006/06/24 13:17:32 smarlownovell Exp $

package org.jgroups.protocols;


import org.jgroups.Address;
import org.jgroups.blocks.ConnectionTable;
import org.jgroups.stack.IpAddress;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.Collection;


/**
 * TCP based protocol. Creates a server socket, which gives us the local address of this group member. For
 * each accept() on the server socket, a new thread is created that listens on the socket.
 * For each outgoing message m, if m.dest is in the ougoing hashtable, the associated socket will be reused
 * to send message, otherwise a new socket is created and put in the hashtable.
 * When a socket connection breaks or a member is removed from the group, the corresponding items in the
 * incoming and outgoing hashtables will be removed as well.<br>
 * This functionality is in ConnectionTable, which isT used by TCP. TCP sends messages using ct.send() and
 * registers with the connection table to receive all incoming messages.
 * @author Bela Ban
 */
public class TCP extends BasicTCP implements ConnectionTable.Receiver {
    private ConnectionTable ct=null;



   public TCP() {
   }

    public String getName() {
        return "TCP";
    }


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


    public String printConnections()     {return ct.toString();}


    /** Setup the Protocol instance acording to the configuration string */
    public boolean setProperties(Properties props) {
        String str;

        super.setProperties(props);
        str=props.getProperty("start_port");
        if(str != null) {
            start_port=Integer.parseInt(str);
            props.remove("start_port");
        }

        str=props.getProperty("end_port");
        if(str != null) {
            end_port=Integer.parseInt(str);
            props.remove("end_port");
        }

        str=props.getProperty("external_addr");
        if(str != null) {
            try {
                external_addr=InetAddress.getByName(str);
            }
            catch(UnknownHostException unknown) {
                if(log.isFatalEnabled()) log.fatal("(external_addr): host " + str + " not known");
                return false;
            }
            props.remove("external_addr");
        }

        str=props.getProperty("reaper_interval");
        if(str != null) {
            reaper_interval=Long.parseLong(str);
            props.remove("reaper_interval");
        }

        str=props.getProperty("conn_expire_time");
        if(str != null) {
            conn_expire_time=Long.parseLong(str);
            props.remove("conn_expire_time");
        }

        str=props.getProperty("sock_conn_timeout");
        if(str != null) {
            sock_conn_timeout=Integer.parseInt(str);
            props.remove("sock_conn_timeout");
        }

        str=props.getProperty("recv_buf_size");
        if(str != null) {
            recv_buf_size=Integer.parseInt(str);
            props.remove("recv_buf_size");
        }

        str=props.getProperty("send_buf_size");
        if(str != null) {
            send_buf_size=Integer.parseInt(str);
            props.remove("send_buf_size");
        }

        str=props.getProperty("skip_suspected_members");
        if(str != null) {
            skip_suspected_members=Boolean.valueOf(str).booleanValue();
            props.remove("skip_suspected_members");
        }

        str=props.getProperty("use_send_queues");
        if(str != null) {
            use_send_queues=Boolean.valueOf(str).booleanValue();
            props.remove("use_send_queues");
        }

        if(props.size() > 0) {
            log.error("the following properties are not recognized: " + props);
            return false;
        }
        return true;
    }

   public void send(Address dest, byte[] data, int offset, int length) throws Exception {
      ct.send(dest, data, offset, length);
   }

   public void retainAll(Collection members) {
      ct.retainAll(members);
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

    public void stop() {
        ct.stop();
        super.stop();
    }




   /**
    * @param reaperInterval
    * @param connExpireTime
    * @param bindAddress
    * @param startPort
    * @throws Exception
    * @return ConnectionTable
    * Sub classes overrides this method to initialize a different version of
    * ConnectionTable.
    */
   protected ConnectionTable getConnectionTable(long reaperInterval, long connExpireTime, InetAddress bindAddress,
                                                InetAddress externalAddress, int startPort, int endPort) throws Exception {
       ConnectionTable cTable;
       if(reaperInterval == 0 && connExpireTime == 0) {
           cTable=new ConnectionTable(this, bindAddress, externalAddress, startPort, endPort);
       }
       else {
           if(reaperInterval == 0) {
               reaperInterval=5000;
               if(warn) log.warn("reaper_interval was 0, set it to " + reaperInterval);
           }
           if(connExpireTime == 0) {
               connExpireTime=1000 * 60 * 5;
               if(warn) log.warn("conn_expire_time was 0, set it to " + connExpireTime);
           }
           cTable=new ConnectionTable(this, bindAddress, externalAddress, startPort, endPort,
                                      reaperInterval, connExpireTime);
       }
       return cTable;
   }




}
