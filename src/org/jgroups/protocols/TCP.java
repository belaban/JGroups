// $Id: TCP.java,v 1.52 2008/07/08 20:01:55 vlada Exp $

package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.blocks.ConnectionTable;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.PortsManager;

import java.net.InetAddress;
import java.util.Collection;

/**
 * TCP based protocol. Creates a server socket, which gives us the local address
 * of this group member. For each accept() on the server socket, a new thread is
 * created that listens on the socket. For each outgoing message m, if m.dest is
 * in the outgoing hash table, the associated socket will be reused to send
 * message, otherwise a new socket is created and put in the hash table. When a
 * socket connection breaks or a member is removed from the group, the
 * corresponding items in the incoming and outgoing hash tables will be removed
 * as well.
 * <p>
 * 
 * This functionality is in ConnectionTable, which is used by TCP. TCP sends
 * messages using ct.send() and registers with the connection table to receive
 * all incoming messages.
 * 
 * @author Bela Ban
 */
public class TCP extends BasicTCP implements ConnectionTable.Receiver { // , BasicConnectionTable.ConnectionListener {
    private ConnectionTable ct=null;



   public TCP() {
   }

    public String getName() {
        return "TCP";
    }


    @ManagedAttribute
    public int getOpenConnections()      {return ct.getNumConnections();}
    @ManagedOperation
    public String printConnections()     {return ct.toString();}

    public void send(Address dest, byte[] data, int offset, int length) throws Exception {
        ct.send(dest, data, offset, length);
    }

    public void retainAll(Collection<Address> members) {
        ct.retainAll(members);
    }

    public void start() throws Exception {
        ct=getConnectionTable(reaper_interval, conn_expire_time, bind_addr, external_addr, bind_port, port_range, pm);       
        ct.setUseSendQueues(use_send_queues);
        ct.setSendQueueSize(send_queue_size);
        ct.setReceiveBufferSize(recv_buf_size);
        ct.setSendBufferSize(send_buf_size);
        ct.setSocketConnectionTimeout(sock_conn_timeout);
        ct.setPeerAddressReadTimeout(peer_addr_read_timeout);
        ct.setTcpNodelay(tcp_nodelay);
        ct.setLinger(linger);
        local_addr=ct.getLocalAddress();
        if(additional_data != null && local_addr instanceof IpAddress)
            ((IpAddress)local_addr).setAdditionalData(additional_data);
        
        //http://jira.jboss.com/jira/browse/JGRP-626
        //we first start threads in TP
        super.start();
    }

    public void stop() {
        //just in case...
        ct.stop();
        super.stop();
    }

    protected void handleConnect() throws Exception {
        boolean startConnectionTable=true;
        if(isSingleton()) {
            startConnectionTable=(connect_count == 0)? true : false;
            super.handleConnect();
        }
        if(startConnectionTable)
            ct.start();
    }

    protected void handleDisconnect() {
        boolean stopConnectionTable=true;
        if(isSingleton()) {
            super.handleDisconnect();
            stopConnectionTable=(connect_count == 0)? true : false;
        }
        if(stopConnectionTable)
            ct.stop();
    }

    /**
     * @param reaperInterval
     * @param connExpireTime
     * @param bindAddress
     * @param startPort
     * @throws Exception
     * @return ConnectionTable Sub classes overrides this method to initialize a
     *         different version of ConnectionTable.
     */
    protected ConnectionTable getConnectionTable(long reaperInterval,
                                                 long connExpireTime,
                                                 InetAddress bindAddress,
                                                 InetAddress externalAddress,
                                                 int startPort,
                                                 int endPort,
                                                 PortsManager pm) throws Exception {
        ConnectionTable cTable;
        if(reaperInterval == 0 && connExpireTime == 0) {
            cTable=new ConnectionTable(this, bindAddress, externalAddress, startPort, endPort, pm);
        }
        else {
            if(reaperInterval == 0) {
                reaperInterval=5000;
                if(log.isWarnEnabled())
                    log.warn("reaper_interval was 0, set it to " + reaperInterval);
            }
            if(connExpireTime == 0) {
                connExpireTime=1000 * 60 * 5;
                if(log.isWarnEnabled())
                    log.warn("conn_expire_time was 0, set it to " + connExpireTime);
            }
            cTable=new ConnectionTable(this, bindAddress, externalAddress, startPort, endPort, reaperInterval, connExpireTime, pm);
        }
        cTable.setThreadFactory(getThreadFactory());
        return cTable;
    }
}
