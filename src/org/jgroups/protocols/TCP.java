// $Id: TCP.java,v 1.59 2009/09/06 13:51:07 belaban Exp $

package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.PhysicalAddress;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.blocks.TCPConnectionMap;

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
public class TCP extends BasicTCP implements TCPConnectionMap.Receiver {
    
    private TCPConnectionMap ct=null;

    public TCP() {}


    @ManagedAttribute
    public int getOpenConnections() {
        return ct.getNumConnections();
    }

    @ManagedOperation
    public String printConnections() {
        return ct.toString();
    }

    public void send(Address dest, byte[] data, int offset, int length) throws Exception {
        ct.send(dest, data, offset, length);
    }

    public void retainAll(Collection<Address> members) {
        ct.retainAll(members);
    }

    public void start() throws Exception {
        ct=createConnectionTable(reaper_interval,
                              conn_expire_time,
                              bind_addr,
                              external_addr,
                              bind_port,
                              bind_port+port_range
                              );
        ct.setReceiveBufferSize(recv_buf_size);      
        ct.setSendQueueSize(send_queue_size);
        ct.setUseSendQueues(use_send_queues);
        ct.setSendBufferSize(send_buf_size);
        ct.setSocketConnectionTimeout(sock_conn_timeout);
        ct.setTcpNodelay(tcp_nodelay);
        ct.setLinger(linger);

        // we first start threads in TP (http://jira.jboss.com/jira/browse/JGRP-626)
        super.start();
    }
    
    public void stop() {
        if(log.isDebugEnabled()) log.debug("closing sockets and stopping threads");
        ct.stop(); //not needed, but just in case
        super.stop();
    }


    protected void handleConnect() throws Exception {
        if(isSingleton()) {
            if(connect_count == 0) {
                ct.start();
            }
            super.handleConnect();
        }
        else
            ct.start();
    }

    protected void handleDisconnect() {
        if(isSingleton()) {
            super.handleDisconnect();
            if(connect_count == 0) {
                ct.stop();
            }
        }
        else
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
    protected TCPConnectionMap createConnectionTable(long reaperInterval,
                                                                        long connExpireTime,
                                                                        InetAddress bindAddress,
                                                                        InetAddress externalAddress,
                                                                        int startPort,
                                                                        int endPort
                                                                        ) throws Exception {
        TCPConnectionMap cTable;
        if(reaperInterval == 0 && connExpireTime == 0) {
            cTable=new TCPConnectionMap(getThreadFactory(),
                                                              this,
                                                              bindAddress,
                                                              externalAddress,
                                                              startPort,
                                                              endPort
                                                              );
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
            cTable=new TCPConnectionMap(getThreadFactory(),
                                                              this,
                                                              bindAddress,
                                                              externalAddress,
                                                              startPort,
                                                              endPort,
                                                              reaperInterval,
                                                              connExpireTime
                                                              );
        }

        return cTable;
    }

    protected PhysicalAddress getPhysicalAddress() {
        return ct != null? (PhysicalAddress)ct.getLocalAddress() : null;
    }
}
