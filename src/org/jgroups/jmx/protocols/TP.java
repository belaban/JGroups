package org.jgroups.jmx.protocols;

import org.jgroups.stack.Protocol;
import org.jgroups.Address;

import java.net.UnknownHostException;
import java.util.List;

import EDU.oswego.cs.dl.util.concurrent.PooledExecutor;
import EDU.oswego.cs.dl.util.concurrent.BoundedLinkedQueue;

/**
 * @author Bela Ban
 * @version $Id: TP.java,v 1.8 2006/12/11 13:43:41 belaban Exp $
 */
public class TP extends org.jgroups.jmx.Protocol implements TPMBean {
    org.jgroups.protocols.TP tp;

    public TP() {
    }

    public TP(Protocol p) {
        super(p);
        tp=(org.jgroups.protocols.TP)p;
    }

    public void attachProtocol(Protocol p) {
        super.attachProtocol(p);
        tp=(org.jgroups.protocols.TP)p;
    }

    public long getMessagesSent() {
        return tp.getNumMessagesSent();
    }

    public long getMessagesReceived() {
        return tp.getNumMessagesReceived();
    }

    public long getBytesSent() {
        return tp.getNumBytesSent();
    }

    public long getBytesReceived() {
        return tp.getNumBytesReceived();
    }

    public Address getLocalAddress() {
        return tp.getLocalAddress();
    }

    public String getBindAddress() {
        return tp.getBindAddress();
    }

    public String getChannelName() {
        return tp.getChannelName();
    }

    public void setBindAddress(String bind_address) throws UnknownHostException {
        tp.setBindAddress(bind_address);
    }

    public boolean isReceiveOnAllInterfaces() {
        return tp.isReceiveOnAllInterfaces();
    }

    public List getReceiveInterfaces() {
        return tp.getReceiveInterfaces();
    }

    public boolean isSendOnAllInterfaces() {
        return tp.isSendOnAllInterfaces();
    }

    public List getSendInterfaces() {
        return tp.getSendInterfaces();
    }

    public boolean isDiscardIncompatiblePackets() {
        return tp.isDiscardIncompatiblePackets();
    }

    public void setDiscardIncompatiblePackets(boolean flag) {
        tp.setDiscardIncompatiblePackets(flag);
    }

    public boolean isEnableBundling() {
        return tp.isEnableBundling();
    }

    public void setEnableBundling(boolean flag) {
        tp.setEnableBundling(flag);
    }

    public int getMaxBundleSize() {
        return tp.getMaxBundleSize();
    }

    public void setMaxBundleSize(int size) {
        tp.setMaxBundleSize(size);
    }

    public long getMaxBundleTimeout() {
        return tp.getMaxBundleTimeout();
    }

    public void setMaxBundleTimeout(long timeout) {
        tp.setMaxBundleTimeout(timeout);
    }

    public int getOutgoingQueueSize() {
        return tp.getOutgoingQueueSize();
    }

    public int getOutgoingQueueMaxSize() {
        return tp.getOutgoingQueueMaxSize();
    }

    public void setOutgoingQueueMaxSize(int new_size) {
        tp.setOutgoingQueueMaxSize(new_size);
    }

    public boolean isLoopback() {
        return tp.isLoopback();
    }

    public void setLoopback(boolean b) {
        tp.setLoopback(b);
    }

    public boolean isUseIncomingPacketHandler() {
        return tp.isUseIncomingPacketHandler();
    }

    public boolean isUseOutgoungPacketHandler() {
        return tp.isUseOutgoingPacketHandler();
    }

    public int getUnmarshallerMinPoolSize() {
        return tp.getUnmarshallerMinPoolSize();
    }

    public void setUnmarshallerMinPoolSize(int size) {
        tp.setUnmarshallerMinPoolSize(size);
    }

    public int getUnmarshallerMaxPoolSize() {
        return tp.getUnmarshallerMaxPoolSize();
    }

    public void setUnmarshallerMaxPoolSize(int size) {
        tp.setUnmarshallerMaxPoolSize(size);
    }

    public int getUnmarshallerPoolSize() {
        return tp.getUnmarshallerPoolSize();
    }

    public long getUnmarshallerKeepAliveTime() {
        return tp.getUnmarshallerKeepAliveTime();
    }

    public void setUnmarshallerKeepAliveTime(long time) {
        tp.setUnmarshallerKeepAliveTime(time);
    }

    public int getUnmarshallerQueueSize() {
        return tp.getUnmarshallerQueueSize();
    }

    public int getUnmarshallerMaxQueueSize() {
        return tp.getUnmarshallerMaxQueueSize();
    }

    public void setUnmarshallerMaxQueueSize(int size) {
        tp.setUnmarshallerMaxQueueSize(size);
    }




    public int getOOBMinPoolSize() {
         return tp.getOOBMinPoolSize();
     }

     public void setOOBMinPoolSize(int size) {
         tp.setOOBMinPoolSize(size);
     }

     public int getOOBMaxPoolSize() {
         return tp.getOOBMaxPoolSize();
     }

     public void setOOBMaxPoolSize(int size) {
         tp.setOOBMaxPoolSize(size);
     }

     public int getOOBPoolSize() {
         return tp.getOOBPoolSize();
     }

    public long getOOBKeepAliveTime() {
        return tp.getOOBKeepAliveTime();
    }

    public void setOOBKeepAliveTime(long time) {
        tp.setOOBKeepAliveTime(time);
    }

    public long getNumOOBMessages() {
        return tp.getOOBMessages();
    }

    public int getOOBQueueSize() {
        return tp.getOOBQueueSize();
    }

    public int getOOBMaxQueueSize() {
        return tp.getOOBMaxQueueSize();
    }

    public void setOOBMaxQueueSize(int size) {
        tp.setOOBMaxQueueSize(size);
    }


    public int getIncomingMinPoolSize() {
         return tp.getIncomingMinPoolSize();
     }

     public void setIncomingMinPoolSize(int size) {
         tp.setIncomingMinPoolSize(size);
     }

     public int getIncomingMaxPoolSize() {
         return tp.getIncomingMaxPoolSize();
     }

     public void setIncomingMaxPoolSize(int size) {
         tp.setIncomingMaxPoolSize(size);
     }

     public int getIncomingPoolSize() {
         return tp.getIncomingPoolSize();
     }

     public long getIncomingKeepAliveTime() {
         return tp.getIncomingKeepAliveTime();
     }

    public void setIncomingKeepAliveTime(long time) {
        tp.setIncomingKeepAliveTime(time);
    }

    public long getIncomingMessages() {
        return tp.getIncomingMessages();
    }

    public int getIncomingQueueSize() {
        return tp.getIncomingQueueSize();
    }

    public int getIncomingMaxQueueSize() {
        return tp.getIncomingMaxQueueSize();
    }

    public void setIncomingMaxQueueSize(int size) {
        tp.setIncomingMaxQueueSize(size);
    }

}
