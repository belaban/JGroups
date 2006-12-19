package org.jgroups.jmx.protocols;

import org.jgroups.jmx.ProtocolMBean;
import org.jgroups.Address;

import java.net.UnknownHostException;
import java.util.List;

/**
 * @author Bela Ban
 * @version $Id: TPMBean.java,v 1.10 2006/12/19 11:03:48 belaban Exp $
 */
public interface TPMBean extends ProtocolMBean {
    Address getLocalAddress();
    String getBindAddress();
    String getChannelName();
    long getMessagesSent();
    long getMessagesReceived();
    long getBytesSent();
    long getBytesReceived();
    public void setBindAddress(String bind_address) throws UnknownHostException;
    boolean isReceiveOnAllInterfaces();
    List getReceiveInterfaces();
    boolean isSendOnAllInterfaces();
    List getSendInterfaces();
    boolean isDiscardIncompatiblePackets();
    void setDiscardIncompatiblePackets(boolean flag);
    boolean isEnableBundling();
    void setEnableBundling(boolean flag);
    int getMaxBundleSize();
    void setMaxBundleSize(int size);
    long getMaxBundleTimeout();
    void setMaxBundleTimeout(long timeout);
    boolean isLoopback();
    void setLoopback(boolean b);
    boolean isUseIncomingPacketHandler();

    int getUnmarshallerMinPoolSize();
    void setUnmarshallerMinPoolSize(int size);
    int getUnmarshallerMaxPoolSize();
    void setUnmarshallerMaxPoolSize(int size);
    int getUnmarshallerPoolSize();
    long getUnmarshallerKeepAliveTime();
    void setUnmarshallerKeepAliveTime(long time);
    int getUnmarshallerQueueSize();
    int getUnmarshallerMaxQueueSize();

    int getOOBMinPoolSize();
    void setOOBMinPoolSize(int size);
    int getOOBMaxPoolSize();
    void setOOBMaxPoolSize(int size);
    int getOOBPoolSize();
    long getOOBKeepAliveTime();
    void setOOBKeepAliveTime(long time);
    long getOOBMessages();
    int getOOBQueueSize();
    int getOOBMaxQueueSize();

    int getIncomingMinPoolSize();
    void setIncomingMinPoolSize(int size);
    int getIncomingMaxPoolSize();
    void setIncomingMaxPoolSize(int size);
    int getIncomingPoolSize();
    long getIncomingKeepAliveTime();
    void setIncomingKeepAliveTime(long time);
    long getIncomingMessages();
    int getIncomingQueueSize();
    int getIncomingMaxQueueSize();
}
