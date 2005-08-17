package org.jgroups.jmx.protocols;

import org.jgroups.jmx.ProtocolMBean;
import org.jgroups.Address;

import java.net.UnknownHostException;

/**
 * @author Bela Ban
 * @version $Id: TPMBean.java,v 1.1 2005/08/17 07:32:29 belaban Exp $
 */
public interface TPMBean extends ProtocolMBean {
    Address getLocalAddress();
    String getBindAddress();
    String getChannelName();
    long getNumMessagesSent();
    long getNumMessagesReceived();
    long getNumBytesSent();
    long getNumBytesReceived();
    public void setBindAddress(String bind_address) throws UnknownHostException;
    boolean getBindToAllInterfaces();
    void setBindToAllInterfaces(boolean flag);
    boolean isDiscardIncompatiblePackets();
    void setDiscardIncompatiblePackets(boolean flag);
    boolean isEnableBundling();
    void setEnableBundling(boolean flag);
    int getMaxBundleSize();
    void setMaxBundleSize(int size);
    long getMaxBundleTimeout();
    void setMaxBundleTimeout(long timeout);
    int getOutgoingQueueSize();
    int getIncomingQueueSize();
    boolean isLoopback();
    void setLoopback(boolean b);
    boolean isLoopbackQueue();
    void setLoopbackQueue(boolean b);
    boolean isUseIncomingPacketHandler();
    boolean isUseOutgoungPacketHandler();
}
