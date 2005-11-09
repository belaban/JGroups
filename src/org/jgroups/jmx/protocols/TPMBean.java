package org.jgroups.jmx.protocols;

import org.jgroups.jmx.ProtocolMBean;
import org.jgroups.Address;

import java.net.UnknownHostException;
import java.util.List;

/**
 * @author Bela Ban
 * @version $Id: TPMBean.java,v 1.5 2005/11/09 17:42:52 belaban Exp $
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
    int getOutgoingQueueSize();
    int getIncomingQueueSize();
    boolean isLoopback();
    void setLoopback(boolean b);
    boolean isUseIncomingPacketHandler();
    boolean isUseOutgoungPacketHandler();
    int getOutgoingQueueMaxSize();
    void setOutgoingQueueMaxSize(int new_size);
}
