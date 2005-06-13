package org.jgroups.jmx.protocols;

import org.jgroups.jmx.ProtocolMBean;

import java.net.UnknownHostException;

/**
 * @author Bela Ban
 * @version $Id: UDPMBean.java,v 1.2 2005/06/13 11:47:05 belaban Exp $
 */
public interface UDPMBean extends ProtocolMBean {
    long getNumMessagesSent();
    long getNumMessagesReceived();
    long getNumBytesSent();
    long getNumBytesReceived();
    String getBindAddress();
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
}
