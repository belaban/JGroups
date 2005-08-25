package org.jgroups.jmx.protocols;

import org.jgroups.stack.Protocol;
import org.jgroups.Address;

import java.net.UnknownHostException;
import java.util.List;

/**
 * @author Bela Ban
 * @version $Id: TP.java,v 1.2 2005/08/25 14:53:08 belaban Exp $
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

    public long getNumMessagesSent() {
        return tp.getNumMessagesSent();
    }

    public long getNumMessagesReceived() {
        return tp.getNumMessagesReceived();
    }

    public long getNumBytesSent() {
        return tp.getNumBytesSent();
    }

    public long getNumBytesReceived() {
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

    public int getIncomingQueueSize() {
        return tp.getIncomingQueueSize();
    }

    public boolean isLoopback() {
        return tp.isLoopback();
    }

    public void setLoopback(boolean b) {
        tp.setLoopback(b);
    }

    public boolean isLoopbackQueue() {
        return tp.isLoopbackQueue();
    }

    public void setLoopbackQueue(boolean b) {
        tp.setLoopbackQueue(b);
    }

    public boolean isUseIncomingPacketHandler() {
        return tp.isUseIncomingPacketHandler();
    }

    public boolean isUseOutgoungPacketHandler() {
        return tp.isUseOutgoingPacketHandler();
    }


}
