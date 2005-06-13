package org.jgroups.jmx.protocols;

import org.jgroups.stack.Protocol;

import java.net.UnknownHostException;

/**
 * @author Bela Ban
 * @version $Id: UDP.java,v 1.2 2005/06/13 11:47:05 belaban Exp $
 */
public class UDP extends org.jgroups.jmx.Protocol implements UDPMBean {
    org.jgroups.protocols.UDP udp;

    public UDP() {
    }

    public UDP(Protocol p) {
        super(p);
        udp=(org.jgroups.protocols.UDP)p;
    }

    public void attachProtocol(Protocol p) {
        super.attachProtocol(p);
        udp=(org.jgroups.protocols.UDP)p;
    }

    public long getNumMessagesSent() {
        return udp.getNumMessagesSent();
    }

    public long getNumMessagesReceived() {
        return udp.getNumMessagesReceived();
    }

    public long getNumBytesSent() {
        return udp.getNumBytesSent();
    }

    public long getNumBytesReceived() {
        return udp.getNumBytesReceived();
    }

    public String getBindAddress() {
        return udp.getBindAddress();
    }

    public void setBindAddress(String bind_address) throws UnknownHostException {
        udp.setBindAddress(bind_address);
    }

    public boolean getBindToAllInterfaces() {
        return udp.getBindToAllInterfaces();
    }

    public void setBindToAllInterfaces(boolean flag) {
        udp.setBindToAllInterfaces(flag);
    }

    public boolean isDiscardIncompatiblePackets() {
        return udp.isDiscardIncompatiblePackets();
    }

    public void setDiscardIncompatiblePackets(boolean flag) {
        udp.setDiscardIncompatiblePackets(flag);
    }

    public boolean isEnableBundling() {
        return udp.isEnableBundling();
    }

    public void setEnableBundling(boolean flag) {
        udp.setEnableBundling(flag);
    }

    public int getMaxBundleSize() {
        return udp.getMaxBundleSize();
    }

    public void setMaxBundleSize(int size) {
        udp.setMaxBundleSize(size);
    }

    public long getMaxBundleTimeout() {
        return udp.getMaxBundleTimeout();
    }

    public void setMaxBundleTimeout(long timeout) {
        udp.setMaxBundleTimeout(timeout);
    }
}
