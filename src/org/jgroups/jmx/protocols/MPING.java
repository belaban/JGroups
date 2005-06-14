package org.jgroups.jmx.protocols;

import java.net.InetAddress;

/**
 * @author Bela Ban
 * @version $Id: MPING.java,v 1.1 2005/06/14 10:10:10 belaban Exp $
 */
public class MPING extends PING implements MPINGMBean {
    org.jgroups.protocols.MPING mping;

    public MPING() {
    }

    public MPING(org.jgroups.stack.Protocol p) {
        super(p);
        this.mping=(org.jgroups.protocols.MPING)p;
    }

    public void attachProtocol(org.jgroups.stack.Protocol p) {
        super.attachProtocol(p);
        this.mping=(org.jgroups.protocols.MPING)p;
    }

    public InetAddress getBindAddr() {
        return mping.getBindAddr();
    }

    public void setBindAddr(InetAddress bind_addr) {
        mping.setBindAddr(bind_addr);
    }

    public boolean isBindToAllInterfaces() {
        return mping.isBindToAllInterfaces();
    }

    public void setBindToAllInterfaces(boolean bind_to_all_interfaces) {
        mping.setBindToAllInterfaces(bind_to_all_interfaces);
    }

    public int getTTL() {
        return mping.getTTL();
    }

    public void setTTL(int ip_ttl) {
        mping.setTTL(ip_ttl);
    }

    public InetAddress getMcastAddr() {
        return mping.getMcastAddr();
    }

    public void setMcastAddr(InetAddress mcast_addr) {
        mping.setMcastAddr(mcast_addr);
    }

    public int getMcastPort() {
        return mping.getMcastPort();
    }

    public void setMcastPort(int mcast_port) {
        mping.setMcastPort(mcast_port);
    }

}
