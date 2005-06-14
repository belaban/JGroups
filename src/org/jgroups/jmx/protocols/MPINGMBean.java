package org.jgroups.jmx.protocols;

import org.jgroups.jmx.ProtocolMBean;

import java.net.InetAddress;

/**
 * @author Bela Ban
 * @version $Id: MPINGMBean.java,v 1.1 2005/06/14 10:10:10 belaban Exp $
 */
public interface MPINGMBean extends PINGMBean {
    InetAddress getBindAddr();
    void setBindAddr(InetAddress bind_addr);
    boolean isBindToAllInterfaces();
    void setBindToAllInterfaces(boolean bind_to_all_interfaces);
    int getTTL();
    void setTTL(int ip_ttl);
    InetAddress getMcastAddr();
    void setMcastAddr(InetAddress mcast_addr);
    int getMcastPort();
    void setMcastPort(int mcast_port);
}
