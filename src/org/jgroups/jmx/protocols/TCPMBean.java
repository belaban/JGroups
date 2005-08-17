package org.jgroups.jmx.protocols;

import java.net.InetAddress;

/**
 * @author Bela Ban
 * @version $Id: TCPMBean.java,v 1.2 2005/08/17 07:32:29 belaban Exp $
 */
public interface TCPMBean extends TPMBean {
    int getOpenConnections();
    InetAddress getBindAddr();
    void setBindAddr(InetAddress bind_addr);
    int getStartPort();
    void setStartPort(int start_port);
    int getEndPort();
    void setEndPort(int end_port);
    long getReaperInterval();
    void setReaperInterval(long reaper_interval);
    long getConnExpireTime();
    void setConnExpireTime(long conn_expire_time);
    String printConnections();
}
