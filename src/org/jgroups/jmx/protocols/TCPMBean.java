package org.jgroups.jmx.protocols;

import org.jgroups.jmx.ProtocolMBean;

import java.net.InetAddress;

/**
 * @author Bela Ban
 * @version $Id: TCPMBean.java,v 1.1 2005/06/14 09:51:08 belaban Exp $
 */
public interface TCPMBean extends ProtocolMBean {
    long getNumMessagesSent();
    long getNumMessagesReceived();
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
    boolean isLoopback();
    void setLoopback(boolean loopback);
    String printConnections();
}
