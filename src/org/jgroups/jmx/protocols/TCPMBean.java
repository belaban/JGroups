package org.jgroups.jmx.protocols;

/**
 * @author Bela Ban
 * @version $Id: TCPMBean.java,v 1.3 2006/10/02 06:47:53 belaban Exp $
 */
public interface TCPMBean extends TPMBean {
    int getOpenConnections();
    String getBindAddr();
    void setBindAddr(String bind_addr);
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
