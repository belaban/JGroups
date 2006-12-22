package org.jgroups.jmx.protocols;

import org.jgroups.jmx.ProtocolMBean;

/**
 * @author Bela Ban
 * @version $Id: FD_ALLMBean.java,v 1.1 2006/12/22 09:34:53 belaban Exp $
 */
public interface FD_ALLMBean extends ProtocolMBean {
    int getHeartbeatsSent();
    int getHeartbeatsReceived();
    int getSuspectEventsSent();
    String getLocalAddress();
    String getMembers();
    long getTimeout();
    void setTimeout(long timeout);
    long getInterval();
    void setInterval(long interval);
    boolean isShun();
    void setShun(boolean flag);
    boolean isRunning();
    String printSuspectHistory();
    String printTimestamps();
}
