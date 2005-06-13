package org.jgroups.jmx.protocols;

import org.jgroups.jmx.ProtocolMBean;

/**
 * @author Bela Ban
 * @version $Id: FDMBean.java,v 1.1 2005/06/13 12:44:06 belaban Exp $
 */
public interface FDMBean extends ProtocolMBean {
    int getNumberOfHeartbeatsSent();
    int getNumSuspectEventsGenerated();
    String getLocalAddress();
    String getMembers();
    String getPingableMembers();
    String getPingDest();
    long getTimeout();
    void setTimeout(long timeout);
    int getMaxTries();
    void setMaxTries(int max_tries);
    int getCurrentNumTries();
    boolean isShun();
    void setShun(boolean flag);
    String printSuspectHistory();
}
