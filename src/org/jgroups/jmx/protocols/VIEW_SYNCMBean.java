package org.jgroups.jmx.protocols;

import org.jgroups.jmx.ProtocolMBean;

/**
 * @author Bela Ban
 * @version $Id: VIEW_SYNCMBean.java,v 1.1 2005/10/27 08:14:43 belaban Exp $
 */
public interface VIEW_SYNCMBean extends ProtocolMBean {
    long getAverageSendInterval();
    void setAverageSendInterval(long send_interval);
    int getNumViewsSent();
    int getNumViewsAdjusted();
    void sendViewRequest();
}
