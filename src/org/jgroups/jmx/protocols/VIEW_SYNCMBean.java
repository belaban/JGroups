package org.jgroups.jmx.protocols;

import org.jgroups.jmx.ProtocolMBean;

/**
 * @author Bela Ban
 * @version $Id: VIEW_SYNCMBean.java,v 1.2 2005/10/27 08:30:14 belaban Exp $
 */
public interface VIEW_SYNCMBean extends ProtocolMBean {
    long getAverageSendInterval();
    void setAverageSendInterval(long send_interval);
    int getNumViewsSent();
    int getNumViewsAdjusted();
    void sendViewRequest();
    // void sendFakeViewForTestingOnly();
}
