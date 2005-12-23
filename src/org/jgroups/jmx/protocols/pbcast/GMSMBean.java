package org.jgroups.jmx.protocols.pbcast;

import org.jgroups.jmx.ProtocolMBean;

/**
 * @author Bela Ban
 * @version $Id: GMSMBean.java,v 1.3 2005/12/23 14:57:05 belaban Exp $
 */
public interface GMSMBean extends ProtocolMBean {
    String getView();
    String getLocalAddress();
    String getMembers();
    int getNumMembers();
    boolean isCoordinator();
    int getNumberOfViews();
    long getJoinTimeout();
    void setJoinTimeout(long t);
    long getJoinRetryTimeout();
    void setJoinRetryTimeout(long t);
    boolean isShun();
    void setShun(boolean s);
    String printPreviousMembers();
    String printPreviousViews();
    int getViewHandlerQueue();
    boolean isViewHandlerSuspended();
    String dumpViewHandlerQueue();
    String dumpHistory();
    void suspendViewHandler();
    void resumeViewHandler();
}
