package org.jgroups.jmx.protocols.pbcast;

import org.jgroups.jmx.ProtocolMBean;

/**
 * @author Bela Ban
 * @version $Id: GMSMBean.java,v 1.2 2005/12/22 14:51:40 belaban Exp $
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
}
