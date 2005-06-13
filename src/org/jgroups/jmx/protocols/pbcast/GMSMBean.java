package org.jgroups.jmx.protocols.pbcast;

import org.jgroups.jmx.ProtocolMBean;

import java.util.Enumeration;

/**
 * @author Bela Ban
 * @version $Id: GMSMBean.java,v 1.1 2005/06/13 15:50:07 belaban Exp $
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
}
