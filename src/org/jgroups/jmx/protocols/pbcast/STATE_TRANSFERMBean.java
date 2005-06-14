package org.jgroups.jmx.protocols.pbcast;

import org.jgroups.jmx.ProtocolMBean;

/**
 * @author Bela Ban
 * @version $Id: STATE_TRANSFERMBean.java,v 1.1 2005/06/14 08:36:50 belaban Exp $
 */
public interface STATE_TRANSFERMBean extends ProtocolMBean {
    int getNumberOfStateRequests();
    long getNumberOfStateBytesSent();
    double getAverageStateSize();
}
