package org.jgroups.jmx.protocols.pbcast;

import org.jgroups.jmx.ProtocolMBean;

/**
 * @author Bela Ban
 * @version $Id: NAKACKMBean.java,v 1.2 2005/06/07 13:28:28 belaban Exp $
 */
public interface NAKACKMBean extends ProtocolMBean {
    long getXmitRequestsReceived();
    long getXmitRequestsSent();
    long getXmitResponsesReceived();
    long getXmitResponsesSent();
}
