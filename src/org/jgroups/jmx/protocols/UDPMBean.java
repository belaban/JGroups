package org.jgroups.jmx.protocols;

import org.jgroups.jmx.ProtocolMBean;

/**
 * @author Bela Ban
 * @version $Id: UDPMBean.java,v 1.1 2005/06/03 08:49:17 belaban Exp $
 */
public interface UDPMBean extends ProtocolMBean {
    public long getNumMessagesSent();
    public long getNumMessagesReceived();
    public long getNumBytesSent();
    public long getNumBytesReceived();
}
