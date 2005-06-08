package org.jgroups.jmx.protocols;

import org.jgroups.jmx.ProtocolMBean;

/**
 * @author Bela Ban
 * @version $Id: DiscoveryMBean.java,v 1.1 2005/06/08 15:17:30 belaban Exp $
 */
public interface DiscoveryMBean extends ProtocolMBean {
    long getTimeout();
    void setTimeout(long timeout);
    int getNumInitialMembers();
    void setNumInitialMembers(int num_initial_members);
    int getNumPingRequests();
    void setNumPingRequests(int num_ping_requests);
}
