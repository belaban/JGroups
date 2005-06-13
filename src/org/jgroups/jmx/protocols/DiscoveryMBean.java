package org.jgroups.jmx.protocols;

import org.jgroups.jmx.ProtocolMBean;

import java.util.Vector;

/**
 * @author Bela Ban
 * @version $Id: DiscoveryMBean.java,v 1.2 2005/06/13 11:10:49 belaban Exp $
 */
public interface DiscoveryMBean extends ProtocolMBean {
    long getTimeout();
    void setTimeout(long timeout);
    int getNumInitialMembers();
    void setNumInitialMembers(int num_initial_members);
    int getNumPingRequests();
    void setNumPingRequests(int num_ping_requests);
    Vector findInitialMembers();
    String findInitialMembersAsString();
}
