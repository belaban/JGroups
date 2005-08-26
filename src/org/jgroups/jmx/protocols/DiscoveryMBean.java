package org.jgroups.jmx.protocols;

import org.jgroups.jmx.ProtocolMBean;

import java.util.Vector;

/**
 * @author Bela Ban
 * @version $Id: DiscoveryMBean.java,v 1.4 2005/08/26 14:19:09 belaban Exp $
 */
public interface DiscoveryMBean extends ProtocolMBean {
    long getTimeout();
    void setTimeout(long timeout);
    int getInitialMembers();
    void setInitialMembers(int num_initial_members);
    int getPingRequests();
    void setPingRequests(int num_ping_requests);
    int getDiscoveryRequestsSent();
    Vector findInitialMembers();
    String findInitialMembersAsString();
}
