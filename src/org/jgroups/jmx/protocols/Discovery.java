package org.jgroups.jmx.protocols;

import org.jgroups.jmx.Protocol;

/**
 * @author Bela Ban
 * @version $Id: Discovery.java,v 1.1 2005/06/08 15:17:30 belaban Exp $
 */
public class Discovery extends Protocol implements DiscoveryMBean {
    org.jgroups.protocols.Discovery p;

    public Discovery() {
    }

    public Discovery(org.jgroups.stack.Protocol p) {
        super(p);
        this.p=(org.jgroups.protocols.Discovery)p;
    }

    public void attachProtocol(org.jgroups.stack.Protocol p) {
        super.attachProtocol(p);
        this.p=(org.jgroups.protocols.Discovery)p;
    }

    public long getTimeout() {
        return p.getTimeout();
    }

    public void setTimeout(long timeout) {
        p.setTimeout(timeout);
    }

    public int getNumInitialMembers() {
        return p.getNumInitialMembers();
    }

    public void setNumInitialMembers(int num_initial_members) {
        p.setNumInitialMembers(num_initial_members);
    }

    public int getNumPingRequests() {
        return p.getNumPingRequests();
    }

    public void setNumPingRequests(int num_ping_requests) {
        p.setNumPingRequests(num_ping_requests);
    }

}
