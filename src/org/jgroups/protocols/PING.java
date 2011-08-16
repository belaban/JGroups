package org.jgroups.protocols;

import org.jgroups.PhysicalAddress;

import java.util.Collection;


/**
 * The PING protocol retrieves the initial membership by mcasting a discovery request (via the multicast capable
 * transport) to all current cluster members<p/>
 * The responses should allow us to determine the coordinator which we have to contact, e.g. in case we want to join
 * the group.  When we are a server (after having received the BECOME_SERVER event), we'll respond to discovery requests
 * with a discovery response.
 * @author Bela Ban
 */
public class PING extends Discovery {

    public boolean isDynamic() {
        return true;
    }

    public Collection<PhysicalAddress> fetchClusterMembers(String cluster_name) {
        return null;  // multicast the discovery request
    }

    public boolean sendDiscoveryRequestsInParallel() {
        return false; // there's just 1 multicast message; no need to send it on a separate thread
    }
}