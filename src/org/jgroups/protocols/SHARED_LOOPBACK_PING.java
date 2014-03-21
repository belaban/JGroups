package org.jgroups.protocols;

import org.jgroups.Event;
import org.jgroups.PhysicalAddress;
import org.jgroups.ViewId;
import org.jgroups.protocols.pbcast.JoinRsp;
import org.jgroups.util.Promise;

import java.util.*;


/**
 * Discovery protocol running over {@link SHARED_LOOPBACK} only. Doesn't send discovery requests and responses, but
 * fetches discovery information directly from SHARED_LOOPBACK. Used mainly by unit test.
 * @author Bela Ban
 * @since  3.5
 */
public class SHARED_LOOPBACK_PING extends Discovery {

    public void init() throws Exception {
        super.init();
        TP tmp=getTransport();
        if(!(tmp instanceof SHARED_LOOPBACK))
            throw new IllegalStateException("the transport must be " + SHARED_LOOPBACK.class.getSimpleName());
        if(tmp.isSingleton())
            throw new IllegalStateException("shared transports are not supported by " + SHARED_LOOPBACK_PING.class.getSimpleName());
    }

    public boolean isDynamic() {
        return true;
    }

    public Collection<PhysicalAddress> fetchClusterMembers(String cluster_name) {
        return null;  // multicast the discovery request
    }

    public boolean sendDiscoveryRequestsInParallel() {
        return false; // there's just 1 multicast message; no need to send it on a separate thread
    }


    protected List<PingData> findMembers(Promise<JoinRsp> promise, int num_expected_rsps, boolean break_on_coord, ViewId view_id) {
        List<PingData> retval=(List<PingData>)down_prot.down(new Event(Event.GET_PING_DATA,group_addr));
        return retval != null? retval : new ArrayList<PingData>(0);
    }
}