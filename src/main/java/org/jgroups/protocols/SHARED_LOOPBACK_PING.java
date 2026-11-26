package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.util.Responses;

import java.util.List;


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
    }

    public boolean isDynamic() {
        return true;
    }


    @Override
    public void findMembers(List<Address> members, boolean initial_discovery, Responses responses) {
        num_discovery_requests++;
        List<PingData> retval=(List<PingData>)down_prot.down(new Event(Event.GET_PING_DATA, cluster_name));
        if(retval != null)
            retval.forEach(data -> responses.addResponse(data, false));
    }

}
