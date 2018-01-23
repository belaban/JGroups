package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Responses;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Protocol to invoke multiple discovery protocols in the same stack. All discovery protocols needs to be _below_ this
 * one, e.g.
 * <pre>{@code
 *   <TCP.../>
 *   <TCPPING initial_hosts="127.0.0.1[7800]"/>
 *   <PING />
 *   <MPING/>
 *   <FILE_PING/>
 *   <MULTI_PING async_discovery="true"/>
 *   <MERGE3 .../>
 *   ...
 * }</pre>
 * @author Bela Ban
 * @since  4.0.8
 */
public class MULTI_PING extends Discovery {
    protected final List<Discovery> discovery_protocols=new ArrayList<>();

    @ManagedAttribute(description="List of discovery protocols")
    public String getDiscoveryProtocols() {
        return discovery_protocols.stream().map(p -> p.getClass().getSimpleName()).collect(Collectors.joining(", "));
    }

    public boolean isDynamic() {
        return discovery_protocols.stream().anyMatch(Discovery::isDynamic);
    }

    public void init() throws Exception {
        super.init();

        // sanity check: we cannot have any discovery protocols _above_ us
        for(Protocol p=up_prot; p != null; p=p.getUpProtocol())
            if(p instanceof Discovery)
                throw new IllegalStateException(String.format("found %s above %s: this is invalid; all discovery " +
                                                                "protocols must be placed below %s", p.getClass().getSimpleName(),
                                                              getClass().getSimpleName(), getClass().getSimpleName()));
        // add all discovery protocols below us to the discovery_protocols list
        for(Protocol p=down_prot; p != null; p=p.getDownProtocol())
            if(p instanceof Discovery)
                discovery_protocols.add((Discovery)p);
    }

    public void weedOutCompletedDiscoveryResponses() {
        ;
    }


    protected void findMembers(List<Address> members, boolean initial_discovery, Responses responses) {
        ; // not used
    }

    public Responses findMembers(List<Address> members, boolean initial_discovery, boolean async) {
        num_discovery_requests++;
        int num_expected=members != null? members.size() : 0;
        int capacity=members != null? members.size() : 16;
        final Responses rsps=new Responses(num_expected, initial_discovery && break_on_coord_rsp, capacity);
        findMembers(members, initial_discovery, rsps, async);
        return rsps;
    }

    protected void findMembers(List<Address> members, boolean initial_discovery, Responses rsps, boolean async) {
        for(Discovery discovery_protocol: discovery_protocols) {
            discovery_protocol.addResponse(rsps);
            if(async || async_discovery)
                timer.execute(() -> discovery_protocol.findMembers(members, initial_discovery, rsps));
            else
                discovery_protocol.findMembers(members, initial_discovery, rsps);
        }
    }


}
