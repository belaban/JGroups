package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.PhysicalAddress;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.util.NameCache;
import org.jgroups.util.Responses;
import org.jgroups.util.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;


/**
 * Discovery protocol for finding members in the local process only. Doesn't send discovery requests and responses, but
 * fetches discovery information directly from a hashmap. Used mainly by unit test.
 * @author Bela Ban
 * @since  4.1.3
 */
public class LOCAL_PING extends Discovery {
    /** Map of cluster names and address-protocol mappings. Used for routing messages to all or single members */
    protected static final Map<String,List<PingData>>      discovery=new ConcurrentHashMap<>();
    protected static final Function<String,List<PingData>> func=k -> new ArrayList<>();

    public boolean isDynamic() {
        return true;
    }

    public void stop() {
        super.stop();
    }

    @ManagedOperation(description="Dumps the contents of the discovery cache")
    public String print() {
        StringBuilder sb=new StringBuilder();
        synchronized(this) {
            for(Map.Entry<String,List<PingData>> e : discovery.entrySet())
                sb.append(e.getKey()).append(": ").append(e.getValue()).append("\n");
        }
        return sb.toString();
    }

    @ManagedAttribute(description="Number of keys in the discovery cache")
    public static int getDiscoveryCacheSize() {
        return discovery.size();
    }

    public Responses findMembers(List<Address> members, boolean initial_discovery, boolean async, long timeout) {
        return super.findMembers(members, initial_discovery, false, timeout);
    }

    @Override
    public void findMembers(List<Address> members, boolean initial_discovery, Responses responses) {
        num_discovery_requests++;
        List<PingData> list=discovery.get(cluster_name);
        synchronized(discovery) {
            if(list != null && !list.isEmpty()) {
                if(list.stream().noneMatch(PingData::isCoord))
                    list.get(0).coord(true);
                list.stream().filter(el -> !el.sender.equals(local_addr))
                  .forEach(d -> {
                      addAddressToLocalCache(d.sender, d.physical_addr);
                      responses.addResponse(d, false);
                  });
            }
        }
        responses.done(); // so waitFor() doesn't block at all
    }

    public Object down(Event evt) {
        if(evt.type() == Event.VIEW_CHANGE && cluster_name != null) {
            // synchronize TP.logical_addr_cache with discovery cache
            List<PingData> data=discovery.get(cluster_name);
            synchronized(discovery) {
                if(data != null && !data.isEmpty()) {
                    for(PingData d : data) {
                        Address sender=d.getAddress();
                        if(down_prot.down(new Event(Event.GET_PHYSICAL_ADDRESS, sender)) == null)
                            down_prot.down(new Event(Event.ADD_PHYSICAL_ADDRESS, new Tuple<>(sender, d.getPhysicalAddr())));
                    }
                }
            }
        }
        return super.down(evt);
    }

    public void handleConnect() {
        if(cluster_name == null || local_addr == null)
            throw new IllegalStateException("cluster name and local address cannot be null");
        String logical_name=NameCache.get(local_addr);
        PhysicalAddress physical_addr=(PhysicalAddress)down_prot.down(new Event(Event.GET_PHYSICAL_ADDRESS, local_addr));
        if(physical_addr != null) {
            PingData data=new PingData(local_addr, is_server, logical_name, physical_addr);
            final List<PingData> list=discovery.computeIfAbsent(cluster_name, func);
            synchronized(discovery) {
                if(list.isEmpty())
                    data.coord(true); // first element is coordinator
                list.add(data);
            }
        }
    }

    public void handleDisconnect() {
        if(local_addr == null || cluster_name == null)
            return;
        List<PingData> list=discovery.get(cluster_name);
        if(list != null) {
            synchronized(discovery) {
                list.removeIf(p -> local_addr.equals(p.getAddress()));
                if(list.isEmpty())
                    discovery.remove(cluster_name);
            }
        }
    }

    public String toString() {
        return String.format("%s(%s)", LOCAL_PING.class.getSimpleName(), local_addr);
    }

    protected void addAddressToLocalCache(Address addr, PhysicalAddress phys_addr) {
        Tuple<Address,PhysicalAddress> tuple=new Tuple(addr, phys_addr);
        down_prot.down(new Event(Event.ADD_PHYSICAL_ADDRESS, tuple));
    }

}
