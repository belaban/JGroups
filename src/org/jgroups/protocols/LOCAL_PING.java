package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.util.NameCache;
import org.jgroups.util.Responses;
import org.jgroups.util.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
    protected static final Function<String,List<PingData>> FUNC=k -> new ArrayList<>();

    public boolean isDynamic() {
        return true;
    }

    public void stop() {
        super.stop();
    }

    @ManagedOperation(description="Dumps the contents of the discovery cache")
    public static String print() {
        StringBuilder sb=new StringBuilder();
        synchronized(discovery) {
            for(Map.Entry<String,List<PingData>> e : discovery.entrySet())
                sb.append(e.getKey()).append(": ").append(e.getValue()).append("\n");
        }
        return sb.toString();
    }

    @ManagedAttribute(description="Number of keys in the discovery cache")
    public static int getDiscoveryCacheSize() {
        synchronized(discovery) {
            return discovery.size();
        }
    }

    public Responses findMembers(List<Address> members, boolean initial_discovery, boolean async, long timeout) {
        return super.findMembers(members, initial_discovery, false, timeout);
    }

    @Override
    public void findMembers(List<Address> members, boolean initial_discovery, Responses responses) {
        num_discovery_requests++;
        synchronized(discovery) {
            List<PingData> list=discovery.get(cluster_name);
            if(list != null) {
                list.forEach(d -> {
                    addAddressToLocalCache(d.sender, d.physical_addr);
                    responses.addResponse(d, false);
                });
            }
        }
        // responses.done(); // so waitFor() doesn't block at all
    }

    public Object down(Event evt) {
        if(evt.type() == Event.VIEW_CHANGE && cluster_name != null) {
            Address old_coord=view != null? view.getCoord() : null;
            boolean was_coord=Objects.equals(local_addr, old_coord);
            Object retval=super.down(evt);

            // synchronize TP.logical_addr_cache with discovery cache
            synchronized(discovery) {
                List<PingData> list=discovery.get(cluster_name);
                if(list != null && !list.isEmpty()) {
                    for(PingData d : list) {
                        Address mbr=d.getAddress();
                        if(down_prot.down(new Event(Event.GET_PHYSICAL_ADDRESS, mbr)) == null)
                            down_prot.down(new Event(Event.ADD_PHYSICAL_ADDRESS, new Tuple<>(mbr, d.getPhysicalAddr())));

                        // set the coordinator based on the new view (https://issues.jboss.org/browse/JGRP-2381)
                        if(Objects.equals(local_addr, mbr)) {
                            if(!was_coord && is_coord) { // this member became coordinator
                                d.coord(true);
                                log.trace("%s: became coordinator (view: %s)", local_addr, view);
                            }
                            if(was_coord && !is_coord) { // this member ceased to be coord, e.g. on a merge
                                d.coord(false);
                                log.trace("%s: ceased to be coordinator (view: %s)", local_addr, view);
                            }
                        }
                    }
                }
            }
            return retval;
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
            synchronized(discovery) {
                final List<PingData> list=discovery.computeIfAbsent(cluster_name, FUNC);
                if(list.isEmpty()) {
                    // the first member will become coord (may be changed by view changes/merges later)
                    // https://issues.jboss.org/browse/JGRP-2395
                    data.coord(true);
                }
                list.add(data);
            }
        }
    }

    public void handleDisconnect() {
        if(local_addr == null || cluster_name == null)
            return;
        synchronized(discovery) {
            List<PingData> list=discovery.get(cluster_name);
            if(list != null) {
                list.removeIf(p -> Objects.equals(local_addr, p.getAddress()));
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
