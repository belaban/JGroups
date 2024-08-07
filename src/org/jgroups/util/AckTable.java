package org.jgroups.util;

import org.jgroups.Address;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Manages ACKs from receivers in {@link org.jgroups.protocols.NAKACK4}
 * @author Bela Ban
 * @since  5.4
 */
public class AckTable {
    protected final Map<Address,Long> acks=Util.createConcurrentMap();
    protected long                    min; // the current minimum, recomputed on ack() and view change

    public long min() {return min;}

    /** Adds an ACK from a sender to the map. Returns the new minimum */
    public long ack(Address sender, long seqno) {
        Long existing=acks.get(sender);
        if(existing != null && existing >= seqno)
            return min;
        acks.put(sender, seqno);
        return min=computeMin();
    }

    /** Removes left members from and adds new members to the map */
    public AckTable adjust(List<Address> mbrs) {
        if(mbrs == null)
            return this;
        acks.keySet().retainAll(mbrs);
        for(Address mbr: mbrs)
            acks.putIfAbsent(mbr, 0L);
        min=computeMin();
        return this;
    }

    public int size() {return acks.size();}

    @Override public String toString() {
        String tmp= acks.entrySet().stream().map(e -> String.format("%s: %d", e.getKey(), e.getValue()))
          .collect(Collectors.joining("\n"));
        return tmp.isEmpty()? "min: " + min : tmp + "\nmin: " + min;
    }

    protected long computeMin() {
        Optional<Long> m=acks.values().stream().min(Long::compareTo);
        return m.orElse(min);
    }
}
