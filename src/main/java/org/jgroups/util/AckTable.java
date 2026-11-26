package org.jgroups.util;

import org.jgroups.Address;
import org.jgroups.annotations.GuardedBy;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * Manages ACKs from receivers in {@link org.jgroups.protocols.NAKACK4}
 * @author Bela Ban
 * @since  5.4
 */
public class AckTable {
    protected final Map<Address,Long> acks=new HashMap<>();
    protected final Lock              lock=new ReentrantLock();
    protected volatile long           min; // the current minimum, recomputed on ack() and view change

    public long min() {
        lock.lock();
        try {
            return min;
        }
        finally {
            lock.unlock();
        }
    }

    public long min(Address mbr) {
        lock.lock();
        try {
            return acks.getOrDefault(mbr, 0L);
        } finally {
            lock.unlock();
        }
    }

    public long[] ack(Address sender, long seqno) {
        return ack(sender, seqno, true);
    }

    /** Adds an ACK from a sender to the map. Returns the old and new minimum */
    public long[] ack(Address sender, long seqno, boolean add_if_absent) {
        lock.lock();
        try {
            long[] retval={min,min};
            Long existing=acks.get(sender);
            if(existing == null) {
                if(add_if_absent)
                    acks.put(sender, seqno);
                else
                    return null;
            }
            else {
                if(existing >= seqno)
                    return retval;
                acks.put(sender, seqno);
            }
            retval[1]=min=computeMin();
            return retval;
        }
        finally {
            lock.unlock();
        }
    }

    /** Removes left members from and adds new members to the map */
    public AckTable adjust(List<Address> mbrs) {
        if(mbrs == null)
            return this;
        lock.lock();
        try {
            acks.keySet().retainAll(mbrs);
            for(Address mbr: mbrs)
                acks.putIfAbsent(mbr, 0L);
            min=computeMin();
            return this;
        }
        finally {
            lock.unlock();
        }
    }

    public AckTable clear() {
        lock.lock();
        try {
            acks.clear();
            min=computeMin();
            return this;
        }
        finally {
            lock.unlock();
        }
    }

    public int size() {
        lock.lock();
        try {
            return acks.size();
        }
        finally {
            lock.unlock();
        }
    }

    // no need for the lock - approximation, may be incorrect, that's ok
    @Override public String toString() {
        String tmp=acks.entrySet().stream().map(e -> String.format("%s: %,d", e.getKey(), e.getValue()))
          .collect(Collectors.joining("\n"));
        return tmp.isEmpty()? String.format("min: %,d", min) : String.format("%s\nmin: %,d", tmp, min);
    }

    @GuardedBy("lock")
    protected long computeMin() {
        Optional<Long> m=acks.values().stream().min(Long::compareTo);
        return m.orElse(min);
    }
}
