package org.jgroups.util;

import org.jgroups.Address;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Maintains the highest received and highest delivered seqno per member
 * @author Bela Ban
 * @version $Id: SeqnoTable.java,v 1.2 2008/03/12 09:57:20 belaban Exp $
 */
public class SeqnoTable {
    private long next_to_receive=0;
    private final ConcurrentMap<Address,Entry> map=new ConcurrentHashMap<Address,Entry>();


    public SeqnoTable(long next_to_receive) {
        this.next_to_receive=next_to_receive;
    }

    public long getHighestReceived(Address member) {
        Entry entry=map.get(member);
        return entry != null? entry.getHighestReceived() : -1;
    }

    public long getNextToReceive(Address member) {
        Entry entry=map.get(member);
        return entry != null? entry.getNextToReceive() : -1;
    }

    public boolean add(Address member, long seqno) {
        Entry entry=map.get(member);
        if(entry == null) {
            entry=new Entry(next_to_receive);
            Entry entry2=map.putIfAbsent(member, entry);
            if(entry2 != null)
                entry=entry2;
        }
        // now entry is not null
        return entry.add(seqno);
    }

    public void remove(Address member) {
        map.remove(member);
    }

    public boolean retainAll(Collection<Address> members) {
        return map.keySet().retainAll(members);
    }

    public void clear() {
        map.clear();
    }

    public String toString() {
        return map.toString();
    }


    private static class Entry {
        long highest_received;
        long next_to_receive;
        final Set<Long> seqnos=new HashSet<Long>();

        private Entry(long initial_seqno) {
            this.next_to_receive=this.highest_received=initial_seqno;
        }

        public synchronized long getHighestReceived() {
            return highest_received;
        }

        public synchronized long getNextToReceive() {
            return next_to_receive;
        }

        public synchronized boolean add(long seqno) {
            try {
                if(seqno == next_to_receive) {
                    next_to_receive++;
                    while(true) {
                        if(seqnos.remove(next_to_receive)) {
                            next_to_receive++;
                        }
                        else
                            break;
                    }
                    return true;
                }

                if(seqno < next_to_receive)
                    return false;

                // seqno > next_to_receive
                seqnos.add(seqno);
                return true;
            }
            finally {
                highest_received=Math.max(highest_received, seqno);
            }
        }

        public String toString() {
            StringBuilder sb=new StringBuilder();
            sb.append(next_to_receive).append(" - ").append(highest_received);
            if(!seqnos.isEmpty())
                sb.append(" ").append(seqnos);
            return sb.toString();
        }
    }
}
