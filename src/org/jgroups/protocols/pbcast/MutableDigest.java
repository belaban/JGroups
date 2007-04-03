package org.jgroups.protocols.pbcast;

import org.jgroups.Address;

import java.util.Iterator;
import java.util.Map;

/**
 * A mutable version of Digest (which is immutable
 * @author Bela Ban
 * @version $Id: MutableDigest.java,v 1.4 2007/04/03 08:29:22 belaban Exp $
 */
public class MutableDigest extends Digest {
    private boolean sealed=false;

    /** Used for externalization */
    public MutableDigest() {
        super();
    }

    public MutableDigest(int size) {
        super(size);
    }


    public MutableDigest(Map<Address,Entry> map) {
        super(map);
    }


    public MutableDigest(Digest digest) {
        super(digest.getSenders());
    }

    public void add(Address sender, long low_seqno, long high_seqno) {
        checkSealed();
        add(sender, low_seqno, high_seqno, -1);
    }


    public void add(Address sender, long low_seqno, long high_seqno, long high_seqno_seen) {
        checkSealed();
        add(sender, new Digest.Entry(low_seqno, high_seqno, high_seqno_seen));
    }

    private void add(Address sender, Entry entry) {
        if(sender == null || entry == null) {
            if(log.isErrorEnabled())
                log.error("sender (" + sender + ") or entry (" + entry + ")is null, will not add entry");
            return;
        }
        checkSealed();
        Object retval=senders.put(sender, entry);
        if(retval != null && warn)
            log.warn("entry for " + sender + " was overwritten with " + entry);
    }


    public void add(Digest d) {
        if(d != null) {
            checkSealed();
            Map.Entry entry;
            Address key;
            Entry val;
            for(Iterator it=d.senders.entrySet().iterator(); it.hasNext();) {
                entry=(Map.Entry)it.next();
                key=(Address)entry.getKey();
                val=(Entry)entry.getValue();
                add(key, val.getLow(), val.getHigh(), val.getHighSeen());
            }
        }
    }

    public void replace(Digest d) {
        if(d != null) {
            clear();
            add(d);
        }
    }

    public boolean set(Address sender, long low_seqno, long high_seqno, long high_seqno_seen) {
        checkSealed();
        Entry entry=senders.put(sender, new Entry(low_seqno, high_seqno, high_seqno_seen));
        return entry == null;
    }

    /**
     * Adds a digest to this digest. This digest must have enough space to add the other digest; otherwise an error
     * message will be written. For each sender in the other digest, the merge() method will be called.
     */
    public void merge(Digest d) {
        if(d == null) {
            if(log.isErrorEnabled()) log.error("digest to be merged with is null");
            return;
        }
        checkSealed();
        Map.Entry entry;
        Address sender;
        Entry val;
        for(Iterator it=d.senders.entrySet().iterator(); it.hasNext();) {
            entry=(Map.Entry)it.next();
            sender=(Address)entry.getKey();
            val=(Entry)entry.getValue();
            if(val != null) {
                merge(sender, val.getLow(), val.getHigh(), val.getHighSeen());
            }
        }
    }


    /**
     * Similar to add(), but if the sender already exists, its seqnos will be modified (no new entry) as follows:
     * <ol>
     * <li>this.low_seqno=min(this.low_seqno, low_seqno)
     * <li>this.high_seqno=max(this.high_seqno, high_seqno)
     * <li>this.high_seqno_seen=max(this.high_seqno_seen, high_seqno_seen)
     * </ol>
     * If the sender doesn not exist, a new entry will be added (provided there is enough space)
     */
    public void merge(Address sender, long low_seqno, long high_seqno, long high_seqno_seen) {
        if(sender == null) {
            if(log.isErrorEnabled()) log.error("sender == null");
            return;
        }
        checkSealed();
        Entry entry=senders.get(sender);
        if(entry == null) {
            add(sender, low_seqno, high_seqno, high_seqno_seen);
        }
        else {
            Entry new_entry=new Entry(Math.min(entry.getLow(),      low_seqno), 
                                      Math.max(entry.getHigh(),     high_seqno),
                                      Math.max(entry.getHighSeen(), high_seqno_seen));
            senders.put(sender, new_entry);
        }
    }



    /**
     * Increments the sender's high_seqno by 1.
     */
    public void incrementHighSeqno(Address sender) {
        Entry entry=senders.get(sender);
        if(entry == null)
            return;
        checkSealed();
        Entry new_entry=new Entry(entry.getLow(), entry.getHigh() +1, entry.getHighSeen());
        senders.put(sender, new_entry);
    }


    /**
     * Resets the seqnos for the sender at 'index' to 0. This happens when a member has left the group,
     * but it is still in the digest. Resetting its seqnos ensures that no-one will request a message
     * retransmission from the dead member.
     */
    public void resetAt(Address sender) {
        Entry entry=senders.get(sender);
        if(entry != null)
            checkSealed();
            senders.put(sender, new Entry());
    }


    public void clear() {
        checkSealed();
        senders.clear();
    }



//    public void setHighSeqnoAt(Address sender, long high_seqno) {
//        Entry entry=senders.get(sender);
//        if(entry != null) {
//            checkSealed();
//            Entry new_entry=new Entry(entry.getLow(), high_seqno, entry.getHighSeen());
//            senders.put(sender, new_entry);
//        }
//    }
//
//    public void setHighSeqnoSeenAt(Address sender, long high_seqno_seen) {
//        Entry entry=senders.get(sender);
//        if(entry != null) {
//            checkSealed();
//            Entry new_entry=new Entry(entry.getLow(), entry.getHigh(), high_seqno_seen);
//            senders.put(sender, new_entry);
//        }
//    }

    public void setHighestDeliveredAndSeenSeqnos(Address sender, long low_seqno, long high_seqno, long high_seqno_seen) {
        Entry entry=senders.get(sender);
        if(entry != null) {
            checkSealed();
            Entry new_entry=new Entry(low_seqno, high_seqno, high_seqno_seen);
            senders.put(sender, new_entry);
        }
    }

    /** Seals the instance against modifications */
    public boolean seal() {
        boolean retval=sealed;
        sealed=true;
        return retval;
    }


    private final void checkSealed() {
        if(sealed)
            throw new IllegalAccessError("instance has been sealed and cannot be modified");
    }

}
