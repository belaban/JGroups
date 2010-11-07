package org.jgroups.util;

import org.jgroups.Address;

import java.util.Iterator;
import java.util.Map;

/**
 * A mutable version of Digest (which is immutable
 * @author Bela Ban
 * @version $Id: MutableDigest.java,v 1.7 2008/05/20 12:55:29 belaban Exp $
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


    public Map<Address, Entry> getSenders() {
        return senders;
    }

    public void add(Address sender, long low_seqno, long highest_delivered_seqno) {
        checkSealed();
        add(sender, low_seqno, highest_delivered_seqno, -1);
    }


    public void add(Address sender, long low_seqno, long highest_delivered_seqno, long highest_received_seqno) {
        checkSealed();
        add(sender, new Digest.Entry(low_seqno, highest_delivered_seqno, highest_received_seqno));
    }

    private void add(Address sender, Entry entry) {
        if(sender == null || entry == null) {
            if(log.isErrorEnabled())
                log.error("sender (" + sender + ") or entry (" + entry + ")is null, will not add entry");
            return;
        }
        checkSealed();
        senders.put(sender, entry);
    }


    public void add(Digest digest) {
        if(digest != null) {
            checkSealed();
            Map.Entry<Address,Entry> entry;
            Address key;
            Entry val;
            for(Iterator<Map.Entry<Address,Entry>> it=digest.senders.entrySet().iterator(); it.hasNext();) {
                entry=it.next();
                key=entry.getKey();
                val=entry.getValue();
                add(key, val.getLow(), val.getHighestDeliveredSeqno(), val.getHighestReceivedSeqno());
            }
        }
    }

    public void replace(Digest d) {
        if(d != null) {
            clear();
            add(d);
        }
    }

    public boolean set(Address sender, long low_seqno, long highest_delivered_seqno, long highest_received_seqno) {
        checkSealed();
        Entry entry=senders.put(sender, new Entry(low_seqno, highest_delivered_seqno, highest_received_seqno));
        return entry == null;
    }

    /**
     * Adds a digest to this digest. This digest must have enough space to add the other digest; otherwise an error
     * message will be written. For each sender in the other digest, the merge() method will be called.
     */
    public void merge(Digest digest) {
        if(digest == null) {
            if(log.isErrorEnabled()) log.error("digest to be merged with is null");
            return;
        }
        checkSealed();
        Map.Entry<Address,Entry> entry;
        Address sender;
        Entry val;
        for(Iterator<Map.Entry<Address,Entry>> it=digest.senders.entrySet().iterator(); it.hasNext();) {
            entry=it.next();
            sender=entry.getKey();
            val=entry.getValue();
            if(val != null) {
                merge(sender, val.getLow(), val.getHighestDeliveredSeqno(), val.getHighestReceivedSeqno());
            }
        }
    }


    /**
     * Similar to add(), but if the sender already exists, its seqnos will be modified (no new entry) as follows:
     * <ol>
     * <li>this.low_seqno=min(this.low_seqno, low_seqno)
     * <li>this.highest_delivered_seqno=max(this.highest_delivered_seqno, highest_delivered_seqno)
     * <li>this.highest_received_seqno=max(this.highest_received_seqno, highest_received_seqno)
     * </ol>
     * If the sender doesn not exist, a new entry will be added (provided there is enough space)
     */
    public void merge(Address sender, long low_seqno, long highest_delivered_seqno, long highest_received_seqno) {
        if(sender == null) {
            if(log.isErrorEnabled()) log.error("sender == null");
            return;
        }
        checkSealed();
        Entry entry=senders.get(sender);
        if(entry == null) {
            add(sender, low_seqno, highest_delivered_seqno, highest_received_seqno);
        }
        else {
            Entry new_entry=new Entry(Math.min(entry.getLow(), low_seqno),
                                      Math.max(entry.getHighestDeliveredSeqno(), highest_delivered_seqno),
                                      Math.max(entry.getHighestReceivedSeqno(), highest_received_seqno));
            senders.put(sender, new_entry);
        }
    }



    /**
     * Increments the sender's high_seqno by 1.
     */
    public void incrementHighestDeliveredSeqno(Address sender) {
        Entry entry=senders.get(sender);
        if(entry == null)
            return;
        checkSealed();
        Entry new_entry=new Entry(entry.getLow(), entry.getHighestDeliveredSeqno() +1, entry.getHighestReceivedSeqno());
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



    public void setHighestDeliveredAndSeenSeqnos(Address sender, long low_seqno, long highest_delivered_seqno, long highest_received_seqno) {
        Entry entry=senders.get(sender);
        if(entry != null) {
            checkSealed();
            Entry new_entry=new Entry(low_seqno, highest_delivered_seqno, highest_received_seqno);
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
