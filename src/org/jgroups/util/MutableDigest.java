package org.jgroups.util;

import org.jgroups.Address;

import java.util.Arrays;
import java.util.Map;

/**
 * A mutable version of Digest. This class is not synchronized because only a single thread at a time will access it
 * @author Bela Ban
 */
public class MutableDigest extends Digest {
    protected static final double RESIZE_FACTOR=1.2;

    protected boolean sealed=false;

    protected int current_index=0; // points to the next index to be written


    protected MutableDigest(Address[] members, long[] seqnos, int current_index) {
        super(members, seqnos);
        this.current_index=current_index;
    }

    /** Used for externalization */
    public MutableDigest() {
        super();
    }

    public MutableDigest(int size) {
        createArrays(size);
    }


    public MutableDigest(Map<Address,long[]> map) {
        super(map);
        current_index=map.size();
    }


    public MutableDigest(Digest digest) {
        super(digest);
        current_index=digest.size();
    }



    public void add(Address member, long highest_delivered_seqno, long highest_received_seqno) {
        add(member, highest_delivered_seqno, highest_received_seqno, true);
    }

    public void add(Address member, long highest_delivered_seqno, long highest_received_seqno, boolean replace) {
        if(member == null)
            return;
        checkSealed();

        if(replace) {
            int index=find(member); // see if the member is already present
            if(index >= 0) {
                seqnos[index * 2]=highest_delivered_seqno;
                seqnos[index * 2 +1]=highest_received_seqno;
                return;
            }
        }

        if(current_index >= members.length)
            resize();
        
        members[current_index]=member;
        seqnos[current_index * 2]=highest_delivered_seqno;
        seqnos[current_index * 2 +1]=highest_received_seqno;
        current_index++;
    }


    public void add(Digest digest) {
        add(digest, true);
    }

    public void add(Digest digest, boolean replace) {
        if(digest == null)
            return;
        checkSealed();
        for(DigestEntry entry: digest)
            add(entry.getMember(), entry.getHighestDeliveredSeqno(), entry.getHighestReceivedSeqno(), replace);
    }

    public void replace(Digest d) {
        if(d == null)
            return;
        clear();
        createArrays(d.size());
        add(d, false); // don't search for existing elements, because there won't be any !
    }


    public MutableDigest copy() {
        return new MutableDigest(Arrays.copyOf(members, members.length), Arrays.copyOf(seqnos, seqnos.length), current_index);
    }

    /**
     * Adds a digest to this digest. This digest must have enough space to add the other digest; otherwise an error
     * message will be written. For each sender in the other digest, the merge() method will be called.
     */
    public void merge(Digest digest) {
        if(digest == null)
            return;
        checkSealed();

        for(DigestEntry entry: digest)
            merge(entry.getMember(), entry.getHighestDeliveredSeqno(), entry.getHighestReceivedSeqno());
    }


    /**
     * Similar to add(), but if the sender already exists, its seqnos will be modified (no new entry) as follows:
     * <ol>
     * <li>this.highest_delivered_seqno=max(this.highest_delivered_seqno, highest_delivered_seqno)
     * <li>this.highest_received_seqno=max(this.highest_received_seqno, highest_received_seqno)
     * </ol>
     * If the member doesn not exist, a new entry will be added (provided there is enough space)
     */
    public void merge(Address member, long highest_delivered_seqno, long highest_received_seqno) {
        if(member == null)
            return;
        checkSealed();
        long[] entry=get(member);
        if(entry == null)
            add(member, highest_delivered_seqno, highest_received_seqno, false); // don't replace as member wasn't found
        else {// replaces existing entry
            add(member, Math.max(entry[0], highest_delivered_seqno), Math.max(entry[1], highest_received_seqno));
        }
    }



    /** Increments the sender's highest delivered seqno by 1 */
    public void incrementHighestDeliveredSeqno(Address member) {
        long[] entry=get(member);
        if(entry == null)
            return;
        checkSealed();

        long new_highest_delivered=entry[0] +1;
        
        // highest_received must be >= highest_delivered, but not smaller !
        long new_highest_received=Math.max(entry[1], new_highest_delivered);
        add(member, new_highest_delivered, new_highest_received, true); // replace
    }



    public void clear() {
        checkSealed();
        current_index=0;
    }



    public void setHighestDeliveredAndSeenSeqnos(Address member, long highest_delivered_seqno, long highest_received_seqno) {
        long[] entry=get(member);
        if(entry != null) {
            checkSealed();
            add(member, highest_delivered_seqno, highest_received_seqno, true);  // replace existing entry
        }
    }

    /** Seals the instance against modifications */
    public void seal() {
        sealed=true;
    }

    public int size() {
        return current_index;
    }


    protected void resize() {
        int new_size=Math.max((int)(members.length * RESIZE_FACTOR), members.length +1);
        members=Arrays.copyOf(members, new_size);
        seqnos=Arrays.copyOf(seqnos, new_size * 2);
    }


    protected final void checkSealed() {
        if(sealed)
            throw new IllegalAccessError("instance has been sealed and cannot be modified");
    }

}
