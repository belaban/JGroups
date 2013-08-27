package org.jgroups.util;

import org.jgroups.Address;

import java.util.Arrays;

/**
 * A mutable version of Digest. Has a fixed size (that of the members), but individual seqnos can be changed.
 * This class is not synchronized
 * @author Bela Ban
 */
public class MutableDigest extends Digest {

    public MutableDigest(Address[] members) {
        super(members, createEmptyArray(members.length *2));
    }


    /** Only used for testing */
    public MutableDigest(Digest digest) {
        super(digest);
    }


    public MutableDigest set(Address member, long highest_delivered_seqno, long highest_received_seqno) {
        if(member == null)
            return this;
        int index=find(member);
        if(index >= 0) {
            seqnos[index * 2]=highest_delivered_seqno;
            seqnos[index * 2 +1]=highest_received_seqno;
        }
        return this;
    }

    /** Returns true if all members have a corresponding seqno >= 0, else false */
    public boolean allSet() {
        for(int i=0; i < seqnos.length; i+=2)
            if(seqnos[i] == -1)
                return false;
        return true;
    }

    /** Returns an array of members whose seqno is not set. Returns an empty array if all are set. */
    public Address[] getNonSetMembers() {
        Address[] retval=new Address[countNonSetMembers()];
        if(retval.length == 0)
            return retval;
        int index=0;
        for(int i=0; i < members.length; i++)
            if(seqnos[i*2] == -1)
                retval[index++]=members[i];
        return retval;
    }

    public MutableDigest set(Digest digest) {
        if(digest == null)
            return this;
        for(Entry entry: digest)
            set(entry.getMember(), entry.getHighestDeliveredSeqno(), entry.getHighestReceivedSeqno());
        return this;
    }


    /**
     * Adds a digest to this digest. For each sender in the other digest, the merge() method will be called.
     */
    public MutableDigest merge(Digest digest) {
        if(digest == null)
            return this;
        for(Entry entry: digest)
            merge(entry.getMember(), entry.getHighestDeliveredSeqno(), entry.getHighestReceivedSeqno());
        return this;
    }


    /**
     * Similar to set(), but if the sender already exists, its seqnos will be modified (no new entry) as follows:
     * <ol>
     * <li>this.highest_delivered_seqno=max(this.highest_delivered_seqno, highest_delivered_seqno)
     * <li>this.highest_received_seqno=max(this.highest_received_seqno, highest_received_seqno)
     * </ol>
     */
    public MutableDigest merge(final Address member, final long highest_delivered_seqno, final long highest_received_seqno) {
        if(member == null)
            return this;
        long[] entry=get(member);
        long hd=entry == null? highest_delivered_seqno : Math.max(entry[0],highest_delivered_seqno);
        long hr=entry == null? highest_received_seqno  : Math.max(entry[1],highest_received_seqno);
        return set(member, hd, hr);
    }


    protected static long[] createEmptyArray(int size) {
        long[] retval=new long[size];
        Arrays.fill(retval, -1);
        return retval;
    }

    protected int countNonSetMembers() {
        int retval=0;
        for(int i=0; i < seqnos.length; i+=2)
            if(seqnos[i] == -1)
                retval++;
        return retval;
    }

}
