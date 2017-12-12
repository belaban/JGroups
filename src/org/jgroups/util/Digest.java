package org.jgroups.util;

import org.jgroups.Address;
import org.jgroups.Constructable;
import org.jgroups.Global;
import org.jgroups.annotations.Immutable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.function.Supplier;

import static java.lang.Math.max;


/**
 * A message digest containing - for each member - the highest seqno delivered (hd) and the highest seqno received (hr).
 * The seqnos are stored according to the order of the members in the associated membership, ie. seqnos[0] is the hd for
 * member members[0], seqnos[1] is the hr for the same member, seqnos[2] is the hd for members[1] and so on.<p/>
 * Field 'members' may refer to the View.members, e.g. in a JoinRsp where we ship a view and a digest referring to
 * the view's membership. This is done to conserve memory.<p/>
 * This class is immutable except for 2 cases:
 * <ul>
 *     <li>The contents are read when unmarshalling (readFrom())</li>
 *     <li>The membership is set with members(). This must only be done directly after unmarshalling</li>
 * </ul>
 * @author Bela Ban
 */
@Immutable
public class Digest implements SizeStreamable, Iterable<Digest.Entry>, Constructable<Digest> {

    // Stores the members corresponding to the seqnos. Example: members[2] --> hd=seqnos[4], hr=seqnos[5]
    protected Address[] members;

    // Stores highest delivered and received seqnos. This array is double the size of members. We store HD-HR pairs,
    // so to get the HD seqno for member P at index i --> seqnos[i*2], to get the HR --> seqnos[i*2 +1]
    protected long[]    seqnos;



    /** Used for serialization */
    public Digest() {
    }

    public Digest(final Address[] members, long[] seqnos) {
        if(members == null) throw new IllegalArgumentException("members is null");
        if(seqnos == null) throw new IllegalArgumentException("seqnos is null");
        this.members=members;
        this.seqnos=seqnos;
        checkPostcondition();
    }

    /** Only used internally, don't use ! */
    public Digest(final Address[] members) {
        if(members == null) throw new IllegalArgumentException("members is null");
        this.members=members;
    }

    /** Only used for testing */
    public Digest(Digest digest) {
        if(digest == null)
            return;
        this.members=digest.members; // the members list is immutable
        this.seqnos=(digest instanceof MutableDigest || this instanceof MutableDigest)?
          Arrays.copyOf((digest).seqnos, digest.seqnos.length) : digest.seqnos;
        checkPostcondition();
    }

    /** Creates a new digest from an existing map by copying the keys and values from map */
    public Digest(Map<Address, long[]> map) {
        createArrays(map);
        checkPostcondition();
    }

    public Digest(Address sender, long highest_delivered, long highest_received) {
        members=new Address[]{sender};
        seqnos=new long[]{highest_delivered,highest_received};
    }

    public Supplier<? extends Digest> create() {
        return Digest::new;
    }

    /** Don't use, this method is reserved for Bela ! :-) */
    public Address[] getMembersRaw() {
        return members;
    }


    public int capacity() {
        return members != null? members.length : 0;
    }

    public boolean contains(Address mbr) {
        if(mbr == null || members == null)
            return false;
        for(Address member: members)
            if(Objects.equals(member, mbr))
                return true;
        return false;
    }

    public boolean containsAll(Address ... members) {
        for(Address member: members)
            if(!contains(member))
                return false;
        return true;
    }


    /** 2 digests are equal if their memberships match and all highest-delivered and highest-received seqnos match */
    public boolean equals(Object obj) {
        if(this == obj)
            return true;
        Digest other=(Digest)obj;
         // return Arrays.equals(members, other.members) && Arrays.equals(seqnos,other.seqnos);

        boolean same_mbrs=Arrays.equals(members, other.members);
        boolean same_seqnos=Arrays.equals(seqnos, other.seqnos);

        return same_mbrs && same_seqnos;
    }

    /**
     * Returns the highest delivered and received seqnos associated with a member.
     * @param member
     * @return An array of 2 elements: highest_delivered and highest_received seqnos
     */
    public long[] get(Address member) {
        int index=find(member);
        if(index < 0)
            return null;
        return new long[]{seqnos[index * 2], seqnos[index * 2 +1]};
    }


    public Iterator<Entry> iterator() {
        return new MyIterator();
    }


    public Digest copy() {
        return new Digest(members, Arrays.copyOf(seqnos, seqnos.length));
    }

    @Override
    public void writeTo(DataOutput out) throws IOException {
        writeTo(out, true);
    }

    public void writeTo(DataOutput out, boolean write_addrs) throws IOException {
        if(write_addrs)
            Util.writeAddresses(members, out);
        else
            out.writeShort(members.length);
        for(int i=0; i < capacity(); i++)
            Bits.writeLongSequence(seqnos[i * 2], seqnos[i * 2 +1], out);
    }

    @Override
    public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        readFrom(in, true);
    }

    public void readFrom(DataInput in, boolean read_addrs) throws IOException, ClassNotFoundException {
        if(read_addrs) {
            members=Util.readAddresses(in);
            seqnos=new long[capacity() * 2];
        }
        else
            seqnos=new long[in.readShort() *2];

        for(int i=0; i < seqnos.length/2; i++)
            Bits.readLongSequence(in, seqnos, i*2);
    }

    @Override
    public int serializedSize() {
        return (int)serializedSize(true);
    }

    public long serializedSize(boolean with_members) {
        long retval=with_members? Util.size(members) : Global.SHORT_SIZE;
        for(int i=0; i < members.length; i++)
            retval+=Bits.size(seqnos[i*2], seqnos[i*2+1]);
        return retval;
    }


    public String toString() {
        return toString(members, true);
    }

    public String toString(final Digest order) {
        return order != null? toString(order.members, true) : toString(members, true);
    }

    public String toString(final Address[] order, boolean print_highest_received) {
        StringBuilder sb=new StringBuilder();
        boolean first=true;
        if(capacity() == 0) return "[]";

        int count=0, capacity=capacity();
        for(Address key: order) {
            long[] tmp_seqnos=key != null? get(key) : null;
            if(key == null || tmp_seqnos == null)
                continue;
            if(!first)
                sb.append(", ");
            else
                first=false;
            sb.append(key).append(": ").append('[').append(tmp_seqnos[0]);
            if(print_highest_received)
                sb.append(" (").append(tmp_seqnos[1]).append(")");
            sb.append("]");
            if(Util.MAX_LIST_PRINT_SIZE > 0 && ++count >= Util.MAX_LIST_PRINT_SIZE) {
                if(capacity > count)
                    sb.append(", ...");
                break;
            }
        }
        return sb.toString();
    }

    protected int find(Address mbr) {
        if(mbr == null || members == null)
            return -1;
        for(int i=0; i < members.length; i++) {
            Address member=members[i];
            if(Objects.equals(member, mbr))
                return i;
        }
        return -1;
    }


    protected void createArrays(Map<Address,long[]> map) {
        int size=map.size();
        members=new Address[size];
        seqnos=new long[size * 2];

        int index=0;
        for(Map.Entry<Address,long[]> entry: map.entrySet()) {
            members[index]=entry.getKey();
            seqnos[index * 2   ]=entry.getValue()[0];
            seqnos[index * 2 +1]=entry.getValue()[1];
            index++;
        }
    }


    protected void checkPostcondition() {
        int size=members.length;
        if(size*2 != seqnos.length)
            throw new IllegalArgumentException("seqnos.length (" + seqnos.length + ") is not twice the members size (" + size + ")");
    }


    protected class MyIterator implements Iterator<Entry> {
        protected int index;

        public boolean hasNext() {
            return index < capacity();
        }

        public Entry next() {
            if(index >= capacity())
                throw new NoSuchElementException("index=" + index + ", capacity=" + capacity());
            Address mbr=members != null? members[index] : null;
            long hd=seqnos != null? seqnos[index*2] : 0, hr=seqnos != null? seqnos[index*2+1] : 0;
            Entry entry=new Entry(mbr, hd, hr);
            index++;
            return entry;
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }


    /** Keeps track of one members plus its highest delivered and received seqnos */
    @Immutable
    public static class Entry {
        protected final Address member;
        protected final long    hd;
        protected final long    hr;

        public Entry(Address member, long highest_delivered, long highest_received) {
            this.member=member;
            this.hd=highest_delivered;
            this.hr=highest_received;
        }

        public Address getMember()                {return member;}
        public long    getHighestDeliveredSeqno() {return hd;}
        public long    getHighestReceivedSeqno()  {return hr;}
        public long    getHighest()               {return max(hd,hr);}

        public boolean equals(Object obj) {
            Entry other=(Entry)obj;
            return member.equals(other.member) && hd == other.hd && hr == other.hr;
        }

        public String toString() {
            return member + ": [" + hd + " (" + hr + ")]";
        }
    }


}
