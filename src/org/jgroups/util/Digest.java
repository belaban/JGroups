package org.jgroups.util;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.annotations.Immutable;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static java.lang.Math.max;


/**
 * A message digest, containing senders and ranges of seqnos, where each sender is associated with its highest delivered
 * and received seqno seen so far.<p/>
 * <p> April 3 2001 (bela): Added high_seqnos_seen member. It is used to disseminate
 * information about the last (highest) message M received from a sender P. Since we might be using a
 * negative acknowledgment message numbering scheme, we would never know if the last message was
 * lost. Therefore we periodically gossip and include the last message seqno. Members who haven't seen
 * it (e.g. because msg was dropped) will request a retransmission. See DESIGN for details.
 * @author Bela Ban
 */
public class Digest implements Streamable, Iterable<Digest.DigestEntry> {

    // Stores all members. Random access is seldom, 99% of all use cases is sequential iteration through members
    protected Address[] members;

    // Stores highest delivered and received seqnos. This array is double the size of members. We store HD-HR pairs,
    // so to get the HD seqno for member P at index I --> seqnos[I * 2], to get the HR --> seqnos[i*2+1]
    protected long[]    seqnos;
    

    protected Digest(Address[] members, long[] seqnos) {
        this.members=members;
        this.seqnos=seqnos;
    }

    /** Used for serialization */
    public Digest() {
    }


    /** Creates a new digest from an existing map by copying the keys and values from map */
    public Digest(Map<Address, long[]> map) {
        createArrays(map);
    }

    public Digest(Digest digest) {
        if(digest == null)
            return;
        createArrays(digest.size());
        System.arraycopy(digest.members, 0, members, 0, digest.size());
        System.arraycopy(digest.seqnos, 0, seqnos, 0, digest.size() *2);
    }

    public Digest(Address sender, long highest_delivered, long highest_received) {
        members=new Address[]{sender};
        seqnos=new long[]{highest_delivered,highest_received};
    }

    public Digest(Address sender, long highest_delivered) {
        this(sender, highest_delivered, highest_delivered);
    }

    public boolean contains(Address member) {
        for(int i=0; i < size(); i++) {
            Address addr=members[i];
            if(addr != null && addr.equals(member))
                return true;
        }
        return false;
    }

    /**
     * Returns true if our 'members' array contains all of the elements in  other.members
     * @param other
     * @return
     */
    public boolean containsAll(Digest other) {
        if(other == null)
            return false;
        for(int i=0; i < other.size(); i++) {
            Address member=other.members[i];
            if(!contains(member))
                return false;
        }
        return true;
    }


    /** Not really used, other than in unit tests and by FLUSH (reconciliation phase), so it doesn't need to be
     * super efficient */
    public boolean equals(Object obj) {
        if(this == obj)
            return true;
        Digest other=(Digest)obj;
        if(!sameSenders(other))
            return false;
        for(DigestEntry entry: this) {
            Address addr=entry.getMember();
            long[] tmp=other.get(addr);
            if(entry.getHighestDeliveredSeqno() != tmp[0] || entry.getHighestReceivedSeqno() != tmp[1])
                return false;
        }
        return true;
    }

    /**
     * Returns the highest delivered and received seqnos associated with a member.
     * This searches the members array sequentially, so use
     * sparingly
     * @param member
     * @return An array of 2 elements: highest_delivered and highest_received seqnos
     */
    public long[] get(Address member) {
        int index=find(member);
        if(index < 0)
            return null;
        return new long[]{seqnos[index * 2], seqnos[index * 2 +1]};
    }


    public Set<Address> getMembers() {
        HashSet<Address> retval=new HashSet<Address>(size());
        for(int i=0; i < size(); i++) {
            Address tmp=members[i];
            if(tmp != null)
                retval.add(tmp);
        }
        return retval;
    }

    /**
     * Compares two digests and returns true if the senders are the same, otherwise false.
     * @param other
     * @return True if senders are the same, otherwise false.
     */
    public boolean sameSenders(Digest other) {
        return other != null && size() == other.size() && containsAll(other);
    }
    

    public Digest difference(Digest other) {
        if(other == null) return copy();

        if(this.equals(other))
            return null;
        
        // find intersection and compare their entries
        Map<Address,long[]> resultMap=new ConcurrentHashMap<Address,long[]>(7);
        Set<Address> intersection=new TreeSet<Address>(this.getMembers());
        intersection.retainAll(other.getMembers());

        for(Address address : intersection) {
            long[] e1=this.get(address);
            long[] e2=other.get(address);
            if(e1[0] != e2[0]) {
                long low=Math.min(e1[0], e2[0]);
                long high=max(e1[0], e2[0]);

                long[] r={low, high};
                resultMap.put(address, r);
            }
        }

        // any entries left in (this - intersection) ?
        // if yes, add them to result
        if(intersection.size() != this.size()) {
            Set<Address> thisMinusInteresection=new TreeSet<Address>(this.getMembers());
            thisMinusInteresection.removeAll(intersection);
            for(Address address : thisMinusInteresection) {
                long[] tmp=this.get(address);
                if(tmp != null)
                    resultMap.put(address, tmp);
            }
        }

        // any entries left in (other - intersection) ?
        // if yes, add them to result
        if(intersection.size() != other.size()) {
            Set<Address> otherMinusInteresection=new TreeSet<Address>(other.getMembers());
            otherMinusInteresection.removeAll(intersection);
            for(Address address : otherMinusInteresection) {
                long[] tmp=other.get(address);
                if(tmp != null)
                    resultMap.put(address, tmp);
            }
        }
        return new Digest(resultMap);
    }
    
    public Digest highestSequence(Digest other) {
        if(other == null) return copy();

        if(this.equals(other))
            return this;

        //find intersection and compare their entries
        Map<Address,long[]> resultMap=new ConcurrentHashMap<Address,long[]>(7);
        Set<Address> intersection=new TreeSet<Address>(this.getMembers());
        intersection.retainAll(other.getMembers());

        for(Address address : intersection) {
            long[] e1=this.get(address);
            long[] e2=other.get(address);
                    
            long high=max(e1[0], e2[0]);

            long[] r={0, high};
            resultMap.put(address, r);
        }

        // any entries left in (this - intersection)?
        // if yes, add them to result
        if(intersection.size() != this.size()) {
            Set<Address> thisMinusInteresection=new TreeSet<Address>(this.getMembers());
            thisMinusInteresection.removeAll(intersection);
            for(Address address : thisMinusInteresection) {
                long[] tmp=get(address);
                if(tmp != null)
                    resultMap.put(address, tmp);
            }
        }

        // any entries left in (other - intersection) ?
        // if yes, add them to result
        if(intersection.size() != other.size()) {
            Set<Address> otherMinusInteresection=new TreeSet<Address>(other.getMembers());
            otherMinusInteresection.removeAll(intersection);
            for(Address address : otherMinusInteresection) {
                long[] tmp=other.get(address);
                if(tmp != null)
                    resultMap.put(address, tmp);
            }
        }
        return new Digest(resultMap);
    }


    public int size() {
        return members.length;
    }


    public long highestDeliveredSeqnoAt(Address sender) {
        long[] entry=get(sender);
        if(entry == null)
            return -1;
        return entry[0];
    }


    public long highestReceivedSeqnoAt(Address sender) {
        long[] entry=get(sender);
        if(entry == null)
            return -1;
        return entry[1];
    }


    /**
     * Returns true if all senders of the current digest have their seqnos >= the ones from other
     * @param other
     * @return
     */
    public boolean isGreaterThanOrEqual(Digest other) {
        if(other == null)
            return true;

        for(DigestEntry entry: this) {
            Address sender=entry.getMember();
            long[] their_entry=other.get(sender);
            if(their_entry == null)
                continue;
            long my_highest=entry.getHighest();
            long their_highest=Math.max(their_entry[0], their_entry[1]);
            if(my_highest < their_highest)
                return false;
        }
        return true;
    }


    public Digest copy() {
        return new Digest(Arrays.copyOf(members, members.length), Arrays.copyOf(seqnos, seqnos.length));
    }

    public String toString() {
        StringBuilder sb=new StringBuilder();
        boolean first=true;
        if(size() == 0) return "[]";

        int count=0, size=size();
        for(DigestEntry entry: this) {
            Address key=entry.getMember();
            if(!first)
                sb.append(", ");
            else
                first=false;
            sb.append(key).append(": ").append('[').append(entry.getHighestDeliveredSeqno());
            if(entry.getHighestReceivedSeqno() >= 0)
                sb.append(" (").append(entry.getHighestReceivedSeqno()).append(")");
            sb.append("]");
            if(Util.MAX_LIST_PRINT_SIZE > 0 && ++count >= Util.MAX_LIST_PRINT_SIZE) {
                if(size > count)
                    sb.append(", ...");
                break;
            }
        }
        return sb.toString();
    }

    public String toStringSorted() {
        return toStringSorted(true);
    }

    public String toStringSorted(boolean print_highest_received) {
        StringBuilder sb=new StringBuilder();
        boolean first=true;
        if(size() == 0) return "[]";

        TreeMap<Address,long[]> copy=new TreeMap<Address,long[]>();
        for(int i=0; i < size(); i++) {
            Address addr=members[i];
            long[] tmp={seqnos[i * 2], seqnos[i * 2 +1]};
            copy.put(addr, tmp);
        }

        int count=0, size=copy.size();
        for(Map.Entry<Address,long[]> entry: copy.entrySet()) {
            Address key=entry.getKey();
            long[] val=entry.getValue();
            if(!first)
                sb.append(", ");
            else
                first=false;
            sb.append(key).append(": ").append('[').append(val[0]);
            if(print_highest_received)
                sb.append(" (").append(val[1]).append(")");
            sb.append("]");
            if(Util.MAX_LIST_PRINT_SIZE > 0 && ++count >= Util.MAX_LIST_PRINT_SIZE) {
                if(size > count)
                    sb.append(", ...");
                break;
            }
        }
        return sb.toString();
    }


    public String printHighestDeliveredSeqnos() {
        return toStringSorted(false);
    }


    public void writeTo(DataOutput out) throws Exception {
        out.writeShort(size());
        for(int i=0; i < size(); i++)
            Util.writeAddress(members[i], out);

        for(int i=0; i < size(); i++)
            Util.writeLongSequence(seqnos[i * 2], seqnos[i * 2 +1], out);
    }


    public void readFrom(DataInput in) throws Exception {
        short size=in.readShort();
        createArrays(size);

        for(int i=0; i < size; i++) {
            Address addr=Util.readAddress(in);
            members[i]=addr;
        }

        for(int i=0; i < size; i++) {
            long[] tmp=Util.readLongSequence(in);
            seqnos[i * 2]=tmp[0];
            seqnos[i * 2 +1]=tmp[1];
        }
    }


    public long serializedSize() {
        long retval=Global.SHORT_SIZE; // number of elements in 'senders'
        if(size() > 0) {
            Address addr=members[0];
            retval+=Util.size(addr) * size();
        }

        for(int i=0; i < size() * 2; i+=2)
            retval+=Util.size(seqnos[i], seqnos[i+1]);
        return retval;
    }


    protected int find(Address member) {
        for(int i=0; i < size(); i++) {
            Address addr=members[i];
            if(addr != null && addr.equals(member))
                return i;
        }
        return -1;
    }




    protected void createArrays(int size) {
        members=new Address[size];
        seqnos=new long[size * 2];
    }

    protected void createArrays(Map<Address,long[]> map) {
        createArrays(map.size());
        int index=0;
        for(Map.Entry<Address,long[]> entry: map.entrySet()) {
            members[index]=entry.getKey();
            seqnos[index * 2   ]=entry.getValue()[0];
            seqnos[index * 2 +1]=entry.getValue()[1];
            index++;
        }
    }

    public Iterator<DigestEntry> iterator() {
        return new MyIterator();
    }


    protected class MyIterator implements Iterator<DigestEntry> {
        int index=0;

        public boolean hasNext() {
            return index < size();
        }

        public DigestEntry next() {
            if(index >= size())
                throw new NoSuchElementException("index=" + index + ", members.length=" + members.length);

            DigestEntry entry=new DigestEntry(members[index], seqnos[index * 2], seqnos[index * 2 +1]);
            index++;
            return entry;
        }

        public void remove() {
            members[index]=null; // doesn't make much sense, but won't be called anyway
        }
    }


    /** Keeps track of one members plus its highest delivered and received seqnos */
    @Immutable
    public static class DigestEntry {
        protected final Address member;
        protected final long    highest_delivered;
        protected final long    highest_received;

        public DigestEntry(Address member, long highest_delivered, long highest_received) {
            this.member=member;
            this.highest_delivered=highest_delivered;
            this.highest_received=highest_received;
        }

        public Address getMember()             {return member;}
        public long getHighestDeliveredSeqno() {return highest_delivered;}
        public long getHighestReceivedSeqno()  {return highest_received;}
        public long getHighest()               {return max(highest_delivered, highest_received);}

        public String toString() {
            return member + ": [" + highest_delivered + " (" + highest_received + ")]";
        }
    }


}
