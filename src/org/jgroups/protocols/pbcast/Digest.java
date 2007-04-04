package org.jgroups.protocols.pbcast;

import static java.lang.Math.max;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.annotations.Immutable;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


/**
 * A message digest, which is used by the PBCAST layer for gossiping (also used by NAKACK for
 * keeping track of current seqnos for all members). It contains pairs of senders and a range of seqnos
 * (low and high), where each sender is associated with its highest and lowest seqnos seen so far.  That
 * is, the lowest seqno which was not yet garbage-collected and the highest that was seen so far and is
 * deliverable (or was already delivered) to the application.  A range of [0 - 0] means no messages have
 * been received yet. 
 * <p> April 3 2001 (bela): Added high_seqnos_seen member. It is used to disseminate
 * information about the last (highest) message M received from a sender P. Since we might be using a
 * negative acknowledgment message numbering scheme, we would never know if the last message was
 * lost. Therefore we periodically gossip and include the last message seqno. Members who haven't seen
 * it (e.g. because msg was dropped) will request a retransmission. See DESIGN for details.
 * @author Bela Ban
 * @version $Id: Digest.java,v 1.35 2007/04/04 05:13:40 belaban Exp $
 */
public class Digest implements Externalizable, Streamable {
	
	public static final Digest EMPTY_DIGEST = new Digest();
    /** Map<Address, Entry> */
    protected final Map<Address,Entry> senders;
    
    protected static final Log log=LogFactory.getLog(Digest.class);
    static final boolean warn=log.isWarnEnabled();



    /** Used for externalization */
    public Digest() {
       senders=createSenders(7);
    }

    public Digest(int size) {
        senders=createSenders(size);
    }

    /** Creates a new digest from an existing map by copying the keys and values from map */
    public Digest(Map<Address, Entry> map) {
        senders=createSenders(map);
    }
    

    public Digest(Digest d) {
        this(d.senders);
    }


    public Digest(Address sender, long low, long high, long high_seen) {
        senders=createSenders(1);
        senders.put(sender, new Entry(low, high, high_seen));
    }

    public Digest(Address sender, long low, long high) {
        senders=createSenders(1);
        senders.put(sender, new Entry(low, high));
    }

    /** Returns an unmodifiable map, so modifications will result in exceptions */
    public Map<Address, Entry> getSenders() {
        return Collections.unmodifiableMap(senders);
    }

    public boolean equals(Object obj) {
        if(obj == null)
            return false;
        Digest other=(Digest)obj;
        return senders.equals(other.senders);
    }


    public boolean contains(Address sender) {
        return senders.containsKey(sender);
    }

    /** Returns the Entry for the given sender. Note that Entry is immutable */
    public Entry get(Address sender) {
        return senders.get(sender);
    }


    /**
     * Compares two digests and returns true if the senders are the same, otherwise false.
     * @param other
     * @return True if senders are the same, otherwise false.
     */
    public boolean sameSenders(Digest other) {
        if(other == null) return false;        
        if(this.senders.size() != other.senders.size()) return false;

        Set<Address> my_senders=senders.keySet(), other_senders=other.senders.keySet();
        return my_senders.equals(other_senders);
    }

    public Digest difference(Digest other) {
        if(other == null) return copy();

        Digest result=EMPTY_DIGEST;
        if(this.equals(other)) {
            return result;
        }
        else {
            //find intersection and compare their entries
            Map<Address, Entry> resultMap=new ConcurrentHashMap<Address, Entry>(7);
            Set<Address> intersection=new TreeSet<Address>(this.senders.keySet());
            intersection.retainAll(other.senders.keySet());

            for(Address address : intersection) {
                Entry e1=this.get(address);
                Entry e2=other.get(address);
                if(e1.getHigh() != e2.getHigh()) {
                    long low=Math.min(e1.high_seqno, e2.high_seqno);
                    long high=max(e1.high_seqno, e2.high_seqno);
                    Entry r=new Entry(low, high);
                    resultMap.put(address, r);
                }
            }

            //any entries left in (this - intersection)?
            //if yes, add them to result
            if(intersection.size() != this.senders.keySet().size()) {
                Set<Address> thisMinusInteresection=new TreeSet<Address>(this.senders.keySet());
                thisMinusInteresection.removeAll(intersection);
                for(Address address : thisMinusInteresection) {
                    resultMap.put(address, new Entry(this.get(address)));
                }
            }

            //any entries left in (other - intersection)?
            //if yes, add them to result
            if(intersection.size() != other.senders.keySet().size()) {
                Set<Address> otherMinusInteresection=new TreeSet<Address>(other.senders.keySet());
                otherMinusInteresection.removeAll(intersection);
                for(Address address : otherMinusInteresection) {
                    resultMap.put(address, new Entry(other.get(address)));
                }
            }
            result=new Digest(resultMap);
        }
        return result;
    }
    
    public Digest highestSequence(Digest other) {
        if(other == null) return copy();

        Digest result=EMPTY_DIGEST;
        if(this.equals(other)) {
            return result;
        }
        else {
            //find intersection and compare their entries
            Map<Address, Entry> resultMap=new ConcurrentHashMap<Address, Entry>(7);
            Set<Address> intersection=new TreeSet<Address>(this.senders.keySet());
            intersection.retainAll(other.senders.keySet());

            for(Address address : intersection) {
                Entry e1=this.get(address);
                Entry e2=other.get(address);                
                    
                long high=max(e1.high_seqno, e2.high_seqno);
                Entry r=new Entry(0, high);
                resultMap.put(address, r);                
            }

            //any entries left in (this - intersection)?
            //if yes, add them to result
            if(intersection.size() != this.senders.keySet().size()) {
                Set<Address> thisMinusInteresection=new TreeSet<Address>(this.senders.keySet());
                thisMinusInteresection.removeAll(intersection);
                for(Address address : thisMinusInteresection) {
                    resultMap.put(address, new Entry(this.get(address)));
                }
            }

            //any entries left in (other - intersection)?
            //if yes, add them to result
            if(intersection.size() != other.senders.keySet().size()) {
                Set<Address> otherMinusInteresection=new TreeSet<Address>(other.senders.keySet());
                otherMinusInteresection.removeAll(intersection);
                for(Address address : otherMinusInteresection) {
                    resultMap.put(address, new Entry(other.get(address)));
                }
            }
            result=new Digest(resultMap);
        }
        return result;
    }


    public int size() {
        return senders.size();
    }


    public long lowSeqnoAt(Address sender) {
        Entry entry=senders.get(sender);
        if(entry == null)
            return -1;
        else
            return entry.low_seqno;
    }


    public long highSeqnoAt(Address sender) {
        Entry entry=senders.get(sender);
        if(entry == null)
            return -1;
        else
            return entry.high_seqno;
    }


    public long highSeqnoSeenAt(Address sender) {
        Entry entry=senders.get(sender);
        if(entry == null)
            return -1;
        else
            return entry.high_seqno_seen;
    }


    /**
     * Returns true if all senders of the current digest have their seqnos >= the ones from other
     * @param other
     * @return
     */
    public boolean isGreaterThanOrEqual(Digest other) {
        if(other == null)
            return true;
        Map<Address,Entry> our_map=getSenders();
        Address sender;
        Entry my_entry, their_entry;
        long my_highest, their_highest;
        for(Map.Entry<Address,Entry> entry: our_map.entrySet()) {
            sender=entry.getKey();
            my_entry=entry.getValue();
            their_entry=other.get(sender);
            if(their_entry == null)
                continue;
            my_highest=my_entry.getHighest();
            their_highest=their_entry.getHighest();
            if(my_highest < their_highest)
                return false;
        }
        return true;
    }


    public Digest copy() {
        return new Digest(senders);
    }


    public String toString() {
        StringBuilder sb=new StringBuilder();
        boolean first=true;
        if(senders.isEmpty()) return "[]";
        Map.Entry<Address,Entry> entry;
        Address key;
        Entry val;

        for(Iterator<Map.Entry<Address,Entry>> it=senders.entrySet().iterator(); it.hasNext();) {
            entry=it.next();
            key=entry.getKey();
            val=entry.getValue();
            if(!first) {
                sb.append(", ");
            }
            else {
                first=false;
            }
            sb.append(key).append(": ").append('[').append(val.low_seqno).append(" : ");
            sb.append(val.high_seqno);
            if(val.high_seqno_seen >= 0)
                sb.append(" (").append(val.high_seqno_seen).append(")");
            sb.append("]");
        }
        return sb.toString();
    }


    public String printHighSeqnos() {
        StringBuilder sb=new StringBuilder("[");
        boolean first=true;
        Map.Entry<Address,Entry> entry;
        Address key;
        Entry val;

        TreeMap copy=new TreeMap(senders);
        for(Iterator<Map.Entry<Address, Entry>> it=copy.entrySet().iterator(); it.hasNext();) {
            entry=it.next();
            key=entry.getKey();
            val=entry.getValue();
            if(!first) {
                sb.append(", ");
            }
            else {
                first=false;
            }
            sb.append(key).append("#").append(val.high_seqno);
        }
        sb.append(']');
        return sb.toString();
    }


    public String printHighSeqnosSeen() {
        StringBuilder sb=new StringBuilder();
        boolean first=true;
        Map.Entry<Address,Entry> entry;
        Address key;
        Entry val;

        for(Iterator<Map.Entry<Address, Entry>> it=senders.entrySet().iterator(); it.hasNext();) {
            entry=it.next();
            key=entry.getKey();
            val=entry.getValue();
            if(!first) {
                sb.append(", ");
            }
            else {
                sb.append('[');
                first=false;
            }
            sb.append(key).append("#").append(val.high_seqno_seen);
        }
        sb.append(']');
        return sb.toString();
    }


    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(senders);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        Map<Address, Entry> tmp = (Map<Address, Entry>)in.readObject();
        senders.clear();
        senders.putAll(tmp);
    }

    public void writeTo(DataOutputStream out) throws IOException {
        out.writeShort(senders.size());
        Map.Entry<Address,Entry> entry;
        Address key;
        Entry val;
        for(Iterator it=senders.entrySet().iterator(); it.hasNext();) {
            entry=(Map.Entry<Address, Entry>)it.next();
            key=entry.getKey();
            val=entry.getValue();
            Util.writeAddress(key, out);
            out.writeLong(val.low_seqno);
            out.writeLong(val.high_seqno);
            out.writeLong(val.high_seqno_seen);
        }
    }


    public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
        short size=in.readShort();
        Map<Address,Entry> tmp=new HashMap<Address, Entry>(size);
        Address key;
        for(int i=0; i < size; i++) {
            key=Util.readAddress(in);
            tmp.put(key, new Entry(in.readLong(), in.readLong(), in.readLong()));
        }
        senders.clear();
        senders.putAll(tmp);
    }


    public long serializedSize() {
        long retval=Global.SHORT_SIZE; // number of elements in 'senders'
        if(!senders.isEmpty()) {
            Address addr=senders.keySet().iterator().next();
            int len=addr.size() +
                    2 * Global.BYTE_SIZE; // presence byte, IpAddress vs other address
            len+=Entry.SIZE; // 3 longs in one Entry
            retval+=len * senders.size();
        }
        return retval;
    }

    private static Map<Address, Entry> createSenders(int size) {
        return new ConcurrentHashMap<Address,Entry>(size);
    }

    private static Map<Address, Entry> createSenders(Map<Address, Entry> map) {
        return new ConcurrentHashMap<Address,Entry>(map);
    }



    /**
     * Class keeping track of the lowest and highest sequence numbers delivered, and the highest
     * sequence numbers received, per member. This class is immutable
     */
    @Immutable
    public static class Entry implements Externalizable, Streamable {
        private long low_seqno, high_seqno, high_seqno_seen=-1;
        final static int SIZE=Global.LONG_SIZE * 3;

        public Entry() {
        }

        public Entry(long low_seqno, long high_seqno, long high_seqno_seen) {
            this.low_seqno=low_seqno;
            this.high_seqno=high_seqno;
            this.high_seqno_seen=high_seqno_seen;
            check();
        }



        public Entry(long low_seqno, long high_seqno) {
            this.low_seqno=low_seqno;
            this.high_seqno=high_seqno;
            check();
        }

        public Entry(Entry other) {
            if(other != null) {
                low_seqno=other.low_seqno;
                high_seqno=other.high_seqno;
                high_seqno_seen=other.high_seqno_seen;
                check();
            }
        }

        public final long getLow() {return low_seqno;}
        public final long getHigh() {return high_seqno;}
        public final long getHighSeen() {return high_seqno_seen;}
        public final long getHighest() {return max(high_seqno, high_seqno_seen);}

        public boolean equals(Object obj) {
            Entry other=(Entry)obj;
            return low_seqno == other.low_seqno && high_seqno == other.high_seqno && high_seqno_seen == other.high_seqno_seen;
        }

        public String toString() {
            return new StringBuffer("low=").append(low_seqno).append(", high=").append(high_seqno).
                    append(", highest seen=").append(high_seqno_seen).toString();
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeLong(low_seqno);
            out.writeLong(high_seqno);
            out.writeLong(high_seqno_seen);
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            low_seqno=in.readLong();
            high_seqno=in.readLong();
            high_seqno_seen=in.readLong();
        }

        public int size() {
            return SIZE;
        }

        public void writeTo(DataOutputStream out) throws IOException {
            out.writeLong(low_seqno);
            out.writeLong(high_seqno);
            out.writeLong(high_seqno_seen);
        }

        public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
            low_seqno=in.readLong();
            high_seqno=in.readLong();
            high_seqno_seen=in.readLong();
        }


        private void check() {
            if(low_seqno > high_seqno)
                throw new IllegalArgumentException("low_seqno (" + low_seqno + ") is greater than high_seqno (" + high_seqno + ")");
        }


    }
}
