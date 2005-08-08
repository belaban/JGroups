// $Id: Digest.java,v 1.15 2005/08/08 12:45:38 belaban Exp $

package org.jgroups.protocols.pbcast;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentReaderHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.*;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;


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
 */
public class Digest implements Externalizable, Streamable {
    /** Map<Address, Entry> */
    Map    senders=null;
    protected static final Log log=LogFactory.getLog(Digest.class);





    public Digest() {
    } // used for externalization

    public Digest(int size) {
        senders=createSenders(size);
    }


    public boolean equals(Object obj) {
        if(obj == null)
            return false;
        Digest other=(Digest)obj;
        if(senders == null && other.senders == null)
            return true;
        return senders.equals(other.senders);
    }




    public void add(Address sender, long low_seqno, long high_seqno) {
        add(sender, low_seqno, high_seqno, -1);
    }


    public void add(Address sender, long low_seqno, long high_seqno, long high_seqno_seen) {
        add(sender, new Entry(low_seqno, high_seqno, high_seqno_seen));
    }

    private void add(Address sender, Entry entry) {
        if(sender == null || entry == null) {
            if(log.isErrorEnabled())
                log.error("sender (" + sender + ") or entry (" + entry + ")is null, will not add entry");
            return;
        }
        Object retval=senders.put(sender, entry);
        if(retval != null && log.isWarnEnabled())
            log.warn("entry for " + sender + " was overwritten with " + entry);
    }


    public void add(Digest d) {
        if(d != null) {
            Map.Entry entry;
            Address key;
            Entry val;
            for(Iterator it=d.senders.entrySet().iterator(); it.hasNext();) {
                entry=(Map.Entry)it.next();
                key=(Address)entry.getKey();
                val=(Entry)entry.getValue();
                add(key, val.low_seqno, val.high_seqno, val.high_seqno_seen);
            }
        }
    }

    public void replace(Digest d) {
        if(d != null) {
            Map.Entry entry;
            Address key;
            Entry val;
            clear();
            for(Iterator it=d.senders.entrySet().iterator(); it.hasNext();) {
                entry=(Map.Entry)it.next();
                key=(Address)entry.getKey();
                val=(Entry)entry.getValue();
                add(key, val.low_seqno, val.high_seqno, val.high_seqno_seen);
            }
        }
    }

    public Entry get(Address sender) {
        return (Entry)senders.get(sender);
    }

    public boolean set(Address sender, long low_seqno, long high_seqno, long high_seqno_seen) {
        Entry entry=(Entry)senders.get(sender);
        if(entry == null)
            return false;
        entry.low_seqno=low_seqno;
        entry.high_seqno=high_seqno;
        entry.high_seqno_seen=high_seqno_seen;
        return true;
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
        Map.Entry entry;
        Address sender;
        Entry val;
        for(Iterator it=d.senders.entrySet().iterator(); it.hasNext();) {
            entry=(Map.Entry)it.next();
            sender=(Address)entry.getKey();
            val=(Entry)entry.getValue();
            if(val != null) {
                merge(sender, val.low_seqno, val.high_seqno, val.high_seqno_seen);
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
        Entry entry=(Entry)senders.get(sender);
        if(entry == null) {
            add(sender, low_seqno, high_seqno, high_seqno_seen);
        }
        else {
            if(low_seqno < entry.low_seqno)
                entry.low_seqno=low_seqno;
            if(high_seqno > entry.high_seqno)
                entry.high_seqno=high_seqno;
            if(high_seqno_seen > entry.high_seqno_seen)
                entry.high_seqno_seen=high_seqno_seen;
        }
    }



    public boolean contains(Address sender) {
        return senders.containsKey(sender);
    }


    /**
     * Compares two digests and returns true if the senders are the same, otherwise false.
     * @param other
     * @return True if senders are the same, otherwise false.
     */
    public boolean sameSenders(Digest other) {
        if(other == null) return false;
        if(this.senders == null || other.senders == null) return false;
        if(this.senders.size() != other.senders.size()) return false;

        Set my_senders=senders.keySet(), other_senders=other.senders.keySet();
        return my_senders.equals(other_senders);
    }


    /** 
     * Increments the sender's high_seqno by 1.
     */
    public void incrementHighSeqno(Address sender) {
        Entry entry=(Entry)senders.get(sender);
        if(entry == null)
            return;
        entry.high_seqno++;
    }


    public int size() {
        return senders.size();
    }




    /**
     * Resets the seqnos for the sender at 'index' to 0. This happens when a member has left the group,
     * but it is still in the digest. Resetting its seqnos ensures that no-one will request a message
     * retransmission from the dead member.
     */
    public void resetAt(Address sender) {
        Entry entry=(Entry)senders.get(sender);
        if(entry != null)
            entry.reset();
    }


    public void clear() {
        senders.clear();
    }

    public long lowSeqnoAt(Address sender) {
        Entry entry=(Entry)senders.get(sender);
        if(entry == null)
            return -1;
        else
            return entry.low_seqno;
    }


    public long highSeqnoAt(Address sender) {
        Entry entry=(Entry)senders.get(sender);
        if(entry == null)
            return -1;
        else
            return entry.high_seqno;
    }


    public long highSeqnoSeenAt(Address sender) {
        Entry entry=(Entry)senders.get(sender);
        if(entry == null)
            return -1;
        else
            return entry.high_seqno_seen;
    }


    public void setHighSeqnoAt(Address sender, long high_seqno) {
        Entry entry=(Entry)senders.get(sender);
        if(entry != null)
            entry.high_seqno=high_seqno;
    }

    public void setHighSeqnoSeenAt(Address sender, long high_seqno_seen) {
        Entry entry=(Entry)senders.get(sender);
        if(entry != null)
            entry.high_seqno_seen=high_seqno_seen;
    }

    public void setHighestDeliveredAndSeenSeqnos(Address sender, long high_seqno, long high_seqno_seen) {
        Entry entry=(Entry)senders.get(sender);
        if(entry != null) {
            entry.high_seqno=high_seqno;
            entry.high_seqno_seen=high_seqno_seen;
        }
    }


    public Digest copy() {
        Digest ret=new Digest(senders.size());
        Map.Entry entry;
        Entry tmp;
        for(Iterator it=senders.entrySet().iterator(); it.hasNext();) {
            entry=(Map.Entry)it.next();
            tmp=(Entry)entry.getValue();
            ret.add((Address)entry.getKey(), tmp.low_seqno, tmp.high_seqno, tmp.high_seqno_seen);
        }
        return ret;
    }


    public String toString() {
        StringBuffer sb=new StringBuffer();
        boolean first=true;
        if(senders == null) return "[]";
        Map.Entry entry;
        Address key;
        Entry val;

        for(Iterator it=senders.entrySet().iterator(); it.hasNext();) {
            entry=(Map.Entry)it.next();
            key=(Address)entry.getKey();
            val=(Entry)entry.getValue();
            if(!first) {
                sb.append(", ");
            }
            else {
                sb.append('[');
                first=false;
            }
            sb.append(key).append(": ").append('[').append(val.low_seqno).append(" : ");
            sb.append(val.high_seqno);
            if(val.high_seqno_seen >= 0)
                sb.append(" (").append(val.high_seqno_seen).append(")]");
        }
        sb.append(']');
        return sb.toString();
    }


    public String printHighSeqnos() {
        StringBuffer sb=new StringBuffer();
        boolean first=true;
        Map.Entry entry;
        Address key;
        Entry val;

        for(Iterator it=senders.entrySet().iterator(); it.hasNext();) {
            entry=(Map.Entry)it.next();
            key=(Address)entry.getKey();
            val=(Entry)entry.getValue();
            if(!first) {
                sb.append(", ");
            }
            else {
                sb.append('[');
                first=false;
            }
            sb.append(key).append("#").append(val.high_seqno);
        }
        sb.append(']');
        return sb.toString();
    }


    public String printHighSeqnosSeen() {
       StringBuffer sb=new StringBuffer();
        boolean first=true;
        Map.Entry entry;
        Address key;
        Entry val;

        for(Iterator it=senders.entrySet().iterator(); it.hasNext();) {
            entry=(Map.Entry)it.next();
            key=(Address)entry.getKey();
            val=(Entry)entry.getValue();
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
        senders=(Map)in.readObject();
    }

    public void writeTo(DataOutputStream out) throws IOException {
        out.writeShort(senders.size());
        Map.Entry entry;
        Address key;
        Entry val;
        for(Iterator it=senders.entrySet().iterator(); it.hasNext();) {
            entry=(Map.Entry)it.next();
            key=(Address)entry.getKey();
            val=(Entry)entry.getValue();
            Util.writeAddress(key, out);
            out.writeLong(val.low_seqno);
            out.writeLong(val.high_seqno);
            out.writeLong(val.high_seqno_seen);
        }
    }


    public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
        short size=in.readShort();
        senders=createSenders(size);
        Address key;
        for(int i=0; i < size; i++) {
            key=Util.readAddress(in);
            add(key, in.readLong(), in.readLong(), in.readLong());
        }
    }


    public long serializedSize() {
        long retval=Global.SHORT_SIZE; // number of elements in 'senders'
        if(senders.size() > 0) {
            Address addr=(Address)senders.keySet().iterator().next();
            int len=addr.size() +
                    2 * Global.BYTE_SIZE; // presence byte, IpAddress vs other address
            len+=3 * Global.LONG_SIZE; // 3 longs in one Entry
            retval+=len * senders.size();
        }
        return retval;
    }

    private Map createSenders(int size) {
        return new ConcurrentReaderHashMap(size);
    }


    /**
     * Class keeping track of the lowest and highest sequence numbers delivered, and the highest
     * sequence numbers received, per member
     */
    public static class Entry {
        public long low_seqno, high_seqno, high_seqno_seen=-1;

        public Entry(long low_seqno, long high_seqno, long high_seqno_seen) {
            this.low_seqno=low_seqno;
            this.high_seqno=high_seqno;
            this.high_seqno_seen=high_seqno_seen;
        }

        public Entry(long low_seqno, long high_seqno) {
            this.low_seqno=low_seqno;
            this.high_seqno=high_seqno;
        }

        public Entry(Entry other) {
            if(other != null) {
                low_seqno=other.low_seqno;
                high_seqno=other.high_seqno;
                high_seqno_seen=other.high_seqno_seen;
            }
        }

        public boolean equals(Object obj) {
            Entry other=(Entry)obj;
            return low_seqno == other.low_seqno && high_seqno == other.high_seqno && high_seqno_seen == other.high_seqno_seen;
        }

        public String toString() {
            return new StringBuffer("low=").append(low_seqno).append(", high=").append(high_seqno).
                    append(", highest seen=").append(high_seqno_seen).toString();
        }

        public void reset() {
            low_seqno=high_seqno=0;
            high_seqno_seen=-1;
        }
    }
}
