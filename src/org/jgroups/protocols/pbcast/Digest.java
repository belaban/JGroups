// $Id: Digest.java,v 1.3 2004/04/05 03:57:27 belaban Exp $

package org.jgroups.protocols.pbcast;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.Address;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


/**
 * A message digest, which is used e.g. by the PBCAST layer for gossiping (also used by NAKACK for
 * keeping track of current seqnos for all members). It contains pairs of senders and a range of seqnos
 * (low and high), where each sender is associated with its highest and lowest seqnos seen so far.  That
 * is, the lowest seqno which was not yet garbage-collected and the highest that was seen so far and is
 * deliverable (or was already delivered) to the application.  A range of [0 - 0] means no messages have
 * been received yet. <p> April 3 2001 (bela): Added high_seqnos_seen member. It is used to disseminate
 * information about the last (highest) message M received from a sender P. Since we might be using a
 * negative acknowledgment message numbering scheme, we would never know if the last message was
 * lost. Therefore we periodically gossip and include the last message seqno. Members who haven't seen
 * it (e.g. because msg was dropped) will request a retransmission. See DESIGN for details.
 * @author Bela Ban
 */
public class Digest implements Externalizable {
    Address[] senders=null;
    long[]    low_seqnos=null;       // lowest seqnos seen
    long[]    high_seqnos=null;      // highest seqnos seen so far *that are deliverable*, initially 0
    long[]    high_seqnos_seen=null; // highest seqnos seen so far (not necessarily deliverable), initially -1
    int       index=0;               // current index of where next member is added
    protected static Log log=LogFactory.getLog(Digest.class);


    public Digest() {
    } // used for externalization

    public Digest(int size) {
        reset(size);
    }


    public void add(Address sender, long low_seqno, long high_seqno) {
        if(index >= senders.length) {
            if(log.isErrorEnabled()) log.error("index " + index +
                    " out of bounds, please create new Digest if you want more members !");
            return;
        }
        if(sender == null) {
            if(log.isErrorEnabled()) log.error("sender is null, will not add it !");
            return;
        }
        senders[index]=sender;
        low_seqnos[index]=low_seqno;
        high_seqnos[index]=high_seqno;
        high_seqnos_seen[index]=-1;
        index++;
    }


    public void add(Address sender, long low_seqno, long high_seqno, long high_seqno_seen) {
        if(index >= senders.length) {
            if(log.isErrorEnabled()) log.error("index " + index +
                    " out of bounds, please create new Digest if you want more members !");
            return;
        }
        if(sender == null) {
            if(log.isErrorEnabled()) log.error("sender is null, will not add it !");
            return;
        }
        senders[index]=sender;
        low_seqnos[index]=low_seqno;
        high_seqnos[index]=high_seqno;
        high_seqnos_seen[index]=high_seqno_seen;
        index++;
    }


    public void add(Digest d) {
        Address sender;
        long low_seqno, high_seqno, high_seqno_seen;

        if(d != null) {
            for(int i=0; i < d.size(); i++) {
                sender=d.senderAt(i);
                low_seqno=d.lowSeqnoAt(i);
                high_seqno=d.highSeqnoAt(i);
                high_seqno_seen=d.highSeqnoSeenAt(i);
                add(sender, low_seqno, high_seqno, high_seqno_seen);
            }
        }
    }


    /**
     * Adds a digest to this digest. This digest must have enough space to add the other digest; otherwise an error
     * message will be written. For each sender in the other digest, the merge() method will be called.
     */
    public void merge(Digest d) {
        Address sender;
        long low_seqno, high_seqno, high_seqno_seen;

        if(d == null) {
            if(log.isErrorEnabled()) log.error("digest to be merged with is null");
            return;
        }
        for(int i=0; i < d.size(); i++) {
            sender=d.senderAt(i);
            low_seqno=d.lowSeqnoAt(i);
            high_seqno=d.highSeqnoAt(i);
            high_seqno_seen=d.highSeqnoSeenAt(i);
            merge(sender, low_seqno, high_seqno, high_seqno_seen);
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
        int index;
        long my_low_seqno, my_high_seqno, my_high_seqno_seen;
        if(sender == null) {
            if(log.isErrorEnabled()) log.error("sender == null");
            return;
        }
        index=getIndex(sender);
        if(index == -1) {
            add(sender, low_seqno, high_seqno, high_seqno_seen);
            return;
        }

        my_low_seqno=lowSeqnoAt(index);
        my_high_seqno=highSeqnoAt(index);
        my_high_seqno_seen=highSeqnoSeenAt(index);
        if(low_seqno < my_low_seqno)
            setLowSeqnoAt(index, low_seqno);
        if(high_seqno > my_high_seqno)
            setHighSeqnoAt(index, high_seqno);
        if(high_seqno_seen > my_high_seqno_seen)
            setHighSeqnoSeenAt(index, high_seqno_seen);
    }


    public int getIndex(Address sender) {
        int ret=-1;

        if(sender == null) return ret;
        for(int i=0; i < senders.length; i++)
            if(sender.equals(senders[i]))
                return i;
        return ret;
    }


    public boolean contains(Address sender) {
        return getIndex(sender) != -1;
    }


    /** Increment the sender's high_seqno by 1 */
    public void incrementHighSeqno(Address sender) {
        if(sender == null) return;
        for(int i=0; i < senders.length; i++) {
            if(senders[i] != null && senders[i].equals(sender)) {
                high_seqnos[i]=high_seqnos[i] + 1;
                break;
            }
        }
    }


    public int size() {
        return senders.length;
    }


    public Address senderAt(int index) {
        if(index < size())
            return senders[index];
        else {
            if(log.isErrorEnabled()) log.error("index " + index + " is out of bounds");
            return null;
        }
    }


    /**
     * Resets the seqnos for the sender at 'index' to 0. This happens when a member has left the group,
     * but it is still in the digest. Resetting its seqnos ensures that no-one will request a message
     * retransmission from the dead member.
     */
    public void resetAt(int index) {
        if(index < size()) {
            low_seqnos[index]=0;
            high_seqnos[index]=0;
            high_seqnos_seen[index]=-1;
        }
        else
            if(log.isErrorEnabled()) log.error("index " + index + " is out of bounds");
    }


    public void reset(int size) {
        senders=new Address[size];
        low_seqnos=new long[size];
        high_seqnos=new long[size];
        high_seqnos_seen=new long[size];
        for(int i=0; i < size; i++)
            high_seqnos_seen[i]=-1;
        index=0;
    }


    public long lowSeqnoAt(int index) {
        if(index < size())
            return low_seqnos[index];
        else {
            if(log.isErrorEnabled()) log.error("index " + index + " is out of bounds");
            return 0;
        }
    }


    public long highSeqnoAt(int index) {
        if(index < size())
            return high_seqnos[index];
        else {
            if(log.isErrorEnabled()) log.error("index " + index + " is out of bounds");
            return 0;
        }
    }

    public long highSeqnoSeenAt(int index) {
        if(index < size())
            return high_seqnos_seen[index];
        else {
            if(log.isErrorEnabled()) log.error("index " + index + " is out of bounds");
            return 0;
        }
    }


    public long highSeqnoAt(Address sender) {
        long ret=-1;
        int index;

        if(sender == null) return ret;
        index=getIndex(sender);
        if(index == -1)
            return ret;
        else
            return high_seqnos[index];
    }


    public long highSeqnoSeenAt(Address sender) {
        long ret=-1;
        int index;

        if(sender == null) return ret;
        index=getIndex(sender);
        if(index == -1)
            return ret;
        else
            return high_seqnos_seen[index];
    }

    public void setLowSeqnoAt(int index, long low_seqno) {
        if(index < size()) {
            low_seqnos[index]=low_seqno;
        }
        else
            if(log.isErrorEnabled()) log.error("index " + index + " is out of bounds");
    }


    public void setHighSeqnoAt(int index, long high_seqno) {
        if(index < size()) {
            high_seqnos[index]=high_seqno;
        }
        else
            if(log.isErrorEnabled()) log.error("index " + index + " is out of bounds");
    }

    public void setHighSeqnoSeenAt(int index, long high_seqno_seen) {
        if(index < size()) {
            high_seqnos_seen[index]=high_seqno_seen;
        }
        else
            if(log.isErrorEnabled()) log.error("index " + index + " is out of bounds");
    }


    public void setHighSeqnoAt(Address sender, long high_seqno) {
        int index=getIndex(sender);
        if(index < 0)
            return;
        else
            setHighSeqnoAt(index, high_seqno);
    }

    public void setHighSeqnoSeenAt(Address sender, long high_seqno_seen) {
        int index=getIndex(sender);
        if(index < 0)
            return;
        else
            setHighSeqnoSeenAt(index, high_seqno_seen);
    }


    public Digest copy() {
        Digest ret=new Digest(senders.length);

        // changed due to JDK bug (didn't work under JDK 1.4.{1,2} under Linux, JGroups bug #791718
        // ret.senders=(Address[])senders.clone();
        if(senders != null)
            System.arraycopy(senders, 0, ret.senders, 0, senders.length);

        ret.low_seqnos=(long[])low_seqnos.clone();
        ret.high_seqnos=(long[])high_seqnos.clone();
        ret.high_seqnos_seen=(long[])high_seqnos_seen.clone();
        return ret;
    }


    public String toString() {
        StringBuffer sb=new StringBuffer();
        boolean first=true;
        for(int i=0; i < senders.length; i++) {
            if(!first) {
                sb.append(", ");
            }
            else {
                sb.append("[");
                first=false;
            }
            sb.append(senders[i]).append(": ").append("[").append(low_seqnos[i]).append(" : ");
            sb.append(high_seqnos[i]);
            if(high_seqnos_seen[i] >= 0)
                sb.append(" (").append(high_seqnos_seen[i]).append(")]");
        }
        sb.append("]");
        return sb.toString();
    }


    public String printHighSeqnos() {
        StringBuffer sb=new StringBuffer();
        boolean first=true;
        for(int i=0; i < senders.length; i++) {
            if(!first) {
                sb.append(", ");
            }
            else {
                sb.append("[");
                first=false;
            }
            sb.append(senders[i]);
            sb.append("#");
            sb.append(high_seqnos[i]);
        }
        sb.append("]");
        return sb.toString();
    }


    public String printHighSeqnosSeen() {
        StringBuffer sb=new StringBuffer();
        boolean first=true;
        for(int i=0; i < senders.length; i++) {
            if(!first) {
                sb.append(", ");
            }
            else {
                sb.append("[");
                first=false;
            }
            sb.append(senders[i]);
            sb.append("#");
            sb.append(high_seqnos_seen[i]);
        }
        sb.append("]");
        return sb.toString();
    }


    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(senders);

        if(low_seqnos == null)
            out.writeInt(0);
        else {
            out.writeInt(low_seqnos.length);
            for(int i=0; i < low_seqnos.length; i++)
                out.writeLong(low_seqnos[i]);
        }

        if(high_seqnos == null)
            out.writeInt(0);
        else {
            out.writeInt(high_seqnos.length);
            for(int i=0; i < high_seqnos.length; i++)
                out.writeLong(high_seqnos[i]);
        }

        if(high_seqnos_seen == null)
            out.writeInt(0);
        else {
            out.writeInt(high_seqnos_seen.length);
            for(int i=0; i < high_seqnos_seen.length; i++)
                out.writeLong(high_seqnos_seen[i]);
        }

        out.writeInt(index);
    }


    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int num;

        senders=(Address[])in.readObject();

        num=in.readInt();
        if(num == 0)
            low_seqnos=null;
        else {
            low_seqnos=new long[num];
            for(int i=0; i < low_seqnos.length; i++)
                low_seqnos[i]=in.readLong();
        }


        num=in.readInt();
        if(num == 0)
            high_seqnos=null;
        else {
            high_seqnos=new long[num];
            for(int i=0; i < high_seqnos.length; i++)
                high_seqnos[i]=in.readLong();
        }

        num=in.readInt();
        if(num == 0)
            high_seqnos_seen=null;
        else {
            high_seqnos_seen=new long[num];
            for(int i=0; i < high_seqnos_seen.length; i++)
                high_seqnos_seen[i]=in.readLong();
        }

        index=in.readInt();
    }


}
