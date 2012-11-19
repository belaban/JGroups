package org.jgroups.util;

import org.jgroups.Global;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * A list of sequence numbers (seqnos). Seqnos have to be added in ascending order, and can be single seqnos
 * or seqno ranges (e.g. [5-10]). This class is unsynchronized. Note that for serialization, we assume that the
 * lowest and highest seqno in the list are not more than 2 ^ 31 apart.
 * @author Bela Ban
 * @since 3.1
 */
public class SeqnoList implements Streamable, Iterable<Long> {
    protected final List<Seqno> seqnos=new ArrayList<Seqno>();

    public SeqnoList() {
    }

    public SeqnoList(long seqno) {
        add(seqno);
    }

    public SeqnoList(long from, long to) {
        add(from, to);
    }

    /** Adds a single seqno */
    public SeqnoList add(long seqno) {
        seqnos.add(new Seqno(seqno));
        return this;
    }

    public SeqnoList add(long ... seqnos) {
        if(seqnos != null) {
            for(long seqno: seqnos)
                add(seqno);
        }
        return this;
    }

    /** Adds a seqno range */
    public SeqnoList add(long from, long to) {
        seqnos.add(new SeqnoRange(from, to));
        return this;
    }

    /** Removes all seqnos <= seqno */
    public void remove(long min_seqno) {
        for(Iterator<Seqno> it=seqnos.iterator(); it.hasNext();) {
            Seqno tmp=it.next();
            if(tmp instanceof SeqnoRange) {
                SeqnoRange range=(SeqnoRange)tmp;
                if(range.to <= min_seqno)
                    it.remove();
                else {
                    if(range.from <= min_seqno)
                        range.from=min_seqno+1;
                }
            }
            else {
                if(tmp.from <= min_seqno)
                    it.remove();
            }
        }
    }


    /** Removes all seqnos > seqno */
    public void removeHigherThan(long max_seqno) {
        for(Iterator<Seqno> it=seqnos.iterator(); it.hasNext();) {
            Seqno tmp=it.next();
            if(tmp instanceof SeqnoRange) {
                SeqnoRange range=(SeqnoRange)tmp;
                if(range.from > max_seqno)
                    it.remove();
                else {
                    if(range.to > max_seqno)
                        range.to=max_seqno;
                }
            }
            else {
                if(tmp.from > max_seqno)
                    it.remove();
            }
        }
    }



    /** Returns the last seqno, this should also be the highest seqno in the list as we're supposed to add seqnos
     * in order
     * @return
     */
    public long getLast() {
        int size=seqnos.size();
        if(size == 0)
            return 0;
        Seqno seqno=seqnos.get(size - 1);
        return seqno instanceof SeqnoRange? ((SeqnoRange)seqno).to : seqno.from;
    }

    public void writeTo(DataOutput out) throws Exception {
        out.writeInt(seqnos.size());
        for(Seqno seqno: seqnos) {
            if(seqno instanceof SeqnoRange) {
                SeqnoRange range=(SeqnoRange)seqno;
                out.writeBoolean(true);
                Util.writeLongSequence(range.from, range.to, out);
            }
            else {
                out.writeBoolean(false);
                Util.writeLong(seqno.from, out);
            }
        }
    }

    public void readFrom(DataInput in) throws Exception {
        int len=in.readInt();
        for(int i=0; i < len; i++) {
            if(in.readBoolean()) {
                long[] tmp=Util.readLongSequence(in);
                seqnos.add(new SeqnoRange(tmp[0], tmp[1]));
            }
            else {
                seqnos.add(new Seqno(Util.readLong(in)));
            }
        }
    }

    public int serializedSize() {
        int retval=Global.INT_SIZE // number of elements in seqnos
          + seqnos.size() * Global.BYTE_SIZE; // plus 1 boolean (Seqno or SeqnoRange) per element
        for(Seqno seqno: seqnos) {
            if(seqno instanceof SeqnoRange) {
                SeqnoRange range=(SeqnoRange)seqno;
                retval+=Util.size(range.from, range.to);
            }
            else {
                retval+=Util.size(seqno.from);
            }
        }
        return retval;
    }

    public int size() {
        int retval=0;
        for(Seqno seqno: seqnos) {
            if(seqno instanceof SeqnoRange) {
                SeqnoRange range=(SeqnoRange)seqno;
                retval+=(range.to - range.from +1);
            }
            else
                retval++;
        }

        return retval;
    }

    public String toString() {
        return seqnos.toString();
    }

    public Iterator<Long> iterator() {
        return new SeqnoListIterator();
    }

    protected static class Seqno {
        protected long from;

        public Seqno(long num) {
            this.from=num;
        }

        public String toString() {
            return String.valueOf(from);
        }
    }

    protected static class SeqnoRange extends Seqno {
        protected long to;

        public SeqnoRange(long from, long to) {
            super(from);
            this.to=to;
            if(to <  from)
                throw new IllegalArgumentException("to (" + to + ") needs to be >= from (" + from + ")");
        }

        public String toString() {
            return super.toString() + "-" + to;
        }
    }


    protected class SeqnoListIterator implements Iterator<Long> {
        protected int         index=0;
        protected SeqnoRange  range=null;
        protected long        range_index=-1;

        public boolean hasNext() {
            return (range != null && range_index < range.to) || index <  seqnos.size();
        }

        public Long next() {
            if(range != null) {
                if(range_index < range.to)
                    return ++range_index;
                else
                    range=null;
            }
            if(index >= seqnos.size())
                throw new NoSuchElementException("index " + index + " is >= size " + seqnos.size());
            Seqno next=seqnos.get(index++);
            if(next instanceof SeqnoRange) {
                range=(SeqnoRange)next;
                range_index=range.from;
                return range_index;
            }
            else
                return next.from;
        }

        public void remove() { // not supported
        }
    }
}
