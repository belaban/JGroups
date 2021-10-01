package org.jgroups.util;

import org.jgroups.Constructable;
import org.jgroups.Global;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

/**
 * A bitset of missing messages with a fixed size. The index (in the bit set) of a seqno is computed as seqno - offset.
 * @author Bela Ban
 * @since  3.1
 */
public class SeqnoList extends FixedSizeBitSet implements SizeStreamable, Iterable<Long>, Constructable<SeqnoList> {
    protected long offset; // first seqno

    /** Only to be used by serialization */
    public SeqnoList() {
    }

    /**
     * Creates a SeqnoList with a capacity for size elements.
     * @param size The max number of seqnos in the bitset
     * @param offset Lowest seqno. Used to compute the index of a given seqno into the bitset: seqno - offset
     */
    public SeqnoList(int size, long offset) {
        super(size);
        this.offset=offset;
    }

    public Supplier<? extends SeqnoList> create() {
        return SeqnoList::new;
    }

    public SeqnoList(int size) {
        this(size, 0);
    }

    /** Adds a single seqno */
    public SeqnoList add(long seqno) {
        super.set(index(seqno));
        return this;
    }

    public SeqnoList add(long ... seqnos) {
        if(seqnos != null)
            for(long seqno: seqnos)
                add(seqno);
        return this;
    }

    /** Adds a seqno range */
    public SeqnoList add(long from, long to) {
        super.set(index(from), index(to));
        return this;
    }


    /** Removes all seqnos > seqno */
    public SeqnoList removeHigherThan(long seqno) {
        int from=index(seqno + 1), to=size-1;
        if(from < 0)
            from=0;
        if(from <= to && from >= 0)
            super.clear(from, to);
        return this;
    }

    /** Removes all seqnos < seqno */
    public SeqnoList removeLowerThan(long seqno) {
        int to=index(seqno-1);
        if(to >= 0)
            super.clear(0, to);
        return this;
    }


    /** Returns the last seqno, this is also the highest seqno in the list as we add seqnos in order */
    public long getLast() {
        int index=previousSetBit(size - 1);
        return index == -1? -1 : seqno(index);
    }

    /** Returns the first seqno, this is also the lowest seqno in the list as we add seqnos in order */
    public long getFirst() {
        int index=nextSetBit(0);
        return index < 0? -1 : seqno(index);
    }

    @Override
    public int serializedSize() {
        return Global.INT_SIZE // number of words
          + (words.length+1) * Global.LONG_SIZE; // words + offset
    }

    @Override
    public void writeTo(DataOutput out) throws IOException {
        out.writeInt(size);
        out.writeLong(offset);
        for(long word: words)
            out.writeLong(word);
    }

    @Override
    public void readFrom(DataInput in) throws IOException {
        size=in.readInt();
        offset=in.readLong();
        words=new long[wordIndex(size - 1) + 1];
        for(int i=0; i < words.length; i++)
            words[i]=in.readLong();
    }



    public int size() {
        return super.cardinality();
    }

    public boolean isEmpty() {return cardinality() == 0;}


    public String toString() {
        if(isEmpty())
            return "{}";
        StringBuilder sb=new StringBuilder("(").append(cardinality()).append("): {");

        boolean first=true;
        int num=Util.MAX_LIST_PRINT_SIZE;
        for(int i=nextSetBit(0); i >= 0; i=nextSetBit(i + 1)) {
            int endOfRun=nextClearBit(i);
            if(first)
                first=false;
            else
                sb.append(", ");

            if(endOfRun != -1 && endOfRun-1 != i) {
                sb.append(seqno(i)).append('-').append(seqno(endOfRun-1));
                i=endOfRun;
            }
            else
                sb.append(seqno(i));
            if(--num <= 0) {
                sb.append(", ... ");
                break;
            }
        }

        sb.append('}');
        return sb.toString();
    }

    public Iterator<Long> iterator() {
        return new SeqnoListIterator();
    }

    protected int index(long seqno) {return (int)(seqno-offset);}

    protected long seqno(int index) {return offset + index;}


    protected class SeqnoListIterator implements Iterator<Long> {
        protected int         index;

        public boolean hasNext() {
            return index < size && nextSetBit(index) != -1;
        }

        public Long next() {
            int next_index=nextSetBit(index);
            if(next_index == -1 || next_index >= size)
                throw new NoSuchElementException("index: " + next_index);
            index=next_index+1;
            return seqno(next_index);
        }

        public void remove() { // not supported
        }
    }
}
