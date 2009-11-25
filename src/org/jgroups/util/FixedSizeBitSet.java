package org.jgroups.util;



/**
 * Class copied from {@link java.util.BitSet}. Changes are that the FixedSizeBitSet doesn't expand, so access to it
 * doesn't need to be synchronized, plus we only need a few methods so most methods of the old class have been removed.
 * @author Bela Ban
 * @version $Id: FixedSizeBitSet.java,v 1.1 2009/11/25 08:41:17 belaban Exp $
 */
public class FixedSizeBitSet {
    /*
     * BitSets are packed into arrays of "words."  Currently a word is a long, which consists of 64 bits, requiring
     * 6 address bits. The choice of word size is determined purely by performance concerns.
     */
    private final static int ADDRESS_BITS_PER_WORD=6;
    private final static int BITS_PER_WORD=1 << ADDRESS_BITS_PER_WORD;

    /* Used to shift left or right for a partial word mask */
    private static final long WORD_MASK=0xffffffffffffffffL;

    private final long[] words;
    private final int size;




    /**
     * Creates a bit set whose initial size is the range <code>0</code> through
     * <code>size-1</code>. All bits are initially <code>false</code>.
     * @param size the initial size of the bit set (in bits).
     * @throws NegativeArraySizeException if the specified initial size is negative
     */
    public FixedSizeBitSet(int size) {
        if(size < 0) // nbits can't be negative; size 0 is OK
            throw new NegativeArraySizeException("size < 0: " + size);
        this.size=size;
        words=new long[wordIndex(size - 1) + 1];
    }

    


    /**
     * Sets the bit at the specified index to <code>true</code>.
     * @param index a bit index.
     * @throws IndexOutOfBoundsException if the specified index is negative.
     */
    public void set(int index) {
        if(index < 0 || index >= size)
            throw new IndexOutOfBoundsException("index: " + index);

        int wordIndex=wordIndex(index);
        words[wordIndex]|=(1L << index); // Restores invariants
    }

   

    /**
     * Sets the bit specified by the index to <code>false</code>.
     * @param index the index of the bit to be cleared.
     * @throws IndexOutOfBoundsException if the specified index is negative.
     */
    public void clear(int index) {
        if(index < 0 || index >= size)
            throw new IndexOutOfBoundsException("index: " + index);

        int wordIndex=wordIndex(index);
        words[wordIndex]&=~(1L << index);
    }



    /**
     * Returns the value of the bit with the specified index. The value
     * is <code>true</code> if the bit with the index <code>index</code>
     * is currently set in this bit set; otherwise, the result is <code>false</code>.
     * @param index the bit index.
     * @return the value of the bit with the specified index.
     * @throws IndexOutOfBoundsException if the specified index is negative.
     */
    public boolean get(int index) {
        if(index < 0 || index >= size)
            throw new IndexOutOfBoundsException("index: " + index);

        int wordIndex=wordIndex(index);
        return (words[wordIndex] & (1L << index)) != 0;
    }



    /**
     * Returns the index of the first bit that is set to <code>true</code> that occurs on or after
     * the specified starting index. If no such bit exists then -1 is returned.
     * <p/>
     * To iterate over the <code>true</code> bits in a <code>BitSet</code>,
     * use the following loop:
     * <p/>
     * <pre>
     * for (int i = bs.nextSetBit(0); i >= 0; i = bs.nextSetBit(i+1)) {
     *     // operate on index i here
     * }</pre>
     * @param fromIndex the index to start checking from (inclusive).
     * @return the index of the next set bit.
     * @throws IndexOutOfBoundsException if the specified index is negative.
     */
    public int nextSetBit(int fromIndex) {
        if(fromIndex < 0)
            throw new IndexOutOfBoundsException("fromIndex: " + fromIndex);
        if(fromIndex >= size)
            return -1;

        int u=wordIndex(fromIndex);
        long word=words[u] & (WORD_MASK << fromIndex);

        while(true) {
            if(word != 0)
                return (u * BITS_PER_WORD) + Long.numberOfTrailingZeros(word);
            if(++u == words.length)
                return -1;
            word=words[u];
        }
    }

    /**
     * Returns the index of the first bit that is set to <code>false</code>
     * that occurs on or after the specified starting index.
     * @param fromIndex the index to start checking from (inclusive).
     * @return the index of the next clear bit.
     * @throws IndexOutOfBoundsException if the specified index is negative.
     */
    public int nextClearBit(int fromIndex) {
        // Neither spec nor implementation handle bitsets of maximal length.
        // See 4816253.
        if(fromIndex < 0)
            throw new IndexOutOfBoundsException("fromIndex: " + fromIndex);
        if(fromIndex >= size)
            return -1;

        int u=wordIndex(fromIndex);
        if(u >= words.length)
            return fromIndex;

        long word=~words[u] & (WORD_MASK << fromIndex);

        while(true) {
            if(word != 0)
                return (u * BITS_PER_WORD) + Long.numberOfTrailingZeros(word);
            if(++u == words.length)
                return -1;
            word=~words[u];
        }
    }





    /**
     * Returns the number of bits set to <tt>true</tt> in this bit set
     * @return the number of bits set to <tt>true</tt> in this bit set
     */
    public int cardinality() {
        int sum=0;
        for(int i=0; i < words.length; i++)
            sum+=Long.bitCount(words[i]);
        return sum;
    }



    public int size() {return size;}




    /**
     * Returns a string representation of this bit set. For every index
     * for which this <code>BitSet</code> contains a bit in the set
     * state, the decimal representation of that index is included in
     * the result. Such indices are listed in order from lowest to
     * highest, separated by ",&nbsp;" (a comma and a space) and
     * surrounded by braces, resulting in the usual mathematical
     * notation for a set of integers.<p>
     * Overrides the <code>toString</code> method of <code>Object</code>.
     * <p>Example:
     * <pre>
     * BitSet drPepper = new BitSet();</pre>
     * Now <code>drPepper.toString()</code> returns "<code>{}</code>".<p>
     * <pre>
     * drPepper.set(2);</pre>
     * Now <code>drPepper.toString()</code> returns "<code>{2}</code>".<p>
     * <pre>
     * drPepper.set(4);
     * drPepper.set(10);</pre>
     * Now <code>drPepper.toString()</code> returns "<code>{2, 4, 10}</code>".
     * @return a string representation of this bit set.
     */
    public String toString() {
        int numBits=(words.length > 128)?
                cardinality() : words.length * BITS_PER_WORD;
        StringBuilder b=new StringBuilder(6 * numBits + 2);
        b.append('{');

        int i=nextSetBit(0);
        if(i != -1) {
            b.append(i);
            for(i=nextSetBit(i + 1); i >= 0; i=nextSetBit(i + 1)) {
                int endOfRun=nextClearBit(i);
                do {
                    b.append(", ").append(i);
                }
                while(++i < endOfRun);
            }
        }

        b.append('}');
        return b.toString();
    }


    private static int wordIndex(int bitIndex) {
        return bitIndex >> ADDRESS_BITS_PER_WORD;
    }
    
}
