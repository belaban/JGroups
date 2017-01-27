package org.jgroups.util;



/**
 * Class copied from {@link java.util.BitSet}. Changes are that the FixedSizeBitSet doesn't expand, so access to it
 * doesn't need to be synchronized, plus we only need a few methods so most methods of the old class have been removed.
 * @author Bela Ban
 */
public class FixedSizeBitSet {
    /*
     * BitSets are packed into arrays of "words."  Currently a word is a long, which consists of 64 bits, requiring
     * 6 address bits. The choice of word size is determined purely by performance concerns.
     */
    protected final static int  ADDRESS_BITS_PER_WORD=6;
    protected final static int  BITS_PER_WORD=1 << ADDRESS_BITS_PER_WORD;

    /* Used to shift left or right for a partial word mask */
    protected static final long WORD_MASK=0xffffffffffffffffL;

    protected       long[]      words;
    protected       int         size;


    public FixedSizeBitSet() {
    }

    /**
     * Creates a bit set whose initial size is the range {@code 0} through
     * {@code size-1}. All bits are initially {@code false}.
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
     * Sets the bit at the specified index to {@code true}.
     * @param index a bit index.
     * @return true if the bit was 0 before, false otherwise
     * @throws IndexOutOfBoundsException if the specified index is negative.
     */
    public boolean set(int index) {
        if(index < 0 || index >= size)
            throw new IndexOutOfBoundsException("index: " + index);

        int wordIndex=wordIndex(index);
        boolean already_set=(words[wordIndex] & (1L << index)) != 0;
        words[wordIndex]|=(1L << index); // Restores invariants
        return !already_set;
    }

    /**
     * Sets the bits from the specified {@code from} (inclusive) to the
     * specified {@code to} (inclusive) to {@code true}.
     *
     * @param  from index of the first bit to be set
     * @param  to index of the last bit to be set
     * @throws IndexOutOfBoundsException if {@code from} is negative, or {@code to} is negative, or {@code from} is
     *         larger than {@code to}
     */
    public void set(int from, int to) {
        if(from < 0 || to < 0 || to < from || to >= size)
            throw new IndexOutOfBoundsException("from=" + from + ", to=" + to);

        // Increase capacity if necessary
        int startWordIndex = wordIndex(from);
        int endWordIndex   = wordIndex(to);

        long firstWordMask = WORD_MASK << from;
        long lastWordMask  = WORD_MASK >>> -(to+1);
        if (startWordIndex == endWordIndex) {
            // Case 1: One word
            words[startWordIndex] |= (firstWordMask & lastWordMask);
        } else {
            // Case 2: Multiple words
            // Handle first word
            words[startWordIndex] |= firstWordMask;

            // Handle intermediate words, if any
            for (int i = startWordIndex+1; i < endWordIndex; i++)
                words[i] = WORD_MASK;

            // Handle last word (restores invariants)
            words[endWordIndex] |= lastWordMask;
        }
    }
   

    /**
     * Sets the bit specified by the index to {@code false}.
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
     * Sets the bits from the specified {@code from} (inclusive) to the
     * specified {@code to} (inclusive) to {@code false}.
     *
     * @param  from index of the first bit to be cleared
     * @param  to index of the last bit to be cleared
     * @throws IndexOutOfBoundsException if {@code from} is negative, or {@code to} is negative,
     *         or {@code from} is larger than {@code to}
     */
    public void clear(int from, int to) {
        if(from < 0 || to < 0 || to < from || to >= size)
            throw new IndexOutOfBoundsException("from=" + from + ", to=" + to);

        int startWordIndex = wordIndex(from);
        int endWordIndex = wordIndex(to);

        long firstWordMask = WORD_MASK << from;
        long lastWordMask  = WORD_MASK >>> -(to+1);
        if (startWordIndex == endWordIndex) {
            // Case 1: One word
            words[startWordIndex] &= ~(firstWordMask & lastWordMask);
        } else {
            // Case 2: Multiple words
            // Handle first word
            words[startWordIndex] &= ~firstWordMask;

            // Handle intermediate words, if any
            for (int i = startWordIndex+1; i < endWordIndex; i++)
                words[i] = 0;

            // Handle last word
            words[endWordIndex] &= ~lastWordMask;
        }
    }


    /**
     * Returns the value of the bit with the specified index. The value
     * is {@code true} if the bit with the index {@code index}
     * is currently set in this bit set; otherwise, the result is {@code false}.
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
     * Returns the index of the first bit that is set to {@code true} that occurs on or after
     * the specified starting index. If no such bit exists then -1 is returned.
     * <p/>
     * To iterate over the {@code true} bits in a {@code BitSet},
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
     * Returns the index of the first bit that is set to {@code false}
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
     * Returns the index of the nearest bit that is set to {@code true}
     * that occurs on or before the specified starting index.
     * If no such bit exists, or if {@code -1} is given as the
     * starting index, then {@code -1} is returned.
     * @param  from the index to start checking from (inclusive)
     * @return the index of the previous set bit, or {@code -1} if there is no such bit
     * @throws IndexOutOfBoundsException if the specified index is less than {@code -1}
     */
    public int previousSetBit(int from) {
        if(from < 0 || from >= size)
            throw new IndexOutOfBoundsException("index: " + from);

        int u = wordIndex(from);

        long word = words[u] & (WORD_MASK >>> -(from+1));

        while (true) {
            if (word != 0)
                return (u+1) * BITS_PER_WORD - 1 - Long.numberOfLeadingZeros(word);
            if (u-- == 0)
                return -1;
            word = words[u];
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
     * Flips all bits: 1 --> 0 and 0 --> 1
     */
    public void flip() {
        int fromIndex=0, toIndex=size();
        if (fromIndex == toIndex)
            return;

        int startWordIndex = wordIndex(fromIndex);
        int endWordIndex   = wordIndex(toIndex);

        long firstWordMask = WORD_MASK << fromIndex;
        long lastWordMask  = WORD_MASK >>> -toIndex;
        if (startWordIndex == endWordIndex) {
            // Case 1: One word
            words[startWordIndex] ^= (firstWordMask & lastWordMask);
        } else {
            // Case 2: Multiple words
            // Handle first word
            words[startWordIndex] ^= firstWordMask;

            // Handle intermediate words, if any
            for (int i = startWordIndex+1; i < endWordIndex; i++)
                words[i] ^= WORD_MASK;

            // Handle last word
            words[endWordIndex] ^= lastWordMask;
        }
    }


    /**
     * Returns a string representation of this bit set. For every index
     * for which this {@code BitSet} contains a bit in the set
     * state, the decimal representation of that index is included in
     * the result. Such indices are listed in order from lowest to
     * highest, separated by ",&nbsp;" (a comma and a space) and
     * surrounded by braces, resulting in the usual mathematical
     * notation for a set of integers.<p>
     * Overrides the {@code toString} method of {@code Object}.
     * <p>Example:
     * <pre>
     * BitSet drPepper = new BitSet();</pre>
     * Now {@code drPepper.toString()} returns "{@code {}}".<p>
     * <pre>
     * drPepper.set(2);</pre>
     * Now {@code drPepper.toString()} returns "{@code {2}}".<p>
     * <pre>
     * drPepper.set(4);
     * drPepper.set(10);</pre>
     * Now {@code drPepper.toString()} returns "{@code {2, 4, 10}}".
     * @return a string representation of this bit set.
     */
    public String toString() {
        if(cardinality() == 0)
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
                sb.append(i).append('-').append(endOfRun-1);
                i=endOfRun;
            }
            else
                sb.append(i);
            if(--num <= 0) {
                sb.append(", ... ");
                break;
            }
        }

        sb.append('}');
        return sb.toString();
    }


    protected static int wordIndex(int bitIndex) {
        return bitIndex >> ADDRESS_BITS_PER_WORD;
    }
    
}
