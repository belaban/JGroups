package org.jgroups.util;

import java.util.List;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Records samples (e.g. message sizes or times) by buckets, e.g. 0-10,11-100,101-1000 etc. All buckets are defined at
 * creation time.
 * @author Bela Ban
 * @since  5.4.9
 */
public class DistributionSampler {
    protected final Bucket[] buckets;
    protected long           min_value=Integer.MAX_VALUE, max_value;
    protected boolean        exception_on_missing_bucket=true;

    /**
     * Creates a new sampler
     * @param buckets The buckets. Needs to be an even number. Each tuple defines min-max size. Needs to be in
     *                ascending order of size.
     */
    public DistributionSampler(long ... buckets) {
        if(buckets.length % 2 != 0)
            throw new IllegalArgumentException("min/max list of buckets needs to be even");
        long curr_size=-1;
        this.buckets=new Bucket[buckets.length/2];
        for(int i=0,index=0; i < buckets.length; i++,index++) {
            long min=buckets[i], max=buckets[++i];
            if(min >= max)
                throw new IllegalArgumentException(String.format("min (%,d) needs to be <= max (%,d)", min, max));
            if(curr_size < 0)
                curr_size=max;
            else {
                if(curr_size >= min)
                    throw new IllegalArgumentException(String.format("buckets must be disjoint: prev.max=%,d, min=%,d", curr_size, min));
                else
                    curr_size=max;
            }
            Bucket b=new Bucket(min, max);
            min_value=Math.min(min, min_value);
            max_value=Math.max(max, max_value);
            this.buckets[index]=b;
        }
    }

    public DistributionSampler(List<Long> buckets) {
        this(listToArray(buckets));
    }

    public long                minValue()                          {return min_value;}
    public long                maxValue()                          {return max_value;}
    public Bucket[]            buckets()                           {return buckets;}
    public boolean             exceptionOnMissingBucket()          {return exception_on_missing_bucket;}
    public DistributionSampler exceptionOnMissingBucket(boolean b) {exception_on_missing_bucket=b; return this;}

    public DistributionSampler add(long value) {
        Bucket b=getBucket(value);
        if(b != null)
            b.increment();
        return this;
    }

    public DistributionSampler reset() {
        Stream.of(buckets).forEach(Bucket::reset);
        return this;
    }

    public long total() {
        long ret=0;
        for(Bucket b: buckets)
            ret+=b.count();
        return ret;
    }

    public int size() {
        return buckets.length;
    }

    @Override
    public String toString() {
        return Stream.of(buckets).map(Bucket::toString).collect(Collectors.joining("\n"));
    }

    protected Bucket getBucket(long value) {
        if(value < min_value || value > max_value) {
            if(exception_on_missing_bucket)
                throw new IllegalArgumentException(String.format("value %,d out of range (min: %,d max: %,d)",
                                                                 value, min_value, max_value));
            return null;
        }
        return binarySearch(0, buckets.length, value);
    }

    protected Bucket binarySearch(int from, int to, long value) {
        int index=(from+to) / 2;
        Bucket b=buckets[index];
        if(value >= b.min && value <= b.max)
            return b;
        if(value > b.max) {
            return binarySearch(index+1, to, value);
        }
        else {
            return binarySearch(from, index-1, value);
        }
    }

    protected static long[] listToArray(List<Long> buckets) {
        long[] tmp=new long[buckets.size()];
        for(int i=0; i < buckets.size(); i++)
            tmp[i]=buckets.get(i);
        return tmp;
    }

    public static class Bucket {
        protected long            min, max;
        protected final LongAdder count=new LongAdder();

        public Bucket(long min, long max) {
            this.min=min;
            this.max=max;
        }

        public long count()     {return count.sum();}
        public void increment() {count.increment();}
        public void reset()     {count.reset();}

        @Override
        public String toString() {
            return String.format("[%,d .. %,d]: %,d", min, max, count.sum());
        }
    }
}
