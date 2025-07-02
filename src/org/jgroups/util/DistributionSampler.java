package org.jgroups.util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Records samples (e.g. message sizes or times) by buckets, e.g. 0-10,11-100,101-1000 etc. All buckets are defined at
 * creation time.
 * @author Bela Ban
 * @since  5.5.0
 */
public class DistributionSampler {
    protected final Bucket[] buckets;
    protected long           min_value=Integer.MAX_VALUE, max_value;
    protected boolean        exception_on_missing_bucket=true;

    /**
     * Creates a new sampler
     * @param buckets The buckets. Needs to be an even number. Each tuple defines min-max size. Needs to be in
     *                ascending order of size and adjacent.
     */
    public DistributionSampler(long ... buckets) {
        if(buckets.length < 2)
            throw new IllegalArgumentException("at least 2 numbers are required to create one bucket");
        // sanity check: all numbers need to be in increasing order
        sanityCheck(buckets);

        long prev_value=-1;
        List<Bucket> list=new ArrayList<>(buckets.length);
        for(int i=0; i < buckets.length; i++) {
            if(prev_value < 0) {
                prev_value=buckets[i];
                continue;
            }
            long min=prev_value, max=buckets[i];
            Bucket b=new Bucket(min, max);
            list.add(b);
            prev_value=max+1;
            min_value=Math.min(min, min_value);
            max_value=Math.max(max, max_value);
        }
        this.buckets=new Bucket[list.size()];
        for(int i=0; i < list.size(); i++)
            this.buckets[i]=list.get(i);
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

    protected static void sanityCheck(long ... buckets) {
        long prev_value=-1;
        for(long i: buckets) {
            if(prev_value >= 0) {
                if(i <= prev_value)
                    throw new IllegalArgumentException("values must be in increasing order");
            }
            prev_value=i;
        }
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
