package org.jgroups.util;

/**
 * Maintains an approximation of an average of values. Done by keeping track of the number of samples, and computing
 * the new average as (count*avg + new-sample) / ++count. We reset the count if count*avg would lead to an overflow.
 * @author Bela Ban
 * @since  3.4
 */
public class Average {
    protected double avg;
    protected long   count;


    public void add(long num) {
        // If the product of the average and the number of samples would be greater than Long.MAX_VALUE, we have
        // to reset the count and average to prevent a long overflow. This will temporarily lose the sample history, and
        // the next sample will be the new average, but with more data points, the average should become more precise.
        // Note that overflow should be extremely seldom, as we usually use Average in cases where we don't have a huge
        // number of sample and the average is pretty small (e.g. an RPC invocation)
        if(Util.productGreaterThan(count, (long)Math.ceil(avg), Long.MAX_VALUE))
            clear();
        double total=count*avg;
        avg=(total + num) / ++count;
    }

    public double getAverage() {
        return avg;
    }

    public long getCount() {return count;}

    public void clear() {
        avg=0.0;
        count=0;
    }

    public String toString() {
        return String.valueOf(getAverage());
    }


   /* protected final long[]  samples;
    protected AtomicInteger index=new AtomicInteger(0);

    public Average() {
        this(64);
    }

    public Average(int size) {
        samples=new long[Util.getNextHigherPowerOfTwo(size)];
        for(int i=0; i < samples.length; i++)
            samples[i]=-1;
    }

    public void add(long sample) {
        // samples[((int)Util.random(samples.length) -1)]=sample;
        int tmp_index=index.getAndIncrement();
        int real_index=tmp_index & (samples.length -1); // fast modulo operation
        samples[real_index]=sample;
        if(tmp_index >= samples.length)
            index.set(0);
    }

    public double getAverage() {
        int  num=0;
        long total=0;
        for(int i=0; i < samples.length; i++) {
            long sample=samples[i];
            if(sample >= 0) {
                num++;
                total+=sample;
            }
        }
        return num > 0? total / (double)num : 0;
    }

    public void clear() {
        for(int i=0; i < samples.length; i++)
            samples[i]=-1;
    }

    public String toString() {
        return String.valueOf(getAverage());
    }*/
}
