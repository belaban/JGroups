package org.jgroups.util;

/**
 * A class which uses a simple array (of configurable length) to store stats samples. The average is then taken as
 * the sum of all values >= 0 (the array is initialized with -1 values) divided by the number of non -1 values. This is
 * best used in cases where we have a lot of sample data entered by different threads. Each thread picks a random
 * array index and sets the value at the index. This will be an <em>approximation</em> of an average as all values
 * are updated after some time.
 * @author Bela Ban
 * @since  3.4
 */
public class Average {
    protected final long[] samples;

    public Average() {
        this(10);
    }

    public Average(int size) {
        samples=new long[size];
        for(int i=0; i < samples.length; i++)
            samples[i]=-1;
    }

    public void add(long sample) {
        samples[((int)Util.random(samples.length) -1)]=sample;
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

}
