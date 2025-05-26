package org.jgroups.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.jgroups.util.Util.printTime;

/**
 * Maintains an approximation of an average of positive (>= 0) values in a window and adding new values at random
 * indices.<br/>
 * This class is lock-less, at the expense of incorrect reads and writes (by multiple threads), but the noise of
 * these errors should be negligible. Note that a bigger capacity (window size) reduces this noise.
 * @author Bela Ban
 * @since  3.4, 5.4
 */
public class Average implements Streamable {
    protected long[]           samples;
    protected TimeUnit         unit;
    protected volatile boolean all_filled;
    protected static final int DEFAULT_CAPACITY=512;

    public Average() {
        this(DEFAULT_CAPACITY);
    }

    public Average(final int capacity) {
        this.samples=new long[capacity];
        Arrays.fill(samples, -1);
    }

    public int                   capacity()       {return samples.length;}
    public TimeUnit              unit()           {return unit;}
    public <T extends Average> T unit(TimeUnit u) {this.unit=u; return (T)this;}
    public double                getAverage()     {return average();}

    public <T extends Average> T add(long num) {
        if(num < 0)
            return (T)this;
        int idx=Util.random(samples.length)-1;
        samples[idx]=num;
        return (T)this;
    }

    /** Merges this average with another one */
    public <T extends Average> T merge(T other) {
        if(other == null)
            return (T)this;
        for(int i=0; i < other.samples.length; i++)
            add(other.samples[i]);
        return (T)this;
    }

    /** Returns the total of all valid (>= 0) values */
    public long total() {
        long ret=0;
        for(int i=0; i < samples.length; i++) {
            long sample=samples[i];
            if(sample >= 0)
                ret+=sample;
        }
        return ret;
    }

    /** Returns the number of valid samples (>= 0) */
    public int count() {
        if(all_filled)
            return samples.length;
        int ret=0;
        for(int i=0; i < samples.length; i++) {
            if(samples[i] >= 0)
                ret++;
        }
        if(ret >= samples.length)
            all_filled=true;
        return ret;
    }

    /** Returns true if there are no valid samples (>= 0), false otherwise */
    public boolean isEmpty() {
        if(all_filled)
            return false;
        for(int i=0; i < samples.length; i++) {
            if(samples[i] >= 0)
                return false;
        }
        return true;
    }

    /** Returns the average of all valid samples divided by the number of valid samples */
    public double average() {
        int count=0;
        long total=0;
        for(int i=0; i < samples.length; i++) {
            long sample=samples[i];
            if(sample >= 0) {
                count++;
                total+=sample;
            }
        }
        if(count == 0)
            return 0.0;
        return total / (double)count;
    }

    public void clear() {
        Arrays.fill(samples, -1);
        all_filled=false;
    }

    public String toString() {
        return unit != null? toString(unit) : String.format("%,.2f %s", average(), unit == null? "" : Util.suffix(unit));
    }

    public String toString(TimeUnit u) {
        if(count() == 0)
            return "n/a";
        return String.format("%s", printTime(average(), u));
    }

    @Override
    public void writeTo(DataOutput out) throws IOException {
        out.writeInt(samples.length);
        for(int i=0; i < samples.length; i++)
            Bits.writeLongCompressed(samples[i], out);
    }

    @Override
    public void readFrom(DataInput in) throws IOException {
        int len=in.readInt();
        samples=new long[len];
        for(int i=0; i < samples.length; i++)
            samples[i]=Bits.readLongCompressed(in);
    }

}
