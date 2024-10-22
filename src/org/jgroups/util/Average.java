package org.jgroups.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.jgroups.util.Util.printTime;

/**
 * Maintains an approximation of an average of positive values by keeping a number of values and overwriting values
 * when new values are added.<br/>
 * This class uses lockless algorithms and is thread-safe
 * @author Bela Ban
 * @since  3.4
 */
public class Average implements Streamable {
    protected final DoubleAdder            total=new DoubleAdder();
    protected AtomicReferenceArray<Double> samples;
    protected int                          index=-1;
    protected final LongAdder              count=new LongAdder();
    protected TimeUnit                     unit;
    protected final Lock                   lock=new ReentrantLock(); // for nextIndex()

    public Average() {
        this(128);
    }

    public Average(final int capacity) {
        this.samples=new AtomicReferenceArray<>(capacity);
    }

    public <T extends Average> T add(double num) {
        if(num < 0)
            return (T)this;
        int idx=nextIndex();
        Double old_value=samples.getAndSet(idx, num);
        total.add(num - (old_value != null? old_value : 0d)); // add 'num' as new sample, subtract 'samples[idx]' as old sample
        count.increment();
        return (T)this;
    }

    public <T extends Average> T add(long num) {
        return add((double)num);
    }

    /** Merges this average with another one */
    public <T extends Average> T merge(T other) {
        if(other == null)
            return (T)this;
        for(int i=0; i < other.samples.length(); i++)
            add(other.samples.get(i));
        return (T)this;
    }

    public double                getAverage()     {return average();}
    public long                  count()          {return count.sum();}
    public double                getTotal()       {return total.sum();}
    public TimeUnit              unit()           {return unit;}
    public <T extends Average> T unit(TimeUnit u) {this.unit=u; return (T)this;}

    public double average() {
        int len=(count.sum() < samples.length()? index+1 : samples.length());
        if(len == 0)
            return 0.0;
        return total.sum() / len;
    }

    public void clear() {
        for(int i=0; i < samples.length(); i++)
            samples.set(i, 0d);
        total.reset();
        count.reset();
    }

    public String toString() {
        return unit != null? toString(unit) : String.format("%,.2f %s", average(), unit == null? "" : Util.suffix(unit));
    }

    public String toString(TimeUnit u) {
        if(count.sum() == 0)
            return "n/a";
        return String.format("%s", printTime(getAverage(), u));
    }

    @Override
    public void writeTo(DataOutput out) throws IOException {
        Bits.writeIntCompressed(samples.length(), out);
        for(int i=0; i < samples.length(); i++)
            Bits.writeDouble(samples.get(i), out);
        Bits.writeDouble(total.sum(), out);
    }

    @Override
    public void readFrom(DataInput in) throws IOException {
        int len=Bits.readIntCompressed(in);
        samples=new AtomicReferenceArray<>(len);
        for(int i=0; i < samples.length(); i++)
            samples.set(i, Bits.readDouble(in));
        total.add(Bits.readDouble(in));
    }

    protected int nextIndex() {
        lock.lock();
        try {
            if(++index >= samples.length())
                index=0;
            return index;
        }
        finally {
            lock.unlock();
        }
        // using a lock is orders of magnitude faster than AtomicInteger.accumulateAndGet()!
        /*return index.accumulateAndGet(1, (l, __) -> {
            if(l+1 >= samples.length())
                return 0;
            else
                return l+1;
        });*/
    }
}
