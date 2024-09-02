package org.jgroups.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.jgroups.util.Util.printTime;

/**
 * Maintains an approximation of an average of positive values by keeping a number of values and overwriting the
 * oldest values when new values are added.
 * This class is not thread-safe and relies on external synchronization.
 * @author Bela Ban
 * @since  3.4
 */
public class Average implements Streamable {
    protected double   total;
    protected double[] samples;
    protected int      index;
    protected long     count;
    protected TimeUnit unit;

    public Average() {
        this(128);
    }

    public Average(final int capacity) {
        this.samples=new double[capacity];
    }

    public <T extends Average> T add(double num) {
        if(num < 0)
            return (T)this;
        total-=samples[index];
        samples[index]=num;
        total+=num;
        if(++index == samples.length) {
            index=0; // cheaper than modulus
        }
        count++;
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

    public double                getAverage()     {return average();}
    public long                  count()          {return count;}
    public double                getTotal()       {return total;}
    public TimeUnit              unit()           {return unit;}
    public <T extends Average> T unit(TimeUnit u) {this.unit=u; return (T)this;}

    public double average() {
        int len=(count < samples.length? index : samples.length);
        if(len == 0)
            return 0.0;
        return total / len;
    }

    public void clear() {
        Arrays.fill(samples, 0d);
        total=count=0;
    }

    public String toString() {
        return unit != null? toString(unit) : String.format("%,.2f %s", average(), unit == null? "" : Util.suffix(unit));
    }

    public String toString(TimeUnit u) {
        if(count == 0)
            return "n/a";
        return String.format("%s", printTime(getAverage(), u));
    }

    @Override
    public void writeTo(DataOutput out) throws IOException {
        Bits.writeIntCompressed(samples.length, out);
        for(int i=0; i < samples.length; i++)
            Bits.writeDouble(samples[i], out);
        Bits.writeDouble(total, out);
    }

    @Override
    public void readFrom(DataInput in) throws IOException {
        int len=Bits.readIntCompressed(in);
        samples=new double[len];
        for(int i=0; i < samples.length; i++)
            samples[i]=Bits.readDouble(in);
        total=Bits.readDouble(in);
    }
}
