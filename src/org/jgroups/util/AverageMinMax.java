package org.jgroups.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.jgroups.util.Util.printTime;

/**
 * Measures min and max in addition to average
 * @author Bela Ban
 * @since  4.0, 3.6.10
 */
public class AverageMinMax extends Average {
    protected long             min=Long.MAX_VALUE, max=0;
    protected List<Long>       values;
    protected volatile boolean sorted;

    public long          min()                   {return min;}
    public long          max()                   {return max;}
    public boolean       usePercentiles()        {return values != null;}
    public AverageMinMax usePercentiles(int cap) {values=cap > 0? new ArrayList<>(cap) : null; return this;}
    public List<Long>    values()                {return values;}

    public <T extends Average> T add(long num) {
        super.add(num);
        min=Math.min(min, num);
        max=Math.max(max, num);
        if(values != null) {
            values.add(num);
            sorted=false;
        }
        return (T)this;
    }

    public <T extends Average> T merge(T other) {
        if(other.count() == 0)
            return (T)this;
        super.merge(other);
        if(other instanceof AverageMinMax) {
            AverageMinMax o=(AverageMinMax)other;
            this.min=Math.min(min, o.min());
            this.max=Math.max(max, o.max());
            if(this.values != null) {
                this.values.addAll(o.values);
                sorted=false;
            }
        }
        return (T)this;
    }

    public void clear() {
        super.clear();
        if(values != null)
            values.clear();
        min=Long.MAX_VALUE; max=0;
    }

    public String percentiles() {
        if(values == null) return "n/a";
        sort();
        double stddev=stddev();
        return String.format("stddev: %.2f, 50: %d, 90: %d, 99: %d, 99.9: %d, 99.99: %d, 99.999: %d, 100: %d\n",
                             stddev, p(50), p(90), p(99), p(99.9), p(99.99), p(99.999), p(100));
    }

    public String toString() {
        return count == 0? "n/a" :
          unit != null? toString(unit) :
          String.format("min/avg/max=%,d/%,.2f/%,d%s",
                        min, getAverage(), max, unit == null? "" : " " + Util.suffix(unit));
    }

    public String toString(TimeUnit u) {
        if(count == 0)
            return "n/a";
        return String.format("%s/%s/%s", printTime(min, u), printTime(getAverage(), u), printTime(max, u));
    }

    public void writeTo(DataOutput out) throws IOException {
        super.writeTo(out);
        Bits.writeLongCompressed(min, out);
        Bits.writeLongCompressed(max, out);
    }

    public void readFrom(DataInput in) throws IOException {
        super.readFrom(in);
        min=Bits.readLongCompressed(in);
        max=Bits.readLongCompressed(in);
    }

    public long percentile(double percentile) {
        return p(percentile);
    }

    public long p(double percentile) {
        if(values == null)
            return -1;
        sort();
        int size=values.size();
        if(size == 0)
            return -1;
        int index=size == 1? 1 : (int)(size * (percentile/100.0));
        return values.get(index-1);
    }

    public double stddev() {
        if(values == null) return -1.0;
        sort();
        double av=average();
        int size=values.size();
        double variance=values.stream().map(v -> (v - av)*(v - av)).reduce(0.0, Double::sum) / size;
        return Math.sqrt(variance);
    }

    public AverageMinMax sort() {
        if(values != null && !sorted) {
            Collections.sort(values);
            sorted=true;
        }
        return this;
    }


}
