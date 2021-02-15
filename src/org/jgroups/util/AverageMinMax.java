package org.jgroups.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Measures min and max in addition to average
 * @author Bela Ban
 * @since  4.0, 3.6.10
 */
public class AverageMinMax extends Average {
    protected long       min=Long.MAX_VALUE, max=0;
    protected List<Long> values;

    public long          min()                        {return min;}
    public long          max()                        {return max;}
    public boolean       usePercentiles()             {return values != null;}
    public AverageMinMax usePercentiles(int capacity) {values=capacity > 0? new ArrayList<>(capacity) : null; return this;}

    public <T extends Average> T add(long num) {
        super.add(num);
        min=Math.min(min, num);
        max=Math.max(max, num);
        if(values != null)
            values.add(num);
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
            if(this.values != null)
                this.values.addAll(o.values);
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
        Collections.sort(values);
        double stddev=stddev();
        return String.format("stddev: %.2f, 50: %d, 90: %d, 99: %d, 99.9: %d, 99.99: %d, 99.999: %d, 100: %d\n",
                             stddev, p(50), p(90), p(99), p(99.9), p(99.99), p(99.999), p(100));
    }

    protected long p(double percentile) {
        if(values == null)
            return -1;
        int size=values.size();
        int index=(int)(size * (percentile/100.0));
        return values.get(index-1);
    }

    protected double stddev() {
        if(values == null) return -1.0;
        double av=average();
        int size=values.size();
        double variance=values.stream().map(v -> (v - av)*(v - av)).reduce(0.0, Double::sum) / size;
        return Math.sqrt(variance);
    }

    public String toString() {
        return count == 0? "n/a" : String.format("min/avg/max=%,d/%,.2f/%,d", min, getAverage(), max);
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


}
