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
    protected double           min=Double.MAX_VALUE, max=0;
    protected List<Double>     values;
    protected volatile boolean sorted;

    public AverageMinMax() {
    }

    public AverageMinMax(int capacity) {
        super(capacity);
    }

    public double        min()                   {return min;}
    public double        max()                   {return max;}
    public boolean       usePercentiles()        {return values != null;}
    public AverageMinMax usePercentiles(int cap) {values=cap > 0? new ArrayList<>(cap) : null; return this;}
    public List<Double>  values()                {return values;}

    public <T extends Average> T add(double num) {
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
        min=Double.MAX_VALUE; max=0;
    }

    public String percentiles() {
        if(values == null) return "n/a";
        sort();
        double stddev=stddev();
        return String.format("stddev: %s, 50: %s, 90: %s, 99: %s, 99.9: %s, 99.99: %s, 99.999: %s, 100: %s\n",
                             printTime(stddev, unit),
                             printTime(p(50), unit),
                             printTime(p(90), unit),
                             printTime(p(99), unit),
                             printTime(p(99.9), unit),
                             printTime(p(99.99), unit),
                             printTime(p(99.999), unit),
                             printTime(p(100), unit));
    }

    public String toString() {
        return count.sum() == 0? "n/a" :
          unit != null? toString(unit) :
          String.format("min/avg/max=%.2f/%.2f/%.2f%s",
                        min, getAverage(), max, unit == null? "" : " " + Util.suffix(unit));
    }

    public String toString(TimeUnit u) {
        if(count.sum() == 0)
            return "n/a";
        return String.format("%s/%s/%s", printTime(min, u), printTime(getAverage(), u), printTime(max, u));
    }

    public void writeTo(DataOutput out) throws IOException {
        super.writeTo(out);
        Bits.writeDouble(min, out);
        Bits.writeDouble(max, out);
    }

    public void readFrom(DataInput in) throws IOException {
        super.readFrom(in);
        min=Bits.readDouble(in);
        max=Bits.readDouble(in);
    }

    public double percentile(double percentile) {
        return p(percentile);
    }

    public double p(double percentile) {
        if(values == null)
            return -1;
        sort();
        int size=values.size();
        if(size == 0)
            return -1;
        int idx=size == 1? 1 : (int)(size * (percentile/100.0));
        return values.get(idx-1);
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
