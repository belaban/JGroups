package org.jgroups.util;

import java.io.DataInput;
import java.io.DataOutput;

/**
 * Measures min and max in addition to average
 * @author Bela Ban
 * @since  4.0, 3.6.10
 */
public class AverageMinMax extends Average {
    protected long min=Long.MAX_VALUE, max=0;

    public long min() {return min;}
    public long max() {return max;}

    public <T extends Average> T add(long num) {
        super.add(num);
        min=Math.min(min, num);
        max=Math.max(max, num);
        return (T)this;
    }

    public <T extends Average> T merge(T other) {
        super.merge(other);
        if(other instanceof AverageMinMax) {
            AverageMinMax o=(AverageMinMax)other;
            this.min=Math.min(min, o.min());
            this.max=Math.max(max, o.max());
        }
        return (T)this;
    }

    public void clear() {
        super.clear();
        min=Long.MAX_VALUE; max=0;
    }

    public String toString() {
        return String.format("min/avg/max=%d/%.2f/%d", min, getAverage(), max);
    }

    public void writeTo(DataOutput out) throws Exception {
        super.writeTo(out);
        Bits.writeLong(min, out);
        Bits.writeLong(max, out);
    }

    public void readFrom(DataInput in) throws Exception {
        super.readFrom(in);
        min=Bits.readLong(in);
        max=Bits.readLong(in);
    }


}
