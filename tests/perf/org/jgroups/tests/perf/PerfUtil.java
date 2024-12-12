package org.jgroups.tests.perf;

import org.jgroups.Global;
import org.jgroups.blocks.MethodCall;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.util.Bits;
import org.jgroups.util.SizeStreamable;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.jgroups.util.Util.printTime;

/**
 * Misc stuff for performance tests
 * @author Bela Ban
 * @since  5.0
 */
public class PerfUtil {

    public static void init() {
        ClassConfigurator.addIfAbsent((short)1040, GetCall.class);
        ClassConfigurator.addIfAbsent((short)1041, PutCall.class);
        ClassConfigurator.addIfAbsent((short)1042, Results.class);
        ClassConfigurator.addIfAbsent((short)1043, Config.class);
        ClassConfigurator.addIfAbsent((short)1044, CustomCall.class);
    }

    public static class PutCall extends MethodCall implements SizeStreamable {
        public PutCall()                                {}
        public PutCall(short method_id, Object... args) {super(method_id, args);}

        public Supplier<? extends MethodCall> create() {return PutCall::new;}

        @Override public void writeTo(DataOutput out) throws IOException {
            Integer key=(Integer)args[0];
            byte[] val=(byte[])args[1];
            out.writeShort(method_id);
            out.writeInt(key);
            out.writeInt(val.length);
            out.write(val, 0, val.length);
        }

        @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            method_id=in.readShort();
            int key=in.readInt(), length=in.readInt();
            byte[] val=new byte[length];
            in.readFully(val);
            args=new Object[]{key, val};
        }

        public int serializedSize() {
            byte[] buf=(byte[])args[1];
            return Global.SHORT_SIZE + Global.INT_SIZE*2 + buf.length;
        }
    }

    public static class GetCall extends MethodCall implements SizeStreamable {
        public GetCall()                                {}
        public GetCall(short method_id, Object... args) {super(method_id, args);}

        public Supplier<? extends MethodCall> create() {return GetCall::new;}

        @Override public void writeTo(DataOutput out) throws IOException {
            Integer key=(Integer)args[0];
            out.writeShort(method_id);
            out.writeInt(key);
        }

        @Override
        public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            method_id=in.readShort();
            args=new Object[]{in.readInt()};
        }

        public int serializedSize() {
            return Global.SHORT_SIZE + Global.INT_SIZE;
        }
    }

    /** Abstract class which uses IDs for methods. Argument serialization and deserialization can be overridden */
    public static class CustomCall extends MethodCall {
        protected static final byte NORMAL=0, EXCEPTION=1, CONFIG=2,RESULTS=3;


        public CustomCall()                                {}
        public CustomCall(short method_id, Object... args) {super(method_id, args);}

        public Supplier<? extends MethodCall> create() {return CustomCall::new;}

        @Override protected void writeArg(DataOutput out, Object obj) throws IOException {
            if(obj instanceof Throwable) {
                Throwable t=(Throwable)obj;
                out.writeByte(EXCEPTION);
                out.writeUTF(t.getMessage());
                return;
            }
            if(obj instanceof Config) {
                out.writeByte(CONFIG);
                ((Config)obj).writeTo(out);
                return;
            }
            if(obj instanceof Results) {
                out.writeByte(RESULTS);
                ((Results)obj).writeTo(out);
                return;
            }
            out.writeByte(NORMAL);
            Util.objectToStream(obj, out);
        }

        protected Object readArg(DataInput in) throws IOException, ClassNotFoundException {
            byte type=in.readByte();
            switch(type) {
                case NORMAL:
                    return Util.objectFromStream(in);
                case EXCEPTION:  // read exception
                    String message=in.readUTF();
                    return new RuntimeException(message);
                case CONFIG:
                    Config cfg=new Config();
                    cfg.readFrom(in);
                    return cfg;
                case RESULTS:
                    Results res=new Results();
                    res.readFrom(in);
                    return res;
                default:
                    throw new IllegalArgumentException("type " + type + " not known");
            }
        }
    }

    public static class AverageSummary implements Streamable {
        protected double   min, avg, max;
        protected TimeUnit unit;

        public AverageSummary() {
        }

        public AverageSummary(double min, double avg, double max) {
            this.min=min;
            this.avg=avg;
            this.max=max;
        }

        public double         min()               {return min;}
        public AverageSummary min(double min)     {this.min=min; return this;}
        public double         avg()               {return avg;}
        public AverageSummary avg(double avg)     {this.avg=avg; return this;}
        public double         max()               {return max;}
        public AverageSummary max(double max)     {this.max=max; return this;}
        public TimeUnit       unit()              {return unit;}
        public AverageSummary unit(TimeUnit unit) {this.unit=unit;return this;}

        public AverageSummary merge(AverageSummary other) {
            if(other == null)
                return this;
            min=Math.min(min, other.min);
            avg=(avg + other.avg) / 2;
            max=Math.max(max, other.max);
            return this;
        }

        /** Overwrites the current values with the given list of new values */
        public AverageSummary set(List<AverageSummary> others) {
            if(others == null)
                return this;
            min=others.stream().map(AverageSummary::min).min(Double::compare).orElse(0.0);
            max=others.stream().map(AverageSummary::max).max(Double::compare).orElse(0.0);
            double avg_sum=others.stream().map(AverageSummary::avg).mapToDouble(d -> d).sum();
            avg=avg_sum / others.size();
            unit=others.stream().map(AverageSummary::unit).filter(Objects::nonNull).findFirst().orElse(null);
            return this;
        }

        public String toString() {
            return unit != null? toString(unit) : String.format("min/avg/max=%.2f/%.2f/%.2f", min, avg, max);
        }

        public String toString(TimeUnit u) {
            return String.format("%s/%s/%s", printTime(min, u), printTime(avg, u), printTime(max, u));
        }

        @Override
        public void writeTo(DataOutput out) throws IOException {
            Bits.writeDouble(min, out);
            Bits.writeDouble(avg, out);
            Bits.writeDouble(max, out);
            int ordinal=unit != null? unit.ordinal() : -1;
            out.writeInt(ordinal);
        }

        @Override
        public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            min=Bits.readDouble(in);
            avg=Bits.readDouble(in);
            max=Bits.readDouble(in);
            int ordinal=in.readInt();
            if(ordinal != -1)
                unit=TimeUnit.values()[ordinal];
        }
    }


    public static class Results implements Streamable {
        protected long           num_gets;
        protected long           num_puts;
        protected long           total_time;     // in ms
        protected AverageSummary avg_gets;       // RTT
        protected AverageSummary avg_puts;       // RTT

        public Results() {

        }

        public Results(int num_gets, int num_puts, long total_time, AverageSummary avg_gets, AverageSummary avg_puts) {
            this.num_gets=num_gets;
            this.num_puts=num_puts;
            this.total_time=total_time;
            this.avg_gets=avg_gets;
            this.avg_puts=avg_puts;
        }

        @Override
        public void writeTo(DataOutput out) throws IOException {
            Bits.writeLongCompressed(num_gets, out);
            Bits.writeLongCompressed(num_puts, out);
            Bits.writeLongCompressed(total_time, out);
            Util.writeStreamable(avg_gets, out);
            Util.writeStreamable(avg_puts, out);
        }

        @Override
        public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            num_gets=Bits.readLongCompressed(in);
            num_puts=Bits.readLongCompressed(in);
            total_time=Bits.readLongCompressed(in);
            avg_gets=Util.readStreamable(AverageSummary::new, in);
            avg_puts=Util.readStreamable(AverageSummary::new, in);
        }

        public String toString() {
            long total_reqs=num_gets + num_puts;
            double total_reqs_per_sec=total_reqs / (total_time / 1000.0);
            return String.format("%,.2f reqs/sec (%,d gets, %,d puts, get RTT %s, put RTT %s)",
                                 total_reqs_per_sec, num_gets, num_puts, Util.printTime(avg_gets.avg(), avg_gets.unit()),
                                 Util.printTime(avg_puts.avg(), avg_puts.unit()));
        }
    }

    public static class Config implements Streamable {
        protected Map<String,Object> values=new HashMap<>();

        public Config() {
        }

        public Map<String,Object> values() {return values;}

        public Config add(String key, Object value) {
            values.put(key, value);
            return this;
        }

        @Override
        public void writeTo(DataOutput out) throws IOException {
            out.writeInt(values.size());
            for(Map.Entry<String,Object> entry: values.entrySet()) {
                Bits.writeString(entry.getKey(),out);
                Util.objectToStream(entry.getValue(), out);
            }
        }

        @Override
        public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            int size=in.readInt();
            for(int i=0; i < size; i++) {
                String key=Bits.readString(in);
                Object value=Util.objectFromStream(in);
                if(key == null)
                    continue;
                values.put(key, value);
            }
        }

        public String toString() {
            return values.toString();
        }
    }

}
