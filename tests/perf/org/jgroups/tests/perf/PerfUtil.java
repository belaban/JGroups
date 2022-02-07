package org.jgroups.tests.perf;

import org.jgroups.Global;
import org.jgroups.blocks.MethodCall;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.util.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

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


    public static class Results implements Streamable {
        protected long          num_gets;
        protected long          num_puts;
        protected long          total_time;     // in ms
        protected AverageMinMax avg_gets; // RTT in ns
        protected AverageMinMax avg_puts; // RTT in ns

        public Results() {

        }

        public Results(int num_gets, int num_puts, long total_time, AverageMinMax avg_gets, AverageMinMax avg_puts) {
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
            avg_gets=Util.readStreamable(AverageMinMax::new, in);
            avg_puts=Util.readStreamable(AverageMinMax::new, in);
        }

        public String toString() {
            long total_reqs=num_gets + num_puts;
            double total_reqs_per_sec=total_reqs / (total_time / 1000.0);
            return String.format("%,.2f reqs/sec (%,d gets, %,d puts, get RTT %,.2f us, put RTT %,.2f us)",
                                 total_reqs_per_sec, num_gets, num_puts, avg_gets.average() / 1000.0,
                                 avg_puts.getAverage()/1000.0);
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
