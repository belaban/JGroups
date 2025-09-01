package org.jgroups.protocols;

import org.jgroups.Global;
import org.jgroups.Header;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.function.Supplier;

public class PerfHeader extends Header {
    protected long start_time; // in ns

    public PerfHeader() {
    }

    public PerfHeader(long start_time) {
        this.start_time=start_time;
    }
    public short getMagicId() {return 84;}
    public Supplier<? extends Header> create() {
        return PerfHeader::new;
    }

    public long startTime() {return start_time;}

    @Override
    public int serializedSize() {
        return Global.LONG_SIZE;
    }

    @Override
    public void writeTo(DataOutput out) throws IOException {
        out.writeLong(start_time);
    }

    @Override
    public void readFrom(DataInput in) throws IOException {
        start_time=in.readLong();
    }
}
