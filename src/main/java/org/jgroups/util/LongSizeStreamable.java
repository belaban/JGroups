package org.jgroups.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.function.LongSupplier;

/**
 * Utility class that implements {@link SizeStreamable} and {@link LongSupplier} to store a {@code long} value.
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public final class LongSizeStreamable implements SizeStreamable, LongSupplier {

    private long value;

    // for unmarshalling
    @SuppressWarnings("unused")
    public LongSizeStreamable() {
    }

    public LongSizeStreamable(long value) {
        this.value = value;
    }

    @Override
    public int serializedSize() {
        return Long.BYTES;
    }

    @Override
    public void writeTo(DataOutput out) throws IOException {
        out.writeLong(value);
    }

    @Override
    public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        value = in.readLong();
    }

    @Override
    public long getAsLong() {
        return value;
    }
}
