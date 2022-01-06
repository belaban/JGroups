package org.jgroups;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class NullAddress implements Address {
    @Override
    public int compareTo(Address that) {
        if (that instanceof NullAddress) {
            return 0;
        } else {
            return -1;
        }
    }

    public boolean equals(Object obj) {
        return obj instanceof NullAddress;
    }

    @Override
    public int serializedSize() {
        return 0;
    }

    @Override
    public void writeTo(DataOutput out) throws IOException {

    }

    @Override
    public void readFrom(DataInput in) throws IOException, ClassNotFoundException {

    }
}
