package org.jgroups.protocols;

import org.jgroups.Global;
import org.jgroups.Header;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.function.Supplier;

/**
 * Header used by various flow control protocols
 * @author Bela Ban
 */
public class FcHeader extends Header {
    public static final byte REPLENISH=1;
    public static final byte CREDIT_REQUEST=2; // the sender of the message is the requester

    byte type=REPLENISH;

    public FcHeader() {

    }

    public FcHeader(byte type) {
        this.type=type;
    }

    public Supplier<? extends Header> create() {
        return FcHeader::new;
    }

    public short getMagicId() {return 59;}

    @Override
    public int serializedSize() {
        return Global.BYTE_SIZE;
    }

    @Override
    public void writeTo(DataOutput out) throws IOException {
        out.writeByte(type);
    }

    @Override
    public void readFrom(DataInput in) throws IOException {
        type=in.readByte();
    }

    public String toString() {
        switch(type) {
            case REPLENISH:
                return "REPLENISH";
            case CREDIT_REQUEST:
                return "CREDIT_REQUEST";
            default:
                return "<invalid type>";
        }
    }
}
