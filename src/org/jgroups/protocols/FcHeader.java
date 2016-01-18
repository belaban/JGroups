package org.jgroups.protocols;

import org.jgroups.Global;
import org.jgroups.AbstractHeader;

import java.io.DataInput;
import java.io.DataOutput;

/**
 * Header used by various flow control protocols
 * @author Bela Ban
 */
public class FcHeader extends AbstractHeader {
    public static final byte REPLENISH=1;
    public static final byte CREDIT_REQUEST=2; // the sender of the message is the requester

    byte type=REPLENISH;

    public FcHeader() {

    }

    public FcHeader(byte type) {
        this.type=type;
    }

    public int size() {
        return Global.BYTE_SIZE;
    }

    public void writeTo(DataOutput out) throws Exception {
        out.writeByte(type);
    }

    public void readFrom(DataInput in) throws Exception {
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
