package org.jgroups.protocols;

import org.jgroups.Global;
import org.jgroups.Header;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

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

    public int size() {
        return Global.BYTE_SIZE;
    }

    public void writeTo(DataOutputStream out) throws IOException {
        out.writeByte(type);
    }

    public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
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
