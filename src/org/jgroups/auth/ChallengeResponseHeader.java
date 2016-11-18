package org.jgroups.auth;

import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.function.Supplier;

/**
 * @author Bela Ban
 * @since  3.3
 */
public class ChallengeResponseHeader extends Header {
    protected static final byte CHALLENGE = 1;
    protected static final byte RESPONSE  = 2;

    protected byte   type;
    protected byte[] payload;  // CHALLENGE
    protected long   hash;     // RESPONSE

    public ChallengeResponseHeader() {
    }

    public ChallengeResponseHeader(byte[] payload) {
        this.type=CHALLENGE;
        this.payload=payload;
    }

    public ChallengeResponseHeader(long hash) {
        type=RESPONSE;
        this.hash=hash;
    }
    public short getMagicId() {return 90;}
    public Supplier<? extends Header> create() {
        return ChallengeResponseHeader::new;
    }

    public void writeTo(DataOutput out) throws Exception {
        out.writeByte(type);
        switch(type) {
            case CHALLENGE:
                Util.writeByteBuffer(payload, out);
                break;
            case RESPONSE:
                out.writeLong(hash);
                break;
        }
    }

    public void readFrom(DataInput in) throws Exception {
        type=in.readByte();
        switch(type) {
            case CHALLENGE:
                payload=Util.readByteBuffer(in);
                break;
            case RESPONSE:
                hash=in.readLong();
                break;
        }
    }

    public int serializedSize() {
        int retval=Global.BYTE_SIZE; // type
        switch(type) {
            case CHALLENGE:
                retval+=Util.size(payload);
                break;
            case RESPONSE:
                retval+=Global.LONG_SIZE;
                break;
        }
        return retval;
    }

    public String toString() {
        return type == CHALLENGE? "CHALLENGE" : "RESPONSE" + ", payload=" + (payload != null? payload.length : 0) + " bytes";
    }
}
