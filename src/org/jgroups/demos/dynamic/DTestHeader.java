package org.jgroups.demos.dynamic;

import org.jgroups.Global;
import org.jgroups.Header;

import java.io.DataInput;
import java.io.DataOutput;

/**
 * Marks a message as either a request or a response. The arguments to the request or return object of the response
 * are in the payload of the message itself.
 * @author Bela Ban
 * @since 3.1
 */
public class DTestHeader extends Header {
    public static final byte REQ = 1;
    public static final byte RSP = 2;

    protected byte type;


    public DTestHeader() {
    }

    public DTestHeader(byte type) {
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
        return type == REQ? "REQ" : (type == RSP? "RSP" : "unknown");
    }
}
