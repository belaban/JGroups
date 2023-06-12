package org.jgroups.protocols.relay;

import org.jgroups.Header;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.function.Supplier;

/**
 * Used to send topology requests and responses in {@link RELAY2}.
 * @author Bela Ban
 * @since  5.2.15
 */
public class TopoHeader extends Header {
    protected static final byte REQ=0, RSP=1;
    protected byte type; // 0: topology request, 1: topology response

    public TopoHeader() {}
    public TopoHeader(byte type) {this.type=type;}

    public Supplier<? extends Header> create()         {return TopoHeader::new;}
    public short                      getMagicId()     {return 99;}
    public int                        serializedSize() {return Byte.BYTES;}
    public byte                       type()           {return type;}
    public TopoHeader                 type(byte t)     {this.type=t; return this;}

    public void writeTo(DataOutput out) throws IOException {
        out.writeByte(type);
    }

    public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        type=in.readByte();
    }

    public String toString() {
        return type == 0? "topo req" : "topo rsp";
    }
}
