
package org.jgroups.protocols;

import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.util.Bits;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.function.Supplier;


/**
 * Used to send discovery requests and responses
 * @author Bela Ban
 */
public class PingHeader extends Header {
    public static final byte GET_MBRS_REQ=1;
    public static final byte GET_MBRS_RSP=2;

    protected byte    type;
    protected String  cluster_name;
    protected boolean initial_discovery;


    public PingHeader() {
    }

    public PingHeader(byte type)                   {this.type=type;}
    public byte       type()                       {return type;}
    public PingHeader clusterName(String name)     {this.cluster_name=name; return this;}
    public boolean    initialDiscovery()           {return initial_discovery;}
    public PingHeader initialDiscovery(boolean b)  {this.initial_discovery=b; return this;}
    public short      getMagicId()                 {return 53;}

    public Supplier<? extends Header> create() {return PingHeader::new;}

    @Override
    public int serializedSize() {
        int retval=Global.BYTE_SIZE *3; // type, cluster_name presence and initial_discovery
        if(cluster_name != null)
            retval += cluster_name.length() +2;
        return retval;
    }

    public String toString() {
        return String.format("[%s cluster=%s initial_discovery=%b]", type2Str(type), cluster_name, initial_discovery);
    }

    static String type2Str(byte t) {
        switch(t) {
            case GET_MBRS_REQ: return "GET_MBRS_REQ";
            case GET_MBRS_RSP: return "GET_MBRS_RSP";
            default:           return "<unkown type (" + t + ")>";
        }
    }

    @Override
    public void writeTo(DataOutput out) throws IOException {
        out.writeByte(type);
        Bits.writeString(cluster_name,out);
        out.writeBoolean(initial_discovery);
    }

    @Override
    public void readFrom(DataInput in) throws IOException {
        type=in.readByte();
        cluster_name=Bits.readString(in);
        initial_discovery=in.readBoolean();
    }
}
