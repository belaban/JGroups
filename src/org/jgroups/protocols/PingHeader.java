
package org.jgroups.protocols;

import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.util.Bits;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.function.Supplier;


/**
 * @author Bela Ban
 */
public class PingHeader extends Header {
    public static final byte GET_MBRS_REQ=1;
    public static final byte GET_MBRS_RSP=2;

    protected byte   type=0;
    protected String cluster_name;


    public PingHeader() {
    }

    public PingHeader(byte type)                               {this.type=type;}
    public byte type()                                         {return type;}
    public PingHeader clusterName(String name)                 {this.cluster_name=name; return this;}

    public short getMagicId() {return 53;}

    public Supplier<? extends Header> create() {return PingHeader::new;}

    public int size() {
        int retval=Global.BYTE_SIZE *2; // type and cluster_name presence
        if(cluster_name != null)
            retval += cluster_name.length() +2;
        return retval;
    }

    public String toString() {
        StringBuilder sb=new StringBuilder();
        sb.append("[type=" + type2Str(type));
        if(cluster_name != null)
            sb.append(", cluster=").append(cluster_name);
        sb.append(']');
        return sb.toString();
    }

    static String type2Str(byte t) {
        switch(t) {
            case GET_MBRS_REQ: return "GET_MBRS_REQ";
            case GET_MBRS_RSP: return "GET_MBRS_RSP";
            default:           return "<unkown type (" + t + ")>";
        }
    }


    public void writeTo(DataOutput outstream) throws Exception {
        outstream.writeByte(type);
        Bits.writeString(cluster_name,outstream);
    }

    public void readFrom(DataInput instream) throws Exception {
        type=instream.readByte();
        cluster_name=Bits.readString(instream);
    }
}
