package org.jgroups.protocols;


import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.util.AsciiString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.function.Supplier;


/**
 * Generic transport header, used by TP.
 * @author Bela Ban
 */
public class TpHeader extends Header {
    protected byte[] cluster_name;

    public TpHeader() { // used for externalization
    }

    public TpHeader(String n) {
        int len=n.length();
        cluster_name=new byte[len];
        for(int i=0; i < len; i++)
            cluster_name[i]=(byte)n.charAt(i);
    }

    public TpHeader(AsciiString n) {
        cluster_name=n != null? n.chars() : null;
    }

    public TpHeader(byte[] n) {
        cluster_name=n;
    }

    public Supplier<? extends Header> create() {return TpHeader::new;}

    public short getMagicId() {return 60;}

    public String toString() {
        return String.format("[cluster=%s]", cluster_name != null ? new String(cluster_name) : "null");
    }

    public byte[] getClusterName() {return cluster_name;}

    @Override
    public int serializedSize() {
        return cluster_name != null? Global.SHORT_SIZE + cluster_name.length : Global.SHORT_SIZE;
    }

    @Override
    public void writeTo(DataOutput out) throws IOException {
        int length=cluster_name != null? cluster_name.length : -1;
        out.writeShort(length);
        if(cluster_name != null)
            out.write(cluster_name, 0, cluster_name.length);
    }

    @Override
    public void readFrom(DataInput in) throws IOException {
        int len=in.readShort();
        if(len >= 0) {
            cluster_name=new byte[len];
            in.readFully(cluster_name, 0, cluster_name.length);
        }
    }
}
