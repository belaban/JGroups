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

    // the next 2 fields are used to measure round-trip times
    public static final byte REQ=1, RSP=2;
    protected byte   flag;  // default: 0, 1: RTT request, 2: RTT response (https://issues.redhat.com/browse/JGRP-2812)
    protected int    index; // used when flag > 0

    public TpHeader() { // used for externalization
    }

    public TpHeader(String n) {
        int len=n.length();
        cluster_name=new byte[len];
        for(int i=0; i < len; i++)
            cluster_name[i]=(byte)n.charAt(i);
    }

    public TpHeader(String n, byte flag, int index) {
        int len=n.length();
        cluster_name=new byte[len];
        for(int i=0; i < len; i++)
            cluster_name[i]=(byte)n.charAt(i);
        this.flag=flag;
        this.index=index;
    }

    public TpHeader(AsciiString n) {
        cluster_name=n != null? n.chars() : null;
    }

    public TpHeader(AsciiString n, byte flag, int index) {
        cluster_name=n != null? n.chars() : null;
        this.flag=flag;
        this.index=index;
    }

    public byte[] getClusterName() {return cluster_name;}
    public byte[] clusterName()    {return cluster_name;}
    public byte   flag()           {return flag;}
    public int    index()          {return index;}


    public TpHeader(byte[] n) {
        cluster_name=n;
    }

    public Supplier<? extends Header> create() {return TpHeader::new;}

    public short getMagicId() {return 60;}

    public String toString() {
        return String.format("[cluster=%s]", cluster_name != null ? new String(cluster_name) : "null");
    }


    @Override
    public int serializedSize() {
        int retval=cluster_name != null? Global.SHORT_SIZE + cluster_name.length : Global.SHORT_SIZE;
        return retval+Byte.BYTES + (flag > 0? Integer.BYTES : 0);
    }

    @Override
    public void writeTo(DataOutput out) throws IOException {
        int length=cluster_name != null? cluster_name.length : -1;
        out.writeShort(length);
        if(cluster_name != null)
            out.write(cluster_name, 0, cluster_name.length);
        out.writeByte(flag);
        if(flag > 0)
            out.writeInt(index);
    }

    @Override
    public void readFrom(DataInput in) throws IOException {
        int len=in.readShort();
        if(len >= 0) {
            cluster_name=new byte[len];
            in.readFully(cluster_name, 0, cluster_name.length);
        }
        flag=in.readByte();
        if(flag > 0)
            index=in.readInt();
    }
}
