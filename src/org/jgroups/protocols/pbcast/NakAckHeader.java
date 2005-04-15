// $Id: NakAckHeader.java,v 1.8 2005/04/15 12:35:42 belaban Exp $

package org.jgroups.protocols.pbcast;


import org.jgroups.Header;
import org.jgroups.util.Range;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.*;


public class NakAckHeader extends Header implements Streamable {
    public static final byte MSG=1;       // regular msg
    public static final byte XMIT_REQ=2;  // retransmit request
    public static final byte XMIT_RSP=3;  // retransmit response (contains one or more messages)


    byte  type=0;
    long  seqno=-1;        // seqno of regular message (MSG)
    Range range=null;      // range of msgs to be retransmitted (XMIT_REQ) or retransmitted (XMIT_RSP)


    public NakAckHeader() {
    }


    /**
     * Constructor for regular messages
     */
    public NakAckHeader(byte type, long seqno) {
        this.type=type;
        this.seqno=seqno;
    }

    /**
     * Constructor for retransmit requests/responses (low and high define the range of msgs)
     */
    public NakAckHeader(byte type, long low, long high) {
        this.type=type;
        range=new Range(low, high);
    }


    public long size() {
        int retval=9; // type (1 byte) = seqno (8 bytes)
        if(range != null)
            retval+=1 + 16; // presence byte, plus 2 times 8 bytes for seqno
        return retval;
    }


    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeByte(type);
        out.writeLong(seqno);
        if(range != null) {
            out.writeBoolean(true);  // wasn't here before, bad bug !
            range.writeExternal(out);
        }
        else
            out.writeBoolean(false);
    }


    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        boolean read_range;
        type=in.readByte();
        seqno=in.readLong();
        read_range=in.readBoolean();
        if(read_range) {
            range=new Range();
            range.readExternal(in);
        }
    }

    public void writeTo(DataOutputStream out) throws IOException {
        out.writeByte(type);
        out.writeLong(seqno);
        Util.writeStreamable(range, out);
    }

    public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
        type=in.readByte();
        seqno=in.readLong();
        range=(Range)Util.readStreamable(Range.class, in);
    }


    public NakAckHeader copy() {
        NakAckHeader ret=new NakAckHeader(type, seqno);
        ret.range=range;
        return ret;
    }


    public static String type2Str(byte t) {
        switch(t) {
            case MSG:
                return "MSG";
            case XMIT_REQ:
                return "XMIT_REQ";
            case XMIT_RSP:
                return "XMIT_RSP";
            default:
                return "<undefined>";
        }
    }


    public String toString() {
        StringBuffer ret=new StringBuffer();
        ret.append("[NAKACK: ").append(type2Str(type)).append(", seqno=").append(seqno);
        ret.append(", range=").append(range);
        ret.append(']');
        return ret.toString();
    }


}
