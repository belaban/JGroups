// $Id: NakAckHeader.java,v 1.4 2004/07/05 05:49:41 belaban Exp $

package org.jgroups.protocols.pbcast;


import org.jgroups.Header;
import org.jgroups.util.Range;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


public class NakAckHeader extends Header {
    public static final int MSG=1;  // regular msg
    public static final int XMIT_REQ=2;  // retransmit request
    public static final int XMIT_RSP=3;  // retransmit response (contains one or more messages)


    int   type=0;
    long  seqno=-1;        // seqno of regular message (MSG)
    Range range=null;      // range of msgs to be retransmitted (XMIT_REQ) or retransmitted (XMIT_RSP)


    public NakAckHeader() {
    }


    /**
     * Constructor for regular messages
     */
    public NakAckHeader(int type, long seqno) {
        this.type=type;
        this.seqno=seqno;
    }

    /**
     * Constructor for retransmit requests/responses (low and high define the range of msgs)
     */
    public NakAckHeader(int type, long low, long high) {
        this.type=type;
        range=new Range(low, high);
    }


    public long size() {
        return 512;
    }


    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(type);
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
        type=in.readInt();
        seqno=in.readLong();
        read_range=in.readBoolean();
        if(read_range) {
            range=new Range();
            range.readExternal(in);
        }
    }


    public NakAckHeader copy() {
        NakAckHeader ret=new NakAckHeader(type, seqno);
        ret.range=range;
        return ret;
    }


    public static String type2Str(int t) {
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
