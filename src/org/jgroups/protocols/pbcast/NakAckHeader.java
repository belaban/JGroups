
package org.jgroups.protocols.pbcast;


import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.AbstractHeader;
import org.jgroups.util.Bits;
import org.jgroups.util.Range;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;


/**
 * @author Bela Ban
 */
public class NakAckHeader extends AbstractHeader {
    public static final byte MSG=1;       // regular msg
    public static final byte XMIT_REQ=2;  // retransmit request
    public static final byte XMIT_RSP=3;  // retransmit response (contains one or more messages)

    byte  type=0;
    long  seqno=-1;        // seqno of regular message (MSG)
    Range range=null;      // range of msgs to be retransmitted (XMIT_REQ)
    Address sender;        // the original sender of the message (for XMIT_REQ)


    public NakAckHeader() {
    }


    public static NakAckHeader createMessageHeader(long seqno) {
        return new NakAckHeader(MSG, seqno);
    }

    public static NakAckHeader createXmitRequestHeader(long low, long high, Address orginal_sender) {
        return new NakAckHeader(XMIT_REQ, low, high, orginal_sender);
    }

    public static NakAckHeader createXmitResponseHeader() {
        return new NakAckHeader(XMIT_RSP, -1);
    }


    /**
     * Constructor for regular messages or XMIT responses
     */
    private NakAckHeader(byte type, long seqno) {
        this.type=type;
        this.seqno=seqno;
    }


    /**
     * Constructor for retransmit requests (XMIT_REQs) (low and high define the range of msgs)
     */
    private NakAckHeader(byte type, long low, long high, Address sender) {
        this.type=type;
        range=new Range(low, high);
        this.sender=sender;
    }

    public byte getType() {
        return type;
    }

    public long getSeqno() {
        return seqno;
    }

    public Range getRange() {
        return range;
    }

    public Address getSender() {
        return sender;
    }


    public void writeTo(DataOutput out) throws Exception {
        out.writeByte(type);
        switch(type) {
            case MSG:
            case XMIT_RSP:
                Bits.writeLong(seqno,out);
                break;
            case XMIT_REQ:
                Util.writeStreamable(range, out);
                Util.writeAddress(sender, out);
                break;
        }
    }

    public void readFrom(DataInput in) throws Exception {
        type=in.readByte();
        switch(type) {
            case MSG:
            case XMIT_RSP:
                seqno=Bits.readLong(in);
                break;
            case XMIT_REQ:
                range=(Range)Util.readStreamable(Range.class, in);
                sender=Util.readAddress(in);
                break;
        }
    }
    

    public int size() {
        int retval=Global.BYTE_SIZE; // type
        switch(type) {
            case MSG:
            case XMIT_RSP:
                return retval + Bits.size(seqno);

            case XMIT_REQ:
                retval+=Global.BYTE_SIZE; // presence for range
                if(range != null)
                    retval+=range.serializedSize();
                retval+=Util.size(sender);
                return retval;
        }
        return retval;
    }


    public NakAckHeader copy() {
        NakAckHeader ret=new NakAckHeader();
        ret.type=type;
        ret.seqno=seqno;
        ret.range=range;
        ret.sender=sender;
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
        StringBuilder ret=new StringBuilder();
        ret.append("[").append(type2Str(type));
        switch(type) {
            case MSG:
            case XMIT_RSP: // seqno and sender
                ret.append(", seqno=").append(seqno);
                break;
            case XMIT_REQ:  // range and sender
                if(range != null)
                    ret.append(", range=" + range);
                break;
        }

        if(sender != null) ret.append(", sender=").append(sender);
        ret.append(']');
        return ret.toString();
    }


}
