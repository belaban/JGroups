
package org.jgroups.protocols.pbcast;


import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.util.Range;
import org.jgroups.util.Util;

import java.io.*;


/**
 * @author Bela Ban
 * @version $Id: NakAckHeader.java,v 1.25 2010/03/08 07:59:39 belaban Exp $
 */
public class NakAckHeader extends Header {
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


    public void writeTo(DataOutputStream out) throws IOException {
        out.writeByte(type);
        switch(type) {
            case MSG:
                out.writeLong(seqno);
                break;
            case XMIT_REQ:
                Util.writeStreamable(range, out);
                Util.writeAddress(sender, out);
                break;
            case XMIT_RSP:
                break;
        }
    }

    public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
        type=in.readByte();
        switch(type) {
            case MSG:
                seqno=in.readLong();
                break;
            case XMIT_REQ:
                range=(Range)Util.readStreamable(Range.class, in);
                sender=Util.readAddress(in);
                break;
            case XMIT_RSP:
                break;
        }
    }
    

    public int size() {
        int retval=Global.BYTE_SIZE; // type
        switch(type) {
            case MSG:
                return retval + Global.LONG_SIZE; // seqno

            case XMIT_REQ:
                retval+=Global.BYTE_SIZE; // presence for range
                if(range != null)
                    retval+=2 * Global.LONG_SIZE; // 2 times 8 bytes for seqno
                retval+=Util.size(sender);
                return retval;

            case XMIT_RSP:
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
            case MSG:           // seqno and sender
                ret.append(", seqno=").append(seqno);
                break;
            case XMIT_REQ:  // range and sender
                if(range != null)
                    ret.append(", range=" + range);
                break;
            case XMIT_RSP:  // seqno and sender
                break;
        }

        if(sender != null) ret.append(", sender=").append(sender);
        ret.append(']');
        return ret.toString();
    }


}
