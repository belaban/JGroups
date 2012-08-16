package org.jgroups.protocols.pbcast;


import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;


/**
 * @author Bela Ban
 */
public class NakAckHeader2 extends Header {
    public static final byte MSG=1;       // regular msg
    public static final byte XMIT_REQ=2;  // retransmit request
    public static final byte XMIT_RSP=3;  // retransmit response (contains one or more messages)

    byte      type=0;
    long      seqno=-1;        // seqno of regular message (MSG)
    Address   sender;          // the original sender of the message (for XMIT_REQ)


    public NakAckHeader2() {
    }


    public static NakAckHeader2 createMessageHeader(long seqno) {
        return new NakAckHeader2(MSG, seqno);
    }

    public static NakAckHeader2 createXmitRequestHeader(Address orginal_sender) {
        return new NakAckHeader2(XMIT_REQ, orginal_sender);
    }

    public static NakAckHeader2 createXmitResponseHeader() {
        return new NakAckHeader2(XMIT_RSP, -1);
    }


    /**
     * Constructor for regular messages or XMIT responses
     */
    private NakAckHeader2(byte type, long seqno) {
        this.type=type;
        this.seqno=seqno;
    }


    /**
     * Constructor for retransmit requests (XMIT_REQs) (low and high define the range of msgs)
     */
    private NakAckHeader2(byte type, Address sender) {
        this.type=type;
        this.sender=sender;
    }

    public byte      getType()    {return type;}
    public long      getSeqno()   {return seqno;}
    public Address   getSender()  {return sender;}


    public void writeTo(DataOutput out) throws Exception {
        out.writeByte(type);
        switch(type) {
            case MSG:
            case XMIT_RSP:
                Util.writeLong(seqno, out);
                break;
            case XMIT_REQ:
                Util.writeAddress(sender, out);
                break;
        }
    }

    public void readFrom(DataInput in) throws Exception {
        type=in.readByte();
        switch(type) {
            case MSG:
            case XMIT_RSP:
                seqno=Util.readLong(in);
                break;
            case XMIT_REQ:
                sender=Util.readAddress(in);
                break;
        }
    }
    

    public int size() {
        int retval=Global.BYTE_SIZE; // type
        switch(type) {
            case MSG:
            case XMIT_RSP:
                return retval + Util.size(seqno);

            case XMIT_REQ:
                retval+=Util.size(sender);
                return retval;
        }
        return retval;
    }


    public NakAckHeader2 copy() {
        NakAckHeader2 ret=new NakAckHeader2();
        ret.type=type;
        ret.seqno=seqno;
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
                break;
        }

        if(sender != null) ret.append(", sender=").append(sender);
        ret.append(']');
        return ret.toString();
    }


}
