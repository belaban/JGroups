package org.jgroups.protocols;


import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.util.Bits;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.function.Supplier;


/**
 * Header used by {@link org.jgroups.protocols.NAKACK3} and {@link org.jgroups.protocols.NAKACK4}
 * @author Bela Ban
 * @since 5.4
 */
public class NakAckHeader extends Header {
    public static final byte MSG           = 1;  // regular msg
    public static final byte XMIT_REQ      = 2;  // retransmit request
    public static final byte XMIT_RSP      = 3;  // retransmit response (contains one or more messages)
    public static final byte HIGHEST_SEQNO = 4;  // the highest sent seqno
    public static final byte ACK           = 5;  // ack of the highest delivered seqno (send from receiver->sender)

    protected byte           type;
    protected long           seqno=-1;        // seqno of regular message (MSG, HIGHEST_SEQNO)
    protected Address        sender;          // the original sender of the message (for XMIT_REQ)


    public NakAckHeader() {
    }
    public short getMagicId() {return 99;}
    @Override public Supplier<? extends Header> create() {
        return NakAckHeader::new;
    }

    public static NakAckHeader createMessageHeader(long seqno) {
        return new NakAckHeader(MSG, seqno);
    }

    public static NakAckHeader createXmitRequestHeader(Address orginal_sender) {
        return new NakAckHeader(XMIT_REQ, orginal_sender);
    }

    public static NakAckHeader createXmitResponseHeader() {
        return new NakAckHeader(XMIT_RSP, -1);
    }

    public static NakAckHeader createHighestSeqnoHeader(long seqno) {return new NakAckHeader(HIGHEST_SEQNO, seqno);}

    public static NakAckHeader createAckHeader(long ack) {return new NakAckHeader(ACK, ack);}


    /** Constructor for regular messages or XMIT responses */
    private NakAckHeader(byte type, long seqno) {
        this.type=type;
        this.seqno=seqno;
    }


    /** Constructor for retransmit requests (XMIT_REQs) (low and high define the range of msgs) */
    private NakAckHeader(byte type, Address sender) {
        this.type=type;
        this.sender=sender;
    }

    public byte      getType()    {return type;}
    public long      getSeqno()   {return seqno;}
    public Address   getSender()  {return sender;}

    @Override
    public void writeTo(DataOutput out) throws IOException {
        out.writeByte(type);
        switch(type) {
            case MSG:
            case XMIT_RSP:
            case HIGHEST_SEQNO:
            case ACK:
                Bits.writeLongCompressed(seqno, out);
                break;
            case XMIT_REQ:
                Util.writeAddress(sender, out);
                break;
        }
    }

    @Override
    public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        type=in.readByte();
        switch(type) {
            case MSG:
            case XMIT_RSP:
            case HIGHEST_SEQNO:
            case ACK:
                seqno=Bits.readLongCompressed(in);
                break;
            case XMIT_REQ:
                sender=Util.readAddress(in);
                break;
        }
    }

    @Override
    public int serializedSize() {
        int retval=Global.BYTE_SIZE; // type
        switch(type) {
            case MSG:
            case XMIT_RSP:
            case HIGHEST_SEQNO:
            case ACK:
                return retval + Bits.size(seqno);

            case XMIT_REQ:
                retval+=Util.size(sender);
                return retval;
        }
        return retval;
    }


    public NakAckHeader copy() {
        NakAckHeader ret=new NakAckHeader();
        ret.type=type;
        ret.seqno=seqno;
        ret.sender=sender;
        return ret;
    }


    public static String type2Str(byte t) {
        switch(t) {
            case MSG:           return "MSG";
            case XMIT_REQ:      return "XMIT_REQ";
            case XMIT_RSP:      return "XMIT_RSP";
            case HIGHEST_SEQNO: return "HIGHEST_SEQNO";
            case ACK:           return "ACK";
            default:            return "<undefined>";
        }
    }


    public String toString() {
        StringBuilder ret=new StringBuilder();
        ret.append(type2Str(type));
        switch(type) {
            case MSG:
            case XMIT_RSP: // seqno and sender
            case HIGHEST_SEQNO:
            case ACK:
                ret.append(", seqno=").append(seqno);
                break;
            case XMIT_REQ:  // range and sender
                break;
        }

        if(sender != null) ret.append(", sender=").append(sender);
        return ret.toString();
    }


}
