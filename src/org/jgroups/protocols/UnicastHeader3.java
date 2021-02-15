package org.jgroups.protocols;

import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.util.Bits;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.function.Supplier;

/**
 * Moved out of {@link UNICAST3} into separate class. Used to attach/remove headers (e.g. seqnos) from {@link UNICAST3}.
 * @author Bela Ban
 * @since  4.0
 */
public class UnicastHeader3 extends Header {
    public static final byte DATA             = 0;
    public static final byte ACK              = 1;
    public static final byte SEND_FIRST_SEQNO = 2;
    public static final byte XMIT_REQ         = 3; // SeqnoList of missing message is in the message's payload
    public static final byte CLOSE            = 4;

    byte    type;
    long    seqno;     // DATA and ACK
    short   conn_id;   // DATA and CLOSE
    boolean first;     // DATA
    int     timestamp; // SEND_FIRST_SEQNO and ACK


    public UnicastHeader3() {} // used for externalization

    protected UnicastHeader3(byte type) {
        this.type=type;
    }

    protected UnicastHeader3(byte type, long seqno) {
        this.type=type;
        this.seqno=seqno;
    }

    protected UnicastHeader3(byte type, long seqno, short conn_id, boolean first) {
        this.type=type;
        this.seqno=seqno;
        this.conn_id=conn_id;
        this.first=first;
    }
    public short getMagicId() {return 82;}
    public Supplier<? extends Header> create() {return UnicastHeader3::new;}

    public static UnicastHeader3 createDataHeader(long seqno, short conn_id, boolean first) {
        return new UnicastHeader3(DATA, seqno, conn_id, first);
    }

    public static UnicastHeader3 createAckHeader(long seqno, short conn_id, int timestamp) {
        return new UnicastHeader3(ACK, seqno, conn_id, false).timestamp(timestamp);
    }

    public static UnicastHeader3 createSendFirstSeqnoHeader(int timestamp) {
        return new UnicastHeader3(SEND_FIRST_SEQNO).timestamp(timestamp);
    }

    public static UnicastHeader3 createXmitReqHeader() {
        return new UnicastHeader3(XMIT_REQ);
    }

    public static UnicastHeader3 createCloseHeader(short conn_id) {
        return new UnicastHeader3(CLOSE, 0, conn_id, false);
    }

    public byte           type()             {return type;}
    public long           seqno()            {return seqno;}
    public short          connId()           {return conn_id;}
    public boolean        first()            {return first;}
    public int            timestamp()        {return timestamp;}
    public UnicastHeader3 timestamp(int ts) {timestamp=ts; return this;}

    public String toString() {
        StringBuilder sb=new StringBuilder();
        sb.append(type2Str(type)).append(", seqno=").append(seqno);
        if(conn_id != 0) sb.append(", conn_id=").append(conn_id);
        if(first) sb.append(", first");
        if(timestamp != 0)
            sb.append(", ts=").append(timestamp);
        return sb.toString();
    }

    public static String type2Str(byte t) {
        switch(t) {
            case DATA:             return "DATA";
            case ACK:              return "ACK";
            case SEND_FIRST_SEQNO: return "SEND_FIRST_SEQNO";
            case XMIT_REQ:         return "XMIT_REQ";
            case CLOSE:            return "CLOSE";
            default:               return "<unknown>";
        }
    }

    public final int serializedSize() {
        int retval=Global.BYTE_SIZE;     // type
        switch(type) {
            case DATA:
                retval+=Bits.size(seqno) // seqno
                  + Global.SHORT_SIZE    // conn_id
                  + Global.BYTE_SIZE;    // first
                break;
            case ACK:
                retval+=Bits.size(seqno)
                  + Global.SHORT_SIZE    // conn_id
                  + Bits.size(timestamp);
                break;
            case SEND_FIRST_SEQNO:
                retval+=Bits.size(timestamp);
                break;
            case XMIT_REQ:
                break;
            case CLOSE:
                retval+=Global.SHORT_SIZE; // conn-id
                break;
        }
        return retval;
    }

    public UnicastHeader3 copy() {
        return new UnicastHeader3(type, seqno, conn_id, first);
    }

    /**
     * The following types and fields are serialized:
     * <pre>
     * | DATA | seqno | conn_id | first |
     * | ACK  | seqno | timestamp |
     * | SEND_FIRST_SEQNO | timestamp |
     * | CLOSE | conn_id |
     * </pre>
     */
    @Override
    public void writeTo(DataOutput out) throws IOException {
        out.writeByte(type);
        switch(type) {
            case DATA:
                Bits.writeLongCompressed(seqno, out);
                out.writeShort(conn_id);
                out.writeBoolean(first);
                break;
            case ACK:
                Bits.writeLongCompressed(seqno, out);
                out.writeShort(conn_id);
                Bits.writeIntCompressed(timestamp, out);
                break;
            case SEND_FIRST_SEQNO:
                Bits.writeIntCompressed(timestamp, out);
                break;
            case XMIT_REQ:
                break;
            case CLOSE:
                out.writeShort(conn_id);
                break;
        }
    }

    @Override
    public void readFrom(DataInput in) throws IOException {
        type=in.readByte();
        switch(type) {
            case DATA:
                seqno=Bits.readLongCompressed(in);
                conn_id=in.readShort();
                first=in.readBoolean();
                break;
            case ACK:
                seqno=Bits.readLongCompressed(in);
                conn_id=in.readShort();
                timestamp=Bits.readIntCompressed(in);
                break;
            case SEND_FIRST_SEQNO:
                timestamp=Bits.readIntCompressed(in);
                break;
            case XMIT_REQ:
                break;
            case CLOSE:
                conn_id=in.readShort();
                break;
        }
    }
}
