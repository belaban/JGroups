package org.jgroups.protocols.tom;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.Objects;
import java.util.function.Supplier;

import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.util.Bits;

/**
 * The header for the Total Order Anycast (TOA) protocol
 *
 * @author Pedro Ruivo
 * @since 3.1
 */
public class ToaHeader extends Header {

    //type
    public static final byte DATA_MESSAGE = 1 << 0;
    public static final byte PROPOSE_MESSAGE = 1 << 1;
    public static final byte FINAL_MESSAGE = 1 << 2;
    public static final byte SINGLE_DESTINATION_MESSAGE = 1 << 3;

    private byte type = 0;
    private MessageID messageID; //address and sequence number
    private long sequencerNumber;
    private long viewId;

    public ToaHeader() {
    }

    private ToaHeader(MessageID messageID, byte type) {
        this.messageID = messageID;
        this.type = type;
    }
    public short getMagicId() {return 79;}
    public Supplier<? extends Header> create() {return ToaHeader::new;}

    public MessageID getMessageID() {
        return messageID;
    }

    public long getSequencerNumber() {
        return sequencerNumber;
    }

    public ToaHeader setSequencerNumber(long sequencerNumber) {
        this.sequencerNumber = sequencerNumber;
        return this;
    }

    public byte getType() {
        return type;
    }

    @Override
    public int serializedSize() {
        return Global.BYTE_SIZE + messageID.serializedSize() + Bits.size(sequencerNumber) + Bits.size(viewId);
    }

    @Override
    public void writeTo(DataOutput out) throws Exception {
        out.writeByte(type);
        messageID.writeTo(out);
        Bits.writeLong(sequencerNumber, out);
        Bits.writeLong(viewId, out);
    }

    @Override
    public void readFrom(DataInput in) throws Exception {
        type = in.readByte();
        messageID = new MessageID();
        messageID.readFrom(in);
        sequencerNumber = Bits.readLong(in);
        viewId = Bits.readLong(in);
    }

    @Override
    public String toString() {
        return "ToaHeader{" +
                "type=" + type2String(type) +
                ", message_id=" + messageID +
                ", sequence_number=" + sequencerNumber +
                ", view_id=" + viewId +
                '}';
    }

    private static String type2String(byte type) {
        switch (type) {
            case DATA_MESSAGE:
                return "DATA_MESSAGE";
            case PROPOSE_MESSAGE:
                return "PROPOSE_MESSAGE";
            case FINAL_MESSAGE:
                return "FINAL_MESSAGE";
            case SINGLE_DESTINATION_MESSAGE:
                return "SINGLE_DESTINATION_MESSAGE";
            default:
                return "UNKNOWN";
        }
    }

    public static ToaHeader newDataMessageHeader(MessageID messageID, long viewId) {
        assertMessageIDNotNull(messageID);
        return new ToaHeader(messageID, DATA_MESSAGE).setViewId(viewId);
    }

    public static ToaHeader newProposeMessageHeader(MessageID messageID, long sequencerNumber) {
        assertMessageIDNotNull(messageID);
        return new ToaHeader(messageID, PROPOSE_MESSAGE).setSequencerNumber(sequencerNumber);
    }

    public static ToaHeader newFinalMessageHeader(MessageID messageID, long sequenceNumber) {
        assertMessageIDNotNull(messageID);
        return new ToaHeader(messageID, FINAL_MESSAGE).setSequencerNumber(sequenceNumber);
    }

    public static ToaHeader createSingleDestinationHeader(MessageID messageID) {
        return new ToaHeader(messageID, SINGLE_DESTINATION_MESSAGE);
    }

    private static void assertMessageIDNotNull(MessageID messageID) {
        Objects.requireNonNull(messageID, "The message ID can't be null.");
    }

    public long getViewId() {
        return viewId;
    }

    private ToaHeader setViewId(long viewId) {
        this.viewId = viewId;
        return this;
    }
}

