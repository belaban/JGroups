package org.jgroups.protocols.tom;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.util.Bits;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Supplier;

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
    private Collection<Address> destinations;

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

    private ToaHeader setDestinations(Collection<Address> addresses) {
        this.destinations = addresses;
        return this;
    }

    public Collection<Address> getDestinations() {
        return Collections.unmodifiableCollection(destinations);
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
        return (int) (Global.BYTE_SIZE + messageID.serializedSize() + Bits.size(sequencerNumber) +
                Util.size(destinations));
    }

    @Override
    public void writeTo(DataOutput out) throws IOException {
        out.writeByte(type);
        messageID.writeTo(out);
        Bits.writeLong(sequencerNumber, out);
        if (type == DATA_MESSAGE) {
            Util.writeAddresses(destinations, out);
        }
    }

    @Override
    public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        type = in.readByte();
        messageID = new MessageID();
        messageID.readFrom(in);
        sequencerNumber = Bits.readLong(in);
        if (type == DATA_MESSAGE) {
            destinations = Util.readAddresses(in, ArrayList::new);
        }
    }

    @Override
    public String toString() {
        return "ToaHeader{" +
                "type=" + type2String(type) +
                ", message_id=" + messageID +
                ", sequence_number=" + sequencerNumber +
                ", destinations=" + destinations +
                '}';
    }

    public static String type2String(byte type) {
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

    public static ToaHeader newDataMessageHeader(MessageID messageID, Collection<Address> destinations) {
        assertMessageIDNotNull(messageID);
        return new ToaHeader(messageID, DATA_MESSAGE).setDestinations(new ArrayList<>(destinations));
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
        if (messageID == null) {
            throw new NullPointerException("The message ID can't be null.");
        }
    }
}

