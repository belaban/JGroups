package org.jgroups.protocols.pmcast.header;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.protocols.pmcast.MessageID;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * The header for the Total Order Multicast protocol
 * 
 * @author Pedro Ruivo
 * @since 3.1
 */
public class GroupMulticastHeader extends Header {

    //type
    public static final byte DATA_MESSAGE = 1;
    public static final byte PROPOSE_MESSAGE = 1 << 1;
    public static final byte FINAL_MESSAGE = 1 << 2;

    private byte type = 0;
    private MessageID messageID; //address and sequence number
    private long sequencerNumber;
    private Set<Address> destination = new HashSet<Address>();

    public GroupMulticastHeader() {
        messageID = new MessageID();
    }

    public MessageID getMessageID() {
        return messageID;
    }

    public Address getOrigin() {
        return messageID.getAddress();
    }

    public void addDestinations(Collection<Address> addresses) {
        destination.addAll(addresses);
    }

    public Set<Address> getDestinations() {
        return destination;
    }

    public long getSequencerNumber() {
        return sequencerNumber;
    }

    public void setSequencerNumber(long sequencerNumber) {
        this.sequencerNumber = sequencerNumber;
    }

    public byte getType() {
        return type;
    }

    @Override
    public int size() {
        return (int) (Global.BYTE_SIZE  + messageID.serializedSize() + Util.size(sequencerNumber) +
                Util.size(destination));
    }

    @Override
    public void writeTo(DataOutput out) throws Exception {
        out.writeByte(type);
        messageID.writeTo(out);
        Util.writeLong(sequencerNumber, out);
        Util.writeAddresses(destination, out);
    }

    @Override
    public void readFrom(DataInput in) throws Exception {
        type = in.readByte();
        messageID.readFrom(in);
        sequencerNumber = Util.readLong(in);
        destination = (Set<Address>) Util.readAddresses(in, HashSet.class);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("GroupMulticastHeader{")
                .append("type=").append(type2String(type))
                .append(", message_id=").append(messageID)
                .append(", sequence_number=").append(sequencerNumber)
                .append(", destinations=").append(destination)
                .append("}");
        return sb.toString();
    }

    public static String type2String(byte type) {
        switch(type) {
            case DATA_MESSAGE: return "DATA_MESSAGE";
            case PROPOSE_MESSAGE: return "PROPOSE_MESSAGE";
            case FINAL_MESSAGE: return"FINAL_MESSAGE";
            default: return "UNKNOWN";
        }
    }

    public static GroupMulticastHeader createNewHeader(byte type, MessageID messageID) {
        if (messageID == null) {
            throw new NullPointerException("The message ID can't be null");
        }
        GroupMulticastHeader header = new GroupMulticastHeader();
        header.setType(type);
        header.setMessageID(messageID);
        return header;
    }

    private void setType(byte type) {
        this.type = type;
    }

    private void setMessageID(MessageID messageID) {
        this.messageID = messageID;
    }
}

