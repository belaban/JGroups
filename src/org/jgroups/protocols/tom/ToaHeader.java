package org.jgroups.protocols.tom;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.ArrayList;
import java.util.Collection;

/**
 * The header for the Total Order Anycast (TOA) protocol
 * 
 * @author Pedro Ruivo
 * @since 3.1
 */
public class ToaHeader extends Header {

    //type
    public static final byte DATA_MESSAGE                = 1 << 0;
    public static final byte PROPOSE_MESSAGE             = 1 << 1;
    public static final byte FINAL_MESSAGE               = 1 << 2;
    public static final byte SINGLE_DESTINATION_MESSAGE  = 1 << 3;

    private byte type = 0;
    private MessageID messageID; //address and sequence number
    private long sequencerNumber;
    private Collection<Address> destinations= new ArrayList<Address>();

    public ToaHeader() {
        messageID = new MessageID();
    }

    public MessageID getMessageID() {
        return messageID;
    }

    public Address getOrigin() {
        return messageID.getAddress();
    }

    public void addDestinations(Collection<Address> addresses) {
        if(addresses != null && !addresses.isEmpty())
            for(Address address: addresses)
                if(!destinations.contains(address))
                    destinations.add(address);
    }

    public Collection<Address> getDestinations() {
        return destinations;
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
                Util.size(destinations));
    }

    @Override
    public void writeTo(DataOutput out) throws Exception {
        out.writeByte(type);
        messageID.writeTo(out);
        Util.writeLong(sequencerNumber, out);
        Util.writeAddresses(destinations, out);
    }

    @Override
    public void readFrom(DataInput in) throws Exception {
        type = in.readByte();
        messageID.readFrom(in);
        sequencerNumber = Util.readLong(in);
        destinations= (Collection<Address>) Util.readAddresses(in, ArrayList.class);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ToaHeader [")
                .append("type=").append(type2String(type))
                .append(", message_id=").append(messageID)
                .append(", sequence_number=").append(sequencerNumber)
                .append(", destinations=").append(destinations)
                .append("]");
        return sb.toString();
    }

    public static String type2String(byte type) {
        switch(type) {
            case DATA_MESSAGE: return "DATA_MESSAGE";
            case PROPOSE_MESSAGE: return "PROPOSE_MESSAGE";
            case FINAL_MESSAGE: return "FINAL_MESSAGE";
            case SINGLE_DESTINATION_MESSAGE: return "SINGLE_DESTINATION_MESSAGE";
            default: return "UNKNOWN";
        }
    }

    public static ToaHeader createNewHeader(byte type, MessageID messageID) {
        if (messageID == null) {
            throw new NullPointerException("The message ID can't be null");
        }
        ToaHeader header = new ToaHeader();
        header.setType(type);
        header.setMessageID(messageID);
        return header;
    }

   public static ToaHeader createSingleDestinationHeader() {      
      ToaHeader header = new ToaHeader();
      header.setType(SINGLE_DESTINATION_MESSAGE);      
      return header;
   }

    private void setType(byte type) {
        this.type = type;
    }

    private void setMessageID(MessageID messageID) {
        this.messageID = messageID;
    }
}

