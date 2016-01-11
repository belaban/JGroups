package org.jgroups.protocols.jzookeeper;

import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Header file that is used to send all messages associated with the AbaaS protocol.
 *
 * @author Ryan Emerson
 * @since 4.0
 */
final public class CSInteractionHeader extends Header {

    public static final byte RESPONSE = 1; // A response from the Z
    public static final byte BROADCAST = 2; // Actual broadcsat of a message to anycast destinations

    private byte type = 0;
    private MessageOrderInfo messageOrderInfo = null;
    

    public static CSInteractionHeader createBoxResponse(MessageOrderInfo info) {
        return new CSInteractionHeader(RESPONSE, info);
    }

    public static CSInteractionHeader createBroadcast(MessageOrderInfo info) {
        return new CSInteractionHeader(BROADCAST, info);
    }

    public CSInteractionHeader() {
    }

    public CSInteractionHeader(byte type) {
        this.type = type;
    }

    public CSInteractionHeader(byte type, MessageOrderInfo messageOrderInfo) {
        this.type = type;
        this.messageOrderInfo = messageOrderInfo;
    }

    public byte getType() {
        return type;
    }

    public void setType(byte type) {
        this.type = type;
    }

    public MessageOrderInfo getMessageOrderInfo() {
        return messageOrderInfo;
    }

    @Override
    public int size() {
        return Global.BYTE_SIZE + (messageOrderInfo != null ? messageOrderInfo.size() : 0);
    }

    @Override
    public void writeTo(DataOutput out) throws Exception {
        out.writeByte(type);
        writeMessageOrderInfo(messageOrderInfo, out);
    }

    @Override
    public void readFrom(DataInput in) throws Exception {
        type = in.readByte();
        messageOrderInfo = readMessageOrderInfo(in);
    }

    @Override
    public String toString() {
        return "CSInteractionHeader{" +
                "type=" + type2String(type) +
                ", messageOrderInfo=" + messageOrderInfo +
                '}';
    }

    public static String type2String(int t) {
        switch(t) {
            case RESPONSE:          return "RESPONSE";
            case BROADCAST:	            return "BROADCAST";
            default:                    return "UNDEFINED(" + t + ")";
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CSInteractionHeader that = (CSInteractionHeader) o;

        if (type != that.type) return false;

        if (messageOrderInfo != null ? !messageOrderInfo.equals(that.messageOrderInfo) : that.messageOrderInfo != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) type;
        result = 31 * result + (messageOrderInfo != null ? messageOrderInfo.hashCode() : 0);
        return result;
    }

    private void writeMessageOrderInfo(MessageOrderInfo info, DataOutput out) throws Exception {
        if (info == null) {
            out.writeShort(-1);
        } else {
            out.writeShort(1);
            info.writeTo(out);
        }
    }

    private MessageOrderInfo readMessageOrderInfo(DataInput in) throws Exception {
        short length = in.readShort();
        if (length < 0) {
            return null;
        } else {
            MessageOrderInfo info = new MessageOrderInfo();
            info.readFrom(in);
            return info;
        }
    }

}