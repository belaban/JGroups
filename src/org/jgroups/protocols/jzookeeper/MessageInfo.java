package org.jgroups.protocols.jzookeeper;

import org.jgroups.ViewId;
import org.jgroups.util.Bits;
import org.jgroups.util.SizeStreamable;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.Arrays;

/**
 * Message Information that is associated with AbaaS requests and responses.
 *
 * @author Ryan Emerson
 * @since 4.0
 */
public class MessageInfo implements Comparable<MessageInfo>, SizeStreamable {
    private MessageID id = null;
    private long ordering = -1; // Sequence provided by the BOX, value created after TOA and before placed in the queue

    public MessageInfo() {
    }

    public MessageInfo(MessageID id) {
    	this.id = id;
        
    }

    public MessageID getId() {
        return id;
    }
    public void setId(MessageID id) {
        this.id = id;
    }

    public long getOrdering() {
        return ordering;
    }

    public void setOrdering(long ordering) {
        this.ordering = ordering;
    }

   
    @Override
    public int size() {
        return id.size() + Bits.size(ordering);
    }

    @Override
    public void writeTo(DataOutput out) throws Exception {
        writeMessageId(id, out);
        Bits.writeLong(ordering, out);
       
    }

    @Override
    public void readFrom(DataInput in) throws Exception {
        id = readMessageId(in);
        ordering = Bits.readLong(in);
    
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MessageInfo that = (MessageInfo) o;

        if (ordering != that.ordering) return false;
        if (id != null ? !id.equals(that.id) : that.id != null) return false;
        return true;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (int) (ordering ^ (ordering >>> 32));
        return result;
    }

    @Override
    public int compareTo(MessageInfo other) {
        if (this.equals(other))
            return 0;
        else if (ordering > other.ordering)
            return 1;
        else
            return -1;
    }

    @Override
    public String toString() {
        return "MessageInfo{" +
                "id=" + id +
                ", ordering=" + ordering +
                '}';
    }

    private void writeMessageId(MessageID id, DataOutput out) throws Exception {
        if (id == null) {
            out.writeShort(-1);
        } else {
            out.writeShort(1);
            id.writeTo(out);
        }
    }

    private MessageID readMessageId(DataInput in) throws Exception {
        short length = in.readShort();
        if (length < 0) {
            return null;
        } else {
            MessageID id = new MessageID();
            id.readFrom(in);
            return id;
        }
    }

    private int longArraySize(long[] array) {
        int total = 0;
        for (long l : array)
            total += Bits.size(l);
        return total;
    }

    private void writeLongArray(long[] array, DataOutput out) throws Exception {
        if(array != null) {
            out.writeShort(array.length);
            for (long l : array)
                Bits.writeLong(l, out);
        } else {
            out.writeShort(-1);
        }
    }

    private long[] readLongArray(DataInput in) throws Exception {
        short length = in.readShort();
        if (length < 0) {
            return null;
        } else {
            long[] array = new long[length];
            for (int i = 0; i < length; i++)
                array[i] = Bits.readLong(in);
            return array;
        }
    }
}