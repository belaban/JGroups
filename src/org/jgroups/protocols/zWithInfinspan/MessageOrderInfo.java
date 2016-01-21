package org.jgroups.protocols.zWithInfinspan;

import org.jgroups.util.Bits;
import org.jgroups.util.SizeStreamable;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;

/**
 * Message Information that is associated with SCInteraction responses.
 *
 * @author Ryan Emerson and Ibrahim EL-Sanosi
 * @since 4.0
 */
public class MessageOrderInfo implements Comparable<MessageOrderInfo>, SizeStreamable {
    private MessageId id = null;
    private long ordering = -1; // Sequence provided by the Zab*** to emulated Infinspan clients.
    private long[] clientsLastOrder = new long[0];
    private byte[] destinations = new byte[0];

    public MessageOrderInfo() {
    }
    
    public MessageOrderInfo(long ordering) {
 	   this.ordering = ordering;
     }

    public MessageOrderInfo(MessageId id) {
    	   this.id = id;
    }
    public MessageOrderInfo(MessageId id, byte[] destinations) {
        this(id, -1, destinations);
    }

    public MessageOrderInfo(MessageId id, long ordering, byte[] destinations) {
        this.id = id;
        this.ordering = ordering;
        this.destinations = destinations;
    }
    
    public MessageOrderInfo(MessageId id, long ordering, byte[] destinations, long[] clientsLastOrder) {
        this.id = id;
        this.ordering = ordering;
        this.destinations = destinations;
        System.out.println(" In public MessageInfoEss " + clientsLastOrder);
        this.clientsLastOrder = clientsLastOrder;

    }

    public MessageId getId() {
        return id;
    }

    public void setId(MessageId id) {
        this.id = id;
    }

    public long getOrdering() {
        return ordering;
    }

    public void setOrdering(long ordering) {
        this.ordering = ordering;
    }

    public long[] getclientsLastOrder() {
        return clientsLastOrder;
    }

    public void setclientsLastOrder(long[] clientsLastOrder) {
        this.clientsLastOrder = clientsLastOrder;
    }

    public byte[] getDestinations() {
        return destinations;
    }

    public void setDestinations(byte[] destinations) {
        this.destinations = destinations;
    }

    @Override
    public int size() {
        return (id != null ? id.size(): 0) + Bits.size(ordering) + longArraySize(clientsLastOrder) + (destinations != null ? Util.size(destinations) : 0);
      }

    @Override
    public void writeTo(DataOutput out) throws Exception {
        writeMessageId(id, out);
        Bits.writeLong(ordering, out);
        writeLongArray(clientsLastOrder, out);
        Util.writeByteBuffer(destinations, out);
    }

    @Override
    public void readFrom(DataInput in) throws Exception {
        id = readMessageId(in);
        ordering = Bits.readLong(in);
        clientsLastOrder = readLongArray(in);
        destinations = Util.readByteBuffer(in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MessageOrderInfo that = (MessageOrderInfo) o;
        if (ordering != that.ordering) return false;
        if (!Arrays.equals(destinations, that.destinations)) return false;
        if (id != null ? !id.equals(that.id) : that.id != null) return false;
        if (!Arrays.equals(clientsLastOrder, that.clientsLastOrder)) return false;
        return true;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (int) (ordering ^ (ordering >>> 32));
        result = 31 * result + (clientsLastOrder != null ? Arrays.hashCode(clientsLastOrder) : 0);
        result = 31 * result + (destinations != null ? Arrays.hashCode(destinations) : 0);
        return result;
    }

    @Override
    public int compareTo(MessageOrderInfo other) {
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
                ", clientsLastOrder=" + Arrays.toString(clientsLastOrder) +
                ", destinations=" + Arrays.toString(destinations) +
                '}';
    }

    private void writeMessageId(MessageId id, DataOutput out) throws Exception {
        if (id == null) {
            out.writeShort(-1);
        } else {
            out.writeShort(1);
            id.writeTo(out);
        }
    }

    private MessageId readMessageId(DataInput in) throws Exception {
        short length = in.readShort();
        if (length < 0) {
            return null;
        } else {
            MessageId id = new MessageId();
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