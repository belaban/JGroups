package org.jgroups.protocols.jzookeeper;

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
public class MessageOrderInfo implements Comparable<MessageOrderInfo>, SizeStreamable, Externalizable, Cloneable, Streamable {
    private static final long serialVersionUID = 8788015472325344611L;
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
        return (id != null ? id.serializedSize(): 0) + Bits.size(ordering) + longArraySize(clientsLastOrder) + (destinations != null ? Util.size(destinations) : 0);
      }

    @Override
    public void writeTo(DataOutput out) throws Exception {
        System.out.println("writeTo start");
        writeMessageId(id, out);
        System.out.println("after id");
        Bits.writeLong(ordering, out);
        System.out.println("after ordering");
        writeLongArray(clientsLastOrder, out);
        System.out.println("after clientsLast");
        Util.writeByteBuffer(destinations, out);
        System.out.println("after destinations");
        System.out.println("writeTo Done");
    }

    @Override
    public void readFrom(DataInput in) throws Exception {
        System.out.println("readFrom starting");
        id = readMessageId(in);
        System.out.println("after id");
        ordering = Bits.readLong(in);
        System.out.println("after ordering");
        clientsLastOrder = readLongArray(in);
        System.out.println("after clientsLast");
        destinations = Util.readByteBuffer(in);
        System.out.println("after destinations");
        System.out.println("readFrom Done");

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
    
    @Override
    public void writeExternal(ObjectOutput objectOutput) throws IOException {
        try {
            writeTo(objectOutput);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public void readExternal(ObjectInput objectInput) throws IOException, ClassNotFoundException {
        try {
            readFrom(objectInput);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
}