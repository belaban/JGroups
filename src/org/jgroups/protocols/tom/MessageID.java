package org.jgroups.protocols.tom;

import org.jgroups.Address;
import org.jgroups.util.Bits;
import org.jgroups.util.SizeStreamable;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;


/**
 * The represents an unique identifier for the messages processed by the Total Order Anycast protocol
 * <p/>
 * Note: it is similar to the ViewId (address + counter)
 *
 * @author Pedro Ruivo
 * @since 3.1
 */
public class MessageID implements Comparable<MessageID>, Cloneable, SizeStreamable {
    private Address address = null;
    private long id = -1;

    public MessageID() {}

    public MessageID(Address address, long id) {
        this.address = address;
        this.id = id;
    }

    @Override
    public int compareTo(MessageID other) {
        if (other == null) {
            throw new NullPointerException();
        }

        return id == other.id ? this.address.compareTo(other.address) :
                id < other.id ? -1 : 1;
    }


    public Address getAddress() {
        return address;
    }

    @Override
    public String toString() {
        return "MessageID{" + address + ":" + id + "}";
    }

    public Object clone() {
        try {
            MessageID dolly = (MessageID) super.clone();
            dolly.address = address;
            return dolly;
        } catch (CloneNotSupportedException e) {
            throw new IllegalStateException();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MessageID messageID = (MessageID) o;

        return id == messageID.id && Objects.equals(address, messageID.address);

    }

    @Override
    public int hashCode() {
        int result = address != null ? address.hashCode() : 0;
        result = 31 * result + (int) (id ^ (id >>> 32));
        return result;
    }

    @Override
    public int serializedSize() {
        return Bits.size(id) + Util.size(address);
    }

    @Override
    public void writeTo(DataOutput out) throws IOException {
        Util.writeAddress(address, out);
        Bits.writeLong(id, out);
    }

    @Override
    public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        address = Util.readAddress(in);
        id = Bits.readLong(in);
    }

}
