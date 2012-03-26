package org.jgroups.protocols.tom;

import org.jgroups.Address;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.*;

/**
 * The represents an unique identifier for the messages processed by the Total Order Anycast protocol
 * 
 * Note: it is similar to the ViewId (address + counter)
 * 
 * @author Pedro Ruivo
 * @since 3.1
 */
public class MessageID implements Externalizable, Comparable<MessageID>, Cloneable, Streamable {
    private static final long serialVersionUID=878801547232534461L;
    private Address address = null;
    private long id = -1;

    public MessageID() {}

    public MessageID(Address address, long id) {
        this.address = address;
        this.id = id;
    }

    public MessageID(Address address) {
        this.address = address;
    }

    public void setID(long id) {
        this.id = id;
    }

    @Override
    public int compareTo(MessageID other) {
        if(other == null) return 1;

        if(this.getId() < other.getId()){
            return -1;
        } else if(this.getId() > other.getId()){
            return 1;
        }

        return this.address.compareTo(other.address);
    }

    public MessageID copy() {
        return (MessageID) clone();
    }

    public long getId() {
        return id;
    }

    public Address getAddress() {
        return address;
    }

    @Override
    public String toString() {
        return "MessageID{" + address + ":" + id + "}";
    }

    public Object clone() {
        return new MessageID(address, id);
    }

    public boolean equals(Object other) {
        return (other instanceof MessageID) && compareTo((MessageID) other) == 0;
    }


    public int hashCode() {
        return (int)id;
    }


    public int serializedSize() {
        return Util.size(id) + Util.size(address);
    }

    @Override
    public void writeTo(DataOutput out) throws Exception {
        Util.writeAddress(address, out);
        Util.writeLong(id, out);
    }

    @Override
    public void readFrom(DataInput in) throws Exception {
        address = Util.readAddress(in);
        id = Util.readLong(in);
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
