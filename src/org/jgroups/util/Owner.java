package org.jgroups.util;


import org.jgroups.Address;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;


/**
 * Represents an 'owner', which is an address and thread ID
 * @author Bela Ban
 */
public class Owner implements Streamable, Comparable<Owner> {
    protected Address address;
    protected long    thread_id;

    public Owner() {
    }

    public Owner(Address address, long thread_id) {
        this.address=address;
        this.thread_id=thread_id;
    }

    public Address getAddress() {
        return address;
    }

    public long getThreadId() {
        return thread_id;
    }

    @Override
    public void writeTo(DataOutput out) throws IOException {
        Util.writeAddress(address, out);
        Bits.writeLong(thread_id, out);
    }

    @Override
    public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        address=Util.readAddress(in);
        thread_id=Bits.readLong(in);
    }

    public boolean equals(Object obj) {
        if(this == obj)
            return true;
        if(obj == null)
            return false;
        Owner other=(Owner)obj;
        return Objects.equals(address, other.address) && thread_id == other.thread_id;
    }

    public int hashCode() {
        return (int)(address.hashCode() + thread_id);
    }

    public int compareTo(Owner o) {
        if(this == o)
            return 0;
        if(o == null)
            return 1; // I'm greater than a null object (hmm.. ?)
        return thread_id < o.thread_id? -1 : thread_id > o.thread_id? 1
          : (o.address == null? 1 : address.compareTo(o.address));
    }

    public String toString() {
        return thread_id < 0? address.toString() : address + "::" + thread_id;
    }


}
