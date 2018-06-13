package org.jgroups.util;


import org.jgroups.Address;

import java.io.DataInput;
import java.io.DataOutput;


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

    public void writeTo(DataOutput out) throws Exception {
        Util.writeAddress(address, out);
        Bits.writeLong(thread_id, out);
    }

    public void readFrom(DataInput in) throws Exception {
        address=Util.readAddress(in);
        thread_id=Bits.readLong(in);
    }

    public boolean equals(Object obj) {
        if(obj == null)
            return false;
        Owner other=(Owner)obj;
        return address.equals(other.address) && thread_id == other.thread_id;
    }

    public int hashCode() {
        return (int)(address.hashCode() + thread_id);
    }

    public int compareTo(Owner o) {
        return thread_id < o.thread_id? -1 : thread_id > o.thread_id? 1 : address.compareTo(o.address);
    }

    public String toString() {
        return thread_id < 0? address.toString() : address + "::" + thread_id;
    }


}
