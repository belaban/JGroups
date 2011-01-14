package org.jgroups.blocks.locking;


import org.jgroups.Address;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;



/**
 * Represents the owner of a lock. Wraps address and thread ID
 * @author Bela Ban
 */
public class Owner implements Streamable {
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

    public void writeTo(DataOutputStream out) throws IOException {
        Util.writeAddress(address, out);
        out.writeLong(thread_id);
    }

    public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
        address=Util.readAddress(in);
        thread_id=in.readLong();
    }

    public boolean equals(Object obj) {
        Owner other=(Owner)obj;
        return address.equals(other.address) && thread_id == other.thread_id;
    }

    public int hashCode() {
        return (int)(address.hashCode() + thread_id);
    }

    public String toString() {
        return address + "::" + thread_id;
    }
}