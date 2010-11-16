package org.jgroups.util;

import java.io.*;

/**
 * Used by RELAY to ship original sender of a message.
 * @author Bela Ban
 */
public class ProxyUUID extends UUID {
    private static final long serialVersionUID=7850262936740849187L;

    protected UUID original=null;


    public ProxyUUID() {
    }


    public ProxyUUID(UUID addr, UUID original) {
        super(addr.mostSigBits, addr.leastSigBits);
        this.original=original;
    }


    public String toString() {
        return super.toString() + ", original=" + original;
    }

    public String toStringLong() {
        return super.toStringLong() + ", original=" + original.toStringLong();
    }

    public void writeTo(DataOutputStream out) throws IOException {
        super.writeTo(out);
        original.writeTo(out);
    }

     public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
         super.readFrom(in);
         original=new UUID();
         original.readFrom(in);
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        original.writeExternal(out);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        original.readExternal(in);
    }

    public int size() {
        return super.size() + original.size();
    }
}
