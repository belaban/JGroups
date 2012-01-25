package org.jgroups.util;

import org.jgroups.Global;

import java.io.*;
import java.security.SecureRandom;

/**
 * Subclass of {@link UUID} which adds a string as payload. An instance of this can be fed to
 * {@link org.jgroups.JChannel#setAddressGenerator(org.jgroups.stack.AddressGenerator)}, with the address generator
 * creating PayloadUUIDs.
 * @author Bela Ban
 */
public class PayloadUUID extends UUID {
    private static final long serialVersionUID=-7383508979230850669L;
    protected String payload;

    public PayloadUUID() {
    }

    protected PayloadUUID(byte[] data, String payload) {
        super(data);
        this.payload=payload;
    }

    public static PayloadUUID randomUUID(String payload) {
        return new PayloadUUID(generateRandomBytes(), payload);
    }

    public static PayloadUUID randomUUID(String logical_name, String payload) {
        PayloadUUID retval=new PayloadUUID(generateRandomBytes(), payload);
        UUID.add(retval, logical_name);
        return retval;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload=payload;
    }

    protected static byte[] generateRandomBytes() {
        SecureRandom ng=numberGenerator;
        if(ng == null)
            numberGenerator=ng=new SecureRandom();

        byte[] randomBytes=new byte[16];
        ng.nextBytes(randomBytes);
        return randomBytes;
    }

    public int size() {
        int retval=super.size() + Global.BYTE_SIZE;
        if(payload != null)
            retval+=payload.length() +2;
        return retval;
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        try {
            payload=Util.readString(in);
        }
        catch(Exception e) {
            throw new IOException(e);
        }
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        try {
            Util.writeString(payload, out);
        }
        catch(Exception e) {
            throw new IOException(e);
        }
    }

    public void writeTo(DataOutput out) throws Exception {
        super.writeTo(out);
        Util.writeString(payload, out);
    }

    public void readFrom(DataInput in) throws Exception {
        super.readFrom(in);
        payload=Util.readString(in);
    }


    public String toString() {
        if(print_uuids)
            return toStringLong() + (payload == null? "" : "(" + payload + ")");
        return super.toString() + (payload == null? "" : "(" + payload + ")");
    }

}
