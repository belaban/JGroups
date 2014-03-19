package org.jgroups.util;

/**
 * Subclass of {@link UUID} which adds a string as payload. An instance of this can be fed to
 * {@link org.jgroups.JChannel#addAddressGenerator(org.jgroups.stack.AddressGenerator)}, with the address generator
 * creating PayloadUUIDs.
 * @author Bela Ban
 * @deprecated Use {@link ExtendedUUID} instead. Will get dropped in 4.0.
 */
@Deprecated
public class PayloadUUID extends ExtendedUUID {
    protected static final byte[] PAYLOAD=Util.stringToBytes("payload");
    private static final long     serialVersionUID=-50118853717142043L;

    public PayloadUUID() {
    }

    protected PayloadUUID(byte[] data, String payload) {
        super(data);
        put(PAYLOAD, Util.stringToBytes(payload));
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
        return Util.bytesToString(get(PAYLOAD));
    }

    public void setPayload(String payload) {
        put(PAYLOAD, Util.stringToBytes(payload));
    }


}
