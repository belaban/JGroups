package org.jgroups.util;

/**
 * Subclass of {@link org.jgroups.util.UUID} which adds a string as payload. An instance of this can be fed to
 * {@link org.jgroups.JChannel#addAddressGenerator(org.jgroups.stack.AddressGenerator)}, with the address generator
 * creating PayloadUUIDs.
 * @author Bela Ban
 * @deprecated Use {@link ExtendedUUID} instead. Will get dropped in 4.0.
 */
@Deprecated
public class AdditionalDataUUID extends ExtendedUUID {
    protected static final byte[] DATA=Util.stringToBytes("data");
    private static final long     serialVersionUID=-4266399823075148290L;

    public AdditionalDataUUID() {
    }

    protected AdditionalDataUUID(byte[] data, byte[] payload) {
        super(data);
        put(DATA, payload);
    }

    public static AdditionalDataUUID randomUUID(byte[] payload) {
        return new AdditionalDataUUID(generateRandomBytes(), payload);
    }

    public static AdditionalDataUUID randomUUID(String logical_name, byte[] payload) {
        AdditionalDataUUID retval=new AdditionalDataUUID(generateRandomBytes(), payload);
        UUID.add(retval, logical_name);
        return retval;
    }

}
