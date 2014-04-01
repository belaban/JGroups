package org.jgroups.util;

import java.util.Arrays;

/**
 * Subclass of {@link org.jgroups.util.ExtendedUUID} which adds 3 strings (siteId, rackId and machineId) to the hashmap.
 * An instance of this can be fed to {@link org.jgroups.JChannel#addAddressGenerator(org.jgroups.stack.AddressGenerator)},
 * with the address generator creating TopologyUUIDs.<p/>
 * Mainly used by TopologyAwareConsistentHash in Infinispan (www.infinispan.org).
 * @author Bela Ban
 * @deprecated Use {@link ExtendedUUID} instead. This class will be dropped in 4.0.
 */
@Deprecated
public class TopologyUUID extends ExtendedUUID {
    protected static final byte[] SITE_ID    = Util.stringToBytes("site-id");
    protected static final byte[] RACK_ID    = Util.stringToBytes("rack-id");
    protected static final byte[] MACHINE_ID = Util.stringToBytes("machine-id");
    private static final long     serialVersionUID=332097758637301279L;


    public TopologyUUID() {
    }

    protected TopologyUUID(byte[] data, String site_id, String rack_id, String machine_id) {
        super(data);
        put(SITE_ID,    Util.stringToBytes(site_id));
        put(RACK_ID,    Util.stringToBytes(rack_id));
        put(MACHINE_ID, Util.stringToBytes(machine_id));
    }

    protected TopologyUUID(long most_sig_bits, long least_sig_bits, String site_id, String rack_id, String machine_id) {
        super(most_sig_bits, least_sig_bits);
        put(SITE_ID,    Util.stringToBytes(site_id));
        put(RACK_ID,    Util.stringToBytes(rack_id));
        put(MACHINE_ID, Util.stringToBytes(machine_id));
    }

    public static TopologyUUID randomUUID(String site_id, String rack_id, String machine_id) {
        return new TopologyUUID(generateRandomBytes(), site_id, rack_id, machine_id);
    }

    public static TopologyUUID randomUUID(String logical_name, String site_id, String rack_id, String machine_id) {
        TopologyUUID retval=new TopologyUUID(generateRandomBytes(), site_id, rack_id, machine_id);
        UUID.add(retval, logical_name);
        return retval;
    }

    public String getSiteId() {
        return Util.bytesToString(get(SITE_ID));
    }

    public void setSiteId(String site_id) {
        put(SITE_ID, Util.stringToBytes(site_id));
    }

    public String getRackId() {
        return Util.bytesToString(get(RACK_ID));
    }

    public void setRackId(String rack_id) {
        put(RACK_ID, Util.stringToBytes(rack_id));
    }

    public String getMachineId() {
        return Util.bytesToString(get(MACHINE_ID));
    }

    public void setMachineId(String machine_id) {
        put(MACHINE_ID, Util.stringToBytes(machine_id));
    }

    public boolean isSameSite(TopologyUUID addr) {
        return addr != null && Arrays.equals(get(SITE_ID), addr.get(SITE_ID));
    }

    public boolean isSameRack(TopologyUUID addr) {
        return addr != null && Arrays.equals(get(RACK_ID), addr.get(RACK_ID));
    }

    public boolean isSameMachine(TopologyUUID addr) {
        return addr != null && Arrays.equals(get(MACHINE_ID), addr.get(MACHINE_ID));
    }


}
