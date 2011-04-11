package org.jgroups.util;

import org.jgroups.Global;

import java.io.*;
import java.security.SecureRandom;

/**
 * Subclass of {@link org.jgroups.util.UUID} which adds 3 strings (siteId, rackId and machineId)as payload.
 * An instance of this can be fed to {@link org.jgroups.JChannel#setAddressGenerator(org.jgroups.stack.AddressGenerator)},
 * with the address generator creating TopologyUUIDs.<p/>
 * Mainly used by TopologyAwareConsistentHash in Infinispan (www.infinispan.org).
 * @author Bela Ban
 */
public class TopologyUUID extends UUID {
    private static final long serialVersionUID = 6057688946243812544L;
    protected String site_id;
    protected String rack_id;
    protected String machine_id;

    public TopologyUUID() {
    }

    protected TopologyUUID(byte[] data, String site_id, String rack_id, String machine_id) {
        super(data);
        this.site_id=site_id;
        this.rack_id=rack_id;
        this.machine_id=machine_id;
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
        return site_id;
    }

    public void setSiteId(String site_id) {
        this.site_id=site_id;
    }

    public String getRackId() {
        return rack_id;
    }

    public void setRackId(String rack_id) {
        this.rack_id=rack_id;
    }

    public String getMachineId() {
        return machine_id;
    }

    public void setMachineId(String machine_id) {
        this.machine_id=machine_id;
    }

    public boolean isSameSite(TopologyUUID addr) {
        return addr != null && site_id != null  && site_id.equals(addr.getSiteId());
    }

    public boolean isSameRack(TopologyUUID addr) {
        return addr != null && rack_id != null  && rack_id.equals(addr.getRackId());
    }

    public boolean isSameMachine(TopologyUUID addr) {
        return addr != null && machine_id != null  && machine_id.equals(addr.getMachineId());
    }


    public int size() {
        int retval=super.size() + 3 * Global.BYTE_SIZE;
        if(site_id != null)
            retval+= site_id.length() +2;
        if(rack_id != null)
            retval+=rack_id.length() +2;
        if(machine_id != null)
            retval+=machine_id.length() +2;
        return retval;
    }

    public void writeTo(DataOutput out) throws IOException {
        super.writeTo(out);
        Util.writeString(site_id, out);
        Util.writeString(rack_id, out);
        Util.writeString(machine_id, out);
    }

    public void readFrom(DataInput in) throws IOException, IllegalAccessException, InstantiationException {
        super.readFrom(in);
        site_id=Util.readString(in);
        rack_id=Util.readString(in);
        machine_id=Util.readString(in);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        site_id=(String)in.readObject();
        rack_id=(String)in.readObject();
        machine_id=(String)in.readObject();
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(site_id);
        out.writeObject(rack_id);
        out.writeObject(machine_id);
    }

    public String toString() {
        if(print_uuids)
            return toStringLong() + (site_id == null? "" : "(" + site_id + ")");
        return super.toString() + (site_id == null? "" : "(" + site_id + ")");
    }

    public String toStringDetailed() {
        if(print_uuids)
            return toStringLong() + "(" + printDetails() + ")";
        return super.toString() + "(" + printDetails() + ")";
    }

    
    protected static byte[] generateRandomBytes() {
        SecureRandom ng=numberGenerator;
        if(ng == null)
            numberGenerator=ng=new SecureRandom();

        byte[] randomBytes=new byte[16];
        ng.nextBytes(randomBytes);
        return randomBytes;
    }

    protected String printDetails() {
        StringBuilder sb=new StringBuilder();
        if(site_id != null)
            sb.append(site_id);
        sb.append(":");
        if(rack_id != null)
            sb.append(rack_id);
        sb.append(":");
        if(machine_id != null)
            sb.append(machine_id);
        return sb.toString();
    }
}
