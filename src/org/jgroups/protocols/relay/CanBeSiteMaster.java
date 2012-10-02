package org.jgroups.protocols.relay;

import org.jgroups.Global;
import org.jgroups.util.UUID;

import java.io.*;
import java.security.SecureRandom;

/**
 * Subclass of {@link org.jgroups.util.UUID} which adds a boolean as payload. The boolean indicates whether the
 * current address can ever become a site master, or not.
 * @author Bela Ban
 */
public class CanBeSiteMaster extends UUID {
    private static final long serialVersionUID=4261548538335553258L;
    protected boolean         can_become_site_master;

    public CanBeSiteMaster() {
    }

    protected CanBeSiteMaster(byte[] data, boolean can_become_site_master) {
        super(data);
        this.can_become_site_master=can_become_site_master;
    }

    protected CanBeSiteMaster(UUID uuid, boolean can_become_site_master) {
        super(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        this.can_become_site_master=can_become_site_master;
    }

    public static CanBeSiteMaster randomUUID(boolean can_become_site_master) {
        return new CanBeSiteMaster(generateRandomBytes(), can_become_site_master);
    }

    public static CanBeSiteMaster randomUUID(String logical_name, boolean can_become_site_master) {
        CanBeSiteMaster retval=new CanBeSiteMaster(generateRandomBytes(), can_become_site_master);
        UUID.add(retval, logical_name);
        return retval;
    }

    public boolean canBecomeSiteMaster() {
        return can_become_site_master;
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
        return super.size() + Global.BYTE_SIZE;
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        can_become_site_master=in.readBoolean();
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeBoolean(can_become_site_master);
    }

    public void writeTo(DataOutput out) throws Exception {
        super.writeTo(out);
        out.writeBoolean(can_become_site_master);
    }

    public void readFrom(DataInput in) throws Exception {
        super.readFrom(in);
        can_become_site_master=in.readBoolean();
    }


    public String toString() {
        if(print_uuids)
            return toStringLong() + (can_become_site_master? "[T]" : "[F]");
        return super.toString() + (can_become_site_master? "[T]" : "[F]");
    }

}
