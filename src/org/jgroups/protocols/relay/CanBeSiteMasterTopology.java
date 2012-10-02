package org.jgroups.protocols.relay;

import org.jgroups.Global;
import org.jgroups.util.TopologyUUID;

import java.io.*;

/**
 * Subclass of {@link org.jgroups.util.TopologyUUID} which adds a boolean as payload. The boolean indicates whether the
 * current address can ever become a site master, or not.
 * @author Bela Ban
 * @since 3.2
 */
public class CanBeSiteMasterTopology extends TopologyUUID {
    private static final long serialVersionUID=4261548538335553258L;
    protected boolean         can_become_site_master;

    public CanBeSiteMasterTopology() {
    }

    protected CanBeSiteMasterTopology(TopologyUUID uuid, boolean can_become_site_master) {
        super(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits(), uuid.getSiteId(), uuid.getRackId(), uuid.getMachineId());
        this.can_become_site_master=can_become_site_master;
    }


    public boolean canBecomeSiteMaster() {
        return can_become_site_master;
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
