package org.jgroups.protocols.relay;

import org.jgroups.util.ExtendedUUID;
import org.jgroups.util.TopologyUUID;

/**
 * Subclass of {@link org.jgroups.util.TopologyUUID} which adds a boolean as payload. The boolean indicates whether the
 * current address can ever become a site master, or not.
 * @author Bela Ban
 * @since 3.2
 * @deprecated Use {@link ExtendedUUID} instead. This class will get dropped in 4.0.
 */
@Deprecated
public class CanBeSiteMasterTopology extends TopologyUUID {
    private static final long serialVersionUID=4597162287820717893L;

    public CanBeSiteMasterTopology() {
    }

    protected CanBeSiteMasterTopology(TopologyUUID uuid, boolean can_become_site_master) {
        super(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits(), uuid.getSiteId(), uuid.getRackId(), uuid.getMachineId());
        if(can_become_site_master)
            setFlag(ExtendedUUID.CAN_BECOME_SITE_MASTER);
    }


    public boolean canBecomeSiteMaster() {
        return isFlagSet(ExtendedUUID.CAN_BECOME_SITE_MASTER);
    }

}
