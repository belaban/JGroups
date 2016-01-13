package org.jgroups.protocols.relay;

import org.jgroups.util.ExtendedUUID;
import org.jgroups.util.UUID;

/**
 * Subclass of {@link org.jgroups.util.ExtendedUUID} using flag {@link ExtendedUUID#CAN_BECOME_SITE_MASTER}. The flag i
 * ndicates whether the current address can become a site master, or not.
 * @author Bela Ban
 * @deprecated Use {@link ExtendedUUID} directly. This class will get dropped in 4.0.
 */
@Deprecated
public class CanBeSiteMaster extends ExtendedUUID {
    private static final long serialVersionUID=711248753245248165L;

    public CanBeSiteMaster() {
    }

    protected CanBeSiteMaster(byte[] data, boolean can_become_site_master) {
        super(data);
        if(can_become_site_master)
            setFlag(ExtendedUUID.CAN_BECOME_SITE_MASTER);
    }

    protected CanBeSiteMaster(UUID uuid, boolean can_become_site_master) {
        super(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        if(can_become_site_master)
            setFlag(ExtendedUUID.CAN_BECOME_SITE_MASTER);
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
        return isFlagSet(ExtendedUUID.CAN_BECOME_SITE_MASTER);
    }

}
