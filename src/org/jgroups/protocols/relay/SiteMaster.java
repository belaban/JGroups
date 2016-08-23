package org.jgroups.protocols.relay;

import org.jgroups.Address;
import org.jgroups.util.UUID;
import org.jgroups.util.Util;

import java.util.Arrays;
import java.util.function.Supplier;

/**
 * Special address with the UUID part being 0: identifies the current (relay) coordinator of a given site. E,g, if we
 * send a message with dest=SiteMaster(SFO) from site LON, then the message will be forwarded to the relay coordinator
 * of the SFO site
 * @author Bela Ban
 * @since 3.2
 */
public class SiteMaster extends SiteUUID {

    public SiteMaster() {
        setFlag(RELAY2.site_master_flag);
    }

    public SiteMaster(String site) {
        this(Util.stringToBytes(site));
    }

    public SiteMaster(byte[] site) {
        super(0, 0, null, site);
        setFlag(RELAY2.site_master_flag);
    }

    public Supplier<? extends UUID> create() {
        return SiteMaster::new;
    }

    public int compareTo(Address other) {
        if(other instanceof SiteMaster) {
            SiteMaster tmp=(SiteMaster)other;
            return Util.compare(get(SITE_NAME), tmp.get(SITE_NAME));
        }
        return super.compareTo(other);
    }

    public boolean equals(Object obj) {
        return compareTo((Address)obj) == 0;
    }

    public int hashCode() {
        return Arrays.hashCode(get(SITE_NAME));
    }

    public UUID copy() {
        return new SiteMaster(get(SITE_NAME));
    }

    public String toString() {
        return "SiteMaster(" + getSite() + ")";
    }
}
