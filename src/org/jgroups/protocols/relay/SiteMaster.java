package org.jgroups.protocols.relay;

import org.jgroups.Address;
import org.jgroups.util.UUID;
import org.jgroups.util.Util;

import java.util.Arrays;

/**
 * Special address with the UUID part being 0: identifies the current (relay) coordinator of a given site. E,g, if we
 * send a message with dest=SiteMaster(SFO) from site LON, then the message will be forwarded to the relay coordinator
 * of the SFO site
 * @author Bela Ban
 * @since 3.2
 */
public class SiteMaster extends SiteUUID {
    private static final long serialVersionUID=-6147979304449032483L;

    public SiteMaster() {
        setFlag(site_master);
    }

    public SiteMaster(String site) {
        this(Util.stringToBytes(site));
    }

    public SiteMaster(byte[] site) {
        super(0, 0, null, site);
        setFlag(site_master);
    }

    public int compareTo(Address other) {
        if(other instanceof SiteMaster) {
            SiteMaster tmp=(SiteMaster)other;
            return Util.compare(get(SITE_NAME), tmp.get(SITE_NAME));
        }
        return super.compareTo(other);
    }

    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (this.getClass() != obj.getClass()) {
            return false;
        }

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
