package org.jgroups.protocols.relay;

import org.jgroups.Address;
import org.jgroups.util.UUID;

/**
 * Special address with the UUID part being 0: identifies the current (relay) coordinator of a given site. E,g, if we
 * send a message with dest=SiteMaster(SFO) from site LON, then the message will be forwarded to the relay coordinator
 * of the SFO site
 * @author Bela Ban
 * @since 3.2
 */
public class SiteMaster extends SiteUUID {
    private static final long serialVersionUID=-1110144992073882353L;

    public SiteMaster() {
    }

    public SiteMaster(String site) {
        super(0, 0, null, site);
    }

    public int compareTo(Address other) {
        if(other instanceof SiteMaster) {
            SiteMaster tmp=(SiteMaster)other;
            return site.compareTo(tmp.site);
        }
        return super.compareTo(other);
    }

    public boolean equals(Object obj) {
        return compareTo((Address)obj) == 0;
    }

    public int hashCode() {
        return site.hashCode();
    }

    public UUID copy() {
        return new SiteMaster(site);
    }

    public String toString() {
        return "SiteMaster(" + site + ")";
    }
}
