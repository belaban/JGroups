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

    public SiteMaster(short site) {
        super(0, 0, null, site);
    }

    public SiteMaster(String site) {
        this(getSite(site));
    }

    public int compareTo(Address other) {
        if(other instanceof SiteMaster) {
            SiteMaster tmp=(SiteMaster)other;
            return site == tmp.site? 0 : site < tmp.site? -1 : 1;
        }
        return super.compareTo(other);
    }

    public boolean equals(Object obj) {
        return compareTo((Address)obj) == 0;
    }

    public int hashCode() {
        return site;
    }

    public UUID copy() {
        return new SiteMaster(site);
    }

    public String toString() {
        String site_name=site_cache.get(site);
        return "SiteMaster(" + (site_name != null? site_name : site) + ")";
    }
}
