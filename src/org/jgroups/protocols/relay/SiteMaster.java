package org.jgroups.protocols.relay;

import org.jgroups.Address;
import org.jgroups.util.UUID;

import java.util.function.Supplier;

/**
 * Special address with the UUID part being 0: identifies the current (relay) coordinator of a given site. E,g, if we
 * send a message with dest=SiteMaster(SFO) from site LON, then the message will be forwarded to the relay coordinator
 * of the SFO site
 * @author Bela Ban
 * @since 3.2
 */
public class SiteMaster extends SiteUUID {
    protected static final SiteMaster ALL_SMS=new SiteMaster(null);
    protected static final int HASH=ALL_SMS.hashCode();

    public SiteMaster() {
    }

    public SiteMaster(String site) {
        super(0, 0, null, site);
    }

    public Supplier<? extends UUID> create() {
        return SiteMaster::new;
    }

    public Type type() {
        return site == null? Type.SM_ALL : Type.SM;
    }

    public int compareTo(Address other) {
        if(other instanceof SiteMaster) {
            SiteMaster tmp=(SiteMaster)other;
            String other_site=tmp.getSite();

            if(this.site == null) {
                if(other_site == null)                  // (1) this.site == null && other.site == null
                    return 0;
                return -1;                              // (2) this.site == null && other.site != null
            }
            else { // this.site != null
                if(other_site == null)                  // (3) this.site != null && other.site == null
                    return 1;
                int rc=this.site.compareTo(other_site); // (4) this.site != null && other.site != null
                //compareTo will check the bits.
                return rc == 0? super.compareTo(other) : rc;
            }
        }
        return super.compareTo(other);
    }

    public boolean equals(Object obj) {
        if(this == obj)
            return true;
        if(!(obj instanceof SiteMaster))
            return false;
        return compareTo((Address)obj) == 0;
    }

    @Override
    public int hashCode() {
        return site == null? HASH : super.hashCode();
    }

    public UUID copy() {
        return new SiteMaster(site);
    }

    public String toString() {
        return String.format("SiteMaster(%s)", site != null? site : "<all sites>");
    }
}
