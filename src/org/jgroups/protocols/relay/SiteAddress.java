package org.jgroups.protocols.relay;

import org.jgroups.Address;

/**
 * Address with a site suffix
 * @author Bela Ban
 * @since 3.2
 */
public interface SiteAddress extends Address {
    /** Returns the ID of the site (all sites need to have a unique site ID) */
    String getSite();
}
