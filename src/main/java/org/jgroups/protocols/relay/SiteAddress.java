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

    enum Type {
        ALL,     // null destination, send to all members of the local cluster
        UNICAST, // send to a given member of the local cluster
        SM,      // send to the site master of a given site
        SM_ALL   // send to all site masters
    }

    /** Temporary kludge to avoid instanceof */
    default Type type() {return Type.UNICAST;}
}
