package org.jgroups.protocols.relay;

import org.jgroups.Address;

/**
 * Gets notified when a site comes up or goes down
 * @author Bela Ban
 * @since  3.4
 */
public interface RouteStatusListener {
    /** The sites just came up */
    void sitesUp(String... sites);
    /** The sites went down */
    void sitesDown(String... sites);

    /** The sites are unreachable (no route to them) */
    default void sitesUnreachable(String ... sites) {

    }

    /** Sent back to the original sender when the unicast destination is not part of the local cluster (site) */
    default void memberUnreachable(Address member) {

    }
}
