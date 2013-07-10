package org.jgroups.protocols.relay;

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
}
