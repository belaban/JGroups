package org.jgroups.protocols.relay;

import org.jgroups.Address;

import java.util.List;

/**
 * Allows an implementation to pick a {@link SiteMaster} or a {@link Route} from a list (if multiple site masters are
 * enabled). An implementation could for example always pick the same site master (or route) for messages from a given
 * sender (sticky site master policy, see https://issues.jboss.org/browse/JGRP-2112).<p/>
 * The default implementation picks a random site master for every message to be relayed, even if they have the same
 * original sender.<p/>
 * If only one site master is configured, then {@link #pickSiteMaster(List,Address)} (List,Address)} or
 * {@link #pickRoute(String,List,Address)} will never be called.
 * @author Bela Ban
 * @since  3.6.12, 4.0
 */
public interface SiteMasterPicker {
    /**
     * Needs to pick a member from a list of addresses of site masters
     * @param site_masters The list of site masters
     * @param original_sender The address of the original member sending a message
     * @return The address of the site master (in the local cluster) to be used to forward the message to
     */
    Address pickSiteMaster(List<Address> site_masters, Address original_sender);

    /**
     * Picks a route to a given remote site from a list of routes.
     * @param site The name of the target (remote) site. Added for informational purposes; may or may not be used
     *             as selection criterium.
     * @param routes The list of routes. A route can be picked for example by using the address of the remote site
     *               master: {@link Route#siteMaster()}
     * @param original_sender The address of the original sender
     * @return A route
     */
    Route   pickRoute(String site, List<Route> routes, Address original_sender);
}
