package org.jgroups.protocols.relay;

import org.jgroups.Address;
import org.jgroups.util.Util;

import java.util.List;
import java.util.function.Supplier;

/**
 * Implementation of {@link SiteMasterPicker} which picks random site masters / routes to site masters
 * @author Bela Ban
 * @since  5.3.1
 */
public class RandomSiteMasterPicker implements SiteMasterPicker {
    protected boolean           verbose;
    protected Supplier<Address> addr_supplier;

    public RandomSiteMasterPicker() {
    }

    public boolean         verbose()                            {return verbose;}
    public SiteMasterPicker verbose(boolean b)                   {verbose=b; return this;}
    public SiteMasterPicker addressSupplier(Supplier<Address> s) {addr_supplier=s; return this;}

    public Address pickSiteMaster(List<Address> site_masters, Address original_sender) {
        Address sm_addr=Util.pickRandomElement(site_masters);
        if(verbose)
            System.out.printf("-- picked local site master %s to forward message to\n", sm_addr);
        return sm_addr;
    }

    public Route pickRoute(String site, List<Route> routes, Address original_sender) {
        Route route=Util.pickRandomElement(routes);
        if(verbose)
            System.out.printf("-- %s picked remote site master %s and bridge %s to route message\n",
                              addr_supplier != null? addr_supplier.get() + ":" : "",
                              route.siteMaster(), route.bridge().address());
        return route;
    }
}
