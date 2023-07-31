package org.jgroups.protocols.relay;

import org.jgroups.Address;
import org.jgroups.logging.Log;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Base class for all relayers
 * @author Bela Ban
 * @since  5.2.17
 */

public abstract class Relayer {
    protected final RELAY                   relay;
    protected final Log                     log;
    /** Flag set when stop() is called. Since a Relayer should not be used after stop() has been called, a new
     * instance needs to be created */
    protected volatile boolean              done;
    protected boolean                       stats;
    /** The routing table. Site IDs are the keys (e.g. "sfo", and list of routes are the values */
    protected final Map<String,List<Route>> routes=new ConcurrentHashMap<>(5);

    public Relayer(RELAY relay, Log log) {
        this.relay=relay;
        this.log=log;
        this.stats=relay.statsEnabled();
    }

    public RELAY   relay() {return relay;}
    public Log     log()   {return log;}
    public boolean done()  {return done;}


    protected Route getRoute(String site) { return getRoute(site, null);}

    protected synchronized Route getRoute(String site, Address sender) {
        List<Route> list=routes.get(site);
        if(list == null)
            return null;
        if(list.size() == 1)
            return list.get(0);
        return relay.site_master_picker.pickRoute(site, list, sender);
    }
}
