package org.jgroups.protocols.relay;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.View;
import org.jgroups.annotations.*;
import org.jgroups.conf.ConfiguratorFactory;
import org.jgroups.protocols.relay.config.RelayConfig;
import org.jgroups.stack.Protocol;
import org.jgroups.util.UUID;
import org.jgroups.util.Util;

import java.io.InputStream;
import java.util.Collection;
import java.util.Map;
import java.util.TreeSet;

/**
 *
 * Design: ./doc/design/RELAY2.txt and at https://github.com/belaban/JGroups/blob/master/doc/design/RELAY2.txt.<p/>
 * JIRA: https://issues.jboss.org/browse/JGRP-1433
 * @author Bela Ban
 * @since 3.2
 */
@Experimental
@MBean(description="RELAY2 protocol")
public class RELAY2 extends Protocol {

    /* ------------------------------------------    Properties     ---------------------------------------------- */
    @Property(description="Name of the site (needs to be defined in the configuration)",writable=false)
    protected String site;

    @Property(description="Name of the relay configuration",writable=false)
    protected String config;

    @Property(description="Whether or not to relay multicast messages (dest=null)")
    protected boolean relay_multicasts=false;

    

    /* ---------------------------------------------    Fields    ------------------------------------------------ */
    @ManagedAttribute(description="The site-ID")
    protected short                              site_id=-1;

    protected Map<String,RelayConfig.SiteConfig> sites;

    protected RelayConfig.SiteConfig             site_config;

    @ManagedAttribute(description="Whether this member is the coordinator")
    protected volatile boolean                   is_coord=false;

    protected volatile Address                   coord;

    protected Relayer                            relayer;

    protected volatile Address                   local_addr;


    public void init() throws Exception {
        super.init();
        if(site == null)
            throw new IllegalArgumentException("site cannot be null");
        if(config == null)
            throw new IllegalArgumentException("config cannot be null");
        parseSiteConfiguration();

        // Sanity check
        Collection<Short> site_ids=new TreeSet<Short>();
        for(RelayConfig.SiteConfig site_config: sites.values()) {
            site_ids.add(site_config.getId());
            if(site.equals(site_config.getName()))
                site_id=site_config.getId();
        }

        int index=0;
        for(short id: site_ids) {
            if(id != index)
                throw new Exception("site IDs need to start with 0 and are required to increase monotonically; " +
                                      "site IDs=" + site_ids);
            index++;
        }

        if(site_id < 0)
            throw new IllegalArgumentException("site_id could not be determined from site \"" + site + "\"");
    }

    public void stop() {
        super.stop();
        is_coord=false;
        if(log.isTraceEnabled())
            log.trace("I ceased to be site master; closing bridges");
        if(relayer != null)
            relayer.stop();
    }

    /**
     * Parses the configuration by reading the config file. Can be used to re-read a configuration.
     * @throws Exception
     */
    @ManagedOperation(description="Reads the configuration file and build the internal config information.")
    public void parseSiteConfiguration() throws Exception {
        InputStream input=null;
        try {
            input=ConfiguratorFactory.getConfigStream(config);
            sites=RelayConfig.parse(input);
            for(RelayConfig.SiteConfig site_config: sites.values())
                SiteUUID.addToCache(site_config.getId(), site_config.getName());
            site_config=sites.get(site);
            if(site_config == null)
                throw new Exception("site configuration for \"" + site + "\" not found in " + config);
            if(log.isTraceEnabled())
                log.trace("site configuration:\n" + site_config);
        }
        finally {
            Util.close(input);
        }
    }


    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;
            case Event.VIEW_CHANGE:
                handleView((View)evt.getArg());
                break;
        }
        return down_prot.down(evt);
    }


    public Object up(Event evt) {
        switch(evt.getType()) {
            case Event.VIEW_CHANGE:
                handleView((View)evt.getArg());
                break;
        }
        return up_prot.up(evt);
    }



    protected void handleView(View view) {
        Address old_coord=coord, new_coord=Util.getCoordinator(view);
        boolean become_coord=new_coord.equals(local_addr) && (old_coord == null || !old_coord.equals(local_addr));
        boolean cease_coord=old_coord != null && old_coord.equals(local_addr) && !new_coord.equals(local_addr);

        coord=new_coord;

        // This member is the new coordinator: start the Relayer
        if(become_coord) {
            is_coord=true;
            String bridge_name="_" + UUID.get(local_addr);
            relayer=new Relayer(site_config.getBridges().size(),
                                site_config,bridge_name);
            try {
                if(log.isTraceEnabled())
                    log.trace("I become site master; starting bridges");
                relayer.start();
            }
            catch(Throwable t) {
                log.error("failed starting relayer", t);
            }
        }
        else {
            if(cease_coord) { // ceased being the coordinator (site master): stop the Relayer
                is_coord=false;
                if(log.isTraceEnabled())
                    log.trace("I ceased to be site master; closing bridges");
                if(relayer != null)
                    relayer.stop();
            }
        }
    }
}
