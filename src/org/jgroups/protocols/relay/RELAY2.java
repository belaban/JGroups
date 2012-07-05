package org.jgroups.protocols.relay;

import org.jgroups.annotations.Experimental;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.conf.ConfiguratorFactory;
import org.jgroups.protocols.S3_PING;
import org.jgroups.protocols.relay.config.RelayConfig;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

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

    protected short                              site_id=0;

    protected Map<String,RelayConfig.SiteConfig> sites;

    protected RelayConfig.SiteConfig             site_config;


    public void init() throws Exception {
        super.init();
        if(site == null)
            throw new IllegalArgumentException("site cannot be null");
        if(config == null)
            throw new IllegalArgumentException("config cannot be null");
        parseSiteConfiguration();
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
}
