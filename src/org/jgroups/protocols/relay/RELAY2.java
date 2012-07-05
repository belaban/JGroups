package org.jgroups.protocols.relay;

import org.jgroups.annotations.Experimental;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;

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
    @Property(description="Name of the site (needs to be defined in the configuration)")
    protected String site;

    @Property(description="Name of the relay configuration",writable=false)
    protected String config;

    @Property(description="Whether or not to relay multicast messages (dest=null)")
    protected boolean relay_multicasts=false;

    

    /* ---------------------------------------------    Fields    ------------------------------------------------ */

    short site_id=0;


    public void init() throws Exception {
        super.init();
        if(site == null)
            throw new IllegalArgumentException("site cannot be null");
    }
}
