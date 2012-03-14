package org.jgroups.protocols;

import org.jgroups.annotations.MBean;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;

/**
 *
 * Design: ./doc/design/RELAY2.txt and at https://github.com/belaban/JGroups/blob/master/doc/design/RELAY2.txt.<p/>
 * JIRA: https://issues.jboss.org/browse/JGRP-1433
 * @author Bela Ban
 * @since 3.1
 */
@MBean(description="RELAY2 protocol")
public class RELAY2 extends Protocol {

    /* ------------------------------------------    Properties     ---------------------------------------------- */
    @Property(description="Whether or not to relay multicast messages (dest=null)")
    protected boolean relay_multicasts=false;

    

    /* ---------------------------------------------    Fields    ------------------------------------------------ */






}
