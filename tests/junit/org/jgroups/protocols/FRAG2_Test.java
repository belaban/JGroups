package org.jgroups.protocols;


import org.jgroups.stack.Protocol;
import org.testng.annotations.Test;

/**
 * Tests the fragmentation (FRAG2) protocol for http://jira.jboss.com/jira/browse/JGRP-216
 * @author Bela Ban
 */
@Test(groups={"temp", "protocols", "single"})
public class FRAG2_Test extends FRAG_Test {

    protected Protocol createProtocol() {
        return new FRAG2();
    }


  

}
