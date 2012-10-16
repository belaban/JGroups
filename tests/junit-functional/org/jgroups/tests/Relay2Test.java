package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.testng.annotations.Test;

/**
 * Various RELAY2-related tests
 * @author Bela Ban
 * @since 3.2
 */
@Test(groups=Global.FUNCTIONAL,sequential=true)
public class Relay2Test {


    /**
     * Tests that routes are correctly registered after a partition and a subsequent merge
     * (https://issues.jboss.org/browse/JGRP-1524)
     */
    public void testMissingRouteAfterMerge() {


    }



    protected JChannel createNode(String site_name, String cluster_name, String node_name) {
        JChannel ch=null;


        return ch;
    }
}
