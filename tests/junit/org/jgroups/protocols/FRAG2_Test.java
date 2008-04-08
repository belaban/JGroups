package org.jgroups.protocols;



import org.testng.annotations.*;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.Protocol;
import org.jgroups.View;
import org.jgroups.Message;
import org.jgroups.Event;
import org.jgroups.Global;
import org.jgroups.debug.Simulator;

import java.util.Vector;
import java.util.Properties;
import java.nio.ByteBuffer;

/**
 * Tests the fragmentation (FRAG2) protocol for http://jira.jboss.com/jira/browse/JGRP-216
 * @author Bela Ban
 */
public class FRAG2_Test extends FRAG_Test {

    public FRAG2_Test(String name) {
        super(name);
    }


    protected Protocol createProtocol() {
        return new FRAG2();
    }


    public void testFragmentation() throws InterruptedException {
        super.testFragmentation();
    }


}
