package org.jgroups.tests;

import org.jgroups.Channel;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.FLUSH;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

/**
 * 
 * Flush and join problems during constant node failures and constant joins
 * https://jira.jboss.org/jira/browse/JGRP-985
 * 
 * 
 * @author vladimir
 * @since   2.8
 */
@Test(groups = {Global.FLUSH,Global.EAP_EXCLUDED}, singleThreaded=true)
public class FlushWithChannelJoinsAndFailuresTest {
    protected static final String cName = "FlushWithChannelFailuresTest";
   
    public void testAndLoop() throws Exception {
        int numChannels = 10;
        Channel channels[] = new Channel[numChannels];
        for (int j = 0; j < numChannels; j++) {
            channels[j] = createChannel(String.valueOf((char)('A' + j)));
            channels[j].connect(cName);
            if(j == 0)
                Util.sleep(1000);
        }
        Util.waitUntilAllChannelsHaveSameView(10000, 500, channels);

        for (int i = 1; i <= 2; i++) {
            int killPositions[] = { 0, 3, 5, 8 };
            for (int index : killPositions)
                Util.shutdown(channels[index]);
            int ch='M';
            for (int index : killPositions) {
                channels[index] = createChannel(String.valueOf((char)ch++));
                channels[index].connect(cName);
            }
            System.out.println("***** Round " + i + " done *****");
        }
        Util.waitUntilAllChannelsHaveSameView(10000, 500, channels);
    }

    protected JChannel createChannel(String name) throws Exception {
        Protocol[] protocols={
          new SHARED_LOOPBACK(),
          new SHARED_LOOPBACK_PING(),
          new FD_ALL().setValue("timeout", 3000).setValue("interval", 1000),
          new NAKACK2(),
          new UNICAST3(),
          new STABLE(),
          new GMS(),
          new FRAG2().fragSize(8000),
          new FLUSH()
        };

        return new JChannel(protocols).name(name);
    }
}