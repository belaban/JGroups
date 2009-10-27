package org.jgroups.tests;

import org.jgroups.Channel;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

/**
 * 
 * Flush and join problems during constant node failures and constant joins
 * https://jira.jboss.org/jira/browse/JGRP-985
 * 
 * 
 * @author vladimir
 * @since 2.8
 */
@Test(groups = Global.FLUSH, sequential = true)
public class FlushWithChannelJoinsAndFailuresTest extends ChannelTestBase {

   private static final String cName = "FlushWithChannelFailuresTest";
   
    @Test
    public void testAndLoop() throws Exception {
        int numChannels = 10;
        Channel channels[] = new Channel[numChannels];
        for (int j = 0; j < numChannels; j++) {
            if (j == 0) {
                channels[j] = createChannel(true, numChannels);
            } else {
                channels[j] = createChannel((JChannel) channels[0]);
            }
            channels[j].connect(cName);
        }
        for (int i = 1; i <= 2; i++) {
            int killPositions[] = { 0, 3, 5, 8 };
            for (int index : killPositions) {
                Util.shutdown(channels[index]);
            }
            for (int index : killPositions) {
                channels[index] = createChannel((JChannel) channels[1]);
                channels[index].connect(cName);
            }
            System.out.println("***** Round " + i + " done *****");
        }
        for (Channel c : channels) {
            assert c.getView().getMembers().size() == 10;
        }
    }
}