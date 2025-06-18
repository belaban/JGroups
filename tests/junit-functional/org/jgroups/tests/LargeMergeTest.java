package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


/**
 * Tests merging with a large number of members
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class LargeMergeTest {
    static final int           NUM=50; // number of members
    protected final JChannel[] channels=new JChannel[NUM];


    @BeforeMethod
    void setUp() throws Exception {
        System.out.print("Connecting channels: ");
        for(int i=0; i < NUM; i++) {
            channels[i]=new JChannel(new SHARED_LOOPBACK(),
                                     new DISCARD().discardAll(true),
                                     new SHARED_LOOPBACK_PING(),
                                     new MERGE3().setMinInterval(1000).setMaxInterval(3000)
                                       .setCheckInterval(6000).setMaxParticipantsInMerge(NUM),
                                     new NAKACK2().useMcastXmit(false)
                                       .logDiscardMessages(false).logNotFoundMessages(false)
                                       .setXmitTableNumRows(5).setXmitTableMsgsPerRow(10),
                                     new UNICAST3().setXmitTableNumRows(5).setXmitTableMsgsPerRow(10).setConnExpiryTimeout(10000),
                                     new STABLE().setMaxBytes(500000),
                                     new GMS().printLocalAddress(false).setJoinTimeout(1).setLeaveTimeout(10)
                                       .logViewWarnings(false).setViewAckCollectionTimeout(2000)
                                       .logCollectMessages(false));
            channels[i].setName(String.valueOf((i + 1)));
            channels[i].connect("LargeMergeTest");
            System.out.print(i + 1 + " ");
        }
        System.out.println();
    }

    @AfterMethod
    void tearDown() throws Exception {
        long start=System.nanoTime();
        for(int i=NUM-1; i >= 0; i--) {
            ProtocolStack stack=channels[i].getProtocolStack();
            String cluster_name=channels[i].getClusterName();
            stack.stopStack(cluster_name);
            stack.destroy();
        }
        long time=System.nanoTime()-start;
        System.out.printf("-- stopping the channels took %s\n", Util.printTime(time, TimeUnit.NANOSECONDS));
    }


    public void testClusterFormationAfterMerge() throws TimeoutException {
        System.out.println("\nEnabling message traffic between members to start the merge");
        for(JChannel ch: channels) {
            DISCARD discard=ch.getProtocolStack().findProtocol(DISCARD.class);
            discard.discardAll(false);
        }

        long start=System.nanoTime();
        Util.waitUntilAllChannelsHaveSameView(20000, 100, channels);
        long time=System.nanoTime()-start;
        System.out.printf("** SUCCESS: merge took %s; all %d channels have view %s (%d members)\n",
                          Util.printTime(time, TimeUnit.NANOSECONDS), channels.length, channels[0].view().getViewId(),
                          channels[0].view().size());
    }


    @Test(enabled=false)
    public static void main(String[] args) throws Exception {
        LargeMergeTest test=new LargeMergeTest();
        test.setUp();
        test.testClusterFormationAfterMerge();
        test.tearDown();
    }
}
