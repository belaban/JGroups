package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.util.DefaultThreadFactory;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.TimeScheduler2;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;


/**
 * Tests merging with a large number of members
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL,sequential=true)
public class LargeMergeTest {
    static final int NUM=40; // number of members

    protected final JChannel[] channels=new JChannel[NUM];


    @BeforeMethod
    void setUp() throws Exception {

        ThreadGroup test_group=new ThreadGroup("LargeMergeTest");
        TimeScheduler timer=new TimeScheduler2(new DefaultThreadFactory(test_group, "merge-", true, true),
                                               5,10,
                                               3000, 1000);
        System.out.print("Connecting channels: ");
        for(int i=0; i < NUM; i++) {
            SHARED_LOOPBACK shared_loopback=(SHARED_LOOPBACK)new SHARED_LOOPBACK().setValue("enable_bundling", false);
            shared_loopback.setValue("enable_diagnostics",false);
            shared_loopback.setValue("timer_min_threads",1).setValue("timer_max_threads", 2);
            shared_loopback.setTimer(timer);

            channels[i]=Util.createChannel(shared_loopback,
                                           new DISCARD().setValue("discard_all", true),
                                           new PING().setValue("timeout", 100),
                                           new MERGE2().setValue("min_interval", 2000).setValue("max_interval", 10000),
                                           // new FD_ALL(),
                                           new NAKACK().setValue("use_mcast_xmit", false)
                                             .setValue("log_discard_msgs", false).setValue("log_not_found_msgs", false),
                                           new UNICAST(),
                                           new STABLE().setValue("max_bytes", 50000),
                                           new GMS().setValue("print_local_addr", false)
                                             .setValue("leave_timeout", 100)
                                             .setValue("log_view_warnings", false));
            channels[i].setName(String.valueOf((i + 1)));
            channels[i].connect("LargeMergeTest");
            System.out.print(i + 1 + " ");
        }
        System.out.println("");
    }

    @AfterMethod
    void tearDown() throws Exception {
        for(int i=NUM-1; i >= 0; i--)
            Util.close(channels[i]);
    }



    public void testClusterFormationAfterMerge() {
        System.out.println("\nEnabling message traffic between members to start the merge");
        for(JChannel ch: channels) {
            DISCARD discard=(DISCARD)ch.getProtocolStack().findProtocol(DISCARD.class);
            discard.setDiscardAll(false);
        }

        boolean merge_completed=true;
        for(int i=0; i < NUM; i++) {
            merge_completed=true;
            System.out.println();

            Map<Integer,Integer> votes=new HashMap<Integer,Integer>();

            for(JChannel ch: channels) {
                int size=ch.getView().size();

                Integer val=votes.get(size);
                if(val == null)
                    votes.put(size, 1);
                else
                    votes.put(size, val.intValue() +1);
                if(size != NUM)
                    merge_completed=false;
            }

            if(i > 0) {
                for(Map.Entry<Integer,Integer> entry: votes.entrySet()) {
                    System.out.println("==> " + entry.getValue() + " members have a view of " + entry.getKey());
                }
            }

            if(merge_completed)
                break;
            Util.sleep(5000);
        }

        if(!merge_completed) {
            System.out.println("\nFinal cluster:");
            for(JChannel ch: channels) {
                int size=ch.getView().size();
                System.out.println(ch.getAddress() + ": " + size + " members - " + (size == NUM? "OK" : "FAIL"));
            }
        }
        for(JChannel ch: channels) {
            int size=ch.getView().size();
            assert size == NUM : "Channel has " + size + " members, but should have " + NUM;
        }
    }


}
