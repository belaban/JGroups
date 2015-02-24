package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.ViewId;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.ProtocolStack;
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
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class LargeMergeTest {
    static final int               NUM=50; // number of members
    protected final JChannel[]     channels=new JChannel[NUM];


    @BeforeMethod
    void setUp() throws Exception {
        System.out.print("Connecting channels: ");
        for(int i=0; i < NUM; i++) {
            channels[i]=new JChannel(new SHARED_LOOPBACK(),
                                     new DISCARD().setValue("discard_all",true),
                                     new SHARED_LOOPBACK_PING(),
                                     new MERGE3().setValue("min_interval",1000)
                                       .setValue("max_interval",3000)
                                       .setValue("check_interval", 6000)
                                       .setValue("max_participants_in_merge", NUM),
                                     new NAKACK2().setValue("use_mcast_xmit",false)
                                       .setValue("discard_delivered_msgs",true)
                                       .setValue("log_discard_msgs",false).setValue("log_not_found_msgs",false)
                                       .setValue("xmit_table_num_rows",5)
                                       .setValue("xmit_table_msgs_per_row",10),
                                     new UNICAST3().setValue("xmit_table_num_rows",5)
                                       .setValue("xmit_table_msgs_per_row",10)
                                       .setValue("conn_expiry_timeout", 10000),
                                     new STABLE().setValue("max_bytes",500000),
                                     new GMS().setValue("print_local_addr",false)
                                       .setValue("use_merger2", true)
                                       .setValue("join_timeout", 1)
                                       .setValue("leave_timeout",100)
                                       .setValue("log_view_warnings",false)
                                       .setValue("view_ack_collection_timeout",2000)
                                       .setValue("log_collect_msgs",false));
            channels[i].setName(String.valueOf((i + 1)));
            channels[i].connect("LargeMergeTest");
            System.out.print(i + 1 + " ");
        }
        System.out.println("");
    }

    @AfterMethod
    void tearDown() throws Exception {
        for(int i=NUM-1; i >= 0; i--) {
            ProtocolStack stack=channels[i].getProtocolStack();
            String cluster_name=channels[i].getClusterName();
            stack.stopStack(cluster_name);
            stack.destroy();
        }
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

            Map<ViewId,Integer> views=new HashMap<>();

            for(JChannel ch: channels) {
                ViewId view_id=ch.getView().getViewId();
                Integer val=views.get(view_id);
                views.put(view_id, val == null? 1 : val+1);
                int size=ch.getView().size();
                if(size != NUM)
                    merge_completed=false;
            }

            if(i++ > 0) {
                int num_singleton_views=0;
                for(Map.Entry<ViewId,Integer> entry: views.entrySet()) {
                    if(entry.getValue() == 1)
                        num_singleton_views++;
                    else {
                        System.out.println("==> " + entry.getKey() + ": " + entry.getValue() + " members");
                    }
                }
                if(num_singleton_views > 0)
                    System.out.println("==> " + num_singleton_views + " singleton views");

                System.out.println("------------------\n" + getStats());
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



    protected String getStats() {
        StringBuilder sb=new StringBuilder();
        int merge_task_running=0;
        int merge_canceller_running=0;
        int merge_in_progress=0;
        int gms_merge_task_running=0;

        for(JChannel ch:  channels) {
            MERGE3 merge3=(MERGE3)ch.getProtocolStack().findProtocol(MERGE3.class);
            if(merge3 != null && merge3.isMergeTaskRunning())
                merge_task_running++;

            GMS gms=(GMS)ch.getProtocolStack().findProtocol(GMS.class);
            if(gms.isMergeKillerRunning())
                merge_canceller_running++;
            if(gms.isMergeInProgress())
                merge_in_progress++;
            if(gms.isMergeTaskRunning())
                gms_merge_task_running++;
        }
        sb.append("merge tasks running: " + merge_task_running).append("\n");
        sb.append("merge killers running: " + merge_canceller_running).append("\n");
        sb.append("merge in progress: " + merge_in_progress).append("\n");
        sb.append("gms.merge tasks running: " + gms_merge_task_running).append("\n");
        return sb.toString();
    }



    @Test(enabled=false)
    public static void main(String args[]) throws Exception {
        LargeMergeTest test=new LargeMergeTest();
        test.setUp();
        test.testClusterFormationAfterMerge();
        test.tearDown();
    }
}
