package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.ViewId;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.DiagnosticsHandler;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.*;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.management.MBeanServer;
import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


/**
 * Tests merging with a large number of members
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL,sequential=true)
public class LargeMergeTest {
    static final int NUM=50; // number of members
    static final int MAX_PARTICIPANTS_IN_MERGE=NUM / 3;

    protected final JChannel[]     channels=new JChannel[NUM];
    protected MyDiagnosticsHandler handler;
    protected ThreadPoolExecutor   oob_thread_pool;
    protected ThreadPoolExecutor   thread_pool;



    @BeforeMethod
    void setUp() throws Exception {
        handler=new MyDiagnosticsHandler(InetAddress.getByName("224.0.75.75"), 7500,
                                         LogFactory.getLog(DiagnosticsHandler.class),
                                         new DefaultSocketFactory(),
                                         new DefaultThreadFactory("", false));
        handler.start();
        
        TimeScheduler timer=new TimeScheduler2(new DefaultThreadFactory("Timer", true, true),
                                               5,20,
                                               3000, 5000, "abort");

        oob_thread_pool=new ThreadPoolExecutor(5, Math.max(5, NUM/4), 3000, TimeUnit.MILLISECONDS,
                                                                  new ArrayBlockingQueue<Runnable>(NUM * NUM));
        oob_thread_pool.setRejectedExecutionHandler(new ThreadPoolExecutor.DiscardPolicy());

        thread_pool=new ThreadPoolExecutor(5, Math.max(5, NUM/4), 3000, TimeUnit.MILLISECONDS,
                                                              new ArrayBlockingQueue<Runnable>(NUM * NUM));
        thread_pool.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());


        MBeanServer server=Util.getMBeanServer();

        System.out.print("Connecting channels: ");
        for(int i=0; i < NUM; i++) {
            SHARED_LOOPBACK shared_loopback=(SHARED_LOOPBACK)new SHARED_LOOPBACK().setValue("enable_bundling", false);
            // UDP shared_loopback=(UDP)new UDP().setValue("enable_bundling", false);
            shared_loopback.setLoopback(false);
            shared_loopback.setTimer(timer);
            shared_loopback.setOOBThreadPool(oob_thread_pool);
            shared_loopback.setDefaultThreadPool(thread_pool);
            shared_loopback.setDiagnosticsHandler(handler);

            channels[i]=Util.createChannel(shared_loopback,
                                           new DISCARD().setValue("discard_all",true),
                                           new PING().setValue("timeout",1).setValue("num_initial_members",50)
                                             .setValue("force_sending_discovery_rsps", true),
                                           new MERGE3().setValue("min_interval",1000)
                                             .setValue("max_interval",5000)
                                             .setValue("max_participants_in_merge", MAX_PARTICIPANTS_IN_MERGE),
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
                                             .setValue("leave_timeout",100)
                                             .setValue("log_view_warnings",false)
                                             .setValue("view_ack_collection_timeout",2000)
                                             .setValue("log_collect_msgs",false));
            channels[i].setName(String.valueOf((i + 1)));

            JmxConfigurator.registerChannel(channels[i], server, "channel-" + (i+1), "LargeMergeTest", true);
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
        handler.destroy();
    }



    public void testClusterFormationAfterMerge() {
        System.out.println("\nEnabling message traffic between members to start the merge");
        for(JChannel ch: channels) {
            Discovery ping=(Discovery)ch.getProtocolStack().findProtocol(PING.class);
            ping.setTimeout(3000);
            DISCARD discard=(DISCARD)ch.getProtocolStack().findProtocol(DISCARD.class);
            discard.setDiscardAll(false);
        }

        boolean merge_completed=true;
        for(int i=0; i < NUM; i++) {
            merge_completed=true;
            System.out.println();

            Map<ViewId,Integer> views=new HashMap<ViewId,Integer>();

            for(JChannel ch: channels) {
                ViewId view_id=ch.getView().getViewId();
                Integer val=views.get(view_id);
                if(val == null) {
                    views.put(view_id, 1);
                }
                else {
                    views.put(view_id, val +1);
                }


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
            MERGE2 merge=(MERGE2)ch.getProtocolStack().findProtocol(MERGE2.class);
            if(merge != null && merge.isMergeTaskRunning())
                merge_task_running++;

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
        sb.append("thread_pool: threads=" + thread_pool.getPoolSize() + ", queue=" + thread_pool.getQueue().size() +
                    ", largest threads=" + thread_pool.getLargestPoolSize() + "\n");
        sb.append("oob_thread_pool: threads=" + oob_thread_pool.getPoolSize() + ", queue=" + oob_thread_pool.getQueue().size() +
                    ", largest threads=" + oob_thread_pool.getLargestPoolSize() + "\n");
        return sb.toString();
    }

    protected static class MyDiagnosticsHandler extends DiagnosticsHandler {

        protected MyDiagnosticsHandler(InetAddress diagnostics_addr, int diagnostics_port, Log log, SocketFactory socket_factory, ThreadFactory thread_factory) {
            super(diagnostics_addr,diagnostics_port,log,socket_factory,thread_factory);
        }

        public void start() throws IOException {
            super.start();
        }

        public void stop() {
        }

        public void destroy() {
            super.stop();
        }
    }


    @Test(enabled=false)
    public static void main(String args[]) throws Exception {
        LargeMergeTest test=new LargeMergeTest();
        test.setUp();
        test.testClusterFormationAfterMerge();
        test.tearDown();
    }
}
