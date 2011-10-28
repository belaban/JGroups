package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK;
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

    protected final JChannel[] channels=new JChannel[NUM];

    protected MyDiagnosticsHandler handler;



    @BeforeMethod
    void setUp() throws Exception {
        handler=new MyDiagnosticsHandler(InetAddress.getByName("224.0.75.75"), 7500,
                                         LogFactory.getLog(DiagnosticsHandler.class),
                                         new DefaultSocketFactory(),
                                         new DefaultThreadFactory(Util.getGlobalThreadGroup(), "", false));
        handler.start();
        
        ThreadGroup test_group=new ThreadGroup("LargeMergeTest");
        TimeScheduler timer=new TimeScheduler2(new DefaultThreadFactory(test_group, "Timer", true, true),
                                               5,10,
                                               3000, 1000);

        ThreadPoolExecutor oob_thread_pool=new ThreadPoolExecutor(5, 50, 3000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000));
        oob_thread_pool.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());

        ThreadPoolExecutor thread_pool=new ThreadPoolExecutor(5, 10, 3000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(10000));
        thread_pool.setRejectedExecutionHandler(new ThreadPoolExecutor.DiscardPolicy());


        MBeanServer server=Util.getMBeanServer();

        System.out.print("Connecting channels: ");
        for(int i=0; i < NUM; i++) {
            SHARED_LOOPBACK shared_loopback=(SHARED_LOOPBACK)new SHARED_LOOPBACK().setValue("enable_bundling", false);
            shared_loopback.setTimer(timer);
            shared_loopback.setOOBThreadPool(oob_thread_pool);
            shared_loopback.setDefaultThreadPool(thread_pool);
            shared_loopback.setDiagnosticsHandler(handler);

            channels[i]=Util.createChannel(shared_loopback,
                                           new DISCARD().setValue("discard_all",true),
                                           new PING().setValue("timeout",100).setValue("num_initial_members", 10),
                                           new MERGE2().setValue("min_interval",8000)
                                             .setValue("max_interval",15000).setValue("merge_fast", false),
                                           new NAKACK().setValue("use_mcast_xmit",false)
                                             .setValue("log_discard_msgs",false).setValue("log_not_found_msgs",false),
                                           new UNICAST(),
                                           new STABLE().setValue("max_bytes",50000),
                                           new GMS().setValue("print_local_addr",false)
                                             .setValue("leave_timeout",100)
                                             .setValue("log_view_warnings",false)
                                             .setValue("view_ack_collection_timeout",50)
                                             .setValue("log_collect_msgs",false)
                                             .setValue("merge_kill_timeout",10000)
                                             .setValue("merge_killer_interval",5000));
            channels[i].setName(String.valueOf((i + 1)));

            JmxConfigurator.registerChannel(channels[i], server, "channel-" + (i+1), channels[i].getClusterName(), true);
            channels[i].connect("LargeMergeTest");
            System.out.print(i + 1 + " ");
        }
        System.out.println("");
    }

    @AfterMethod
    void tearDown() throws Exception {
        for(int i=NUM-1; i >= 0; i--) {
            // Util.close(channels[i]);
            ProtocolStack stack=channels[i].getProtocolStack();
            String cluster_name=channels[i].getClusterName();
            stack.stopStack(cluster_name);
            stack.destroy();
        }
        handler.destroy();
    }



    public void testClusterFormationAfterMerge() {
        // Util.keyPress("<enter>");
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
