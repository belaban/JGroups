package org.jgroups.tests;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.management.MBeanServer;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.blocks.MessageDispatcher;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.DISCARD;
import org.jgroups.protocols.MERGE2;
import org.jgroups.protocols.PING;
import org.jgroups.protocols.RSVP;
import org.jgroups.protocols.SHARED_LOOPBACK;
import org.jgroups.protocols.UNICAST2;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.stack.DiagnosticsHandler;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.DefaultSocketFactory;
import org.jgroups.util.DefaultThreadFactory;
import org.jgroups.util.RspList;
import org.jgroups.util.SocketFactory;
import org.jgroups.util.ThreadFactory;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.TimeScheduler2;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Tests tthe {@link org.jgroups.protocols.RSVP} protocol
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL,sequential=true)
public class MessageDispatcherRSVPTest {
    private static final Log log = LogFactory.getLog(MessageDispatcherRSVPTest.class);
    protected static final int     NUM=2; // number of members
    protected final JChannel[]     channels=new JChannel[NUM];
    protected final MessageDispatcher[] dispatchers=new MessageDispatcher[NUM];
    protected MyDiagnosticsHandler handler;
    protected ThreadPoolExecutor   oob_thread_pool;
    protected ThreadPoolExecutor   thread_pool;



    @BeforeMethod
    void setUp() throws Exception {
        handler=new MyDiagnosticsHandler(InetAddress.getByName("224.0.75.75"), 7500,
                                         LogFactory.getLog(DiagnosticsHandler.class),
                                         new DefaultSocketFactory(),
                                         new DefaultThreadFactory(new ThreadGroup("MessageDispatcherRSVPTest"), "", false));
        handler.start();

        ThreadGroup test_group=new ThreadGroup("MessageDispatcherRSVPTest");
        TimeScheduler timer=new TimeScheduler2(new DefaultThreadFactory(test_group, "Timer", true, true),
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
                                           new DISCARD(),
                                           new PING().setValue("timeout",1000).setValue("num_initial_members",NUM)
                                             .setValue("force_sending_discovery_rsps", true),
                                           new MERGE2().setValue("min_interval", 1000).setValue("max_interval", 3000),
                                           new NAKACK2().setValue("use_mcast_xmit",false)
                                             .setValue("discard_delivered_msgs",true)
                                             .setValue("log_discard_msgs", false).setValue("log_not_found_msgs", false)
                                             .setValue("xmit_table_num_rows",5)
                                             .setValue("xmit_table_msgs_per_row", 10),
                                           // new UNICAST(),
                                           new UNICAST2().setValue("xmit_table_num_rows",5).setValue("xmit_interval", 300)
                                             .setValue("xmit_table_msgs_per_row",10)
                                             .setValue("conn_expiry_timeout", 10000)
                                             .setValue("stable_interval", 30000)
                                             .setValue("max_bytes", 50000),
                                           new RSVP().setValue("timeout", 10000),
                                           // new STABLE().setValue("max_bytes",500000).setValue("desired_avg_gossip", 60000),
                                           new GMS().setValue("print_local_addr",false)
                                             .setValue("leave_timeout",100)
                                             .setValue("log_view_warnings",false)
                                             .setValue("view_ack_collection_timeout",2000)
                                             .setValue("log_collect_msgs",false));
            channels[i].setName(String.valueOf((i + 1)));
            dispatchers[i]=new MessageDispatcher(channels[i], null, null);
            JmxConfigurator.registerChannel(channels[i], server, "channel-" + (i+1), "MessageDispatcherRSVPTest", true);
            channels[i].connect("MessageDispatcherRSVPTest");
            System.out.print(i + 1 + " ");
            if(i == 0)
                Util.sleep(2000);
        }
        Util.waitUntilAllChannelsHaveSameSize(30000, 1000, channels);
        System.out.println("");
    }

    @AfterMethod
    void tearDown() throws Exception {
        for(int i=NUM-1; i >= 0; i--) {
            ProtocolStack stack=channels[i].getProtocolStack();
            String cluster_name=channels[i].getClusterName();
            if(channels[i].isOpen())
                JmxConfigurator.unregisterChannel(channels[i], Util.getMBeanServer(), "channel-" + (i+1),cluster_name);
            stack.stopStack(cluster_name);
            stack.destroy();
        }
        handler.destroy();
    }



    public void testSendingMessageOnClosedChannel() throws Exception {
        channels[0].close();

        short value=(short)Math.abs((short)Util.random(10000));
        Message msg=new Message(null, null, value);

        long nanosStart = System.nanoTime();
        RspList<Object> rsps = dispatchers[0].castMessage(Collections.singleton(channels[1].getAddress()), msg, RequestOptions.SYNC());
        log.debug("Received responses: " + rsps);

        long nanosEnd = System.nanoTime();
        long seconds = TimeUnit.NANOSECONDS.toSeconds(nanosEnd - nanosStart);
        Assert.assertTrue(seconds < 2);
    }

    public void testSendingMessageOnClosedChannelRSVP() throws Exception {
        channels[0].close();

        short value=(short)Math.abs((short)Util.random(10000));
        Message msg=new Message(null, null, value);
        msg.setFlag(Message.Flag.RSVP);

        long nanosStart = System.nanoTime();
        RspList<Object> rsps = dispatchers[0].castMessage(Collections.singleton(channels[1].getAddress()), msg, RequestOptions.SYNC());
        log.debug("Received responses: " + rsps);

        long nanosEnd = System.nanoTime();
        long seconds = TimeUnit.NANOSECONDS.toSeconds(nanosEnd - nanosStart);
        Assert.assertTrue(seconds < 2);
    }


    protected static class MyDiagnosticsHandler extends DiagnosticsHandler {

        protected MyDiagnosticsHandler(InetAddress diagnostics_addr, int diagnostics_port, Log log, SocketFactory socket_factory, ThreadFactory thread_factory) {
            super(diagnostics_addr,diagnostics_port,log,socket_factory,thread_factory);
        }

        public void start() throws IOException {super.start();}
        public void stop() {}
        public void destroy() {super.stop();}
    }
}
