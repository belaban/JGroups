package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.DiagnosticsHandler;
import org.jgroups.util.*;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


/**
 * Tests merging with a large number of members
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class LargeJoinTest {
    static final int NUM=50; // number of members

    protected final JChannel[]     channels=new JChannel[NUM];
    protected MyDiagnosticsHandler handler;
    protected Connector[]          connectors=new Connector[NUM];
    protected ThreadPoolExecutor   oob_thread_pool, thread_pool, internal_thread_pool;



    @BeforeMethod
    void setUp() throws Exception {
        handler=new MyDiagnosticsHandler(InetAddress.getByName("224.0.75.75"), 7500,
                                         LogFactory.getLog(DiagnosticsHandler.class),
                                         new DefaultSocketFactory(),
                                         new DefaultThreadFactory("", false));
        handler.start();
        
        TimeScheduler timer=new TimeScheduler2(new DefaultThreadFactory("Timer", true, true),
                                               2,6,
                                               3000, 5000, "abort");

        oob_thread_pool=new ThreadPoolExecutor(5, Math.max(5, NUM/4), 3000, TimeUnit.MILLISECONDS,
                                                                  new ArrayBlockingQueue<Runnable>(NUM * NUM));
        oob_thread_pool.setRejectedExecutionHandler(new ThreadPoolExecutor.DiscardPolicy());

        thread_pool=new ThreadPoolExecutor(5, Math.max(5, NUM/4), 3000, TimeUnit.MILLISECONDS,
                                                              new ArrayBlockingQueue<Runnable>(NUM * NUM));
        thread_pool.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());

        internal_thread_pool=new ThreadPoolExecutor(5, Math.max(5, NUM/4), 3000, TimeUnit.MILLISECONDS,
                                                    new ArrayBlockingQueue<Runnable>(NUM * NUM));
        internal_thread_pool.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());


        // MBeanServer server=Util.getMBeanServer();

        System.out.println("Creating channels");
        for(int i=0; i < NUM; i++) {
           /* SHARED_LOOPBACK shared_loopback=new SHARED_LOOPBACK();
            shared_loopback.setTimer(timer);
            shared_loopback.setOOBThreadPool(oob_thread_pool);
            shared_loopback.setDefaultThreadPool(thread_pool);
            shared_loopback.setDiagnosticsHandler(handler);*/

            // TP transport=new TCP();
            TP transport=(TP)new UDP().setValue("ip_mcast", false);
            transport.setValue("bind_port", 7700 + i + 1);
            transport.setValue("port_range", 0);
            transport.setValue("enable_diagnostics", false);
            transport.setTimer(timer);
            transport.setOOBThreadPool(oob_thread_pool);
            transport.setDefaultThreadPool(thread_pool);
            transport.setInternalThreadPool(internal_thread_pool);
            transport.setDiagnosticsHandler(handler);
            transport.setValue("internal_thread_pool_min_threads", 1)
              .setValue("internal_thread_pool_max_threads", 2)
              .setValue("bundler_type", "sender-sends");
              // .setValue("use_send_queues", false)
              // .setValue("tcp_nodelay", true)
              // .setValue("conn_expire_time", 10000)
              // .setValue("reaper_interval", 5000);

            System.out.print(i + 1 + " ");
            channels[i]=Util.createChannel(transport,
                                           // new SHARED_LOOPBACK_PING(),

                                           new GOOGLE_PING().setValue("location", "jgroups-bucket")
                                             .setValue("access_key", "GOOGI7QGNPOE3JLTZ4FZ")
                                             .setValue("secret_access_key", "xWB2gSZxrRRQPsdEKV6X5Qk6tCKX2EacQaHrb9rD")
                                             .setValue("skip_bucket_existence_check", true),
                                             // new TCPPING().setValue("initial_hosts", Arrays.asList(new IpAddress("192.168.1.5",7800))),

                                           new MERGE3().setValue("min_interval", 10000)
                                             .setValue("max_interval",50000),
                                           new NAKACK2().setValue("use_mcast_xmit",false)
                                             .setValue("discard_delivered_msgs",true)
                                             .setValue("log_discard_msgs",false).setValue("log_not_found_msgs",false)
                                             .setValue("xmit_table_num_rows",5)
                                             .setValue("xmit_table_msgs_per_row",10),
                                           new UNICAST3().setValue("xmit_table_num_rows",5)
                                             .setValue("xmit_table_msgs_per_row",10)
                                             .setValue("conn_expiry_timeout", 10000)
                                             .setValue("conn_close_timeout", 10000),
                                           new STABLE().setValue("max_bytes",500000),
                                           new GMS().setValue("print_local_addr",false)
                                             .setValue("join_timeout", 20000)
                                             .setValue("leave_timeout",100)
                                             .setValue("log_view_warnings",false)
                                             .setValue("view_ack_collection_timeout",2000)
                                             .setValue("log_collect_msgs",false)
                                             .setValue("view_bundling", true).setValue("max_bundling_time", 1000));

            channels[i].setName(String.valueOf((i + 1)));
            channels[i].addAddressGenerator(new OneTimeAddressGenerator(i+1));
            connectors[i]=new Connector(channels[i]);
            // JmxConfigurator.registerChannel(channels[i], server, "channel-" + (i + 1), "LargeJoinTest", true);
        }

        System.out.println("");
    }

   /* @AfterMethod
    void tearDown() throws Exception {
        System.out.println("Closing channels");
        for(int i=NUM-1; i >= 0; i--) {
            final JChannel ch=channels[i];
            System.out.print((i+1) + " ");
            new Thread() {
                public void run() {
                    Util.close(ch);
                }
            }.start();
        }
        handler.destroy();
    }*/



    public void testLargeJoin() throws Exception {
        channels[0].connect("LargeJoinTest");

        Util.sleep(1000);
        System.out.print("Connecting channels: ");
        for(int i=1; i < NUM; i++) {
            connectors[i].start();
        }

        Util.sleep(2000);
        System.out.println("\n");
        for(JChannel ch: channels) {
            /*String name=ch.getName();
            UUID uuid=(UUID)ch.getAddress();
            PhysicalAddress phys_addr=ch.getProtocolStack().getTransport().getPhysAddr();
            System.out.println(name + " " + uuid.toStringLong() + " " + phys_addr);*/
        }

        for(int i=1; i < NUM; i++) {
            connectors[i].join();
            if(connectors[i].getException() != null)
                throw new Exception("Connector " + (i+1) + " failed: " + connectors[i].getException());
        }

        Util.waitUntilAllChannelsHaveSameSize(10000, 500, channels);
        System.out.println("\nView of first: " + channels[0].getView());

        System.out.println("confirming that all channels have a view of " + NUM);
        for(JChannel ch: channels) {
            if(ch.getView().size() == NUM)
                System.out.print(".");
            else
                throw new Exception("Channel " + ch.getAddress() + " has a view of " + ch.getView().size() + ": " + ch.getView());
        }
        System.out.println("\nOK");
    }





    protected static class Connector extends Thread {
        protected final JChannel ch;
        protected Throwable      t;

        public Connector(JChannel ch) {
            this.ch=ch;
        }

        public Throwable getException() {
            return t;
        }

        public void run() {
            try {
                // Util.sleepRandom(100,5000);
                ch.connect("LargeJoinTest");
                System.out.print(ch.getAddress() + " ");
            }
            catch(Throwable e) {
                t=e;
            }
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
        LargeJoinTest test=new LargeJoinTest();
        test.setUp();
        test.testLargeJoin();
        // test.tearDown();
    }
}
