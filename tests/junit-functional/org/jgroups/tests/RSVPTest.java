package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK;
import org.jgroups.stack.DiagnosticsHandler;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.*;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.management.MBeanServer;
import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


/**
 * Tests tthe {@link RSVP} protocol
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL,sequential=true)
public class RSVPTest {
    static final int               NUM=5; // number of members

    protected final JChannel[]     channels=new JChannel[NUM];
    protected final MyReceiver[]   receivers=new MyReceiver[NUM];
    protected MyDiagnosticsHandler handler;
    protected ThreadPoolExecutor   oob_thread_pool;
    protected ThreadPoolExecutor   thread_pool;



    @BeforeMethod
    void setUp() throws Exception {
        handler=new MyDiagnosticsHandler(InetAddress.getByName("224.0.75.75"), 7500,
                                         LogFactory.getLog(DiagnosticsHandler.class),
                                         new DefaultSocketFactory(),
                                         new DefaultThreadFactory(Util.getGlobalThreadGroup(), "", false));
        handler.start();
        
        ThreadGroup test_group=new ThreadGroup("LargeMergeTest");
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
                                           new NAKACK().setValue("use_mcast_xmit",false)
                                             .setValue("discard_delivered_msgs",true)
                                             .setValue("log_discard_msgs",false).setValue("log_not_found_msgs",false)
                                             .setValue("xmit_table_num_rows",5)
                                             .setValue("xmit_table_msgs_per_row",10),
                                           // new UNICAST(),
                                           new UNICAST2().setValue("xmit_table_num_rows",5).setValue("exponential_backoff", 300)
                                             .setValue("xmit_table_msgs_per_row",10)
                                             .setValue("conn_expiry_timeout", 10000)
                                             .setValue("stable_interval", 30000)
                                             .setValue("max_bytes", 50000),
                                           new RSVP().setValue("timeout", 10000).setValue("throw_exception_on_timeout", false),
                                           // new STABLE().setValue("max_bytes",500000).setValue("desired_avg_gossip", 60000),
                                           new GMS().setValue("print_local_addr",false)
                                             .setValue("leave_timeout",100)
                                             .setValue("log_view_warnings",false)
                                             .setValue("view_ack_collection_timeout",2000)
                                             .setValue("log_collect_msgs",false));
            channels[i].setName(String.valueOf((i + 1)));
            receivers[i]=new MyReceiver();
            channels[i].setReceiver(receivers[i]);
            JmxConfigurator.registerChannel(channels[i], server, "channel-" + (i+1), "RSVPTest", true);
            channels[i].connect("RSVPTest");
            System.out.print(i + 1 + " ");
        }
        System.out.println("");
    }

    @AfterMethod
    void tearDown() throws Exception {
        for(int i=NUM-1; i >= 0; i--) {
            ProtocolStack stack=channels[i].getProtocolStack();
            String cluster_name=channels[i].getClusterName();
            JmxConfigurator.unregisterChannel(channels[i], Util.getMBeanServer(), "channel-" + (i+1),cluster_name);
            stack.stopStack(cluster_name);
            stack.destroy();
        }
        handler.destroy();
    }



    public void testSynchronousMulticastSend() throws Exception {
        for(JChannel ch: channels)
            assert ch.getView().size() == NUM : "channel " + ch.getAddress() + ": view  is " + ch.getView();

        // test with a multicast message:
        short value=(short)Math.abs((short)Util.random(10000));
        Message msg=new Message(null, null, value);
        msg.setFlag(Message.Flag.RSVP);

        DISCARD discard=(DISCARD)channels[0].getProtocolStack().findProtocol(DISCARD.class);
        discard.setDropDownMulticasts(1);

        long start=System.currentTimeMillis();
        channels[0].send(msg);

        long diff=System.currentTimeMillis() - start;
        System.out.println("sending took " + diff + " ms");

        int cnt=1;
        for(MyReceiver receiver: receivers) {
            System.out.println("receiver " + cnt++ + ": value=" + receiver.getValue());
        }
        for(MyReceiver receiver: receivers) {
            long tmp_value=receiver.getValue();
            assert tmp_value == value  : "value is " + tmp_value + ", but should be " + value;
        }
    }

    public void testSynchronousUnicastSend() throws Exception {
        for(JChannel ch: channels)
            assert ch.getView().size() == NUM : "channel " + ch.getAddress() + ": view  is " + ch.getView();

        // test with a multicast message:
        short value=(short)Math.abs((short)Util.random(10000));
        Message msg=new Message(channels[1].getAddress(), null, value);
        msg.setFlag(Message.Flag.RSVP);

        DISCARD discard=(DISCARD)channels[0].getProtocolStack().findProtocol(DISCARD.class);
        discard.setDropDownUnicasts(1);
        
        long start=System.currentTimeMillis();
        channels[0].send(msg);

        long diff=System.currentTimeMillis() - start;
        System.out.println("sending took " + diff + " ms");

        System.out.println("receiver: value=" + receivers[1].getValue());

        long tmp_value=receivers[1].getValue();
        assert tmp_value == value : "value is " + tmp_value + ", but should be " + value;
    }




    protected static class MyReceiver extends ReceiverAdapter {
        short value=0;

        public short getValue() {return value;}

        public void receive(Message msg) {
            value=(Short)msg.getObject();
        }
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
