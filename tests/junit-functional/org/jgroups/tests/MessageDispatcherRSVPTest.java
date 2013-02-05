package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.blocks.MessageDispatcher;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.stack.DiagnosticsHandler;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.*;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


/**
 * Tests the {@link org.jgroups.protocols.RSVP} protocol
 * @author Dan Berindei
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL,sequential=true)
public class MessageDispatcherRSVPTest {
    protected static final int          NUM=2; // number of members
    protected final JChannel[]          channels=new JChannel[NUM];
    protected final MessageDispatcher[] dispatchers=new MessageDispatcher[NUM];
    protected MyDiagnosticsHandler      handler;
    protected ThreadPoolExecutor        oob_thread_pool;
    protected ThreadPoolExecutor        thread_pool;



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


        System.out.print("Connecting channels: ");
        for(int i=0; i < NUM; i++) {
            SHARED_LOOPBACK shared_loopback=(SHARED_LOOPBACK)new SHARED_LOOPBACK().setValue("enable_bundling", false);
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
                                             .setValue("log_discard_msgs", false).setValue("log_not_found_msgs", false),
                                           new UNICAST2().setValue("xmit_table_num_rows",5).setValue("xmit_interval", 300),
                                           new RSVP().setValue("timeout", 10000).setValue("throw_exception_on_timeout", true),
                                           new GMS().setValue("print_local_addr",false)
                                             .setValue("leave_timeout",100)
                                             .setValue("log_view_warnings",false)
                                             .setValue("view_ack_collection_timeout",2000)
                                             .setValue("log_collect_msgs",false));
            channels[i].setName(String.valueOf((i + 1)));
            dispatchers[i]=new MessageDispatcher(channels[i], null, null);
            channels[i].connect("MessageDispatcherRSVPTest");
            System.out.print(i + 1 + " ");
            if(i == 0)
                Util.sleep(1000);
        }
        Util.waitUntilAllChannelsHaveSameSize(30000, 1000, channels);
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


    /**
     * First send a message, drop it (using DISCARD) and then close the channel. The caller invoking castMessage() should
     * get an exception, as the channel was closed
     */
    public void testCancellationByClosingChannel() throws Exception {
        testCancellationByClosing(false, // multicast
                                  new Closer(channels[0]));
    }

    public void testCancellationByClosingChannelUnicast() throws Exception {
        testCancellationByClosing(true, // unicast
                                  new Closer(channels[0]));
    }
    

    /**
     * Sends a message via the MessageDispatcher on a closed channel. This should immediately throw an exception.
     */
    public void testSendingMessageOnClosedChannel() throws Exception {
        // unicast
        sendMessageOnClosedChannel(new Message(channels[1].getAddress(), "bla"));

        // multicast
        sendMessageOnClosedChannel(new Message(null,"bla"));
    }

    public void testSendingMessageOnClosedChannelRSVP() throws Exception {
        // unicast
        Message msg=new Message(channels[1].getAddress(), null, "bla");
        msg.setFlag(Message.Flag.RSVP);
        sendMessageOnClosedChannel(msg);

        // multicast
        msg=new Message(null, "bla");
        msg.setFlag(Message.Flag.RSVP);
        sendMessageOnClosedChannel(msg);
    }

    protected void testCancellationByClosing(boolean unicast, Thread closer) throws Exception {
        DISCARD discard=(DISCARD)channels[0].getProtocolStack().findProtocol(DISCARD.class);
        discard.setDiscardAll(true);

        try {
            Address target=unicast? channels[1].getAddress() : null;
            Message msg=new Message(target, "bla");
            msg.setFlag(Message.Flag.RSVP);
            closer.start();
            if(unicast) {
                System.out.println("sending unicast message to " + target);
                dispatchers[0].sendMessage(msg, RequestOptions.SYNC());
                assert false: "sending the message on a closed channel should have thrown an exception";
            }
            else {
                System.out.println("sending multicast message");
                RspList<Object> rsps=dispatchers[0].castMessage(Collections.singleton(channels[1].getAddress()),msg,RequestOptions.SYNC());
                System.out.println("rsps = " + rsps);
                assert rsps.size() == 1;
                Rsp<Object> rsp=rsps.iterator().next();
                System.out.println("rsp = " + rsp);
                assert rsp.hasException();
                Throwable ex=rsp.getException();
                assert ex instanceof IllegalStateException;
            }
        }
        catch(IllegalStateException t) {
            System.out.println("received \"" + t + "\" as expected");
        }
    }


    protected void sendMessageOnClosedChannel(Message msg) throws Exception {
        channels[0].close();
        Address target=msg.getDest();
        try {
            if(target == null) { // multicast
                dispatchers[0].castMessage(Collections.singleton(channels[1].getAddress()), msg, RequestOptions.SYNC());
            }
            else {
                dispatchers[0].sendMessage(msg, RequestOptions.SYNC());
            }
            assert false: "sending the message on a closed channel should have thrown an exception";
        }
        catch(IllegalStateException t) {
            System.out.println("received \"" + t + "\" as expected");
        }
    }


    protected static class Closer extends Thread {
        protected final JChannel ch;

        public Closer(JChannel ch) {this.ch=ch;}

        public void run() {
            Util.sleep(2000);
            System.out.println("closing channel");
            Util.close(ch);
        };
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
;