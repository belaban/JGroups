package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.RpcDispatcher;
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
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


/**
 * Tests the {@link RSVP} protocol
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class RSVPTest {
    protected static final int     NUM=5; // number of members
    protected final JChannel[]     channels=new JChannel[NUM];
    protected final MyReceiver[]   receivers=new MyReceiver[NUM];
    protected MyDiagnosticsHandler handler;
    protected ThreadPoolExecutor   oob_thread_pool;
    protected ThreadPoolExecutor   thread_pool;



    @BeforeMethod
    void setup() throws Exception {
        handler=new MyDiagnosticsHandler(InetAddress.getByName("224.0.75.75"), 7500,
                                         LogFactory.getLog(DiagnosticsHandler.class),
                                         new DefaultSocketFactory(),
                                         new DefaultThreadFactory("", false));
        handler.start();
        
        TimeScheduler timer=new TimeScheduler3(new DefaultThreadFactory("Timer", true, true),
                                               5,20,
                                               3000, 5000, "abort");

        oob_thread_pool=new ThreadPoolExecutor(5, Math.max(5, NUM/4), 3000, TimeUnit.MILLISECONDS,
                                                                  new ArrayBlockingQueue<Runnable>(NUM * NUM));
        oob_thread_pool.setRejectedExecutionHandler(new ThreadPoolExecutor.DiscardPolicy());

        thread_pool=new ThreadPoolExecutor(5, Math.max(5, NUM/4), 3000, TimeUnit.MILLISECONDS,
                                                              new ArrayBlockingQueue<Runnable>(NUM * NUM));
        thread_pool.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());


        System.out.print("\nConnecting channels: ");
        for(int i=0; i < NUM; i++) {
            SHARED_LOOPBACK shared_loopback=new SHARED_LOOPBACK();
            shared_loopback.setTimer(timer);
            shared_loopback.setOOBThreadPool(oob_thread_pool);
            shared_loopback.setDefaultThreadPool(thread_pool);
            shared_loopback.setDiagnosticsHandler(handler);

            channels[i]=Util.createChannel(shared_loopback,
                                           new DISCARD(),
                                           new SHARED_LOOPBACK_PING(),
                                           new MERGE3().setValue("min_interval", 1000).setValue("max_interval", 3000),
                                           new NAKACK2().setValue("use_mcast_xmit", false)
                                             .setValue("discard_delivered_msgs", true)
                                             .setValue("log_discard_msgs", false).setValue("log_not_found_msgs", false)
                                             .setValue("xmit_table_num_rows", 5)
                                             .setValue("xmit_table_msgs_per_row", 10),
                                           new UNICAST3().setValue("xmit_table_num_rows", 5).setValue("xmit_interval", 300)
                                             .setValue("xmit_table_msgs_per_row", 10)
                                             .setValue("conn_expiry_timeout", 10000),
                                           new RSVP().setValue("timeout", 10000).setValue("throw_exception_on_timeout", false)
                                             .setValue("resend_interval", 500),
                                           new GMS().setValue("print_local_addr", false).setValue("join_timeout", 100)
                                             .setValue("leave_timeout", 100)
                                             .setValue("log_view_warnings", false)
                                             .setValue("view_ack_collection_timeout", 2000)
                                             .setValue("log_collect_msgs", false));
            channels[i].setName(String.valueOf((i + 1)));
            receivers[i]=new MyReceiver();
            channels[i].setReceiver(receivers[i]);
            channels[i].connect("RSVPTest");
            System.out.print(i + 1 + " ");
        }
        Util.waitUntilAllChannelsHaveSameView(30000, 1000, channels);
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
        // test with a unicast message:
        short value=(short)Math.abs((short)Util.random(10000));
        Message msg=new Message(channels[1].getAddress(), value).setFlag(Message.Flag.RSVP);

        DISCARD discard=(DISCARD)channels[0].getProtocolStack().findProtocol(DISCARD.class);
        discard.setDropDownUnicasts(1);
        
        long start=System.currentTimeMillis();
        channels[0].send(msg);

        long diff=System.currentTimeMillis() - start;
        System.out.println("sending took " + diff + " ms");

        // UNICAST3 retransmission will resend the message *not* RSVP
        for(int i=0; i < 20; i++) {
            if(receivers[1].getValue() == value)
                break;
            Util.sleep(500);
        }

        long tmp_value=receivers[1].getValue();
        assert tmp_value == value : "value is " + tmp_value + ", but should be " + value;
    }

    /** We block on an entry, but closing the channel cancels the task, so we return */
    public void testCancellationByClosingChannel() throws Exception {
        // test with a multicast message:
        short value=(short)Math.abs((short)Util.random(10000));
        Message msg=new Message(null, null, value).setFlag(Message.Flag.RSVP);

        DISCARD discard=(DISCARD)channels[0].getProtocolStack().findProtocol(DISCARD.class);
        discard.setDiscardAll(true);

        RSVP rsvp=(RSVP)channels[0].getProtocolStack().findProtocol(RSVP.class);
        rsvp.setValue("throw_exception_on_timeout", true).setValue("timeout", 5000).setValue("resend_interval", 500);

        try {
            Thread closer=new Thread() {
                public void run() {
                    Util.sleep(2000);
                    System.out.println("closer closing channel");
                    channels[0].close();
                }
            };
            closer.start();
            channels[0].send(msg); // this will be unsuccessful as the other 4 members won't receive it
            // test fails if we get a TimeoutException
        }
        finally {
            discard.setDiscardAll(false);
            rsvp.setValue("throw_exception_on_timeout", false);
        }
    }

    public String getGreeting() { // to be called via a remote RPC
        return "hello-" + (short)Util.random(1000);
    }

    public void testRpcWithFuture() throws Exception {
        final Method method=getClass().getMethod("getGreeting");
        RpcDispatcher[] dispatchers=new RpcDispatcher[channels.length];
        for(int i=0; i < dispatchers.length; i++) {
            channels[i].setReceiver(null);
            dispatchers[i]=new RpcDispatcher(channels[i], this);
            dispatchers[i].start();
        }

        DISCARD discard=(DISCARD)channels[0].getProtocolStack().findProtocol(DISCARD.class);
        discard.setDropDownMulticasts(1);
        RequestOptions opts=RequestOptions.SYNC().setFlags(Message.Flag.RSVP_NB);

        long start=System.currentTimeMillis();
        Future<RspList<String>> future=dispatchers[0].callRemoteMethodsWithFuture(null, new MethodCall(method), opts);
        long rpc_time=System.currentTimeMillis() - start;

        start=System.currentTimeMillis();
        RspList<String> rsps=future.get(3000, TimeUnit.MILLISECONDS);
        long get_time=System.currentTimeMillis() - start;

        System.out.printf("rsps=\n%s\nRPC time=%d ms, Get time=%d ms", rsps, rpc_time, get_time);

        assert rsps.size() == channels.length;
        for(Rsp rsp: rsps)
            assert rsp.wasReceived() && rsp.getValue() != null;
        assert rpc_time < 500; // take a GC into account
    }

    /** Tests that async RSVP tasks that are lost are removed after timeout ms */
    public void testAsyncLostRSVPMessages() throws Exception {
        // test with a multicast message:
        short value=(short)Math.abs((short)Util.random(10000));
        Message msg=new Message(null, value).setFlag(Message.Flag.RSVP_NB);

        DISCARD discard=(DISCARD)channels[0].getProtocolStack().findProtocol(DISCARD.class);
        discard.setDiscardAll(true);
        RSVP rsvp=(RSVP)channels[0].getProtocolStack().findProtocol(RSVP.class);
        rsvp.setValue("timeout", 2000).setValue("resend_interval", 200);
        channels[0].send(msg);
        assert rsvp.getPendingRsvpRequests() == 1;
        for(int i=0; i < 10; i++) {
            if(rsvp.getPendingRsvpRequests() == 0)
                break;
            Util.sleep(1000);
        }
        assert rsvp.getPendingRsvpRequests() == 0;
    }



    protected static class MyReceiver extends ReceiverAdapter {
        short value=0;
        public short getValue()           {return value;}
        public void  receive(Message msg) {value=(Short)msg.getObject();}
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
