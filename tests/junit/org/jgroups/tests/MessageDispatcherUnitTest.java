package org.jgroups.tests;

import org.jgroups.BytesMessage;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.blocks.MessageDispatcher;
import org.jgroups.blocks.RequestHandler;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.util.ByteArray;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tests return values from MessageDispatcher.castMessage()
 * 
 * @author Bela Ban
 */
@Test(groups=Global.STACK_DEPENDENT, singleThreaded=true)
public class MessageDispatcherUnitTest extends ChannelTestBase {
    protected MessageDispatcher      da, db;
    protected JChannel               a, b;
    protected static final ByteArray buf;

    static {
        byte[] data="bla".getBytes();
        buf=new ByteArray(data, 0, data.length);
    }

    @BeforeClass
    protected void setUp() throws Exception {
        a=createChannel().name("A");
        GMS gms=a.getProtocolStack().findProtocol(GMS.class);
        if(gms != null)
            gms.printLocalAddress(false);
        da=new MessageDispatcher(a);
        a.connect("MessageDispatcherUnitTest");
    }

    @AfterClass
    protected void tearDown() throws Exception {
        Util.close(da, a);
    }

    @AfterMethod
    protected void closeSecondChannel() {
        Util.close(db,b);
    }

    public void testNullMessageToSelf() throws Exception {
        MyHandler handler=new MyHandler(null);
        da.setRequestHandler(handler);
        RspList<byte[]> rsps=da.castMessage(null, new BytesMessage(null, buf),
                                            new RequestOptions(ResponseMode.GET_ALL, 0));
        System.out.println("rsps:\n" + rsps);
        assert rsps != null;
        assert 1 == rsps.size();
        Object obj=rsps.getFirst();
        assert obj == null;
    }

    public void test200ByteMessageToSelf() throws Exception {
        sendMessage(200);
    }

    public void test2000ByteMessageToSelf() throws Exception {
        sendMessage(2000);
    }

    public void test20000ByteMessageToSelf() throws Exception {
        sendMessage(20000);
    }

    public void testNullMessageToAll() throws Exception {
        da.setRequestHandler(new MyHandler(null));
        b=createChannel().name("B");
        long stop, start=System.currentTimeMillis();
        db=new MessageDispatcher(b, new MyHandler(null));
        stop=System.currentTimeMillis();
        b.connect("MessageDispatcherUnitTest");
        Assert.assertEquals(2,b.getView().size());
        System.out.println("view: " + b.getView());

        System.out.println("casting message");
        start=System.currentTimeMillis();
        RspList<byte[]> rsps=da.castMessage(null, new BytesMessage(null, buf),
                                            new RequestOptions(ResponseMode.GET_ALL, 0));
        stop=System.currentTimeMillis();
        System.out.println("rsps:\n" + rsps);
        System.out.println("call took " + (stop - start) + " ms");
        assert rsps != null;
        assert 2 == rsps.size();
        Rsp<byte[]> rsp=rsps.get(a.getAddress());
        assert rsp != null;
        Object ret=rsp.getValue();
        assert ret == null;

        rsp=rsps.get(b.getAddress());
        assert rsp != null;
        ret=rsp.getValue();
        assert ret == null;
    }


    public void test200ByteMessageToAll() throws Exception {
        sendMessageToBothChannels(200);
    }

    public void test2000ByteMessageToAll() throws Exception {
        sendMessageToBothChannels(2000);
    }

    public void test20000ByteMessageToAll() throws Exception {
        sendMessageToBothChannels(20000);
    }

    /**
     * In this scenario we block the second member so to verify the sender actually waits;
     * this won't trigger a timeout but will produce a response having "received=false".
     * It's hard to otherwise make sure casting isn't being done asynchronously.
     */
    public void testBlockingSecondMember() throws Exception {
        RequestOptions requestOptions = RequestOptions.SYNC()
                .exclusionList(a.getAddress())//redundant - simplifies debugging
                .setMode(ResponseMode.GET_ALL)//redundant - implied by SYNC()
                .setTransientFlags(Message.TransientFlag.DONT_LOOPBACK)//redundant - self is excluded
                .setTimeout(1000); //Speed up the test execution
        b = createChannel().name("B");
        BlockableRequestHandler blockableHandler = new BlockableRequestHandler();
        db= new MessageDispatcher(b).setRequestHandler(blockableHandler);
        b.connect("MessageDispatcherUnitTest");
        assert 2 == b.getView().size();
        blockableHandler.installThreadTrap();
        try {
            RspList<Object> rsps = da.castMessage(null, new BytesMessage(null, buf), requestOptions);
            System.out.printf("responses: %s\n", rsps);
            assert 1 == rsps.size();
            Rsp rsp = rsps.get(b.getAddress());
            assert rsp != null;
            assert !rsp.wasReceived();
            assert !rsp.wasSuspected();
        } catch (Exception e) {
            Assert.fail("exception returned by castMessage", e);
        } finally {
            blockableHandler.releaseBlockedThreads();
        }
        assert blockableHandler.receivedAnything();
    }

    private void sendMessage(int size) throws Exception {
        long start, stop;
        MyHandler handler=new MyHandler(new byte[size]);
        da.setRequestHandler(handler);

        Message msg=new BytesMessage(null, buf);
        RequestOptions opts=new RequestOptions(ResponseMode.GET_ALL, 0);
        start=System.currentTimeMillis();
        RspList<byte[]> rsps=da.castMessage(null, msg, opts);
        stop=System.currentTimeMillis();
        System.out.println("rsps:\n" + rsps);
        System.out.println("call took " + (stop - start) + " ms");
        assert rsps != null;
        Assert.assertEquals(1, rsps.size());
        byte[] tmp=rsps.getFirst();
        assert tmp != null;
        Assert.assertEquals(size, tmp.length);
    }

    private void sendMessageToBothChannels(int size) throws Exception {
        long start, stop;
        da.setRequestHandler(new MyHandler(new byte[size]));

        b=createChannel();
        b.setName("B");
        db=new MessageDispatcher(b, new MyHandler(new byte[size]));
        b.connect("MessageDispatcherUnitTest");
        assert 2 == b.getView().size();

        System.out.println("casting message");
        start=System.currentTimeMillis();

        // RspList<Object> rsps=d1.castMessage(null, buf, new RequestOptions(ResponseMode.GET_ALL, 0));
        RspList<Object> rsps=da.castMessage(null, new BytesMessage(null, buf), new RequestOptions(ResponseMode.GET_ALL, 0));

        stop=System.currentTimeMillis();
        System.out.println("rsps:\n" + rsps);
        System.out.println("call took " + (stop - start) + " ms");
        assert rsps != null;
        Assert.assertEquals(2,rsps.size());
        Rsp rsp=rsps.get(a.getAddress());
        assert rsp != null;
        byte[] ret=(byte[])rsp.getValue();
        Assert.assertEquals(size, ret.length);

        rsp=rsps.get(b.getAddress());
        assert rsp != null;
        ret=(byte[])rsp.getValue();
        Assert.assertEquals(size, ret.length);

        Util.close(b);
    }


    private static class MyHandler implements RequestHandler {
        byte[] retval=null;

        public MyHandler(byte[] retval) {
            this.retval=retval;
        }

        public Object handle(Message msg) throws Exception {
            return retval;
        }
    }

    private static final class BlockableRequestHandler implements RequestHandler {

        private final AtomicReference<CountDownLatch> threadTrap = new AtomicReference<>();
        private final AtomicBoolean receivedAnything = new AtomicBoolean(false);

        @Override
        public Object handle(Message msg) throws Exception {
            receivedAnything.set(true);
            countDownAndJoin();
            return "ok";
        }

        public boolean receivedAnything() {
            return receivedAnything.get();
        }

        public void installThreadTrap() {
            boolean ok = threadTrap.compareAndSet(null, new CountDownLatch(2));
            if (!ok) {
                throw new IllegalStateException("Resetting a latch without having released the previous one! Illegal as some threads might be stuck.");
            }
        }

        public void releaseBlockedThreads() {
            final CountDownLatch latch = threadTrap.getAndSet(null);
            if (latch!=null) {
                while (latch.getCount() > 0) {
                    latch.countDown();
                }
            }
        }

        public void countDownAndJoin() {
            final CountDownLatch latch = threadTrap.get();
            if (latch==null) return;
            System.out.println( "Blocking on incoming message [PREJOIN] Timestamp: " + System.nanoTime() );
            try {
                latch.countDown();
                //Wait "forever" until we are awoken;
                //cap the definition of "forever" to 2 minutes to abort the test if something goes wrong
                //but this should not be necessary if the test is written correctly:
                //the main test thread will release the latch sooner so a large timeout should not
                //have a negative impact on the testsuite duration.
                latch.await( 1, TimeUnit.MINUTES );
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.out.println("Early termination. Test killed?");
            }
            System.out.println( "Blocking on incoming message [POSTJOIN] Timestamp: " + System.nanoTime() );
        }
    }
}
