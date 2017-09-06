package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.blocks.MessageDispatcher;
import org.jgroups.blocks.RequestHandler;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.util.*;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Tests return values from MessageDispatcher.castMessage()
 * 
 * @author Bela Ban
 */
@Test(groups=Global.STACK_DEPENDENT, singleThreaded=true)
public class MessageDispatcherUnitTest extends ChannelTestBase {
    MessageDispatcher d1, d2;
    JChannel          a, b;

    @BeforeClass
    protected void setUp() throws Exception {
        a=createChannel(true, 2, "A");
        GMS gms=(GMS)a.getProtocolStack().findProtocol(GMS.class);
        if(gms != null)
            gms.setPrintLocalAddress(false);
        d1=new MessageDispatcher(a, null, null, null);
        a.connect("MessageDispatcherUnitTest");
    }

    @AfterClass
    protected void tearDown() throws Exception {
        d1.stop();
        a.close();
        Util.sleep(500);
    }

    @AfterMethod
    protected void closeSecondChannel() {
        if(b != null) {
            d2.stop();
            b.close();
            Util.sleep(500);
        }
    }

    public void testNullMessageToSelf() throws Exception {
        MyHandler handler=new MyHandler(null);
        d1.setRequestHandler(handler);
        RspList rsps=d1.castMessage(null, new Message(), new RequestOptions(ResponseMode.GET_ALL, 0));
        System.out.println("rsps:\n" + rsps);
        assertNotNull(rsps);
        Assert.assertEquals(1, rsps.size());
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
        d1.setRequestHandler(new MyHandler(null));
        b=createChannel(a, "B");
        long stop, start=System.currentTimeMillis();
        d2=new MessageDispatcher(b, null, null, new MyHandler(null));
        stop=System.currentTimeMillis();
        b.connect("MessageDispatcherUnitTest");
        Assert.assertEquals(2,b.getView().size());
        System.out.println("view: " + b.getView());

        System.out.println("casting message");
        start=System.currentTimeMillis();
        RspList rsps=d1.castMessage(null, new Message(), new RequestOptions(ResponseMode.GET_ALL, 0));
        stop=System.currentTimeMillis();
        System.out.println("rsps:\n" + rsps);
        System.out.println("call took " + (stop - start) + " ms");
        assertNotNull(rsps);
        Assert.assertEquals(2,rsps.size());
        Rsp rsp=rsps.get(a.getAddress());
        assertNotNull(rsp);
        Object ret=rsp.getValue();
        assert ret == null;

        rsp=rsps.get(b.getAddress());
        assertNotNull(rsp);
        ret=rsp.getValue();
        assert ret == null;

        Util.close(b);
    }

    /**
     * Tests MessageDispatcher.castMessageXX() with a message whose destination is not null:
     * https://issues.jboss.org/browse/JGRP-1617
     */
    public void testCastMessageWithNonNullDest() throws Exception {
        b=createChannel(a, "B");
        d2=new MessageDispatcher(b, null, null, null);
        b.connect("MessageDispatcherUnitTest");
        Util.waitUntilAllChannelsHaveSameView(10000, 1000, a, b);

        d1.setRequestHandler(new MyHandler(new byte[]{'d', '1'}));
        d2.setRequestHandler(new MyHandler(new byte[]{'d', '2'}));

        Message msg=new Message(a.getAddress()); // non-null message
        try {
            d1.castMessage(null,msg,RequestOptions.SYNC().setTimeout(3000));
            assert false : " multicast RPC with a non-null dest for message";
        }
        catch(IllegalArgumentException ex) {
            System.out.println("received exception as expected: " + ex);
        }
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

    public void testCastToMissingNode() throws Exception {
        d1.setRequestHandler(new MyHandler(new byte[10]));
        b=createChannel(a);
        b.setName("B");

        final CountDownLatch targetLatch=new CountDownLatch(1);
        d2=new MessageDispatcher(b, null, null, new RequestHandler() {
            @Override
            public Object handle(Message msg) throws Exception {
                targetLatch.await();
                return null;
            }
        });

        b.connect("MessageDispatcherUnitTest");
        Assert.assertEquals(2, b.getView().size());

        final CountDownLatch futureLatch=new CountDownLatch(1);
        FutureListener listener=new FutureListener() {
            @Override public void futureDone(Future future) {
                futureLatch.countDown();
            }
        };


        List<Address> dests=Collections.singletonList(b.getAddress());
        byte[] buf=new byte[1];
        Message msg=new Message();
        msg.setBuffer(buf);
        NotifyingFuture<RspList<Object>> future=d1.castMessageWithFuture(dests, msg, RequestOptions.SYNC(), null);

        b.disconnect();

        Thread.sleep(100);

        future.setListener(listener);
        assertTrue(futureLatch.await(10, TimeUnit.SECONDS));
        targetLatch.countDown();
    }


    private void sendMessage(int size) throws Exception {
        long start, stop;
        MyHandler handler=new MyHandler(new byte[size]);
        d1.setRequestHandler(handler);
        start=System.currentTimeMillis();
        RspList rsps=d1.castMessage(null, new Message(), new RequestOptions(ResponseMode.GET_ALL, 0));
        stop=System.currentTimeMillis();
        System.out.println("rsps:\n" + rsps);
        System.out.println("call took " + (stop - start) + " ms");
        assertNotNull(rsps);
        Assert.assertEquals(1, rsps.size());
        byte[] buf=(byte[])rsps.getFirst();
        assertNotNull(buf);
        Assert.assertEquals(size, buf.length);
    }

    private void sendMessageToBothChannels(int size) throws Exception {
        long start, stop;
        d1.setRequestHandler(new MyHandler(new byte[size]));

        b=createChannel(a);
        b.setName("B");
        d2=new MessageDispatcher(b, null, null, new MyHandler(new byte[size]));
        b.connect("MessageDispatcherUnitTest");
        Assert.assertEquals(2,b.getView().size());

        System.out.println("casting message");
        start=System.currentTimeMillis();
        RspList rsps=d1.castMessage(null, new Message(), new RequestOptions(ResponseMode.GET_ALL, 0));
        stop=System.currentTimeMillis();
        System.out.println("rsps:\n" + rsps);
        System.out.println("call took " + (stop - start) + " ms");
        assertNotNull(rsps);
        Assert.assertEquals(2,rsps.size());
        Rsp rsp=rsps.get(a.getAddress());
        assertNotNull(rsp);
        byte[] ret=(byte[])rsp.getValue();
        Assert.assertEquals(size, ret.length);

        rsp=rsps.get(b.getAddress());
        assertNotNull(rsp);
        ret=(byte[])rsp.getValue();
        Assert.assertEquals(size, ret.length);

        Util.close(b);
    }


    private static class MyHandler implements RequestHandler {
        byte[] retval;

        public MyHandler(byte[] retval) {
            this.retval=retval;
        }

        public Object handle(Message msg) throws Exception {
            return retval;
        }
    }
}
