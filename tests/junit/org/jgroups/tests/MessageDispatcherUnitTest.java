package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.blocks.MessageDispatcher;
import org.jgroups.blocks.RequestHandler;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.util.Buffer;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Tests return values from MessageDispatcher.castMessage()
 * 
 * @author Bela Ban
 */
@Test(groups=Global.STACK_DEPENDENT, singleThreaded=true)
public class MessageDispatcherUnitTest extends ChannelTestBase {
    protected MessageDispatcher   d1, d2;
    protected JChannel            a, b;
    protected static final Buffer buf;

    static {
        byte[] data="bla".getBytes();
        buf=new Buffer(data, 0, data.length);
    }

    @BeforeClass
    protected void setUp() throws Exception {
        a=createChannel(true, 2, "A");
        GMS gms=a.getProtocolStack().findProtocol(GMS.class);
        if(gms != null)
            gms.setPrintLocalAddress(false);
        d1=new MessageDispatcher(a);
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
        RspList rsps=d1.castMessage(null, buf, new RequestOptions(ResponseMode.GET_ALL, 0));
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
        d2=new MessageDispatcher(b, new MyHandler(null));
        stop=System.currentTimeMillis();
        b.connect("MessageDispatcherUnitTest");
        Assert.assertEquals(2,b.getView().size());
        System.out.println("view: " + b.getView());

        System.out.println("casting message");
        start=System.currentTimeMillis();
        RspList<Object> rsps=d1.castMessage(null, buf, new RequestOptions(ResponseMode.GET_ALL, 0));
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


    public void test200ByteMessageToAll() throws Exception {
        sendMessageToBothChannels(200);
    }

    public void test2000ByteMessageToAll() throws Exception {
        sendMessageToBothChannels(2000);
    }

    public void test20000ByteMessageToAll() throws Exception {
        sendMessageToBothChannels(20000);
    }

    private void sendMessage(int size) throws Exception {
        long start, stop;
        MyHandler handler=new MyHandler(new byte[size]);
        d1.setRequestHandler(handler);
        start=System.currentTimeMillis();
        RspList rsps=d1.castMessage(null, buf, new RequestOptions(ResponseMode.GET_ALL, 0));
        stop=System.currentTimeMillis();
        System.out.println("rsps:\n" + rsps);
        System.out.println("call took " + (stop - start) + " ms");
        assertNotNull(rsps);
        Assert.assertEquals(1, rsps.size());
        byte[] tmp=(byte[])rsps.getFirst();
        assertNotNull(tmp);
        Assert.assertEquals(size, tmp.length);
    }

    private void sendMessageToBothChannels(int size) throws Exception {
        long start, stop;
        d1.setRequestHandler(new MyHandler(new byte[size]));

        b=createChannel(a);
        b.setName("B");
        d2=new MessageDispatcher(b, new MyHandler(new byte[size]));
        b.connect("MessageDispatcherUnitTest");
        Assert.assertEquals(2,b.getView().size());

        System.out.println("casting message");
        start=System.currentTimeMillis();
        RspList<Object> rsps=d1.castMessage(null, buf, new RequestOptions(ResponseMode.GET_ALL, 0));
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
        byte[] retval=null;

        public MyHandler(byte[] retval) {
            this.retval=retval;
        }

        public Object handle(Message msg) throws Exception {
            return retval;
        }
    }
}
