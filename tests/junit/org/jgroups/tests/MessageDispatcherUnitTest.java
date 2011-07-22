package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.blocks.*;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.stack.ProtocolStack;
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
@Test(groups=Global.STACK_DEPENDENT, sequential=true)
public class MessageDispatcherUnitTest extends ChannelTestBase {
    MessageDispatcher d1, d2;
    JChannel          c1, c2;

    @BeforeClass
    protected void setUp() throws Exception {
        c1=createChannel(true);
        c1.setName("A");
        GMS gms=(GMS)c1.getProtocolStack().findProtocol(GMS.class);
        if(gms != null)
            gms.setPrintLocalAddress(false);
        disableBundling(c1);
        d1=new MessageDispatcher(c1, null, null, null);
        c1.connect("MessageDispatcherUnitTest");
    }

    @AfterClass
    protected void tearDown() throws Exception {
        d1.stop();
        c1.close();
        Util.sleep(500);
    }

    @AfterMethod
    protected void closeSecondChannel() {
        if(c2 != null) {
            d2.stop();
            c2.close();
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

        c2=createChannel(c1);
        c2.setName("B");
        disableBundling(c2);
        long stop, start=System.currentTimeMillis();
        d2=new MessageDispatcher(c2, null, null, new MyHandler(null));
        stop=System.currentTimeMillis();
        c2.connect("MessageDispatcherUnitTest");
        Assert.assertEquals(2, c2.getView().size());
        System.out.println("view: " + c2.getView());

        System.out.println("casting message");
        start=System.currentTimeMillis();
        RspList rsps=d1.castMessage(null, new Message(), new RequestOptions(ResponseMode.GET_ALL, 0));
        stop=System.currentTimeMillis();
        System.out.println("rsps:\n" + rsps);
        System.out.println("call took " + (stop - start) + " ms");
        assertNotNull(rsps);
        Assert.assertEquals(2, rsps.size());
        Rsp rsp=rsps.get(c1.getAddress());
        assertNotNull(rsp);
        Object ret=rsp.getValue();
        assert ret == null;

        rsp=rsps.get(c2.getAddress());
        assertNotNull(rsp);
        ret=rsp.getValue();
        assert ret == null;

        Util.close(c2);
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

        c2=createChannel(c1);
        c2.setName("B");
        disableBundling(c2);
        d2=new MessageDispatcher(c2, null, null, new MyHandler(new byte[size]));
        c2.connect("MessageDispatcherUnitTest");
        Assert.assertEquals(2, c2.getView().size());

        System.out.println("casting message");
        start=System.currentTimeMillis();
        RspList rsps=d1.castMessage(null, new Message(), new RequestOptions(ResponseMode.GET_ALL, 0));
        stop=System.currentTimeMillis();
        System.out.println("rsps:\n" + rsps);
        System.out.println("call took " + (stop - start) + " ms");
        assertNotNull(rsps);
        Assert.assertEquals(2, rsps.size());
        Rsp rsp=rsps.get(c1.getAddress());
        assertNotNull(rsp);
        byte[] ret=(byte[])rsp.getValue();
        Assert.assertEquals(size, ret.length);

        rsp=rsps.get(c2.getAddress());
        assertNotNull(rsp);
        ret=(byte[])rsp.getValue();
        Assert.assertEquals(size, ret.length);

        Util.close(c2);
    }

    private static void disableBundling(JChannel ch) {
        ProtocolStack stack=ch.getProtocolStack();
        TP transport=stack.getTransport();
        if(transport != null) {
            transport.setEnableBundling(false);
        }
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
