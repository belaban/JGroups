package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.blocks.MessageDispatcher;
import org.jgroups.blocks.RequestHandler;
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
 * @version $Id: MessageDispatcherUnitTest.java,v 1.10 2008/04/14 08:18:39
 *          belaban Exp $
 */
@Test(groups=Global.STACK_DEPENDENT, sequential=true)
public class MessageDispatcherUnitTest extends ChannelTestBase {
    MessageDispatcher disp, disp2;
    JChannel ch, ch2;

    @BeforeClass
    protected void setUp() throws Exception {
        ch=createChannel(true);
        GMS gms=(GMS)ch.getProtocolStack().findProtocol(GMS.class);
        if(gms != null)
            gms.setPrintLocalAddress(false);
        disableBundling(ch);
        disp=new MessageDispatcher(ch, null, null, null);
        ch.connect("MessageDispatcherUnitTest");
    }

    @AfterClass
    protected void tearDown() throws Exception {
        disp.stop();
        ch.close();
        Util.sleep(500);
    }

    @AfterMethod
    protected void closeSecondChannel() {
        if(ch2 != null) {
            disp2.stop();
            ch2.close();
            Util.sleep(500);
        }
    }

    public void testNullMessageToSelf() {
        MyHandler handler=new MyHandler(null);
        disp.setRequestHandler(handler);
        RspList rsps=disp.castMessage(null, new Message(), GroupRequest.GET_ALL, 0);
        System.out.println("rsps:\n" + rsps);
        assertNotNull(rsps);
        Assert.assertEquals(1, rsps.size());
        Object obj=rsps.getFirst();
        assert obj == null;
    }

    public void test200ByteMessageToSelf() {
        sendMessage(200);
    }

    public void test2000ByteMessageToSelf() {
        sendMessage(2000);
    }

    public void test20000ByteMessageToSelf() {
        sendMessage(20000);
    }

    public void testNullMessageToAll() throws Exception {
        disp.setRequestHandler(new MyHandler(null));

        ch2=createChannel(ch);
        disableBundling(ch2);
        long stop, start=System.currentTimeMillis();
        disp2=new MessageDispatcher(ch2, null, null, new MyHandler(null));
        stop=System.currentTimeMillis();
        ch2.connect("MessageDispatcherUnitTest");
        Assert.assertEquals(2, ch2.getView().size());
        System.out.println("view: " + ch2.getView());

        System.out.println("casting message");
        start=System.currentTimeMillis();
        RspList rsps=disp.castMessage(null, new Message(), GroupRequest.GET_ALL, 0);
        stop=System.currentTimeMillis();
        System.out.println("rsps:\n" + rsps);
        System.out.println("call took " + (stop - start) + " ms");
        assertNotNull(rsps);
        Assert.assertEquals(2, rsps.size());
        Rsp rsp=rsps.get(ch.getAddress());
        assertNotNull(rsp);
        Object ret=rsp.getValue();
        assert ret == null;

        rsp=rsps.get(ch2.getAddress());
        assertNotNull(rsp);
        ret=rsp.getValue();
        assert ret == null;

        Util.close(ch2);
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

    private void sendMessage(int size) {
        long start, stop;
        MyHandler handler=new MyHandler(new byte[size]);
        disp.setRequestHandler(handler);
        start=System.currentTimeMillis();
        RspList rsps=disp.castMessage(null, new Message(), GroupRequest.GET_ALL, 0);
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
        disp.setRequestHandler(new MyHandler(new byte[size]));

        ch2=createChannel(ch);
        disableBundling(ch2);
        disp2=new MessageDispatcher(ch2, null, null, new MyHandler(new byte[size]));
        ch2.connect("MessageDispatcherUnitTest");
        Assert.assertEquals(2, ch2.getView().size());

        System.out.println("casting message");
        start=System.currentTimeMillis();
        RspList rsps=disp.castMessage(null, new Message(), GroupRequest.GET_ALL, 0);
        stop=System.currentTimeMillis();
        System.out.println("rsps:\n" + rsps);
        System.out.println("call took " + (stop - start) + " ms");
        assertNotNull(rsps);
        Assert.assertEquals(2, rsps.size());
        Rsp rsp=rsps.get(ch.getAddress());
        assertNotNull(rsp);
        byte[] ret=(byte[])rsp.getValue();
        Assert.assertEquals(size, ret.length);

        rsp=rsps.get(ch2.getAddress());
        assertNotNull(rsp);
        ret=(byte[])rsp.getValue();
        Assert.assertEquals(size, ret.length);

        Util.close(ch2);
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

        public Object handle(Message msg) {
            return retval;
        }
    }
}
