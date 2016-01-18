package org.jgroups.blocks;

import org.jgroups.*;
import org.jgroups.blocks.mux.MuxMessageDispatcher;
import org.jgroups.blocks.mux.MuxUpHandler;
import org.jgroups.tests.ChannelTestBase;
import org.jgroups.util.Rsp;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;

/**
 * @author Paul Ferraro
 */
@Test(groups=Global.STACK_DEPENDENT)
public class MuxMessageDispatcherTest extends ChannelTestBase {
    private final JChannel[]            channels = new JChannel[2];
    private final MessageDispatcher[]   dispatchers  = new MessageDispatcher[2];
    private final MessageDispatcher[][] muxDispatchers = new MessageDispatcher[2][2];
    private static final MethodCall     method = new MethodCall("getName", new Object[0], new Class[0]);
    
    @BeforeClass
    void setUp() throws Exception {
        channels[0] = createChannel(true);
        channels[1] = createChannel(channels[0]);
        
        for (int i = 0; i < dispatchers.length; i++) {
            dispatchers[i] = new MessageDispatcher(channels[i], null, null, new MuxRequestListener("dispatcher[" + i + "]"));
            channels[i].setUpHandler(new MuxUpHandler(dispatchers[i].getProtocolAdapter()));

            for (int j = 0; j < muxDispatchers[i].length; j++) {
                muxDispatchers[i][j] = new MuxMessageDispatcher((short) j, channels[i], null, null, new MuxRequestListener("muxDispatcher[" + i + "][" + j + "]"));
            }
            channels[i].connect("MuxMessageDispatcherTest");
            Util.sleep(1000);
        }
    }

    @AfterClass
    void tearDown() throws Exception {
        for (int i = 0; i < dispatchers.length; ++i) {
            channels[i].close();
            dispatchers[i].stop();
            for (int j = 0; j < muxDispatchers[i].length; ++j)
                muxDispatchers[i][j].stop();
        }
    }

    public void testCastMessage() throws Exception {

        Message message = new Message();
        
        // Validate normal dispatchers
        Map<Address, Rsp<Object>> responses = dispatchers[0].castMessage(null, message, RequestOptions.SYNC());

        Assert.assertEquals(responses.size(), 2);
        
        for (int i = 0; i < dispatchers.length; ++i) {
            
            verifyResponse(responses, channels[i], "dispatcher[" + i + "]");
        }

        // Validate muxed dispatchers
        for (int j = 0; j < muxDispatchers[0].length; ++j) {
            
            responses = muxDispatchers[0][j].castMessage(null, message, RequestOptions.SYNC());

            Assert.assertEquals(responses.size(), 2);
            
            for (int i = 0; i < dispatchers.length; ++i) {
                
                verifyResponse(responses, channels[i], "muxDispatcher[" + i + "][" + j + "]");
            }
        }
        
        final Address address = channels[0].getAddress();
        
        RspFilter filter = new RspFilter() {

            public boolean isAcceptable(Object response, Address sender) {
                return !sender.equals(address);
            }

            public boolean needMoreResponses() {
                return true;
            }
        };
        
        // Validate muxed rpc dispatchers w/filter
        responses = muxDispatchers[0][0].castMessage(null, message, RequestOptions.SYNC().setRspFilter(filter));

        Assert.assertEquals(responses.size(), 2);
        verifyResponse(responses, channels[0], null);
        verifyResponse(responses, channels[1], "muxDispatcher[1][0]");
        
        muxDispatchers[1][0].stop();
        
        // Validate stopped mux dispatcher response is auto-filtered
        responses = muxDispatchers[0][0].castMessage(null, message, RequestOptions.SYNC().setRspFilter(null));

        Assert.assertEquals(responses.size(), 2);
        verifyResponse(responses, channels[0], "muxDispatcher[0][0]");
        verifyResponse(responses, channels[1], null);
        
        // Validate stopped mux dispatcher response is auto-filtered and custom filter is applied
        responses = muxDispatchers[0][0].castMessage(null, message, RequestOptions.SYNC().setRspFilter(filter));

        Assert.assertEquals(responses.size(), 2);
        verifyResponse(responses, channels[0], null);
        verifyResponse(responses, channels[1], null);
        
        muxDispatchers[1][0].start();
        
        // Validate restarted mux dispatcher functions normally
        responses = muxDispatchers[0][0].castMessage(null, message, RequestOptions.SYNC().setRspFilter(null));

        Assert.assertEquals(responses.size(), 2);
        verifyResponse(responses, channels[0], "muxDispatcher[0][0]");
        verifyResponse(responses, channels[1], "muxDispatcher[1][0]");
    }

    public void testSendMessage() throws Exception {

        final Address address = channels[1].getAddress();
        Message message = new Message(address);
        
        // Validate normal dispatchers
        Object response = dispatchers[0].sendMessage(message, RequestOptions.SYNC());

        Assert.assertEquals(response, "dispatcher[1]");

        // Validate muxed dispatchers
        for (int j = 0; j < muxDispatchers[0].length; ++j) {
            
            response = muxDispatchers[0][j].sendMessage(message, RequestOptions.SYNC());

            Assert.assertEquals(response, "muxDispatcher[1][" + j + "]");
        }
        
        // Filter testing is disabled for now pending filter improvements in JGroups 3
        
//        // Validate muxed rpc dispatchers w/filter
//        
//        RspFilter filter = new RspFilter() {
//
//            @Override
//            public boolean isAcceptable(Object response, Address sender) {
//                return !sender.equals(address);
//            }
//
//            @Override
//            public boolean needMoreResponses() {
//                return true;
//            }
//        };
//        
//        response = muxDispatchers[0][0].callRemoteMethod(address, method, RequestOptions.SYNC.setRspFilter(filter));
//
//        Assert.assertNull(address);
//        
//        // Validate stopped mux dispatcher response is auto-filtered
//        muxDispatchers[1][0].stop();
//        
//        response = muxDispatchers[0][0].callRemoteMethod(address, method, RequestOptions.SYNC.setRspFilter(null));
//
//        Assert.assertNull(address);
//        
//        // Validate restarted mux dispatcher functions normally
//        muxDispatchers[1][0].start();
//        
//        response = muxDispatchers[0][0].callRemoteMethod(address, method, RequestOptions.SYNC.setRspFilter(null));
//
//        Assert.assertEquals(response, "muxDispatcher[1][0]");
    }

    private static void verifyResponse(Map<Address, Rsp<Object>> responses, AbstractChannel channel, Object expected) {
        Rsp<?> response = responses.get(channel.getAddress());
        String address = channel.getAddress().toString();
        Assert.assertNotNull(response, address);
        Assert.assertFalse(response.wasSuspected(), address);
        if (expected != null) {
            Assert.assertTrue(response.wasReceived(), address);
            Assert.assertEquals(response.getValue(), expected, address);
        } else {
            Assert.assertFalse(response.wasReceived(), address);
        }
    }

    public static class MuxRequestListener implements RequestHandler {
        private final String name;

        public MuxRequestListener(String name) {
            this.name = name;
        }

        public Object handle(Message msg) throws Exception {
            return this.name;
        }
    }
}