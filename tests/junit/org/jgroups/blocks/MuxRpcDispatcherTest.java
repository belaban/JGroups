package org.jgroups.blocks;

import org.jgroups.*;
import org.jgroups.blocks.mux.MuxRpcDispatcher;
import org.jgroups.blocks.mux.MuxUpHandler;
import org.jgroups.tests.ChannelTestBase;
import org.jgroups.util.Rsp;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.*;

import java.util.Map;

/**
 * @author Paul Ferraro
 */
@Test(groups={Global.STACK_DEPENDENT},singleThreaded=true)
public class MuxRpcDispatcherTest extends ChannelTestBase {

    private JChannel[]        channels = null;
    private RpcDispatcher[]   dispatchers  = null;
    private RpcDispatcher[][] muxDispatchers = null;

    @BeforeMethod
    void setUp() throws Exception {
        channels       = new JChannel[2];
        dispatchers    = new RpcDispatcher[2];
        muxDispatchers = new RpcDispatcher[2][2];

        channels[0] = createChannel(true, 2, "A");
        channels[1] = createChannel(channels[0], "B");
        
        for (int i = 0; i < dispatchers.length; i++) {

            dispatchers[i] = new RpcDispatcher(channels[i], new Server("dispatcher[" + i + "]"));

            channels[i].setUpHandler(new MuxUpHandler(dispatchers[i].getProtocolAdapter()));

            for (int j = 0; j < muxDispatchers[i].length; j++) {
                muxDispatchers[i][j] = new MuxRpcDispatcher((short) j, channels[i], null, null, new Server("muxDispatcher[" + i + "][" + j + "]"));
            }
 
            channels[i].connect("MuxRpcDispatcherTest");

            Util.sleep(1000);
        }

        Util.waitUntilAllChannelsHaveSameView(10000, 1000, channels);
    }

    @AfterMethod
    void tearDown() throws Exception {
        for (int i = 0; i < dispatchers.length; ++i) {
            dispatchers[i].stop();
            for (int j = 0; j < muxDispatchers[i].length; ++j)
                muxDispatchers[i][j].stop();
        }
        for(JChannel ch: channels)
            Util.close(ch);
    }



    public void testUnicastRPCs() throws Exception {
        RequestOptions options=RequestOptions.SYNC().setTimeout(30000);
        MethodCall method = new MethodCall("getName", new Object[0], new Class[0]);

        final Address address = channels[1].getAddress();
        
        // Validate normal dispatchers
        Object response = dispatchers[0].callRemoteMethod(address, method, options);
        Assert.assertEquals(response, "dispatcher[1]");

        // Validate muxed dispatchers
        for (int j = 0; j < muxDispatchers[0].length; ++j) {
            response = muxDispatchers[0][j].callRemoteMethod(address, method, options);
            Assert.assertEquals(response, "muxDispatcher[1][" + j + "]");
        }
        
        // Filter testing is disabled for now pending filter improvements in JGroups 3
        
        // Validate muxed rpc dispatchers w/filter

        RspFilter filter = new RspFilter() {
            @Override public boolean isAcceptable(Object response, Address sender) {return !sender.equals(address);}
            @Override public boolean needMoreResponses() {return true;}
        };

        response = muxDispatchers[0][0].callRemoteMethod(address, method, RequestOptions.SYNC().setRspFilter(filter));
        assert response == null;

        // Validate stopped mux dispatcher response is auto-filtered
        muxDispatchers[1][0].stop();

        try {
            response = muxDispatchers[0][0].callRemoteMethod(address, method, RequestOptions.SYNC().setTimeout(2000).setRspFilter(null));
            assert false : "should have run into a TimeoutException";
        }
        catch(TimeoutException timeout) {
            System.out.println("received " + timeout + " - as expected");
        }

        Assert.assertNull(response);

        // Validate restarted mux dispatcher functions normally
        muxDispatchers[1][0].start();

        response = muxDispatchers[0][0].callRemoteMethod(address, method, RequestOptions.SYNC().setRspFilter(null));

        Assert.assertEquals(response, "muxDispatcher[1][0]");
    }

    public void testMulticastRPCs() throws Exception {
        MethodCall method = new MethodCall("getName", new Object[0], new Class[0]);
        RequestOptions options=RequestOptions.SYNC().setTimeout(30000);

        // Validate normal dispatchers
        Map<Address, Rsp<String>> responses = dispatchers[0].callRemoteMethods(null, method, options);
        Assert.assertEquals(responses.size(), 2);

        for (int i = 0; i < dispatchers.length; ++i)
            verifyResponse(responses, channels[i], "dispatcher[" + i + "]");

        // Validate muxed dispatchers
        for (int j = 0; j < muxDispatchers[0].length; ++j) {
            responses = muxDispatchers[0][j].callRemoteMethods(null, method, options);
            Assert.assertEquals(responses.size(), 2);
            for (int i = 0; i < dispatchers.length; ++i)
                verifyResponse(responses, channels[i], "muxDispatcher[" + i + "][" + j + "]");
        }

        // Validate muxed rpc dispatchers w/filter
        final Address address = channels[0].getAddress();

        RspFilter filter = new RspFilter() {
            public boolean isAcceptable(Object response, Address sender) {
                return !sender.equals(address);
            }
            public boolean needMoreResponses() {
                return true;
            }
        };

        responses = muxDispatchers[0][0].callRemoteMethods(null,method,RequestOptions.SYNC().setTimeout(30000).setRspFilter(filter));

        Assert.assertEquals(responses.size(), 2);
        verifyResponse(responses, channels[0], null);
        verifyResponse(responses, channels[1], "muxDispatcher[1][0]");

        // Validate stopped mux dispatcher response is auto-filtered
        muxDispatchers[1][0].stop();

        responses = muxDispatchers[0][0].callRemoteMethods(null, method, options.setTimeout(2000));
        Assert.assertEquals(responses.size(), 2);
        verifyResponse(responses, channels[0], "muxDispatcher[0][0]");
        verifyResponse(responses, channels[1], null);

        // Validate stopped mux dispatcher response is auto-filtered and custom filter is applied
        responses = muxDispatchers[0][0].callRemoteMethods(null,method,RequestOptions.SYNC().setTimeout(2000).setRspFilter(filter));

        Assert.assertEquals(responses.size(), 2);
        verifyResponse(responses, channels[0], null);
        verifyResponse(responses, channels[1], null);

        // Validate restarted mux dispatcher functions normally
        muxDispatchers[1][0].start();

        responses = muxDispatchers[0][0].callRemoteMethods(null, method, options.setTimeout(30000));

        Assert.assertEquals(responses.size(), 2);
        verifyResponse(responses, channels[0], "muxDispatcher[0][0]");
        verifyResponse(responses, channels[1], "muxDispatcher[1][0]");
    }


    private static void verifyResponse(Map<Address,Rsp<String>> responses, Channel channel, Object expected) {
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

    public static class Server {
        private final String name;

        public Server(String name) {
            this.name = name;
        }

        public String getName() {
            return this.name;
        }
    }
}
