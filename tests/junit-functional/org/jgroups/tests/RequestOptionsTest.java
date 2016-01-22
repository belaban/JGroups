package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.blocks.*;
import org.jgroups.blocks.mux.MuxRpcDispatcher;
import org.jgroups.blocks.mux.NoMuxHandlerRspFilter;
import org.jgroups.util.Util;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class RequestOptionsTest {
    protected JChannel channel;
    protected RequestOptions reqOpt = new RequestOptions(ResponseMode.GET_ALL, 5000);
    protected static final String simple_props="SHARED_LOOPBACK:PING:" +
      "pbcast.NAKACK2(log_discard_msgs=false;log_not_found_msgs=false)" +
      ":UNICAST3:pbcast.STABLE(stability_delay=200):pbcast.GMS:MFC:UFC:FRAG2";


    @BeforeMethod
    protected void start() throws Exception {
        channel=new JChannel(simple_props);
    }

    protected void stop() throws Exception {
        Util.close(channel);
    }

    /**
     * Tests https://issues.jboss.org/browse/JGRP-1369
     */
	public void testRequestOptionsChaining() throws Exception {
        MuxRpcDispatcher muxRpc = new MuxRpcDispatcher((short) 1, channel, null, null, new Server());
        channel.connect("group");
        for(int i=0; i < 20; i++)
            muxRpc.callRemoteMethods(null, new MethodCall(Server.class.getMethod("foo")), reqOpt);

        RspFilter filter=reqOpt.getRspFilter();
        int count=count(filter);
        System.out.println("count=" + count);
        assert count == 1;
    }

    protected static int count(RspFilter filter) {
        if(filter instanceof NoMuxHandlerRspFilter)
            return 1 + count(((NoMuxHandlerRspFilter)filter).getFilter());
        return 0;
    }
	
	static public class Server {
		public static void foo() {System.out.println("Entering foo"); }
	}
}
