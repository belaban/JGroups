
package org.jgroups.tests;


import org.jgroups.*;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Test the distributed RPC deadlock detection mechanism, using one or two channels.
 *
 * @author John Giorgiadis
 * @author Ovidiu Feodorov <ovidiuf@users.sourceforge.net>
 * *
 * @version $Revision: 1.23 $
 */
@Test(groups=Global.STACK_DEPENDENT,singleThreaded=true)
public class Deadlock2Test extends ChannelTestBase {
    private static final String name = "Deadlock2Test";

    private JChannel c1, c2;


    @AfterMethod
    void cleanup() {
        Util.close(c1, c2);
    }


    /**
     * Tests the deadlock resolution using self-calls on a single channel. The deadlock detection
     * is turned on so the method call should go straight through. If there is a problem, JUnit will
     * timeout.
     *
     * @throws Exception
     */
    public void testOneChannel() throws Exception {
        c1 = createChannel(true);
        ServerObject serverObject = new ServerObject("obj1");
        RpcDispatcher disp=new RpcDispatcher(c1, serverObject);
        serverObject.setRpcDispatcher(disp);
        c1.connect(name);
        Address localAddress = c1.getAddress();

        // call the nested group method on itself
        MethodCall call = new MethodCall("outerMethod", new Object[0], new Class[0]);
        log("calling outerMethod() on all members");
        RspList rspList = disp.callRemoteMethods(null, call, new RequestOptions(ResponseMode.GET_ALL, 0));
        log("results of outerMethod(): " + rspList);

        Assert.assertEquals(1, rspList.size());
        assertEquals("outerMethod[innerMethod]", rspList.getValue(localAddress));
        assertTrue(rspList.isReceived(localAddress));
        assertFalse(rspList.isSuspected(localAddress));
    }


    /**
     * Tests the deadlock resolution using two different channels. The deadlock detection
     * is turned on. It implements the following scenario:
     *
     * Channel1                              Channel2
     *    |                                     |
     *    + -------------------------------> outerMethod()
     *    |                                    RPC
     *    |                                     |
     *    |                                     |
     *    |                                     |
     *    | <-- innerMethod() <-----------------+ ---------+
     *    |                                     |          |
     *    |                                     | <-- innerMethod()
     *
     * If there is a deadlock, JUnit will timeout and fail the test.
     *
     */
    public void testTwoChannels() throws Throwable {
        ServerObject obj1, obj2 = null;

        c1 = createChannel(true);
        obj1 = new ServerObject("obj1");
        RpcDispatcher disp1=new RpcDispatcher(c1, obj1);
        obj1.setRpcDispatcher(disp1);
        c1.connect(name);

        c2 = createChannel(c1);
        obj2 = new ServerObject("obj2");
        RpcDispatcher disp2=new RpcDispatcher(c2, obj2);
        obj2.setRpcDispatcher(disp2);
        c2.connect(name);
        Address localAddress2 = c2.getAddress();

        // call a point-to-point method on Member 2 that triggers a nested distributed RPC
        MethodCall call = new MethodCall("outerMethod", new Object[0], new Class[0]);
        log("calling outerMethod() on " + localAddress2);
        Object retval = disp1.callRemoteMethod(localAddress2, call, new RequestOptions(ResponseMode.GET_ALL, 0));
        log("results of outerMethod(): " + retval);
    }


    public void testTwoChannelsWithInitialMulticast() throws Exception {
        ServerObject obj1, obj2 = null;

        c1 = createChannel(true);
        obj1 = new ServerObject("obj1");
        RpcDispatcher disp1=new RpcDispatcher(c1, obj1);
        obj1.setRpcDispatcher(disp1);
        c1.connect(name);

        c2 = createChannel(c1);
        obj2 = new ServerObject("obj2");
        RpcDispatcher disp2=new RpcDispatcher(c2, obj2);
        obj2.setRpcDispatcher(disp2);
        c2.connect(name);

        List<Address> dests=new ArrayList<>();
        dests.add(c1.getAddress());
        dests.add(c2.getAddress());

        // call a point-to-point method on Member 2 that triggers a nested distributed RPC
        MethodCall call = new MethodCall("outerMethod", new Object[0], new Class[0]);
        log("calling outerMethod() on all members");
        RspList rsps = disp1.callRemoteMethods(dests, call, new RequestOptions(ResponseMode.GET_ALL, 0));
        log("results of outerMethod():\n" + rsps);
        Assert.assertEquals(2, rsps.size());
    }


    static void log(String msg) {
        System.out.println("[" + Thread.currentThread() + "] " + msg);
    }


    public static class ServerObject {
        String myName;

        public ServerObject(String name) {
            this.myName=name;
        }

        private RpcDispatcher disp;

        /**
        * The ServerObject keeps a reference to its dispatcher to be able to send nested group
        * RPCs.
        */
        public void setRpcDispatcher(RpcDispatcher rpcDispatcher) {
            this.disp = rpcDispatcher;
        }

        public String outerMethod() throws Exception {
            log("**** outerMethod() received, calling innerMethod() on all members");
            MethodCall call = new MethodCall("innerMethod", new Object[0], new Class[0]);
            // RspList rspList = disp.callRemoteMethods(null, call, GroupResponseMode.GET_ALL, 5000);
            RequestOptions opts=new RequestOptions(ResponseMode.GET_ALL, 0, false, null, (Message.Flag[])null);
            opts.flags(Message.Flag.OOB);
            RspList<String> rspList = disp.callRemoteMethods(null, call, opts);
            List<String> results = rspList.getResults();
            log("results of calling innerMethod():\n" + rspList);
            StringBuilder sb=new StringBuilder("outerMethod[");
            boolean first=true;
            for(String s: results) {
                if(first)
                    first=false;
                else
                    sb.append(";");
                sb.append(s);
            }
            sb.append("]");
            return sb.toString();
        }

        public static String innerMethod() {
            log("**** innerMethod() received, returning result");
            return "innerMethod";
        }
	}



}
