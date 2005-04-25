// $Id: Deadlock2Test.java,v 1.5 2005/04/25 15:13:51 belaban Exp $

package org.jgroups.tests;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.util.RspList;

import java.util.Enumeration;
import java.util.Vector;

/**
 * Test the distributed RPC deadlock detection mechanism, using one or two channels.
 *
 * @author John Giorgiadis
 * @author Ovidiu Feodorov <ovidiuf@users.sourceforge.net>
 * *
 * @version $Revision: 1.5 $
 */
public class Deadlock2Test extends TestCase {

    private static boolean DEADLOCK_DETECTION = true;

    private String name = "Deadlock2Test";


    public Deadlock2Test(String name) {
        super(name);
    }


    /**
     * Tests the deadlock resolution using self-calls on a single channel. The deadlock detection
     * is turned on so the method call should go straight through. If there is a problem, JUnit will
     * timeout.
     *
     * @throws Exception
     */
    public void testOneChannel() throws Exception {
        JChannel channel = new JChannel();
        ServerObject serverObject = new ServerObject("obj1");
        RpcDispatcher disp=new RpcDispatcher(channel, null, null, serverObject, DEADLOCK_DETECTION);
        serverObject.setRpcDispatcher(disp);
        channel.connect(name);
        Address localAddress = channel.getLocalAddress();

        // call the nested group method on itself
        MethodCall call = new MethodCall("outerMethod", new Object[0], new Class[0]);
        log("calling outerMethod() on all members");
        RspList rspList = disp.callRemoteMethods(null, call, GroupRequest.GET_ALL, 0);
        log("results of outerMethod(): " + rspList);

        assertEquals(1, rspList.size());
        assertEquals("outerMethod[innerMethod]", rspList.get(localAddress));
        assertTrue(rspList.isReceived(localAddress));
        assertFalse(rspList.isSuspected(localAddress));

        channel.disconnect();
        channel.close();

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
    public void testTwoChannels() throws Exception {
        ServerObject obj1, obj2 = null;

        JChannel c1 = new JChannel();
        obj1 = new ServerObject("obj1");
        RpcDispatcher disp1=new RpcDispatcher(c1, null, null, obj1, DEADLOCK_DETECTION);
        obj1.setRpcDispatcher(disp1);
        c1.connect(name);

        JChannel c2 = new JChannel();
        obj2 = new ServerObject("obj2");
        RpcDispatcher disp2=new RpcDispatcher(c2, null, null, obj2, DEADLOCK_DETECTION);
        obj2.setRpcDispatcher(disp2);
        c2.connect(name);
        Address localAddress2 = c2.getLocalAddress();

        try {
            // call a point-to-point method on Member 2 that triggers a nested distributed RPC
            MethodCall call = new MethodCall("outerMethod", new Object[0], new Class[0]);
            Vector dests = new Vector();
            dests.add(localAddress2);
            log("calling outerMethod() on " + localAddress2);
            Object retval = disp1.callRemoteMethod(localAddress2, call, GroupRequest.GET_ALL, 0);
            log("results of outerMethod(): " + retval);
        }
        finally {
            c2.close();
            c1.close();
        }
    }


    public static Test suite() {
        TestSuite s=new TestSuite(Deadlock2Test.class);
        return s;
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
        System.exit(0);
    }


    static void log(String msg) {
        System.out.println("[" + Thread.currentThread() + "] " + msg);
    }


    public class ServerObject {
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

        public String outerMethod() {
            log("**** outerMethod() received, calling innerMethod() on all members");
            MethodCall call = new MethodCall("innerMethod", new Object[0], new Class[0]);
            // RspList rspList = disp.callRemoteMethods(null, call, GroupRequest.GET_ALL, 5000);
            RspList rspList = disp.callRemoteMethods(null, call, GroupRequest.GET_ALL, 0);
            Vector results = rspList.getResults();
            log("results of calling innerMethod():\n" + rspList);
            StringBuffer sb = new StringBuffer("outerMethod[");
            for(Enumeration e = results.elements(); e.hasMoreElements(); ) {
                String s = (String)e.nextElement();
                sb.append(s);
                if (e.hasMoreElements()) {
                    sb.append(";");
                }
            }
            sb.append("]");
            return sb.toString();
        }

        public String innerMethod() {
            log("**** innerMethod() received, returning result");
            return "innerMethod";
        }
	}



}
