// $Id: Deadlock2Test.java,v 1.1 2004/09/05 04:51:54 ovidiuf Exp $

package org.jgroups.tests;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.*;
import org.jgroups.util.RspList;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.tests.stack.Utilities;
import org.jgroups.tests.stack.GossipTest;
import org.jgroups.stack.GossipData;
import org.jgroups.stack.GossipRouter;
import org.jgroups.stack.IpAddress;

import java.io.EOFException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.StreamCorruptedException;
import java.net.Socket;
import java.util.Vector;
import java.util.Enumeration;

/**
 * Test the distributed RPC deadlock detection mechanism, using one or two channels.
 *
 * @author John Giorgiadis
 * @author Ovidiu Feodorov <ovidiuf@users.sourceforge.net>
 * *
 * @version $Revision: 1.1 $
 */
public class Deadlock2Test extends TestCase {

    private static boolean DEADLOCK_DETECTION = true;
    private static boolean NO_DEADLOCK_DETECTION = false;

    private String name = "Deadlock2Test";
    private ListenerImpl listener;


    public Deadlock2Test(String name) {
        super(name);
    }

    public void setUp() throws Exception {

        super.setUp();
        listener = new ListenerImpl();
    }

    public void tearDown() throws Exception {

        super.tearDown();
        listener = null;
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
        ServerObject serverObject = new ServerObject();
        RpcDispatcher rpcDispatcher =
                new RpcDispatcher(channel, listener, listener, serverObject, DEADLOCK_DETECTION);
        serverObject.setRpcDispatcher(rpcDispatcher);
        channel.connect(name);
        Address localAddress = channel.getLocalAddress();


        // call the nested group method on itself
        MethodCall call = new MethodCall("outerMethod", new Object[0], new Class[0]);
        RspList rspList = rpcDispatcher.callRemoteMethods(null, call, GroupRequest.GET_ALL, 0);


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

        ServerObject serverObject = null;

        JChannel channel1 = new JChannel();
        serverObject = new ServerObject();
        RpcDispatcher rpcDispatcher1 =
                new RpcDispatcher(channel1, listener, listener, serverObject, DEADLOCK_DETECTION);
        serverObject.setRpcDispatcher(rpcDispatcher1);
        channel1.connect(name);
        Address localAddress1 = channel1.getLocalAddress();

        JChannel channel2 = new JChannel();
        serverObject = new ServerObject();
        RpcDispatcher rpcDispatcher2 =
                new RpcDispatcher(channel2, listener, listener, serverObject, DEADLOCK_DETECTION);
        serverObject.setRpcDispatcher(rpcDispatcher2);
        channel2.connect(name);
        Address localAddress2 = channel2.getLocalAddress();

        // call a point-to-point method on Member 2 that triggers a nested distributed RPC
        MethodCall call = new MethodCall("outerMethod", new Object[0], new Class[0]);
        Vector dests = new Vector();
        dests.add(localAddress2);
        RspList rspList = rpcDispatcher1.callRemoteMethods(dests, call, GroupRequest.GET_ALL, 0);


        assertEquals(1, rspList.size());
        assertEquals("outerMethod[innerMethod;innerMethod]", rspList.get(localAddress1));
        assertTrue(rspList.isReceived(localAddress1));
        assertFalse(rspList.isSuspected(localAddress1));

        channel1.disconnect();
        channel1.close();
        channel2.disconnect();
        channel2.close();

    }


    public static Test suite() {
        TestSuite s=new TestSuite(Deadlock2Test.class);
        return s;
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
        System.exit(0);
    }

    //
    //
    //

    public class ServerObject {

        private RpcDispatcher rpcDispatcher;

        /**
        * The ServerObject keeps a reference to its dispatcher to be able to send nested group
        * RPCs.
        */
        public void setRpcDispatcher(RpcDispatcher rpcDispatcher) {
            this.rpcDispatcher = rpcDispatcher;
        }

        public String outerMethod() {

            MethodCall call = new MethodCall("innerMethod", new Object[0], new Class[0]);
            RspList rspList = rpcDispatcher.callRemoteMethods(null, call, GroupRequest.GET_ALL, 0);
            Vector results = rspList.getResults();
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
            return "innerMethod";
        }
	}


	private class ListenerImpl implements MessageListener, MembershipListener {

		public ListenerImpl() {
            super();
        }

        //
        // MessageListener
        //
		public byte[] getState() {
            return(null);
        }

		public void setState(byte[] state) {
        }

		public void receive(Message msg) {
        }

        //
		// MembershipListener
        //

		public void block() {
        }

		public void suspect(Address suspect) {
        }

		public void viewAccepted(View view) {
        }
	}


}
