// $Id: LazyRoutingClientTest.java,v 1.4 2004/07/23 02:29:00 belaban Exp $

package org.jgroups.tests.stack;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.stack.GossipRouter;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.net.Socket;
import java.net.SocketException;


/**
 * Tests a special routing protocol case when a routing client is lazy to send
 * its request type and group identification. In this situation, the server
 * should close connection after a timeout. The GossipRouter in question is a
 * post-2.2.1 one.
 *
 * @author Ovidiu Feodorov <ovidiuf@users.sourceforge.net>
 * @version $Revision: 1.4 $
 * @since 2.2.1
 */
public class LazyRoutingClientTest extends TestCase {

    private int routerPort=-1;

    private long routingClientReplyTimeout=30000;

    public LazyRoutingClientTest(String name) {
        super(name);
    }

    public void setUp() throws Exception {
        super.setUp();
        routerPort=
                Utilities.startGossipRouter(GossipRouter.EXPIRY_TIME,
                        GossipRouter.GOSSIP_REQUEST_TIMEOUT,
                        routingClientReplyTimeout);
    }

    public void tearDown() throws Exception {
        super.tearDown();
        Utilities.stopGossipRouter();
    }


    /**
     * Tests the situation when a routing client is lazy to send its
     * request type and group identification. In this situation, the server
     * should close connection after timeout.
     */
    public void testLazyClient() throws Exception {

        int len;
        byte[] buffer;

        Socket s=new Socket("localhost", routerPort);
        DataInputStream dis=new DataInputStream(s.getInputStream());
        DataOutputStream dos=new DataOutputStream(s.getOutputStream());

        // read the IpAddress sent by GossipRouter
        len=dis.readInt();
        buffer=new byte[len];
        dis.readFully(buffer, 0, len);
        IpAddress localAddr=(IpAddress)Util.objectFromByteBuffer(buffer);
        assertEquals(localAddr.getIpAddress(), s.getLocalAddress());
        assertEquals(localAddr.getPort(), s.getLocalPort());

        // send GET request later than GossipRouter's routingClientReplyTimeout
        Thread.sleep(routingClientReplyTimeout + 500);

        // I expect the socket to be closed by now. I test this in a different
        // way on Java 1.3 and Java 1.4

        Exception expected13=null;
        Exception expected14=null;

        try {
            dos.writeInt(GossipRouter.GET);
            dos.writeUTF("testgroup");
        }
        catch(Exception e) {
            // on Java 1.4, writing on a closed socket fails
            expected14=e;
        }

        try {
            dis.readInt();
        }
        catch(Exception e) {
            // on Java 1.3, trying to read from the closet socket fails
            expected13=e;
        }

        assertTrue(expected14 instanceof SocketException ||
                expected13 instanceof EOFException);

        dis.close();
        dos.close();
        s.close();
    }


    public static Test suite() {
        TestSuite s=new TestSuite(LazyRoutingClientTest.class);
        return s;
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
        System.exit(0);
    }

}
