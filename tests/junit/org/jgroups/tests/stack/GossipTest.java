// $Id: GossipTest.java,v 1.3 2004/03/30 06:47:30 belaban Exp $

package org.jgroups.tests.stack;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.Address;
import org.jgroups.stack.GossipData;
import org.jgroups.stack.GossipRouter;
import org.jgroups.stack.IpAddress;

import java.io.EOFException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.StreamCorruptedException;
import java.net.Socket;
import java.util.Vector;

/**
 * Tests Gossip protocol primitives with the new GossipRouter. Since 2.2.1, the
 * GossipRouter is supposed to answer Gossip requests too.
 * <p/>
 * It is possible to switch all tests to use an old GossipServer by setting
 * USE_ROUTER to false;
 *
 * @author Ovidiu Feodorov <ovidiuf@users.sourceforge.net>
 * @version $Revision: 1.3 $
 * @since 2.2.1
 */
public class GossipTest extends TestCase {

    // configures the test to use the new gossip-enabled Router or the old
    // GossipServer
    private final boolean USE_ROUTER=true;

    private int port=-1;

    private long expiryTime=10000;

    public GossipTest(String name) {
        super(name);
    }

    public void setUp() throws Exception {
        super.setUp();
        if(USE_ROUTER) {
            port=Utilities.startGossipRouter(expiryTime);
        }
        else {
            port=Utilities.startGossipServer(expiryTime);
        }
    }

    public void tearDown() throws Exception {
        super.tearDown();
        if(USE_ROUTER) {
            Utilities.stopGossipRouter();
        }
        else {
            Utilities.stopGossipServer(port);
        }
    }

    /**
     * Sends a Gossip GET request for an inexistent group.
     */
    public void testEmptyGET() throws Exception {

        String groupName="nosuchgroup";
        Socket s=new Socket("localhost", port);
        ObjectOutputStream oos=new ObjectOutputStream(s.getOutputStream());
        GossipData greq=
                new GossipData(GossipData.GET_REQ, groupName, null, null);
        oos.writeObject(greq);
        oos.flush();
        ObjectInputStream ois=new ObjectInputStream(s.getInputStream());
        GossipData gres=(GossipData)ois.readObject();
        assertEquals(GossipData.GET_RSP, gres.getType());
        assertEquals(groupName, gres.getGroup());
        Vector mbrs=gres.getMbrs();
        assertNull(mbrs);

        oos.close();
        ois.close();
        s.close();

    }


    /**
     * Tests the situation when the gossip client is late in sending the
     * serialized GossipData, after it opened the connection. If using a
     * GossipServer, this shouldn't be an issue.  If using the GossipRouter,
     * the client will get a StreamCorruptedException and the call will fail.
     */
    public void testLazyClient() throws Exception {

        String groupName="TESTGROUP";
        Socket s=new Socket("localhost", port);
        ObjectOutputStream oos=new ObjectOutputStream(s.getOutputStream());
        GossipData greq=
                new GossipData(GossipData.GET_REQ, groupName, null, null);

        Thread.currentThread().
                sleep(GossipRouter.GOSSIP_REQUEST_TIMEOUT + 500);

        oos.writeObject(greq);
        oos.flush();
        ObjectInputStream ois=null;

        if(USE_ROUTER) {
            try {
                ois=new ObjectInputStream(s.getInputStream());
                fail("Stream creation should have failed");
            }
            catch(Exception e) {
                assertTrue(e instanceof StreamCorruptedException);
            }
        }
        else {
            // old GossipServer
            ois=new ObjectInputStream(s.getInputStream());
            GossipData gres=(GossipData)ois.readObject();
            assertEquals(GossipData.GET_RSP, gres.getType());
            assertEquals(groupName, gres.getGroup());
            Vector mbrs=gres.getMbrs();
            assertNull(mbrs);
            ois.close();
        }

        oos.close();
        s.close();

        // send the second request, to make sure the server didn't hang

        s=new Socket("localhost", port);
        oos=new ObjectOutputStream(s.getOutputStream());
        greq=new GossipData(GossipData.GET_REQ, groupName, null, null);

        oos.writeObject(greq);
        oos.flush();
        ois=new ObjectInputStream(s.getInputStream());
        GossipData gres=(GossipData)ois.readObject();
        assertEquals(GossipData.GET_RSP, gres.getType());
        assertEquals(groupName, gres.getGroup());
        Vector mbrs=gres.getMbrs();
        assertNull(mbrs);

        oos.close();
        ois.close();
        s.close();

    }


    /**
     * Registers an Address with a group and then sends a GET request for that
     * group.
     */
    public void test_REGISTER_GET() throws Exception {

        String groupName="TESTGROUP";
        int mbrPort=7777;

        Socket s=new Socket("localhost", port);
        ObjectOutputStream oos=new ObjectOutputStream(s.getOutputStream());
        Address mbr=new IpAddress("localhost", mbrPort);
        GossipData greq=
                new GossipData(GossipData.REGISTER_REQ, groupName, mbr, null);
        oos.writeObject(greq);
        oos.flush();
        
        // test for end of stream
        try {
            s.getInputStream();
        }
        catch(EOFException e) {
            if(!(e instanceof EOFException)) {
                fail("The input stream was supposed to throw EOFException");
            }
        }

        oos.close();
        s.close();

        // send GET
        s=new Socket("localhost", port);
        oos=new ObjectOutputStream(s.getOutputStream());
        greq=new GossipData(GossipData.GET_REQ, groupName, null, null);
        oos.writeObject(greq);
        oos.flush();
        ObjectInputStream ois=new ObjectInputStream(s.getInputStream());
        GossipData gres=(GossipData)ois.readObject();
        assertEquals(GossipData.GET_RSP, gres.getType());
        assertEquals(groupName, gres.getGroup());
        Vector mbrs=gres.getMbrs();
        assertEquals(1, mbrs.size());
        assertEquals(new IpAddress("localhost", mbrPort), mbrs.get(0));

        oos.close();
        ois.close();
        s.close();

    }

    /**
     * Test if a member is removed from group after EXPIRY_TIME ms.
     */
    public void testSweep() throws Exception {

        String groupName="TESTGROUP";
        int mbrPort=7777;

        Socket s=new Socket("localhost", port);
        ObjectOutputStream oos=new ObjectOutputStream(s.getOutputStream());
        Address mbr=new IpAddress("localhost", mbrPort);
        GossipData greq=
                new GossipData(GossipData.REGISTER_REQ, groupName, mbr, null);
        oos.writeObject(greq);
        oos.flush();
        oos.close();
        s.close();

        // send GET
        s=new Socket("localhost", port);
        oos=new ObjectOutputStream(s.getOutputStream());
        greq=new GossipData(GossipData.GET_REQ, groupName, null, null);
        oos.writeObject(greq);
        oos.flush();
        ObjectInputStream ois=new ObjectInputStream(s.getInputStream());
        GossipData gres=(GossipData)ois.readObject();
        assertEquals(GossipData.GET_RSP, gres.getType());
        assertEquals(groupName, gres.getGroup());
        Vector mbrs=gres.getMbrs();
        assertEquals(1, mbrs.size());
        assertEquals(new IpAddress("localhost", mbrPort), mbrs.get(0));

        oos.close();
        ois.close();
        s.close();

        // because the sweep is ran at fixed expiryTime intervals, if
        // an entry was added immediately after a sweep run, it actually 
        // spends almost 2*expiryTime in cache.
        Thread.currentThread().sleep(2 * expiryTime);

        // send a second GET after more than EXPIRY_TIME ms

        s=new Socket("localhost", port);
        oos=new ObjectOutputStream(s.getOutputStream());
        greq=new GossipData(GossipData.GET_REQ, groupName, null, null);
        oos.writeObject(greq);
        oos.flush();
        ois=new ObjectInputStream(s.getInputStream());
        gres=(GossipData)ois.readObject();
        assertEquals(GossipData.GET_RSP, gres.getType());
        assertEquals(groupName, gres.getGroup());
        mbrs=gres.getMbrs();
        assertEquals(0, mbrs.size());

        oos.close();
        ois.close();
        s.close();

    }

    public static Test suite() {
        TestSuite s=new TestSuite(GossipTest.class);
        return s;
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
        System.exit(0);
    }


}
