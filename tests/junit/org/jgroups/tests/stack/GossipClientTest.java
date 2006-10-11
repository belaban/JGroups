// $Id: GossipClientTest.java,v 1.1 2006/10/11 08:01:12 belaban Exp $

package org.jgroups.tests.stack;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.Address;
import org.jgroups.stack.GossipData;
import org.jgroups.stack.GossipRouter;
import org.jgroups.stack.IpAddress;

import java.io.*;
import java.net.Socket;
import java.util.Vector;

/**
 * Tests Gossip protocol primitives with the new GossipRouter. Since 2.2.1, the
 * GossipRouter is supposed to answer Gossip requests too.
 *
 * @author Ovidiu Feodorov <ovidiuf@users.sourceforge.net>
 * @version $Revision: 1.1 $
 * @since 2.2.1
 */
public class GossipClientTest extends TestCase {
    private int port=-1;
    private long expiryTime=10000;

    public GossipClientTest(String name) {
        super(name);
    }

    public void setUp() throws Exception {
        super.setUp();
        port=Utilities.startGossipRouter(expiryTime);
    }

    public void tearDown() throws Exception {
        super.tearDown();
        Utilities.stopGossipRouter();
    }

    /**
     * Sends a Gossip GET request for an inexistent group.
     */
    public void testEmptyGET() throws Exception {
        String groupName="nosuchgroup";
        Socket s=new Socket("localhost", port);
        DataOutputStream oos=new DataOutputStream(s.getOutputStream());
        GossipData greq=new GossipData(GossipRouter.GET, groupName, null, null);
        greq.writeTo(oos);
        oos.flush();
        DataInputStream ois=new DataInputStream(s.getInputStream());
        GossipData gres=new GossipData();
        gres.readFrom(ois);
        assertEquals(GossipRouter.GET_RSP, gres.getType());
        assertEquals(groupName, gres.getGroup());
        Vector mbrs=gres.getMembers();
        assertNull(mbrs);
        oos.close();
        ois.close();
        s.close();
    }


    /**
     * Tests the situation when the gossip client is late in sending the
     * serialized GossipData, after it opened the connection. If using the GossipRouter,
     * the client will get a StreamCorruptedException and the call will fail.
     */
    public void testLazyClient() throws Exception {
        String groupName="TESTGROUP";
        Socket s=new Socket("localhost", port);
        DataOutputStream oos=new DataOutputStream(s.getOutputStream());
        GossipData greq=new GossipData(GossipRouter.GET, groupName, null, null);
        Thread.sleep(GossipRouter.GOSSIP_REQUEST_TIMEOUT + 500);
        greq.writeTo(oos);
        oos.flush();
        DataInputStream ois=null;

        try {
            ois=new DataInputStream(s.getInputStream());
            fail("Stream creation should have failed");
        }
        catch(Exception e) {
            assertTrue(e instanceof StreamCorruptedException);
        }
        oos.close();
        s.close();

        // send the second request, to make sure the server didn't hang

        s=new Socket("localhost", port);
        oos=new DataOutputStream(s.getOutputStream());
        greq=new GossipData(GossipRouter.GET, groupName, null, null);
        greq.writeTo(oos);
        oos.flush();
        ois=new DataInputStream(s.getInputStream());
        GossipData gres=new GossipData();
        gres.readFrom(ois);
        assertEquals(GossipRouter.GET_RSP, gres.getType());
        assertEquals(groupName, gres.getGroup());
        Vector mbrs=gres.getMembers();
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
        DataOutputStream oos=new DataOutputStream(s.getOutputStream());
        Address mbr=new IpAddress("localhost", mbrPort);
        GossipData greq = new GossipData(GossipRouter.REGISTER, groupName, mbr, null);
        greq.writeTo(oos);
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
        oos=new DataOutputStream(s.getOutputStream());
        greq=new GossipData(GossipRouter.GET, groupName, null, null);
        greq.writeTo(oos);
        oos.flush();
        DataInputStream ois=new DataInputStream(s.getInputStream());
        GossipData gres=new GossipData();
        gres.readFrom(ois);
        assertEquals(GossipRouter.GET_RSP, gres.getType());
        assertEquals(groupName, gres.getGroup());
        Vector mbrs=gres.getMembers();
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
        DataOutputStream oos=new DataOutputStream(s.getOutputStream());
        Address mbr=new IpAddress("localhost", mbrPort);
        GossipData greq=new GossipData(GossipRouter.REGISTER, groupName, mbr, null);
        greq.writeTo(oos);
        oos.flush();
        oos.close();
        s.close();

        // send GET
        s=new Socket("localhost", port);
        oos=new DataOutputStream(s.getOutputStream());
        greq=new GossipData(GossipRouter.GET, groupName, null, null);
        greq.writeTo(oos);
        oos.flush();
        DataInputStream ois=new DataInputStream(s.getInputStream());
        GossipData gres=new GossipData();
        gres.readFrom(ois);
        assertEquals(GossipRouter.GET_RSP, gres.getType());
        assertEquals(groupName, gres.getGroup());
        Vector mbrs=gres.getMembers();
        assertEquals(1, mbrs.size());
        assertEquals(new IpAddress("localhost", mbrPort), mbrs.get(0));
        oos.close();
        ois.close();
        s.close();

        // because the sweep is ran at fixed expiryTime intervals, if
        // an entry was added immediately after a sweep run, it actually 
        // spends almost 2*expiryTime in cache.
        Thread.sleep(2 * expiryTime);

        // send a second GET after more than EXPIRY_TIME ms

        s=new Socket("localhost", port);
        oos=new DataOutputStream(s.getOutputStream());
        greq=new GossipData(GossipRouter.GET, groupName, null, null);
        greq.writeTo(oos);
        oos.flush();
        ois=new DataInputStream(s.getInputStream());
        gres=new GossipData();
        gres.readFrom(ois);
        assertEquals(GossipRouter.GET_RSP, gres.getType());
        assertEquals(groupName, gres.getGroup());
        mbrs=gres.getMembers();
        assertTrue(mbrs == null || mbrs.size() == 0);
        oos.close();
        ois.close();
        s.close();

    }

    public static Test suite() {
        return new TestSuite(GossipClientTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
        System.exit(0);
    }


}
