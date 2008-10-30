// $Id: GossipClientTest.java,v 1.2.6.1 2008/10/30 14:01:38 belaban Exp $

package org.jgroups.tests.stack;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.Address;
import org.jgroups.stack.GossipClient;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.GossipRouter;

import java.util.List;

/**
 * Tests Gossip protocol primitives with the new GossipRouter. Since 2.2.1, the
 * GossipRouter is supposed to answer Gossip requests too.
 *
 * @author Ovidiu Feodorov <ovidiuf@users.sourceforge.net>
 * @author Bela Ban
 * @version $Revision: 1.2.6.1 $
 * @since 2.2.1
 */
public class GossipClientTest extends TestCase {
    GossipClient client;
    private long expiryTime=1000;
    private GossipRouter router=null;

    public GossipClientTest(String name) {
        super(name);
    }

    public void setUp() throws Exception {
        super.setUp();
        router=new GossipRouter();
        router.setExpiryTime(expiryTime);
        router.start();
        client=new GossipClient(new IpAddress("127.0.0.1", 12001), expiryTime);
        client.setRefresherEnabled(false); // don't refresh the registrations
    }

    public void tearDown() throws Exception {
        super.tearDown();
        client.stop();
        router.stop();
    }


    public void testEmptyGET() throws Exception {
        String groupName="nosuchgroup";
        List mbrs=client.getMembers(groupName);
        assertNotNull(mbrs);
        assertEquals(0, mbrs.size());
    }


    /**
     * Registers an address with a group and then sends a GET request for that group.
     */
    public void test_REGISTER_GET() throws Exception {
        String groupName="TESTGROUP";
        int mbrPort=7777;
        Address mbr=new IpAddress("127.0.0.1", mbrPort);
        client.register(groupName, mbr);

        List mbrs=client.getMembers(groupName);
        assertEquals(1, mbrs.size());
        assertEquals(new IpAddress("127.0.0.1", mbrPort), mbrs.get(0));
    }

    public void test_REGISTER_UNREGISTER_GET() throws Exception {
        String groupName="TESTGROUP";
        int mbrPort=7777;
        Address mbr=new IpAddress("127.0.0.1", mbrPort);
        client.register(groupName, mbr);

        List mbrs=client.getMembers(groupName);
        assertEquals(1, mbrs.size());
        assertEquals(new IpAddress("127.0.0.1", mbrPort), mbrs.get(0));

        client.unregister(groupName, mbr);
        mbrs=client.getMembers(groupName);
        assertNotNull(mbrs);
        assertEquals(0, mbrs.size());
    }


    /**
     * Test if a member is removed from group after EXPIRY_TIME ms.
     */
    public void testSweep() throws Exception {
        String groupName="TESTGROUP";
        int mbrPort=7777;
        Address mbr=new IpAddress("127.0.0.1", mbrPort);

        client.register(groupName, mbr);

        List mbrs=client.getMembers(groupName);
        assertEquals(1, mbrs.size());
        assertEquals(new IpAddress("127.0.0.1", mbrPort), mbrs.get(0));

        // because the sweep is ran at fixed expiryTime intervals, if
        // an entry was added immediately after a sweep run, it actually 
        // spends almost 2*expiryTime in cache.
        Thread.sleep(2 * expiryTime);

        // send a second GET after more than EXPIRY_TIME ms
        mbrs=client.getMembers(groupName);
        assertTrue(mbrs == null || mbrs.isEmpty());
    }

    public static Test suite() {
        return new TestSuite(GossipClientTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
        System.exit(0);
    }


}
