// $Id: GossipClientTest.java,v 1.4 2008/04/08 07:19:14 belaban Exp $

package org.jgroups.tests.stack;

import org.jgroups.Address;
import org.jgroups.stack.GossipClient;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;

/**
 * Tests Gossip protocol primitives with the new GossipRouter. Since 2.2.1, the
 * GossipRouter is supposed to answer Gossip requests too.
 *
 * @author Ovidiu Feodorov <ovidiuf@users.sourceforge.net>
 * @author Bela Ban
 * @version $Revision: 1.4 $
 * @since 2.2.1
 */
public class GossipClientTest {
    GossipClient client;
    private int port=-1;
    private long expiryTime=1000;

    public GossipClientTest(String name) {
    }

    @BeforeMethod
    public void setUp() throws Exception {
        ;
        port=Utilities.startGossipRouter(expiryTime, "127.0.0.1");
        client=new GossipClient(new IpAddress("127.0.0.1", port), expiryTime, 1000, null);
        client.setRefresherEnabled(false); // don't refresh the registrations
    }

    @AfterMethod
    public void tearDown() throws Exception {
        ;
        client.stop();
        Utilities.stopGossipRouter();
    }


    @Test
    public void testEmptyGET() throws Exception {
        String groupName="nosuchgroup";
        List mbrs=client.getMembers(groupName);
        assert mbrs != null;
        Assert.assertEquals(0, mbrs.size());
    }


    /**
     * Registers an address with a group and then sends a GET request for that group.
     */
    @Test
    public void test_REGISTER_GET() throws Exception {
        String groupName="TESTGROUP";
        int mbrPort=7777;
        Address mbr=new IpAddress("127.0.0.1", mbrPort);
        client.register(groupName, mbr, true);

        List mbrs=client.getMembers(groupName);
        Assert.assertEquals(1, mbrs.size());
        Assert.assertEquals(new IpAddress("127.0.0.1", mbrPort), mbrs.get(0));
    }

    @Test
    public void test_REGISTER_UNREGISTER_GET() throws Exception {
        String groupName="TESTGROUP";
        int mbrPort=7777;
        Address mbr=new IpAddress("127.0.0.1", mbrPort);
        client.register(groupName, mbr);

        List mbrs=client.getMembers(groupName);
        Assert.assertEquals(1, mbrs.size());
        Assert.assertEquals(new IpAddress("127.0.0.1", mbrPort), mbrs.get(0));

        client.unregister(groupName, mbr);// done asynchronous, on a separate thread
        Util.sleep(500);
        mbrs=client.getMembers(groupName);
        assert mbrs != null;
        Assert.assertEquals(0, mbrs.size());
    }


    /**
     * Test if a member is removed from group after EXPIRY_TIME ms.
     */
    @Test
    public void testSweep() throws Exception {
        String groupName="TESTGROUP";
        int mbrPort=7777;
        Address mbr=new IpAddress("127.0.0.1", mbrPort);

        client.register(groupName, mbr);

        List mbrs=client.getMembers(groupName);
        Assert.assertEquals(1, mbrs.size());
        Assert.assertEquals(new IpAddress("127.0.0.1", mbrPort), mbrs.get(0));

        // because the sweep is ran at fixed expiryTime intervals, if
        // an entry was added immediately after a sweep run, it actually 
        // spends almost 2*expiryTime in cache.
        Thread.sleep(2 * expiryTime);

        // send a second GET after more than EXPIRY_TIME ms
        mbrs=client.getMembers(groupName);
        assert mbrs == null || mbrs.isEmpty();
    }

  


}
