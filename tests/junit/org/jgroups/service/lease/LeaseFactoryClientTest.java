package org.jgroups.service.lease;

import junit.framework.TestCase;
import org.jgroups.JChannel;

public class LeaseFactoryClientTest extends TestCase {

    public static final String SERVER_PROTOCOL_STACK=""
            + "UDP(mcast_addr=224.0.0.35;mcast_port=12345;ip_ttl=1;"
            + "mcast_send_buf_size=150000;mcast_recv_buf_size=80000;down_thread=false)"
            + ":PING(timeout=500;num_initial_members=1;down_thread=false;up_thread=false)"
            + ":FD(down_thread=false;up_thread=false)"
            + ":VERIFY_SUSPECT(timeout=1500;down_thread=false;up_thread=false)"

            + ":pbcast.NAKACK(gc_lag=50;retransmit_timeout=300,600,1200,2400,4800;down_thread=false)"
            + ":FRAG(frag_size=4096;down_thread=false)"
            + ":UNICAST(timeout=5000)"
            + ":pbcast.STABLE(desired_avg_gossip=200;down_thread=false;up_thread=false)"
            + ":pbcast.GMS(join_timeout=5000;join_retry_timeout=1000;"
            + "shun=false;print_local_addr=false;down_thread=true;up_thread=true)"
            //+ ":SPEED_LIMIT(down_queue_limit=10)"
            + ":pbcast.STATE_TRANSFER(down_thread=false)"
            ;

    public static final String CLIENT_PROTOCOL_STACK=""
            + "UDP(mcast_addr=224.0.0.36;mcast_port=56789;ip_ttl=1;"
            + "mcast_send_buf_size=150000;mcast_recv_buf_size=80000;down_thread=false)"
            + ":FRAG(frag_size=4096;down_thread=false;up_thread=false)"
            //+ ":pbcast.STABLE(desired_avg_gossip=200;down_thread=false;up_thread=false)"
            //+ ":pbcast.NAKACK(gc_lag=50;retransmit_timeout=300,600,1200,2400,4800;down_thread=false;up_thread=false)"
            + ":UNICAST(timeout=5000;use_gms=false)"
            ;


    public LeaseFactoryClientTest(String testName) {
        super(testName);
    }

    protected JChannel svcServerChannel;

    protected JChannel svcClientChannel;

    protected JChannel clientChannel;

    protected LeaseFactoryService leaseFactory;

    protected LeaseFactoryClient leaseClient;

    protected void setUp() throws Exception {

        svcServerChannel=new JChannel(SERVER_PROTOCOL_STACK);
        svcServerChannel.setOpt(JChannel.GET_STATE_EVENTS, Boolean.TRUE);
        svcServerChannel.connect("server");

        try {
            Thread.sleep(1000);
        }
        catch(InterruptedException ex) {
        }


        svcClientChannel=new JChannel(CLIENT_PROTOCOL_STACK);
        svcClientChannel.connect("client");

        clientChannel=new JChannel(CLIENT_PROTOCOL_STACK);
        clientChannel.connect("client");

        leaseFactory=new LeaseFactoryService(svcServerChannel, svcClientChannel);
        leaseClient=new LeaseFactoryClient(clientChannel);
    }

    protected void tearDown() throws Exception {
        clientChannel.close();
        svcClientChannel.close();
        svcServerChannel.close();
    }

    public void test() throws Exception {
        System.out.println("testing obtaining lease...");
        Lease lease=leaseClient.newLease("1", "foo", 10000, false);

        assertTrue("lease should be granted", lease != null);
        System.out.println("lease was granted.");

        try {
            Thread.sleep(100);
        }
        catch(InterruptedException ex) {
        }

        try {
            leaseClient.newLease("1", "bar", 1000, false);
            assertTrue("Lease should not be granted.", false);
        }
        catch(LeaseDeniedException ex) {
            // everyting is fine
        }

        System.out.println("trying to cancel lease...");
        leaseClient.cancelLease(lease);

        System.out.println("lease was canceled.");

        lease=leaseClient.newLease("1", "bar", 1000, false);
        assertTrue("new lease should have been granted.", lease != null);

        try {
            Thread.sleep(500);
        }
        catch(InterruptedException ex) {
        }

        System.out.println("renewing lease...");
        lease=leaseClient.renewLease(lease, 1000, false);
        assertTrue("lease should have been renewed", lease != null);
        System.out.println("lease renewed.");

        try {
            Thread.sleep(2000);
        }
        catch(InterruptedException ex) {
        }

        try {
            lease=leaseClient.renewLease(lease, 1000, false);
            assertTrue("lease should be expired", false);
        }
        catch(LeaseDeniedException ex) {
            // everything is fine
        }

        lease=leaseClient.newLease("1", "bar", 1000, false);
        leaseClient.cancelLease(lease);
    }


    public static void main(String[] args) {
        String[] testCaseName={LeaseFactoryClientTest.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
        System.out.println("press key to return");
        try {
            System.in.read();
        }
        catch(Exception ex) {
            log.error(ex);
        }
    }

}
