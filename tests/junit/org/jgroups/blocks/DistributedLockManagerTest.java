package org.jgroups.blocks;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.JChannel;

public class DistributedLockManagerTest extends TestCase {

    public static final String SERVER_PROTOCOL_STACK = ""
            + "UDP(mcast_addr=228.3.11.76;mcast_port=12345;ip_ttl=1;"
            + "mcast_send_buf_size=150000;mcast_recv_buf_size=80000)"
//        + "JMS(topicName=topic/testTopic;cf=UILConnectionFactory;"
//        + "jndiCtx=org.jnp.interfaces.NamingContextFactory;"
//        + "providerURL=localhost;ttl=10000)"
            + ":PING(timeout=500;num_initial_members=1)"
            + ":FD"
            + ":VERIFY_SUSPECT(timeout=1500)" 
            + ":pbcast.NAKACK(gc_lag=50;retransmit_timeout=300,600,1200,2400,4800)"
            + ":UNICAST(timeout=5000)"
            + ":pbcast.STABLE(desired_avg_gossip=200)"
            + ":FRAG(frag_size=4096)"
            + ":pbcast.GMS(join_timeout=5000;join_retry_timeout=1000;"
            +     "shun=false;print_local_addr=false)"
//        + ":SPEED_LIMIT(down_queue_limit=10)"
//        + ":pbcast.STATE_TRANSFER(down_thread=false)"
        ;
    
    
    public DistributedLockManagerTest(String testName) {
            super(testName);
    }

    public static Test suite() {
            return new TestSuite(DistributedLockManagerTest.class);
    }

    
    private JChannel channel1;
    private JChannel channel2;

    protected VotingAdapter adapter1;
    protected VotingAdapter adapter2;
    
    protected LockManager lockManager1;
    protected LockManager lockManager2;

    protected static
            boolean logConfigured;

    public void setUp() throws Exception {
        


        channel1 = new JChannel(SERVER_PROTOCOL_STACK);
        adapter1 = new VotingAdapter(channel1);
        channel1.connect("voting");

        
        lockManager1 = new DistributedLockManager(adapter1, "1");

        // give some time for the channel to become a coordinator
        try {
                Thread.sleep(1000);
        } catch(Exception ex) {
        }
        
        channel2 = new JChannel(SERVER_PROTOCOL_STACK);
        adapter2 = new VotingAdapter(channel2);
        lockManager2 = new DistributedLockManager(adapter2, "2");
        
        channel2.connect("voting");
        
        try {
            Thread.sleep(1000);
        }
        catch (InterruptedException ex) {
        }
    }

    public void tearDown() throws Exception {
        channel2.close();
        
        try {
            Thread.sleep(1000);
        }
        catch (InterruptedException ex) {
        }
        
        
        channel1.close();
    }

    public void test() throws Exception {
        lockManager1.lock("obj1", "owner1", 10000);
        
        try {
            lockManager1.lock("obj1", "owner2", 10000);
            assertTrue("obj1 should not be locked.", false);
        } catch (LockNotGrantedException ex) {
            // everything is ok
        }
        
        lockManager2.lock("obj2", "owner2", 1000);
        
        lockManager1.unlock("obj1", "owner1");
        
        try {
            lockManager1.unlock("obj2", "owner1");
            assertTrue("obj2 should not be released.", false);
        }
        catch (LockNotReleasedException ex) {
            // everything is ok
        }
        
        lockManager1.unlock("obj2", "owner2");
        
    }

    public static void main(String[] args) {
	junit.textui.TestRunner.run(suite());
    }
}
