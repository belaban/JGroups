package org.jgroups.blocks;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.JChannel;

public class VotingAdapterTest extends TestCase {

    public static final String SERVER_PROTOCOL_STACK = ""
            + "UDP(mcast_addr=228.3.11.76;mcast_port=12345;ip_ttl=1;"
            + "mcast_send_buf_size=150000;mcast_recv_buf_size=80000;down_thread=false)"
//        + "JMS(topicName=topic/testTopic;cf=UILConnectionFactory;"
//        + "jndiCtx=org.jnp.interfaces.NamingContextFactory;"
//        + "providerURL=localhost;ttl=10000)"
            + ":PING(timeout=500;num_initial_members=1;down_thread=false;up_thread=false)"
            + ":FD(down_thread=false;up_thread=false)"
            + ":VERIFY_SUSPECT(timeout=1500;down_thread=false;up_thread=false)"

            + ":pbcast.NAKACK(gc_lag=50;retransmit_timeout=300,600,1200,2400,4800;down_thread=false)"
            + ":FRAG(frag_size=4096;down_thread=false)"
            + ":UNICAST(timeout=5000)"
            + ":pbcast.STABLE(desired_avg_gossip=200;down_thread=false;up_thread=false)"
            + ":pbcast.GMS(join_timeout=5000;join_retry_timeout=1000;"
            +     "shun=false;print_local_addr=false;down_thread=true;up_thread=true)"
            //+ ":SPEED_LIMIT(down_queue_limit=10)"
            + ":pbcast.STATE_TRANSFER(down_thread=false)"
            ;


    public VotingAdapterTest(String testName) {
            super(testName);
    }

    public static Test suite() {
            return new TestSuite(VotingAdapterTest.class);
    }

    
    private JChannel channel1;
    private JChannel channel2;

    protected VotingAdapter adapter1;
    protected VotingAdapter adapter2;

    protected TestVoteChannelListener listener1;
    protected TestVoteChannelListener listener2;
    protected TestVoteChannelListener listener3;
    protected TestVoteChannelListener listener4;

    protected static
            boolean logConfigured;

    public void setUp() throws Exception {
        


        listener1 = new TestVoteChannelListener(true);
        listener2 = new TestVoteChannelListener(true);
        listener3 = new TestVoteChannelListener(false);
        listener4 = new TestVoteChannelListener(false);

        channel1 = new JChannel(SERVER_PROTOCOL_STACK);
        adapter1 = new VotingAdapter(channel1);
        
        channel1.connect("voting");

        // give some time for the channel to become a coordinator
		try {
			Thread.sleep(1000);
		} catch(Exception ex) {
		}
        
        channel2 = new JChannel(SERVER_PROTOCOL_STACK);
        adapter2 = new VotingAdapter(channel2);
        
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

    public void testVoteAll() throws Exception {
    
        adapter1.addVoteListener(listener1);
        adapter2.addVoteListener(listener2);

        boolean voting1 = adapter1.vote("object1", VotingAdapter.VOTE_ALL, 1000);
    
        assertTrue("Result of voting1 should be 'true'.", voting1);

        adapter1.addVoteListener(listener3);

        boolean voting2 = adapter1.vote("object2", VotingAdapter.VOTE_ALL, 1000);
    
        assertTrue("Result of voting2 should be 'false'.", !voting2);
        
    }

    /**
     * This class always vote according to the parameter passed on the
     * object creation.
     */
    public static class TestVoteChannelListener implements VotingListener {
            private boolean vote;

            public TestVoteChannelListener(boolean vote) {
                    this.vote = vote;
            }

            public boolean vote(Object decree) {
                    return vote;
            }
    }




    public static void main(String[] args) {
	junit.textui.TestRunner.run(suite());
    }
}
