package org.jgroups.blocks;


import org.jgroups.Channel;
import org.jgroups.JChannel;
import org.jgroups.util.Util;
import org.jgroups.tests.ChannelTestBase;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


@Test(groups="temp")
public class VotingAdapterTest extends ChannelTestBase {
    private JChannel channel1;
    private JChannel channel2;

    protected VotingAdapter adapter1;
    protected VotingAdapter adapter2;

    protected TestVoteChannelListener listener1;
    protected TestVoteChannelListener listener2;
    protected TestVoteChannelListener listener3;
    protected TestVoteChannelListener listener4;

    protected static boolean logConfigured=false;

    @BeforeMethod
    void setUp() throws Exception {
        listener1=new TestVoteChannelListener(true);
        listener2=new TestVoteChannelListener(true);
        listener3=new TestVoteChannelListener(false);
        listener4=new TestVoteChannelListener(false);

        channel1=createChannel(true);
        adapter1=new VotingAdapter(channel1);
        channel1.connect("VotingAdapterTest");

        channel2=createChannel(channel1);
        adapter2=new VotingAdapter(channel2);
        channel2.connect("VotingAdapterTest");
    }

    @AfterMethod
    void tearDown() throws Exception {
        Util.close(channel2, channel1);
    }

    public void testVoteAll() throws Exception {
        adapter1.addVoteListener(listener1);
        adapter2.addVoteListener(listener2);

        boolean voting1 = adapter1.vote("object1", VotingAdapter.VOTE_ALL, 1000);
    
        assert voting1 : "Result of voting1 should be 'true'";

        adapter1.addVoteListener(listener3);

        boolean voting2 = adapter1.vote("object2", VotingAdapter.VOTE_ALL, 1000);
    
        assert !voting2 : "Result of voting2 should be 'false'";
        
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


}
