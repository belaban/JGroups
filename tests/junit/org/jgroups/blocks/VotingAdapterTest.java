package org.jgroups.blocks;


import org.testng.annotations.*;

import org.jgroups.Channel;
import org.jgroups.tests.ChannelTestBase;

public class VotingAdapterTest extends ChannelTestBase {
    private Channel channel1;
    private Channel channel2;

    protected VotingAdapter adapter1;
    protected VotingAdapter adapter2;

    protected TestVoteChannelListener listener1;
    protected TestVoteChannelListener listener2;
    protected TestVoteChannelListener listener3;
    protected TestVoteChannelListener listener4;

    protected static boolean logConfigured=false;

    public void setUp() throws Exception {
        ;
        listener1=new TestVoteChannelListener(true);
        listener2=new TestVoteChannelListener(true);
        listener3=new TestVoteChannelListener(false);
        listener4=new TestVoteChannelListener(false);

        channel1=createChannel("A");
        adapter1=new VotingAdapter(channel1);

        channel1.connect("voting");

        // give some time for the channel to become a coordinator
        try {
            Thread.sleep(1000);
        }
        catch(Exception ex) {
        }

        channel2=createChannel("A");
        adapter2=new VotingAdapter(channel2);

        channel2.connect("voting");

        try {
            Thread.sleep(1000);
        }
        catch(InterruptedException ex) {
        }
    }

    public void tearDown() throws Exception {
        channel2.close();

        try {
            Thread.sleep(1000);
        }
        catch(InterruptedException ex) {
        }


        channel1.close();
        ;
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


}
