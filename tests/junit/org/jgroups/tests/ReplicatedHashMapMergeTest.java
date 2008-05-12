package org.jgroups.tests;

import org.jgroups.Channel;
import org.jgroups.JChannel;
import org.jgroups.blocks.ReplicatedHashMap;
import org.jgroups.protocols.DISCARD;
import org.jgroups.protocols.FD;
import org.jgroups.protocols.MERGE2;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;

public class ReplicatedHashMapMergeTest extends ChannelTestBase {

    private Channel[] channels=new Channel[2];

    private ReplicatedHashMap<String,String> map1;
    private ReplicatedHashMap<String,String> map2;

    public ReplicatedHashMapMergeTest(String testName) {
        super(testName);
    }

    protected void setUp() throws Exception {
        super.setUp();
        CHANNEL_CONFIG=System.getProperty("channel.conf.flush", "flush-udp.xml");

        channels[0]=myChannel();
        this.map1=new ReplicatedHashMap<String,String>(channels[0], false);
        map1.setBlockingUpdates(true);
        channels[0].connect("demo");
        this.map1.start(5000);

        channels[1]=myChannel();
        this.map2=new ReplicatedHashMap<String,String>(channels[1], false);
        map2.setBlockingUpdates(true);
        channels[1].connect("demo");
        this.map2.start(5000);
    }

    protected void tearDown() throws Exception {
        this.map1.stop();
        this.map2.stop();
        super.tearDown();
    }

    public void testConsistencyAfterPartition() {
        assertTrue(map1.isEmpty());
        assertTrue(map2.isEmpty());

        // Partition.
        discard(true);
        Util.sleep(15000);

        // Divergent state.
        map1.put("key1", "true");
        map2.put("key2", "true");

        // Merge.
        discard(false);
        Util.sleep(15000);
        assertEquals(map1, map2);
    }

    private JChannel myChannel() throws Exception {
        final JChannel result=new JChannel(CHANNEL_CONFIG);
        addDiscardProtocol(result);
        modifyFDAndMergeSettings(result);
        return result;
    }

    private static void modifyFDAndMergeSettings(JChannel ch) {
        ProtocolStack stack=ch.getProtocolStack();

        FD fd=(FD)stack.findProtocol("FD");
        if(fd != null) {
            fd.setMaxTries(3);
            fd.setTimeout(1000);
        }
        MERGE2 merge=(MERGE2)stack.findProtocol("MERGE2");
        if(merge != null) {
            merge.setMinInterval(5000);
            merge.setMaxInterval(10000);
        }
    }

    private static void addDiscardProtocol(JChannel ch) throws Exception {
        ProtocolStack stack=ch.getProtocolStack();
        Protocol transport=stack.getTransport();
        DISCARD discard=new DISCARD();
        discard.setProtocolStack(ch.getProtocolStack());
        discard.start();
        stack.insertProtocol(discard, ProtocolStack.ABOVE, transport.getName());
    }

    private void discard(final boolean discard_on) {
        for(final Channel channel:channels) {
            DISCARD discard=(DISCARD)((JChannel)channel).getProtocolStack().findProtocol("DISCARD");
            discard.setDiscardAll(discard_on);
        }
    }
}
