package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.blocks.ReplicatedHashMap;
import org.jgroups.protocols.pbcast.FLUSH;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.STATE_TRANSFER;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests concurrent startup or replicated hashmap.
 * 
 * @author vlada
 */
@Test(groups= {Global.FLUSH,Global.EAP_EXCLUDED}, singleThreaded=true)
public class ReplicatedHashMapStartupTest {

    public void testConcurrentStartup4Members() throws Exception {
        concurrentStartupHelper(4);
    }

    public void testConcurrentStartup8Members() throws Exception {
        concurrentStartupHelper(8);
    }


    protected void concurrentStartupHelper(int channelCount) throws Exception {
        List<ReplicatedHashMap<Address,Integer>> maps=new ArrayList<>(channelCount);
        JChannel[] channels=new JChannel[channelCount];
        try {
            for(int i=0;i < channelCount;i++) {
                channels[i]=createChannel(String.valueOf((char)('A' + i)));
                modifyGMS(channels[i]);
                ReplicatedHashMap<Address,Integer> map=new ReplicatedHashMap<>(channels[i]);
                maps.add(map);
                map.setBlockingUpdates(true);
            }

            // do a very concurrent startup
            for(ReplicatedHashMap<Address,Integer> map:maps) {
                map.getChannel().connect("ReplicatedHashMapStartupTest");
                map.start(0);
                map.put(map.getChannel().getAddress(),1);
                Util.sleep(100);
            }

           Util.waitUntilAllChannelsHaveSameSize(10000, 500, channels); // verify all views are correct

            // verify all maps have all elements
            for(ReplicatedHashMap<Address,Integer> map:maps)
                Assert.assertEquals(map.size(), channelCount, "Correct size");

        }
        finally {
            for(ReplicatedHashMap<Address,Integer> map:maps)
                map.stop();
        }
    }

    protected JChannel createChannel(String name) throws Exception {
        return new JChannel(Util.getTestStack(new STATE_TRANSFER(), new FLUSH())).name(name);
    }

    protected static void modifyGMS(JChannel c) {
        ProtocolStack stack=c.getProtocolStack();
        GMS gms=(GMS)stack.findProtocol(GMS.class);
        if(gms != null)
            gms.setLogCollectMessages(false);
    }

}
