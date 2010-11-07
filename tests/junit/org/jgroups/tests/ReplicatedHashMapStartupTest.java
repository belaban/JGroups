package org.jgroups.tests;

import static java.util.concurrent.TimeUnit.SECONDS;

import org.jgroups.*;
import org.jgroups.blocks.ReplicatedHashMap;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Vector;

/**
 * Tests concurrent startup or replicated hashmap.
 * 
 * @author vlada
 * @version $Id: ReplicatedHashMapStartupTest.java,v 1.6 2008/06/09 13:03:06
 *          belaban Exp $
 */
@Test(groups= { Global.FLUSH }, sequential=true)
public class ReplicatedHashMapStartupTest extends ChannelTestBase {

    public void testConcurrentStartup4Members() {
        List<ReplicatedHashMap<Address,Integer>> channels=new ArrayList<ReplicatedHashMap<Address,Integer>>(4);
        try {
            concurrentStartupHelper(channels, 4);
        }
        catch(Exception e) {
            log.warn("Exception while running testConcurrentStartup4Members", e);
            for(ReplicatedHashMap<Address,Integer> map:channels) {
                map.stop();
                Util.sleep(1000);
            }
        }
    }

    public void testConcurrentStartup8Members() {
        List<ReplicatedHashMap<Address,Integer>> channels=new ArrayList<ReplicatedHashMap<Address,Integer>>(8);
        try {
            concurrentStartupHelper(channels, 8);
        }
        catch(Exception e) {
            log.warn("Exception while running testConcurrentStartup8Members", e);
            for(ReplicatedHashMap<Address,Integer> map:channels) {
                map.stop();
                Util.sleep(1000);
            }
        }
    }

    protected void concurrentStartupHelper(List<ReplicatedHashMap<Address,Integer>> channels,
                                           int channelCount) throws Exception {
        MyNotification<Address,Integer> n=new MyNotification<Address,Integer>();

        JChannel first=null;
        for(int i=0;i < channelCount;i++) {
            JChannel c;
            if(i == 0) {
                c=createChannel(true, channelCount);
                modifyGMS(c);
                first=c;
            }
            else {
                c=createChannel(first);
            }
            ReplicatedHashMap<Address,Integer> map=new ReplicatedHashMap<Address,Integer>(c);
            channels.add(map);
            map.addNotifier(n);
            map.setBlockingUpdates(true);
        }

        //do a very concurrent startup
        for(ReplicatedHashMap<Address,Integer> map:channels) {
            map.getChannel().connect("ReplicatedHashMapStartupTest");
            map.start(0);
            map.put(map.getChannel().getAddress(), new Integer(1));
            Util.sleep(100);
        }

        boolean converged=false;
        for(int timeoutToConverge=120,counter=0;counter < timeoutToConverge && !converged;SECONDS.sleep(1),counter++) {
            for(ReplicatedHashMap<Address,Integer> map:channels) {
                converged=map.getChannel().getView().size() == channelCount;
                if(!converged)
                    break;
            }
        }

        //verify all view are correct
        for(ReplicatedHashMap<Address,Integer> map:channels) {
            Assert.assertEquals(map.getChannel().getView().size(), channelCount, "Correct view");
        }

        for(ReplicatedHashMap<Address,Integer> map:channels) {
            map.removeNotifier(n);
        }

        //verify all maps have all elements
        for(ReplicatedHashMap<Address,Integer> map:channels) {
            Assert.assertEquals(map.size(), channelCount, "Correct size");
        }

        log.info("stopping replicated hash maps...");
        for(ReplicatedHashMap<Address,Integer> map:channels) {
            map.stop();
            Util.sleep(1000);
        }
    }

    private static void modifyGMS(JChannel c) {
        ProtocolStack stack=c.getProtocolStack();
        GMS gms=(GMS)stack.findProtocol(GMS.class);
        if(gms != null)
            gms.setLogCollectMessages(false);
    }

    private class MyNotification<K extends Serializable, V extends Serializable> implements
            org.jgroups.blocks.ReplicatedHashMap.Notification<K,V> {

        public void contentsCleared() {}

        public void contentsSet(Map<K,V> new_entries) {}

        public void entryRemoved(K key) {}

        public void entrySet(K key, V value) {}

        public void viewChange(View view, Vector<Address> new_mbrs, Vector<Address> old_mbrs) {
            log.info("Got view in ReplicatedHashMap notifier " + view);
        }
    }
}
