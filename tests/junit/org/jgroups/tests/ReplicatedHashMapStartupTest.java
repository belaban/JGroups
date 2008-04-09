package org.jgroups.tests;


import org.testng.annotations.*;
import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.ChannelException;
import org.jgroups.View;
import org.jgroups.blocks.ReplicatedHashMap;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Vector;

/**
 * Tests concurrent startup or replicated hashmap.
 * 
 * @author vlada
 * @version $Id: ReplicatedHashMapStartupTest.java,v 1.5 2008/04/09 15:01:35 belaban Exp $
 */
public class ReplicatedHashMapStartupTest extends ChannelTestBase {

    private static final int FACTORY_COUNT = 8;


    protected int getMuxFactoryCount() {
        return FACTORY_COUNT;
    }

    protected boolean useBlocking() {
        return true;
    }

    @Test
    public void testConcurrentStartup4Members() {
        concurrentStartupHelper(4);
    }

    @Test
    public void testConcurrentStartup8Members() {
        concurrentStartupHelper(8);
    }

    protected void concurrentStartupHelper(int channelCount) {
        List<ReplicatedHashMap<Address, Integer>> channels = new ArrayList<ReplicatedHashMap<Address, Integer>>(channelCount);        
        MyNotification<Address, Integer> n = new MyNotification<Address, Integer>();
       
        for (int i = 0; i < channelCount; i++) {
            try {
                Channel c = createChannel();
                c.setOpt(Channel.AUTO_RECONNECT, true);                
                ReplicatedHashMap<Address, Integer> map = new ReplicatedHashMap<Address, Integer>(c);
                channels.add(map);
                map.addNotifier(n);
                map.setBlockingUpdates(true);                              
            } catch (Exception e) {
                e.printStackTrace();
            } 
        }
        
        //do a very concurrent startup
        for (ReplicatedHashMap<Address, Integer> map : channels) {
            try {
                map.getChannel().connect("bla");
                map.start(0);
                map.put(map.getChannel().getLocalAddress(), new Integer(1));
                Util.sleep(1000);
            } catch (ChannelException e) {               
                e.printStackTrace();
            }
        }
        
        //verify all view are correct
        for (ReplicatedHashMap<Address, Integer> map : channels) {
            Assert.assertEquals(map.getChannel().getView().size(), channelCount, "Correct view");
        }
               
        for (ReplicatedHashMap<Address, Integer> map : channels) {
            map.removeNotifier(n);
        }
        
        //verify all maps have all elements
        for (ReplicatedHashMap<Address, Integer> map : channels) {
            Assert.assertEquals(map.size(), channelCount, "Correct size");
        }
       
        System.out.println("stopping");
        for (ReplicatedHashMap<Address, Integer> map : channels) {
            map.stop();
            Util.sleep(1000);
        }
              
    }
    private static class MyNotification<K extends Serializable, V extends Serializable>
            implements org.jgroups.blocks.ReplicatedHashMap.Notification<K, V> {

        public void contentsCleared() {           
        }

        public void contentsSet(Map<K, V> new_entries) {       
        }

        public void entryRemoved(K key) {     
        }

        public void entrySet(K key, V value) {                    
        }

        public void viewChange(View view, Vector<Address> new_mbrs,
                Vector<Address> old_mbrs) {
            System.out.println("Got view " + view);                      
        }

      
    }

}
