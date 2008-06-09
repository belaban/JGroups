package org.jgroups.tests;


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
 * @version $Id: ReplicatedHashMapStartupTest.java,v 1.6 2008/06/09 13:03:06 belaban Exp $
 */
@Test(groups={"temp"},sequential=true)
public class ReplicatedHashMapStartupTest extends ChannelTestBase {


    protected boolean useBlocking() {
        return true;
    }

    public void testConcurrentStartup4Members() {
        concurrentStartupHelper(4);
    }

    public void testConcurrentStartup8Members() {
        concurrentStartupHelper(8);
    }

    protected void concurrentStartupHelper(int channelCount) {
        List<ReplicatedHashMap<Address, Integer>> channels = new ArrayList<ReplicatedHashMap<Address, Integer>>(channelCount);        
        MyNotification<Address, Integer> n = new MyNotification<Address, Integer>();

        JChannel first=null;
        for (int i = 0; i < channelCount; i++) {
            try {
                JChannel c;
                if(i == 0) {
                    c = createChannel(true, channelCount);
                    modifyGMS(c);
                    first=c;
                }
                else
                    c=createChannel(first);
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

    private static void modifyGMS(JChannel c) {
        ProtocolStack stack=c.getProtocolStack();
        GMS gms=(GMS)stack.findProtocol(GMS.class);
        if(gms != null)
            gms.setLogCollectMessages(false);
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
