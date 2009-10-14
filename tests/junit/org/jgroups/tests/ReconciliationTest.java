package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.DISCARD;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests the FLUSH protocol, requires flush-udp.xml in ./conf to be present and
 * configured to use FLUSH
 * 
 * @author Bela Ban
 * @version $Id: ReconciliationTest.java,v 1.24 2009/10/14 09:41:57 belaban Exp $
 */
@Test(groups=Global.FLUSH,sequential=true)
public class ReconciliationTest extends ChannelTestBase {

    private List<JChannel> channels;

    private List<MyReceiver> receivers;

    @AfterMethod
    void tearDown() throws Exception {
        if(channels != null) {
            for(JChannel channel:channels) {
                channel.close();
            }
        }
        Util.sleep(500);
    }

    /**
     * Test scenario:
     * <ul>
     * <li>3 members: A,B,C
     * <li>All members have DISCARD which does <em>not</em> discard any
     * messages !
     * <li>B (in DISCARD) ignores all messages from C
     * <li>C multicasts 5 messages to the cluster, A and C receive them
     * <li>New member D joins
     * <li>Before installing view {A,B,C,D}, FLUSH updates B with all of C's 5
     * messages
     * </ul>
     */
    public void testReconciliationFlushTriggeredByNewMemberJoin() throws Exception {

        FlushTrigger t=new FlushTrigger() {
            public void triggerFlush() {
                log.info("Joining D, this will trigger FLUSH and a subsequent view change to {A,B,C,D}");
                JChannel newChannel;
                try {
                    newChannel=createChannel(channels.get(0));
                    newChannel.connect("ReconciliationTest");
                    channels.add(newChannel);
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            };
        };
        String apps[]={"A", "B", "C"};
        reconciliationHelper(apps, t);
    }

    /**
     * Test scenario:
     * <ul>
     * <li>3 members: A,B,C
     * <li>All members have DISCARD which does <em>not</em> discard any
     * messages !
     * <li>B (in DISCARD) ignores all messages from C
     * <li>C multicasts 5 messages to the cluster, A and C receive them
     * <li>A then runs a manual flush by calling Channel.start/stopFlush()
     * <li>Before installing view {A,B}, FLUSH makes A sends its 5 messages
     * received from C to B
     * </ul>
     */
    public void testReconciliationFlushTriggeredByManualFlush() throws Exception {

        FlushTrigger t=new FlushTrigger() {
            public void triggerFlush() {
                JChannel channel=channels.get(0);
                boolean rc=Util.startFlush(channel);
                log.info("manual flush success=" + rc);
                channel.stopFlush();
            };
        };
        String apps[]={"A", "B", "C"};
        reconciliationHelper(apps, t);
    }

    /**
     * Test scenario:
     * <ul>
     * <li>3 members: A,B,C
     * <li>All members have DISCARD which does <em>not</em> discard any
     * messages !
     * <li>B (in DISCARD) ignores all messages from C
     * <li>C multicasts 5 messages to the cluster, A and C receive them
     * <li>C then 'crashes' (Channel.shutdown())
     * <li>Before installing view {A,B}, FLUSH makes A sends its 5 messages
     * received from C to B
     * </ul>
     */
    public void testReconciliationFlushTriggeredByMemberCrashing() throws Exception {

        FlushTrigger t=new FlushTrigger() {
            public void triggerFlush() {
                JChannel channel=channels.remove(channels.size() - 1);
                try {
                    Util.shutdown(channel);
                }
                catch(Exception e) {
                    log.error("failed shutting down the channel", e);
                }
            };
        };
        String apps[]={"A", "B", "C"};
        reconciliationHelper(apps, t);
    }

    private void reconciliationHelper(String[] names, FlushTrigger ft) throws Exception {

        // create channels and setup receivers
        int channelCount=names.length;
        channels=new ArrayList<JChannel>(names.length);
        receivers=new ArrayList<MyReceiver>(names.length);
        for(int i=0;i < channelCount;i++) {
            JChannel channel;
            if(i == 0)
                channel=createChannel(true, names.length+2);
            else
                channel=createChannel(channels.get(0));
            MyReceiver r=new MyReceiver(channel, names[i]);
            receivers.add(r);
            channels.add(channel);
            channel.setReceiver(r);
            channel.connect("ReconciliationTest");
            Util.sleep(250);
        }
        JChannel last=channels.get(channels.size() - 1);
        JChannel nextToLast=channels.get(channels.size() - 2);

        insertDISCARD(nextToLast, last.getAddress());

        String lastsName=names[names.length - 1];
        String nextToLastName=names[names.length - 2];
        printDigests(channels, "\nDigests before " + lastsName + " sends any messages:");

        // now last sends 5 messages:
        log.info("\n" + lastsName
                 + " sending 5 messages;"
                 + nextToLastName
                 + " will ignore them, but others will receive them");
        for(int i=1;i <= 5;i++) {
            last.send(null, null, new Integer(i));
        }
        Util.sleep(1000); // until al messages have been received, this is
        // asynchronous so we need to wait a bit

        printDigests(channels, "\nDigests after " + lastsName + " sent messages:");

        MyReceiver lastReceiver=receivers.get(receivers.size() - 1);
        MyReceiver nextToLastReceiver=receivers.get(receivers.size() - 2);

        // check last (must have received its own messages)
        Map<Address,List<Integer>> map=lastReceiver.getMsgs();
        Assert.assertEquals(map.size(), 1, "we should have only 1 sender, namely C at this time");
        List<Integer> list=map.get(last.getAddress());
        log.info(lastsName + ": messages received from " + lastsName + ",list=" + list);
        Assert.assertEquals(list.size(), 5, "correct msgs: " + list);

        // check nextToLast (should have received none of last messages)
        map=nextToLastReceiver.getMsgs();
        Assert.assertEquals(map.size(), 0, "we should have no sender at this time");
        list=map.get(last.getAddress());
        log.info(nextToLastName + ": messages received from " + lastsName + " : " + list);
        assert list == null;

        List<MyReceiver> otherReceivers=receivers.subList(0, receivers.size() - 2);

        // check other (should have received last's messages)
        for(MyReceiver receiver:otherReceivers) {
            map=receiver.getMsgs();
            Assert.assertEquals(map.size(), 1, "we should have only 1 sender");
            list=map.get(last.getAddress());
            log.info(receiver.name + " messages received from " + lastsName + ":" + list);
            Assert.assertEquals(list.size(), 5, "correct msgs" + list);
        }

        removeDISCARD(nextToLast);

        Address address=last.getAddress();
        ft.triggerFlush();

        int cnt=1000;
        View v;
        while((v=channels.get(0).getView()) != null && cnt > 0) {
            cnt--;
            if(v.size() == channels.size())
                break;
            Util.sleep(500);
        }

        printDigests(channels, "");

        // check that member with discard (should have received all missing
        // messages
        map=nextToLastReceiver.getMsgs();
        Assert.assertEquals(map.size(), 1, "we should have 1 sender at this time");
        list=map.get(address);
        log.info(nextToLastName + ": messages received from " + lastsName + " : " + list);
        Assert.assertEquals(5, list.size());
    }

    private void printDigests(List<JChannel> channels, String message) {
        log.info(message);
        for(JChannel channel:channels) {
            log.info(channel.downcall(Event.GET_DIGEST_EVT).toString());
        }
    }

    private static void insertDISCARD(JChannel ch, Address exclude) throws Exception {
        DISCARD discard=new DISCARD();
        discard.setExcludeItself(true);
        discard.addIgnoreMember(exclude); // ignore messages from this member
        ch.getProtocolStack().insertProtocol(discard, ProtocolStack.BELOW, "NAKACK");
    }

    private static void removeDISCARD(JChannel...channels) throws Exception {
        for(JChannel ch:channels) {
            ch.getProtocolStack().removeProtocol("DISCARD");
        }
    }

    private interface FlushTrigger {
        void triggerFlush();
    }

    private class MyReceiver extends ExtendedReceiverAdapter {
        Map<Address,List<Integer>> msgs=new HashMap<Address,List<Integer>>(10);

        Channel channel;

        String name;

        public MyReceiver(Channel ch,String name) {
            this.channel=ch;
            this.name=name;
        }

        public Map<Address,List<Integer>> getMsgs() {
            return msgs;
        }

        public void reset() {
            msgs.clear();
        }

        public void receive(Message msg) {
            List<Integer> list=msgs.get(msg.getSrc());
            if(list == null) {
                list=new ArrayList<Integer>();
                msgs.put(msg.getSrc(), list);
            }
            list.add((Integer)msg.getObject());
            log.debug("[" + name
                               + " / "
                               + channel.getAddress()
                               + "]: received message from "
                               + msg.getSrc()
                               + ": "
                               + msg.getObject());
        }

        public void viewAccepted(View new_view) {
            log.debug("[" + name + " / " + channel.getLocalAddress() + "]: " + new_view);
        }
    }

    public void testVirtualSynchrony() throws Exception {
        JChannel c1 = createChannel(true,2);
        Cache cache_1 = new Cache(c1, "cache-1");
        c1.connect("testVirtualSynchrony");

        JChannel c2 = createChannel(c1);
        Cache cache_2 = new Cache(c2, "cache-2");
        c2.connect("testVirtualSynchrony");
        Assert.assertEquals(c2.getView().size(), 2, "view: " + c1.getView());

        // start adding messages
        flush(c1, 5000); // flush all pending message out of the system so
        // everyone receives them

        for(int i = 1;i <= 20;i++){
            if(i % 2 == 0){
                cache_1.put("key-" + i, Boolean.TRUE); // even numbers
            }else{
                cache_2.put("key-" + i, Boolean.TRUE); // odd numbers
            }
        }

        flush(c1, 5000);
        log.debug("cache_1 (" + cache_1.size()
                           + " elements): "
                           + cache_1
                           + "\ncache_2 ("
                           + cache_2.size()
                           + " elements): "
                           + cache_2);
        Assert.assertEquals(cache_1.size(), cache_2.size());
        Assert.assertEquals(20, cache_1.size());

        Util.close(c1,c2);

    }

    private void flush(Channel channel, long timeout) {
        if(channel.flushSupported()) {
            boolean success=Util.startFlush(channel);
            channel.stopFlush();
            log.debug("startFlush(): " + success);
            assertTrue(success);
        }
        else
            Util.sleep(timeout);
    }

    private class Cache extends ExtendedReceiverAdapter {
        protected final Map<Object,Object> data;

        Channel ch;

        String name;

        public Cache(Channel ch,String name) {
            this.data=new HashMap<Object,Object>();
            this.ch=ch;
            this.name=name;
            this.ch.setReceiver(this);
        }

        protected Object get(Object key) {
            synchronized(data) {
                return data.get(key);
            }
        }

        protected void put(Object key, Object val) throws Exception {
            Object[] buf=new Object[2];
            buf[0]=key;
            buf[1]=val;
            Message msg=new Message(null, null, buf);
            ch.send(msg);
        }

        protected int size() {
            synchronized(data) {
                return data.size();
            }
        }

        public void receive(Message msg) {
            Object[] modification=(Object[])msg.getObject();
            Object key=modification[0];
            Object val=modification[1];
            synchronized(data) {
                // System.out.println("****** [" + name + "] received PUT(" +
                // key + ", " + val + ") " + " from " + msg.getSrc() + "
                // *******");
                data.put(key, val);
            }
        }

        public byte[] getState() {
            byte[] state=null;
            synchronized(data) {
                try {
                    state=Util.objectToByteBuffer(data);
                }
                catch(Exception e) {
                    e.printStackTrace();
                    return null;
                }
            }
            return state;
        }

        public byte[] getState(String state_id) {
            return getState();
        }

        public void setState(byte[] state) {
            Map<Object,Object> m;
            try {
                m=(Map<Object,Object>)Util.objectFromByteBuffer(state);
                synchronized(data) {
                    data.clear();
                    data.putAll(m);
                }
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }

        public void setState(String state_id, byte[] state) {
            setState(state);
        }

        public void getState(OutputStream ostream) {
            ObjectOutputStream oos=null;
            try {
                oos=new ObjectOutputStream(ostream);
                synchronized(data) {
                    oos.writeObject(data);
                }
                oos.flush();
            }
            catch(IOException e) {
            }
            finally {
                try {
                    if(oos != null)
                        oos.close();
                }
                catch(IOException e) {
                    System.err.println(e);
                }
            }
        }

        public void getState(String state_id, OutputStream ostream) {
            getState(ostream);
        }

        public void setState(InputStream istream) {
            ObjectInputStream ois=null;
            try {
                ois=new ObjectInputStream(istream);
                Map<Object,Object> m=(Map<Object,Object>)ois.readObject();
                synchronized(data) {
                    data.clear();
                    data.putAll(m);
                }

            }
            catch(Exception e) {
            }
            finally {
                try {
                    if(ois != null)
                        ois.close();
                }
                catch(IOException e) {
                    System.err.println(e);
                }
            }
        }

        public void setState(String state_id, InputStream istream) {
            setState(istream);
        }

        public void clear() {
            synchronized(data) {
                data.clear();
            }
        }

        public void viewAccepted(View new_view) {
            log("view is " + new_view);
        }

        public String toString() {
            synchronized(data) {
                return data.toString();
            }
        }

        private void log(String msg) {
            log.debug("-- [" + name + "] " + msg);
        }
    }
}