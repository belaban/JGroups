package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.FLUSH;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.io.*;
import java.util.*;

/**
 * Various tests for the FLUSH protocol
 * @author Bela Ban
 */
@Test(groups={Global.FLUSH,Global.EAP_EXCLUDED},singleThreaded=true)
public class ReconciliationTest {
    protected List<JChannel>   channels;
    protected List<MyReceiver> receivers;

    @AfterMethod void tearDown() throws Exception {
        if(channels != null)
            for(Closeable closeable: channels)
                Util.close(closeable);
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
                System.out.println("Joining D, this will trigger FLUSH and a subsequent view change to {A,B,C,D}");
                JChannel newChannel;
                try {
                    newChannel=createChannel("X");
                    newChannel.connect("ReconciliationTest");
                    channels.add(newChannel);
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            };
        };
        reconciliationHelper(new String[]{"A", "B", "C"}, t);
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
                System.out.println("manual flush success=" + rc);
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
                    e.printStackTrace();
                }
            };
        };
        String apps[]={"A", "B", "C"};
        reconciliationHelper(apps, t);
    }


    /**
     * Tests reconciliation. Creates N channels, based on 'names'. Say we have A, B and C. Then we have the second but
     * last node (B) discard all messages from the last node (C). Then the last node (C) multicasts 5 messages. We check
     * that the 5 messages have been received correctly by all nodes but the second-but-last node (B). Then we remove
     * DISCARD from B and trigger a manual flush. After the flush, B should also have received the 5 messages sent
     * by C.
     */
    protected void reconciliationHelper(String[] names, FlushTrigger ft) throws Exception {

        // create channels and setup receivers
        int channelCount=names.length;
        channels=new ArrayList<JChannel>(names.length);
        receivers=new ArrayList<MyReceiver>(names.length);
        for(int i=0;i < channelCount;i++) {
            JChannel channel=createChannel(names[i]);
            modifyNAKACK(channel);
            MyReceiver r=new MyReceiver(channel, names[i]);
            receivers.add(r);
            channels.add(channel);
            channel.setReceiver(r);
            channel.connect("ReconciliationTest");
            Util.sleep(i == 0? 1000 : 250);
        }

        View view=channels.get(channels.size() -1).getView();
        System.out.println("view: " + view);
        assert view.size() == channels.size();

        JChannel last=channels.get(channels.size() - 1);
        JChannel nextToLast=channels.get(channels.size() - 2);

        System.out.println(nextToLast.getAddress() + " is now discarding messages from " + last.getAddress());
        insertDISCARD(nextToLast, last.getAddress());

        String lastsName=names[names.length - 1];
        String nextToLastName=names[names.length - 2];
        printDigests(channels, "\nDigests before " + lastsName + " sends any messages:");

        // now last sends 5 messages:
        System.out.println("\n" + lastsName + " sending 5 messages; " + nextToLastName + " will ignore them, but others will receive them");
        for(int i=1;i <= 5;i++)
            last.send(null, new Integer(i));

        Util.sleep(1000); // until al messages have been received, this is asynchronous so we need to wait a bit

        printDigests(channels, "\nDigests after " + lastsName + " sent messages:");

        MyReceiver lastReceiver=receivers.get(receivers.size() - 1);
        MyReceiver nextToLastReceiver=receivers.get(receivers.size() - 2);

        // check last (must have received its own messages)
        Map<Address,List<Integer>> map=lastReceiver.getMsgs();
        Assert.assertEquals(map.size(), 1, "we should have only 1 sender, namely C at this time");
        List<Integer> list=map.get(last.getAddress());
        System.out.println("\n" + lastsName + ": messages received from " + lastsName + ": " + list);
        Assert.assertEquals(list.size(), 5, "correct msgs: " + list);

        // check nextToLast (should have received none of last messages)
        map=nextToLastReceiver.getMsgs();
        Assert.assertEquals(map.size(), 0, "we should have no sender at this time");
        list=map.get(last.getAddress());
        System.out.println(nextToLastName + ": messages received from " + lastsName + ": " + list);
        assert list == null;

        List<MyReceiver> otherReceivers=receivers.subList(0, receivers.size() - 2);

        // check other (should have received last's messages)
        for(MyReceiver receiver:otherReceivers) {
            map=receiver.getMsgs();
            Assert.assertEquals(map.size(), 1, "we should have only 1 sender");
            list=map.get(last.getAddress());
            System.out.println(receiver.name + ": messages received from " + lastsName + ": " + list);
            Assert.assertEquals(list.size(), 5, "correct msgs" + list);
        }

        removeDISCARD(nextToLast);

        Address address=last.getAddress();
        ft.triggerFlush();

        int cnt=20;
        View v;
        while((v=channels.get(0).getView()) != null && cnt > 0) {
            cnt--;
            if(v.size() == channels.size())
                break;
            Util.sleep(1000);
        }
        assert channels.get(0).getView().size() == channels.size();
        printDigests(channels, "\nDigests after reconciliation (B should have received the 5 messages from B now):");

        // check that member with discard (should have received all missing
        // messages
        map=nextToLastReceiver.getMsgs();
        Assert.assertEquals(map.size(), 1, "we should have 1 sender at this time");
        list=map.get(address);
        System.out.println("\n" + nextToLastName + ": messages received from " + lastsName + " : " + list);
        Assert.assertEquals(5, list.size());
    }

    protected JChannel createChannel(String name) throws Exception {
        Protocol[] protocols={
          new SHARED_LOOPBACK(),
          new SHARED_LOOPBACK_PING(),
          new FD_ALL().setValue("timeout", 3000).setValue("interval", 1000),
          new NAKACK2(),
          new UNICAST3(),
          new STABLE(),
          new GMS(),
          new FRAG2().fragSize(8000),
          new FLUSH()
        };

        return new JChannel(protocols).name(name);
    }

    /** Sets discard_delivered_msgs to false */
    protected void modifyNAKACK(JChannel ch) {
        if(ch == null) return;
        NAKACK2 nakack=(NAKACK2)ch.getProtocolStack().findProtocol(NAKACK2.class);
        if(nakack != null)
            nakack.setDiscardDeliveredMsgs(false);
    }

    private static void printDigests(List<JChannel> channels, String message) {
        System.out.println(message);
        for(JChannel channel:channels) {
            System.out.println("[" + channel.getAddress() + "] " + channel.down(Event.GET_DIGEST_EVT).toString());
        }
    }

    private static void insertDISCARD(JChannel ch, Address exclude) throws Exception {
        DISCARD discard=new DISCARD().localAddress(ch.getAddress());
        discard.setExcludeItself(true);
        discard.addIgnoreMember(exclude); // ignore messages from this member
        ch.getProtocolStack().insertProtocol(discard, ProtocolStack.BELOW, NAKACK2.class);
    }

    private static void removeDISCARD(JChannel...channels) throws Exception {
        for(JChannel ch:channels)
            ch.getProtocolStack().removeProtocol(DISCARD.class);
    }

    private interface FlushTrigger {
        void triggerFlush();
    }

    protected static class MyReceiver extends ReceiverAdapter {
        protected final Map<Address,List<Integer>> msgs=new HashMap<Address,List<Integer>>(10);
        protected final Channel channel;
        protected final String  name;

        public MyReceiver(Channel ch,String name) {
            this.channel=ch;
            this.name=name;
        }

        public Map<Address,List<Integer>> getMsgs() {return msgs;}
        public void                       reset()   {msgs.clear();}

        public void receive(Message msg) {
            List<Integer> list=msgs.get(msg.getSrc());
            if(list == null) {
                list=new ArrayList<Integer>();
                msgs.put(msg.getSrc(), list);
            }
            list.add((Integer)msg.getObject());
            System.out.println(name + ": <-- " + msg.getObject() + " from " + msg.getSrc());
        }
    }

    public void testVirtualSynchrony() throws Exception {
        JChannel a = createChannel("A");
        Cache cache_1 = new Cache(a, "cache-1");
        a.connect("testVirtualSynchrony");

        JChannel b = createChannel("B");
        Cache cache_2 = new Cache(b, "cache-2");
        b.connect("testVirtualSynchrony");
        Util.waitUntilAllChannelsHaveSameSize(10000, 500, a, b);

        // start adding messages
        flush(a); // flush all pending message out of the system so everyone receives them

        for(int i = 1; i <= 20;i++) {
            if(i % 2 == 0)
                cache_1.put(i, true); // even numbers
            else
                cache_2.put(i, true); // odd numbers
        }

        System.out.println("Starting flush on C1");
        flush(a);
        System.out.println("Starting flush on C2");
        flush(b);
        System.out.println("flush done");

        System.out.println("cache_1 (" + cache_1.size()
                             + " elements): "
                             + cache_1
                             + "\ncache_2 ("
                             + cache_2.size()
                             + " elements): "
                             + cache_2);
        Assert.assertEquals(cache_1.size(), 20, "cache 1: " + cache_1);
        Assert.assertEquals(cache_2.size(), 20, "cache 2: " + cache_2);
        Util.close(b,a);
    }

    protected static void flush(Channel channel) {
        try {
            assert Util.startFlush(channel);
        }
        finally {
            channel.stopFlush();
        }
    }

    protected static class Cache extends ReceiverAdapter {
        protected final Map<Object,Object> data;
        protected Channel                  ch;
        protected String                   name;

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
            ch.send(new Message(null, null, new Object[]{key,val}));
        }

        protected int size() {
            synchronized(data) {
                return data.size();
            }
        }

        public void receive(Message msg) {
            Object[] modification=(Object[])msg.getObject();
            synchronized(data) {
                data.put(modification[0], modification[1]);
            }
        }


        public void getState(OutputStream ostream) throws Exception {
            synchronized(data) {
                Util.objectToStream(data, new DataOutputStream(ostream));
            }
        }

        @SuppressWarnings("unchecked")
        public void setState(InputStream istream) throws Exception {
            Map<Object,Object> m=(Map<Object,Object>)Util.objectFromStream(new DataInputStream(istream));
            synchronized(data) {
                data.clear();
                data.putAll(m);
            }
        }

        public String toString() {
            synchronized(data) {
                TreeMap<Object,Object> map=new TreeMap<Object,Object>(data);
                return map.keySet().toString();
            }
        }
    }
}