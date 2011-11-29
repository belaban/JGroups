package org.jgroups.tests;


import org.jgroups.*;
import org.jgroups.protocols.DUPL;
import org.jgroups.protocols.UNICAST2;
import org.jgroups.protocols.pbcast.NAKACK;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Tuple;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

/**
 * Tests whether UNICAST or NAKACK prevent delivery of duplicate messages. JGroups guarantees that a message is
 * delivered once and only once. The test inserts DUPL below both UNICAST and NAKACK and makes it duplicate (1)
 * unicast, (2) multicast, (3) regular and (4) OOB messages. The receiver(s) then check for the presence of duplicate
 * messages. 
 * @author Bela Ban
 */
@Test(groups=Global.STACK_DEPENDENT,sequential=true)
public class DuplicateTest extends ChannelTestBase {
    private JChannel c1, c2, c3;
    protected Address a1, a2, a3;
    private MyReceiver r1, r2, r3;


    @BeforeMethod
    void init() throws Exception {
        createChannels(true, true, (short)5, (short)5);
        a1=c1.getAddress();
        a2=c2.getAddress();
        a3=c3.getAddress();

        r1=new MyReceiver("C1");
        r2=new MyReceiver("C2");
        r3=new MyReceiver("C3");
        c1.setReceiver(r1);
        c2.setReceiver(r2);
        c3.setReceiver(r3);
    }

    @AfterMethod
    void tearDown() throws Exception {
        removeDUPL(c3, c2, c1);
        Util.close(c3, c2, c1);
    }



    public void testRegularUnicastsToSelf() throws Exception {
        send(c1, c1.getAddress(), false, 10);
        sendStableMessages(c1, c2, c3);
        check(r1, 1, false, new Tuple<Address,Integer>(a1, 10));
    }

    public void testOOBUnicastsToSelf() throws Exception {
        send(c1, c1.getAddress(), true, 10);
        sendStableMessages(c1,c2,c3);
        check(r1, 1, true, new Tuple<Address,Integer>(a1, 10));
    }

    public void testRegularUnicastsToOthers() throws Exception {
        send(c1, c2.getAddress(), false, 10);
        send(c1, c3.getAddress(), false, 10);
        sendStableMessages(c1,c2,c3);
        check(r2, 1, false, new Tuple<Address,Integer>(a1, 10));
        check(r3, 1, false, new Tuple<Address,Integer>(a1, 10));
    }

    @Test(invocationCount=10)
    public void testOOBUnicastsToOthers() throws Exception {
        send(c1, c2.getAddress(), true, 10);
        send(c1, c3.getAddress(), true, 10);
        sendStableMessages(c1,c2,c3);
        check(r2, 1, true, new Tuple<Address,Integer>(a1, 10));
        check(r3, 1, true, new Tuple<Address,Integer>(a1, 10));
    }


    public void testRegularMulticastToAll() throws Exception {
        send(c1, null /** multicast */, false, 10);
        sendStableMessages(c1,c2,c3);
        check(r1, 1, false, new Tuple<Address,Integer>(a1, 10));
        check(r2, 1, false, new Tuple<Address,Integer>(a1, 10));
        check(r3, 1, false, new Tuple<Address,Integer>(a1, 10));
    }


    public void testOOBMulticastToAll() throws Exception {
        send(c1, null /** multicast */, true, 10);
        sendStableMessages(c1,c2,c3);
        check(r1, 1, true, new Tuple<Address,Integer>(a1, 10));
        check(r2, 1, true, new Tuple<Address,Integer>(a1, 10));
        check(r3, 1, true, new Tuple<Address,Integer>(a1, 10));
    }


    public void testRegularMulticastToAll3Senders() throws Exception {
        send(c1, null /** multicast */, false, 10);
        send(c2, null /** multicast */, false, 10);
        send(c3, null /** multicast */, false, 10);
        sendStableMessages(c1,c2,c3);
        check(r1, 3, false, new Tuple<Address,Integer>(a1, 10), new Tuple<Address,Integer>(a2, 10), new Tuple<Address,Integer>(a3, 10));
        check(r2, 3, false, new Tuple<Address,Integer>(a1, 10), new Tuple<Address,Integer>(a2, 10), new Tuple<Address,Integer>(a3, 10));
        check(r3, 3, false, new Tuple<Address,Integer>(a1, 10), new Tuple<Address,Integer>(a2, 10), new Tuple<Address,Integer>(a3, 10));
    }

    @Test(invocationCount=5)
    public void testOOBMulticastToAll3Senders() throws Exception {
        send(c1, null /** multicast */, true, 10);
        send(c2, null /** multicast */, true, 10);
        send(c3, null /** multicast */, true, 10);
        sendStableMessages(c1,c2,c3);
        check(r1, 3, true, new Tuple<Address,Integer>(a1, 10), new Tuple<Address,Integer>(a2, 10), new Tuple<Address,Integer>(a3, 10));
        check(r2, 3, true, new Tuple<Address,Integer>(a1, 10), new Tuple<Address,Integer>(a2, 10), new Tuple<Address,Integer>(a3, 10));
        check(r3, 3, true, new Tuple<Address,Integer>(a1, 10), new Tuple<Address,Integer>(a2, 10), new Tuple<Address,Integer>(a3, 10));
    }

    public void testMixedMulticastsToAll3Members() throws Exception {
        send(c1, null /** multicast */, false, true, 10);
        send(c2, null /** multicast */, false, true, 10);
        send(c3, null /** multicast */, false, true, 10);
        sendStableMessages(c1,c2,c3);
        check(r1, 3, true, new Tuple<Address,Integer>(a1, 10), new Tuple<Address,Integer>(a2, 10), new Tuple<Address,Integer>(a3, 10));
        check(r2, 3, true, new Tuple<Address,Integer>(a1, 10), new Tuple<Address,Integer>(a2, 10), new Tuple<Address,Integer>(a3, 10));
        check(r3, 3, true, new Tuple<Address,Integer>(a1, 10), new Tuple<Address,Integer>(a2, 10), new Tuple<Address,Integer>(a3, 10));
    }


     private static void send(Channel sender_channel, Address dest, boolean oob, int num_msgs) throws Exception {
         send(sender_channel, dest, oob, false, num_msgs);
     }

     private static void send(Channel sender_channel, Address dest, boolean oob, boolean mixed, int num_msgs) throws Exception {
         long seqno=1;
         for(int i=0; i < num_msgs; i++) {
             Message msg=new Message(dest, null, seqno++);
             if(mixed) {
                 if(i % 2 == 0)
                     msg.setFlag(Message.OOB);
             }
             else if(oob) {
                 msg.setFlag(Message.OOB);
             }

             sender_channel.send(msg);
         }
     }


    private static void sendStableMessages(JChannel ... channels) {
        for(JChannel ch: channels) {
            STABLE stable=(STABLE)ch.getProtocolStack().findProtocol(STABLE.class);
            if(stable != null)
                stable.runMessageGarbageCollection();
        }
    }

    protected static void removeDUPL(JChannel ... channels) {
        for(JChannel ch: channels) {
            DUPL dupl=(DUPL)ch.getProtocolStack().findProtocol(DUPL.class);
            if(dupl != null) {
                dupl.setCopyMulticastMsgs(false);
                dupl.setCopyUnicastMsgs(false);
            }
        }
    }


    private static void sendUnicastStableMessages(JChannel ... channels) {
        for(JChannel ch: channels) {
            UNICAST2 unicast=(UNICAST2)ch.getProtocolStack().findProtocol(UNICAST2.class);
            if(unicast != null)
                unicast.sendStableMessages();
        }
    }


    private void createChannels(boolean copy_multicasts, boolean copy_unicasts, int num_outgoing_copies, int num_incoming_copies) throws Exception {
        c1=createChannel(true, 3);
        DUPL dupl=new DUPL(copy_multicasts, copy_unicasts, num_incoming_copies, num_outgoing_copies);
        ProtocolStack stack=c1.getProtocolStack();
        stack.insertProtocol(dupl, ProtocolStack.BELOW, NAKACK.class);

        c2=createChannel(c1);
        c3=createChannel(c1);

        c1.setName("C1");
        c2.setName("C2");
        c3.setName("c3");

        c1.connect("DuplicateTest");
        c2.connect("DuplicateTest");
        c3.connect("DuplicateTest");

        Util.waitUntilAllChannelsHaveSameSize(20000, 1000, c1, c2, c3);
    }


    private void check(MyReceiver receiver, int expected_size, boolean oob, Tuple<Address,Integer>... vals) {
        Map<Address, Collection<Long>> msgs=receiver.getMsgs();

        for(int i=0; i < 10; i++) {
            if(msgs.size() >= expected_size)
                break;
            Util.sleep(1000);
        }
        assert msgs.size() == expected_size : "expected size=" + expected_size + ", msgs: " + msgs.keySet();


        for(Tuple<Address,Integer> tuple: vals) {
            Address addr=tuple.getVal1();
            Collection<Long> list=msgs.get(addr);
            assert list != null : "no list available for " + addr;

            int expected_values=tuple.getVal2();
            for(int i=0; i < 20; i++) {
                if(list.size() >= expected_values)
                    break;
                Util.sleep(1000);
                sendStableMessages(c1,c2,c3);
            }

            System.out.println("[" + receiver.getName() + "]: " + addr + ": " + list);
            assert list.size() == expected_values : addr + "'s list's size is not " + tuple.getVal2() +
                    ", list: " + list + " (size=" + list.size() + ")";
            if(!oob) // if OOB messages, ordering is not guaranteed
                check(addr, list);
            else
                checkPresence(list);
        }
    }


    private static void check(Address addr, Collection<Long> list) {
        long id=list.iterator().next();
        for(long val: list) {
            assert val == id : "[" + addr + "]: val=" + val + " (expected " + id + "): list is " + list;
            id++;
        }
    }

    private static void checkPresence(Collection<Long> list) {
        for(long l=1; l <= 10; l++) {
            assert list.contains(l) : l + " is not in the list " + list;
        }
    }




    private static class MyReceiver extends ReceiverAdapter {
        final String name;
        private final ConcurrentMap<Address, Collection<Long>> msgs=new ConcurrentHashMap<Address,Collection<Long>>();

        public MyReceiver(String name) {
            this.name=name;
        }

        public String getName() {
            return name;
        }

        public Map<Address, Collection<Long>> getMsgs() {
            return msgs;
        }

        public void receive(Message msg) {
            Address addr=msg.getSrc();
            Long val=(Long)msg.getObject();

            Collection<Long> list=msgs.get(addr);
            if(list == null) {
                list=new ConcurrentLinkedQueue<Long>();
                Collection<Long> tmp=msgs.putIfAbsent(addr, list);
                if(tmp != null)
                    list=tmp;
            }
            list.add(val);
        }

        public void clear() {
            msgs.clear();
        }


        public String toString() {
            StringBuilder sb=new StringBuilder();
            sb.append("receiver " + name).append(":\n");
            for(Map.Entry<Address,Collection<Long>> entry: msgs.entrySet()) {
                sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
            }
            return sb.toString();
        }

    }

}