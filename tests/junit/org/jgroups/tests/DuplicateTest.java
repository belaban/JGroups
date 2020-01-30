package org.jgroups.tests;


import org.jgroups.*;
import org.jgroups.protocols.DUPL;
import org.jgroups.protocols.pbcast.NAKACK2;
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
@Test(groups=Global.STACK_DEPENDENT,singleThreaded=true)
public class DuplicateTest extends ChannelTestBase {
    private JChannel   a, b, c;
    protected Address  a1, a2, a3;
    private MyReceiver r1, r2, r3;


    @BeforeMethod
    void init() throws Exception {
        createChannels(true, true, (short)5, (short)5);
        a1=a.getAddress();
        a2=b.getAddress();
        a3=c.getAddress();

        r1=new MyReceiver("A");
        r2=new MyReceiver("B");
        r3=new MyReceiver("C");
        a.setReceiver(r1);
        b.setReceiver(r2);
        c.setReceiver(r3);
    }

    @AfterMethod
    void tearDown() throws Exception {
        removeDUPL(c,b,a);
        Util.close(c,b,a);
    }



    public void testRegularUnicastsToSelf() throws Exception {
        send(a, a.getAddress(), false, 10);
        sendStableMessages(a,b, c);
        check(r1, 1, false, new Tuple<>(a1, 10));
    }

    public void testOOBUnicastsToSelf() throws Exception {
        send(a, a.getAddress(), true, 10);
        sendStableMessages(a,b,c);
        check(r1, 1, true, new Tuple<>(a1, 10));
    }

    public void testRegularUnicastsToOthers() throws Exception {
        send(a, b.getAddress(), false, 10);
        send(a, c.getAddress(), false, 10);
        sendStableMessages(a,b,c);
        check(r2, 1, false, new Tuple<>(a1, 10));
        check(r3, 1, false, new Tuple<>(a1, 10));
    }

    @Test(invocationCount=10)
    public void testOOBUnicastsToOthers() throws Exception {
        send(a, b.getAddress(), true, 10);
        send(a, c.getAddress(), true, 10);
        sendStableMessages(a,b,c);
        check(r2, 1, true, new Tuple<>(a1, 10));
        check(r3, 1, true, new Tuple<>(a1, 10));
    }


    public void testRegularMulticastToAll() throws Exception {
        send(a, null, false, 10);
        sendStableMessages(a,b,c);
        check(r1, 1, false, new Tuple<>(a1, 10));
        check(r2, 1, false, new Tuple<>(a1, 10));
        check(r3, 1, false, new Tuple<>(a1, 10));
    }


    public void testOOBMulticastToAll() throws Exception {
        send(a, null, true, 10);
        sendStableMessages(a,b,c);
        check(r1,1,true,new Tuple<>(a1,10));
        check(r2, 1, true, new Tuple<>(a1, 10));
        check(r3, 1, true, new Tuple<>(a1, 10));
    }


    public void testRegularMulticastToAll3Senders() throws Exception {
        send(a, null, false, 10);
        send(b, null, false, 10);
        send(c, null, false, 10);
        sendStableMessages(a,b,c);
        check(r1, 3, false, new Tuple<>(a1, 10), new Tuple<>(a2, 10), new Tuple<>(a3, 10));
        check(r2, 3, false, new Tuple<>(a1, 10), new Tuple<>(a2, 10), new Tuple<>(a3, 10));
        check(r3, 3, false, new Tuple<>(a1, 10), new Tuple<>(a2, 10), new Tuple<>(a3, 10));
    }

    @Test(invocationCount=5)
    public void testOOBMulticastToAll3Senders() throws Exception {
        send(a, null, true, 10);
        send(b, null, true, 10);
        send(c, null, true, 10);
        sendStableMessages(a,b,c);
        check(r1, 3, true, new Tuple<>(a1, 10), new Tuple<>(a2, 10), new Tuple<>(a3, 10));
        check(r2, 3, true, new Tuple<>(a1, 10), new Tuple<>(a2, 10), new Tuple<>(a3, 10));
        check(r3, 3, true, new Tuple<>(a1, 10), new Tuple<>(a2, 10), new Tuple<>(a3, 10));
    }

    public void testMixedMulticastsToAll3Members() throws Exception {
        send(a, null, false, true, 10);
        send(b, null, false, true, 10);
        send(c, null, false, true, 10);
        sendStableMessages(a,b,c);
        check(r1, 3, true, new Tuple<>(a1, 10), new Tuple<>(a2, 10), new Tuple<>(a3, 10));
        check(r2, 3, true, new Tuple<>(a1, 10), new Tuple<>(a2, 10), new Tuple<>(a3, 10));
        check(r3, 3, true, new Tuple<>(a1, 10), new Tuple<>(a2, 10), new Tuple<>(a3, 10));
    }


     private static void send(JChannel sender_channel, Address dest, boolean oob, int num_msgs) throws Exception {
         send(sender_channel, dest, oob, false, num_msgs);
     }

     private static void send(JChannel sender_channel, Address dest, boolean oob, boolean mixed, int num_msgs) throws Exception {
         long seqno=1;
         for(int i=0; i < num_msgs; i++) {
             Message msg=new BytesMessage(dest, seqno++);
             if(mixed) {
                 if(i % 2 == 0)
                     msg.setFlag(Message.Flag.OOB);
             }
             else if(oob) {
                 msg.setFlag(Message.Flag.OOB);
             }

             sender_channel.send(msg);
         }
     }


    private static void sendStableMessages(JChannel ... channels) {
        for(JChannel ch: channels) {
            STABLE stable=ch.getProtocolStack().findProtocol(STABLE.class);
            if(stable != null)
                stable.gc();
        }
    }

    protected static void removeDUPL(JChannel ... channels) {
        for(JChannel ch: channels) {
            DUPL dupl=ch.getProtocolStack().findProtocol(DUPL.class);
            if(dupl != null) {
                dupl.setCopyMulticastMsgs(false);
                dupl.setCopyUnicastMsgs(false);
            }
        }
    }



    private void createChannels(boolean copy_multicasts, boolean copy_unicasts, int num_outgoing_copies, int num_incoming_copies) throws Exception {
        a=createChannel(true, 3, "A");
        DUPL dupl=new DUPL(copy_multicasts, copy_unicasts, num_incoming_copies, num_outgoing_copies);
        ProtocolStack stack=a.getProtocolStack();
        stack.insertProtocol(dupl,ProtocolStack.Position.BELOW,NAKACK2.class);

        b=createChannel(a, "B");
        c=createChannel(a, "C");

        a.connect("DuplicateTest");
        b.connect("DuplicateTest");
        c.connect("DuplicateTest");

        Util.waitUntilAllChannelsHaveSameView(20000, 1000, a, b, c);
    }


    @SafeVarargs
    private final void check(MyReceiver receiver, int expected_size, boolean oob, Tuple<Address,Integer>... vals) {
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
                sendStableMessages(a,b,c);
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




    private static class MyReceiver implements Receiver {
        final String name;
        private final ConcurrentMap<Address, Collection<Long>> msgs=new ConcurrentHashMap<>();

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
            Long val=msg.getObject();

            Collection<Long> list=msgs.get(addr);
            if(list == null) {
                list=new ConcurrentLinkedQueue<>();
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