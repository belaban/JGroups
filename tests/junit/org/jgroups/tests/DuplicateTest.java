package org.jgroups.tests;


import org.jgroups.*;
import org.jgroups.protocols.DUPL;
import org.jgroups.protocols.pbcast.NAKACK;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Tuple;
import org.jgroups.util.Util;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Tests whether UNICAST or NAKACK prevent delivery of duplicate messages. JGroups guarantees that a message is
 * delivered once and only once. The test inserts DUPL below both UNICAST and NAKACK and makes it duplicate (1)
 * unicast, (2) multicast, (3) regular and (4) OOB messages. The receiver(s) then check for the presence of duplicate
 * messages. 
 * @author Bela Ban
 * @version $Id: DuplicateTest.java,v 1.9 2008/08/08 17:07:12 vlada Exp $
 */
@Test(groups=Global.STACK_DEPENDENT,sequential=true)
public class DuplicateTest extends ChannelTestBase {
    private JChannel c1, c2, c3;
    protected Address a1, a2, a3;
    private MyReceiver r1, r2, r3;


    @BeforeClass
    void classInit() throws Exception {
        createChannels(true, true, (short)2, (short)2);
        a1=c1.getLocalAddress();
        a2=c2.getLocalAddress();
        a3=c3.getLocalAddress();
    }

    @BeforeMethod
    void init() throws Exception {
        r1=new MyReceiver("C1");
        r2=new MyReceiver("C2");
        r3=new MyReceiver("C3");
        c1.setReceiver(r1);
        c2.setReceiver(r2);
        c3.setReceiver(r3);
    }

    @AfterClass
    void tearDown() throws Exception {
        Util.close(c3, c2, c1);
    }



    public void testRegularUnicastsToSelf() throws Exception {
        send(c1, c1.getLocalAddress(), false, 10);
        check(r1, 1, false, new Tuple<Address,Integer>(a1, 10));
    }

    public void testOOBUnicastsToSelf() throws Exception {
        send(c1, c1.getLocalAddress(), true, 10);
        check(r1, 1, true, new Tuple<Address,Integer>(a1, 10));
    }

    public void testRegularUnicastsToOthers() throws Exception {
        send(c1, c2.getLocalAddress(), false, 10);
        send(c1, c3.getLocalAddress(), false, 10);
        check(r2, 1, false, new Tuple<Address,Integer>(a1, 10));
        check(r3, 1, false, new Tuple<Address,Integer>(a1, 10));
    }

    public void testOOBUnicastsToOthers() throws Exception {
        send(c1, c2.getLocalAddress(), true, 10);
        send(c1, c3.getLocalAddress(), true, 10);
        check(r2, 1, true, new Tuple<Address,Integer>(a1, 10));
        check(r3, 1, true, new Tuple<Address,Integer>(a1, 10));
    }


    public void testRegularMulticastToAll() throws Exception {
        send(c1, null /** multicast */, false, 10);
        check(r1, 1, false, new Tuple<Address,Integer>(a1, 10));
        check(r2, 1, false, new Tuple<Address,Integer>(a1, 10));
        check(r3, 1, false, new Tuple<Address,Integer>(a1, 10));
    }


    public void testOOBMulticastToAll() throws Exception {
        send(c1, null /** multicast */, true, 10);
        check(r1, 1, true, new Tuple<Address,Integer>(a1, 10));
        check(r2, 1, true, new Tuple<Address,Integer>(a1, 10));
        check(r3, 1, true, new Tuple<Address,Integer>(a1, 10));
    }


    public void testRegularMulticastToAll3Senders() throws Exception {
        send(c1, null /** multicast */, false, 10);
        send(c2, null /** multicast */, false, 10);
        send(c3, null /** multicast */, false, 10);
        check(r1, 3, false, new Tuple<Address,Integer>(a1, 10), new Tuple<Address,Integer>(a2, 10), new Tuple<Address,Integer>(a3, 10));
        check(r2, 3, false, new Tuple<Address,Integer>(a1, 10), new Tuple<Address,Integer>(a2, 10), new Tuple<Address,Integer>(a3, 10));
        check(r3, 3, false, new Tuple<Address,Integer>(a1, 10), new Tuple<Address,Integer>(a2, 10), new Tuple<Address,Integer>(a3, 10));
    }

    public void testOOBMulticastToAll3Senders() throws Exception {
        send(c1, null /** multicast */, true, 10);
        send(c2, null /** multicast */, true, 10);
        send(c3, null /** multicast */, true, 10);
        check(r1, 3, true, new Tuple<Address,Integer>(a1, 10), new Tuple<Address,Integer>(a2, 10), new Tuple<Address,Integer>(a3, 10));
        check(r2, 3, true, new Tuple<Address,Integer>(a1, 10), new Tuple<Address,Integer>(a2, 10), new Tuple<Address,Integer>(a3, 10));
        check(r3, 3, true, new Tuple<Address,Integer>(a1, 10), new Tuple<Address,Integer>(a2, 10), new Tuple<Address,Integer>(a3, 10));
    }

    public void testMixedMulticastsToAll3Members() throws Exception {
         send(c1, null /** multicast */, false, true, 10);
         send(c2, null /** multicast */, false, true, 10);
         send(c3, null /** multicast */, false, true, 10);
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
         // sending is asynchronous, we need to give the receivers some time to receive all msgs. Retransmission
         // can for example delay message delivery
         Util.sleep(2000);
     }


   /* private static void send(Channel sender_channel, Address dest, boolean oob, int num_msgs) throws Exception {
        long seqno=1;
        for(int i=0; i < num_msgs; i++) {
            Message msg=new Message(dest, null, seqno++);
            if(oob)
                msg.setFlag(Message.OOB);
            sender_channel.send(msg);
        }
        // sending is asynchronous, we need to give the receivers some time to receive all msgs. Retransmission
        // can for example delay message delivery
        Util.sleep(500);
    }*/


    private void createChannels(boolean copy_multicasts, boolean copy_unicasts, int num_outgoing_copies, int num_incoming_copies) throws Exception {
        c1=createChannel(true, 3);
        DUPL dupl=new DUPL(copy_multicasts, copy_unicasts, num_incoming_copies, num_outgoing_copies);
        ProtocolStack stack=c1.getProtocolStack();
        stack.insertProtocol(dupl, ProtocolStack.BELOW, NAKACK.class);

        c2=createChannel(c1);
        c3=createChannel(c1);

        c1.connect("DuplicateTest");
        c2.connect("DuplicateTest");
        c3.connect("DuplicateTest");

        assert c3.getView().size() == 3 : "view was " + c1.getView() + " but should have been 3";
    }


    private static void check(MyReceiver receiver, int expected_size, boolean oob, Tuple<Address,Integer>... vals) {
        Map<Address, List<Long>> msgs=receiver.getMsgs();
        assert msgs.size() == expected_size : "expected size=" + expected_size + ", msgs: " + msgs.keySet();
        for(Tuple<Address,Integer> tuple: vals) {
            Address addr=tuple.getVal1();
            List<Long> list=msgs.get(addr);
            System.out.println("[" + receiver.getName() + "]: " + addr + ": " + list);
            assert list != null : "no list available for " + addr;
            assert list.size() == tuple.getVal2() : "list's size is not " + tuple.getVal2() +", list: " + list;
            if(!oob) // if OOB messages, ordering is not guaranteed
                check(addr, list);
            else
                checkPresence(addr, list);
        }
    }


    private static void check(Address addr, List<Long> list) {
        long id=list.get(0);
        for(long val: list) {
            assert val == id : "[" + addr + "]: val=" + val + " (expected " + id + "): list is " + list;
            id++;
        }
    }

    private static void checkPresence(Address addr, List<Long> list) {
        for(long l=1; l <= 10; l++) {
            assert list.contains(l) : l + " is not in the list " + list;
        }
    }




    private static class MyReceiver extends ReceiverAdapter {
        final String name;
        private final ConcurrentMap<Address, List<Long>> msgs=new ConcurrentHashMap<Address,List<Long>>();

        public MyReceiver(String name) {
            this.name=name;
        }

        public String getName() {
            return name;
        }

        public ConcurrentMap<Address, List<Long>> getMsgs() {
            return msgs;
        }

        public void receive(Message msg) {
            Address addr=msg.getSrc();
            Long val=(Long)msg.getObject();
            List<Long> list=msgs.get(addr);
            if(list == null) {
                list=new LinkedList<Long>();
                List<Long> tmp=msgs.putIfAbsent(addr, list);
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
            for(Map.Entry<Address,List<Long>> entry: msgs.entrySet()) {
                sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
            }
            return sb.toString();
        }

    }

}