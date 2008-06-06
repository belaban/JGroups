package org.jgroups.tests;


import org.jgroups.*;
import org.jgroups.protocols.DUPL;
import org.jgroups.protocols.pbcast.NAKACK;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
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
 * @version $Id: DuplicateTest.java,v 1.2 2008/06/06 09:43:16 belaban Exp $
 */
@Test(groups="temp",sequential=true)
public class DuplicateTest extends ChannelTestBase {
    private JChannel c1, c2, c3;
    private MyReceiver r1, r2, r3;

    @BeforeMethod
    void init() throws Exception {
        r1=new MyReceiver("C1");
        r2=new MyReceiver("C2");
        r3=new MyReceiver("C3");

        createChannels(true, true, (short)2, (short)2);
    }

    @AfterMethod
    void tearDown() throws Exception {
        Util.close(c3, c2, c1);
    }



    public void testRegularUnicasts() throws Exception {
        send(c1, c1.getLocalAddress(), false, 10);
    }


    private static void send(Channel sender_channel, Address dest, boolean oob, int num_msgs) throws Exception {
        long seqno=1;
        for(int i=0; i < num_msgs; i++) {
            Message msg=new Message(dest, null, seqno++);
            if(oob)
                msg.setFlag(Message.OOB);
            sender_channel.send(msg);
        }
    }


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

        c1.setReceiver(r1);
        c2.setReceiver(r2);
        c3.setReceiver(r3);
    }
    

    private static void check(Map<Address,List<Long>> msgs) {
        for(Map.Entry<Address,List<Long>> entry: msgs.entrySet()) {
            Address addr=entry.getKey();
            List<Long> list=entry.getValue();
            check(addr, list);
        }
    }

    private static void check(Address addr, List<Long> list) {
        long id=list.get(0);
        for(long val: list) {
            assert val == id : "[" + addr + "]: val=" + val + " (expected " + id + "): list is " + list;
            id++;
        }
    }


    private static class MyReceiver extends ReceiverAdapter {
        final String name;
        private final ConcurrentMap<Address, List<Long>> msgs=new ConcurrentHashMap<Address,List<Long>>();

        public MyReceiver(String name) {
            this.name=name;
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