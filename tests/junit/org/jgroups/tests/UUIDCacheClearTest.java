package org.jgroups.tests;


import org.jgroups.*;
import org.jgroups.blocks.LazyRemovalCache;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Test whether physical addresses are fetched correctly after the UUID-physical address cache has been cleared
 * @author Bela Ban
 */
@Test(groups=Global.STACK_DEPENDENT,singleThreaded=true)
public class UUIDCacheClearTest extends ChannelTestBase {


    public void testCacheClear() throws Exception {
        JChannel   a=null, b=null;
        MyReceiver r1=new MyReceiver(), r2=new MyReceiver();

        try {
            a=createChannel().name("A");
            a.setReceiver(r1);
            b=createChannel().name("B");
            b.setReceiver(r2);
            boolean unicast_absent=Stream.of(a, b).map(JChannel::getProtocolStack)
              .anyMatch(st -> st.findProtocol(Util.getUnicastProtocols()) == null);
            makeUnique(a,b);

            a.connect("UUIDCacheClearTest");
            b.connect("UUIDCacheClearTest");
            Util.waitUntilAllChannelsHaveSameView(10000, 1000, a, b);


            // send one unicast message from a to b and vice versa
            a.send(b.getAddress(), "one");
            b.send(a.getAddress(), "one");

            List<Message> c1_list=r1.getList();
            List<Message> c2_list=r2.getList();

            Util.waitUntil(10000, 500, () -> !c1_list.isEmpty() && !c2_list.isEmpty());
            assert c1_list.size() == 1 && c2_list.size() == 1;

            // now clear the caches and send message "two"
            printCaches(a,b);

            System.out.println("clearing the caches");
            clearCache(a,b);
            printCaches(a,b);

            r1.clear();
            r2.clear();

            r1.enablePrinting(true); r2.enablePrinting(true);

            // send one unicast message from a to b and vice versa
            a.send(b.getAddress(), "one");
            b.send(a.getAddress(), "two");

            // With UNICAST{3,4} present, the above 2 messages will get dropped, as the cache doesn't have the dest
            // addrs and discovery is *async*. However, retransmission ensures that they're retransmitted. With
            // UNICAST{3,4} *absent*, this won't happen, so we're waiting for async discovery to complete and then
            // resend the messages. Kind of a kludge to make this test pass.
            if(unicast_absent) {
                LazyRemovalCache<Address,PhysicalAddress> cache_a, cache_b;
                cache_a=a.getProtocolStack().getTransport().getLogicalAddressCache();
                cache_b=b.getProtocolStack().getTransport().getLogicalAddressCache();
                Util.waitUntilTrue(5000, 200, () -> cache_a.size() >= 2 && cache_b.size() >= 2);
                a.send(b.getAddress(), "one");
                b.send(a.getAddress(), "two");
            }

            Util.waitUntil(10000, 500, () -> !c1_list.isEmpty() && !c2_list.isEmpty());
            r1.enablePrinting(false); r2.enablePrinting(false);

            assert c1_list.size() == 1 && c2_list.size() == 1;
            Message msg_from_1=c2_list.get(0);
            Message msg_from_2=c1_list.get(0);

            assert msg_from_1.getSrc().equals(a.getAddress());
            assert msg_from_1.getObject().equals("one");

            assert msg_from_2.getSrc().equals(b.getAddress());
            assert msg_from_2.getObject().equals("two");
        }
        finally {
            Util.close(b, a);
        }
    }

    private static void clearCache(JChannel ... channels) {
        for(JChannel ch: channels) {
            ch.getProtocolStack().getTransport().clearLogicalAddressCache();
            for(Protocol p=ch.getProtocolStack().getTopProtocol(); p != null; p=p.getDownProtocol())
                p.setAddress(ch.getAddress());
        }
    }


    private static void printCaches(JChannel ... channels) {
        System.out.println("caches:\n");
        for(JChannel ch: channels)
            System.out.println(ch.getAddress() + ":\n" + ch.getProtocolStack().getTransport().printLogicalAddressCache());
    }

    private static class MyReceiver implements Receiver {
        private final List<Message> msgs=new ArrayList<>(4);
        private boolean             print_msgs=false;

        public void receive(Message msg) {
            msgs.add(msg);
            if(print_msgs)
                System.out.println("<< " + msg.getObject() + " from " + msg.getSrc());
        }

        public void clear() {msgs.clear();}
        public void enablePrinting(boolean flag) {print_msgs=flag;}
        public List<Message> getList() {return msgs;}
    }



}