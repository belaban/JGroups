package org.jgroups.tests;


import org.jgroups.*;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Test whether physical addresses are fetched correctly after the UUID-physical address cache has been cleared
 * @author Bela Ban
 * @version $Id: UUIDCacheClearTest.java,v 1.2 2009/04/09 09:11:16 belaban Exp $
 */
@Test(groups=Global.STACK_DEPENDENT,sequential=true)
public class UUIDCacheClearTest extends ChannelTestBase {


    @Test
    public void testCacheClear() throws Exception {
        JChannel c1=null, c2=null;
        Address c1_addr, c2_addr;
        MyReceiver r1=new MyReceiver(), r2=new MyReceiver();

        try {
            c1=createChannel(true, 2);
            c1.setReceiver(r1);
            c1.connect("testCacheClear");
            c2=createChannel(c1);
            c2.setReceiver(r2);
            c2.connect("testCacheClear");

            assert c2.getView().size() == 2 : "view is " + c2.getView();

            c1_addr=c1.getAddress();
            c2_addr=c2.getAddress();

            // send one unicast message from c1 to c2 and vice versa
            c1.send(c2_addr, null, "one");
            c2.send(c1_addr, null, "one");

            List<Message> c1_list=r1.getList();
            List<Message> c2_list=r2.getList();

            for(int i=0; i < 10; i++) { // poor man's way of waiting until we have 1 message in each receiver... :-)
                if(!c1_list.isEmpty() && !c2_list.isEmpty())
                    break;
                Util.sleep(500);
            }
            assert c1_list.size() == 1 && c2_list.size() == 1;

            // now clear the caches and send message "two"
            clearCache(c1, c2);
            r1.clear();
            r2.clear();

            // send one unicast message from c1 to c2 and vice versa
            c1.send(c2_addr, null, "two");
            c2.send(c1_addr, null, "two");
            for(int i=0; i < 10; i++) { // poor man's way of waiting until we have 1 message in each receiver... :-)
                if(!c1_list.isEmpty() && !c2_list.isEmpty())
                    break;
                Util.sleep(1000);
            }
            assert c1_list.size() == 1 && c2_list.size() == 1;
            Message msg_from_1=c2_list.get(0);
            Message msg_from_2=c1_list.get(0);

            assert msg_from_1.getSrc().equals(c1_addr);
            assert msg_from_1.getObject().equals("two");

            assert msg_from_2.getSrc().equals(c2_addr);
            assert msg_from_2.getObject().equals("two");
        }
        finally {
            Util.close(c2, c1);
        }
    }

    private static void clearCache(JChannel ... channels) {
        for(JChannel ch: channels) {
            ch.getProtocolStack().getTransport().clearLogicalAddressCache();
            ch.down(new Event(Event.SET_LOCAL_ADDRESS, ch.getAddress()));
        }
    }

    private static class MyReceiver extends ReceiverAdapter {
        private final List<Message> msgs=new ArrayList<Message>(4);

        public void receive(Message msg) {
            msgs.add(msg);
        }

        public void clear() {msgs.clear();}
        public List<Message> getList() {return msgs;}
    }



}