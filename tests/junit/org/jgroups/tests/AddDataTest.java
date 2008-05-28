package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * 
 * @author Bela Ban
 * @version $Id: AddDataTest.java,v 1.18 2008/05/28 09:11:42 belaban Exp $
 */
@Test(groups={"temp","single"},sequential=false)
public class AddDataTest extends ChannelTestBase {

    @Test
    public void testAdditionalData() throws Exception {
        for(int i=1;i <= 2;i++) {
            System.out.println("-- attempt # " + i + "/2");
            Channel c=createChannel(true, 1);
            try {
                Map<String,Object> m=new HashMap<String,Object>();
                m.put("additional_data", new byte[] { 'b', 'e', 'l', 'a' });
                c.down(new Event(Event.CONFIG, m));
                c.connect("AddDataTest.testadditionalData()");
                IpAddress addr=(IpAddress)c.getLocalAddress();
                System.out.println("address is " + addr);
                assert addr.getAdditionalData() != null;
                assert addr.getAdditionalData()[0] == 'b';
            }
            finally {
                c.close();
            }
        }
    }

    @Test
    public void testBetweenTwoChannelsMcast() throws Exception {
        _testWithProps(true, "AddDataTest.testBetweenTwoChannelsMcast");
    }

    @Test
    public void testBetweenTwoChannelsUnicast() throws Exception {
        _testWithProps(false, "AddDataTest.testBetweenTwoChannelsUnicast");
    }



    private void _testWithProps(boolean mcast, String cluster_name) throws Exception {
        Map<String,Object> m=new HashMap<String,Object>();
        m.put("additional_data", new byte[] { 'b', 'e', 'l', 'a' });
        byte[] buf=new byte[1000];
        JChannel ch1=null, ch2=null;

        try {
            ch1=createChannel(true, 2);
            ch1.down(new Event(Event.CONFIG, m));
            String props=ch1.getProperties();

            ch2=createChannelWithProps(props); // same props as ch1 above
            ch2.down(new Event(Event.CONFIG, m));
            MyReceiver receiver=new MyReceiver();
            ch2.setReceiver(receiver);
            ch1.connect(cluster_name);
            ch2.connect(cluster_name);

            if(mcast)
                ch1.send(new Message(null, null, buf));
            else {
                Address dest=ch2.getLocalAddress();
                ch1.send(new Message(dest, null, buf));
            }

            Util.sleep(500); // msgs are sent asynchronously, give ch2 some time to receive them
            List<Message> list=receiver.getMsgs();
            assert !list.isEmpty();
            Message msg=list.get(0);
            IpAddress src=(IpAddress)msg.getSrc();
            assert src != null;
            assert src.getAdditionalData() != null;
            assert src.getAdditionalData().length == 4;
        }
        finally {
            if(ch2 != null) ch2.close();
            if(ch1 != null) ch1.close();
        }
    }



    private static class MyReceiver extends ReceiverAdapter {
        final List<Message> msgs=new LinkedList<Message>();

        public List<Message> getMsgs() {return msgs;}

        public void clear() {msgs.clear();}

        public void receive(Message msg) {
            System.out.println("received " + msg);
            msgs.add(msg);
    }
}
}
