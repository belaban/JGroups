package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * 
 * @author Bela Ban
 * @version $Id: AddDataTest.java,v 1.14 2008/04/08 07:18:56 belaban Exp $
 */
public class AddDataTest extends ChannelTestBase {
    JChannel ch1, ch2;


    @AfterMethod
    protected void tearDown() throws Exception {
        if(ch2 != null)
            ch2.close();
        if(ch1 != null)
            ch1.close();
    }

    @Test
    public void testAdditionalData() throws Exception {
        try {
            for(int i=1;i <= 5;i++) {
                System.out.println("-- attempt # " + i + "/10");
                Channel c=createChannel();
                Map<String,Object> m=new HashMap<String,Object>();
                m.put("additional_data", new byte[] { 'b', 'e', 'l', 'a' });
                c.down(new Event(Event.CONFIG, m));
                c.connect("bla");
                IpAddress addr=(IpAddress)c.getLocalAddress();
                System.out.println("address is " + addr);
                assertNotNull(addr.getAdditionalData());
                Assert.assertEquals('b', addr.getAdditionalData()[0]);
                c.close();
            }
        }
        catch(ChannelException e) {
            e.printStackTrace();
            assert false : e.toString();
        }
    }

    @Test
    public void testBetweenTwoChannelsMcast() throws Exception {
        _testWithProps(true);
    }

    @Test
    public void testBetweenTwoChannelsUnicast() throws Exception {
        _testWithProps(false);
    }

    @Test
    public void testBetweenTwoChannelsWithBundlingMcast() throws Exception {
        _testWithProps(true);
    }

    @Test
    public void testBetweenTwoChannelsWithBundlingUnicast() throws Exception {
        _testWithProps(false);
    }

    private void _testWithProps(boolean mcast) throws Exception {
        Map<String,Object> m=new HashMap<String,Object>();
        m.put("additional_data", new byte[] { 'b', 'e', 'l', 'a' });
        byte[] buf=new byte[1000];

        ch1=createChannel();
        ch1.down(new Event(Event.CONFIG, m));

        ch2=createChannel();
        ch2.down(new Event(Event.CONFIG, m));
        MyReceiver receiver=new MyReceiver();
        ch2.setReceiver(receiver);
        ch1.connect("group");
        ch2.connect("group");

        if(mcast)
            ch1.send(new Message(null, null, buf));
        else {
            Address dest=ch2.getLocalAddress();
            ch1.send(new Message(dest, null, buf));
        }

        Util.sleep(500); // msgs are sent asynchronously, give ch2 some time to receive them
        List<Message> list=receiver.getMsgs();
        assertTrue(!list.isEmpty());
        Message msg=list.get(0);
        IpAddress src=(IpAddress)msg.getSrc();
        assertNotNull(src);
        assertNotNull(src.getAdditionalData());
        Assert.assertEquals(4, src.getAdditionalData().length);
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
