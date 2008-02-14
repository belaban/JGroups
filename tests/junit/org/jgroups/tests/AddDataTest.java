package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.stack.IpAddress;

import java.util.HashMap;
import java.util.Map;

/**
 * 
 * @author Bela Ban
 * @version $Id: AddDataTest.java,v 1.12 2008/02/14 01:49:37 vlada Exp $
 */
public class AddDataTest extends ChannelTestBase {
    JChannel ch1, ch2;

    public AddDataTest(String name) {
        super(name);
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        if(ch2 != null)
            ch2.close();
        if(ch1 != null)
            ch1.close();
    }

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
                assertEquals('b', addr.getAdditionalData()[0]);
                c.close();
            }
        }
        catch(ChannelException e) {
            e.printStackTrace();
            fail(e.toString());
        }
    }

    public void testBetweenTwoChannelsMcast() throws Exception {
        _testWithProps(true);
    }

    public void testBetweenTwoChannelsUnicast() throws Exception {
        _testWithProps(false);
    }

    public void testBetweenTwoChannelsWithBundlingMcast() throws Exception {
        _testWithProps(true);
    }

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
        ch1.connect("group");
        ch2.connect("group");
        while(ch2.peek(10) != null) {
            System.out.println("-- received " + ch2.receive(100));
        }
        if(mcast)
            ch1.send(new Message(null, null, buf));
        else {
            Address dest=ch2.getLocalAddress();
            ch1.send(new Message(dest, null, buf));
        }
        Message msg=(Message)ch2.receive(10000);
        System.out.println("received " + msg);
        IpAddress src=(IpAddress)msg.getSrc();
        System.out.println("src=" + src);

        // Thread.sleep(600000); // todo: remove

        assertNotNull(src);
        assertNotNull(src.getAdditionalData());
        assertEquals(4, src.getAdditionalData().length);
    }

    public static void main(String[] args) {
        String[] testCaseName= { AddDataTest.class.getName() };
        junit.textui.TestRunner.main(testCaseName);
    }
}
