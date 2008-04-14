package org.jgroups.tests;

import org.jgroups.*;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests unicasts to self (loopback of transport protocol)
 * @author Bela Ban Dec 31 2003
 * @version $Id: UnicastLoopbackTest.java,v 1.10 2008/04/14 08:42:56 belaban Exp $
 */
public class UnicastLoopbackTest extends ChannelTestBase {
    JChannel channel=null;


    @BeforeMethod
    protected void setUp() throws Exception {
        channel=createChannel();
        channel.connect("demo-group");
    }

    @AfterMethod
    protected void tearDown() throws Exception {
        if(channel != null) {
            channel.close();
            channel=null;
        }
    }


    @Test
    public void testUnicastMsgs() throws ChannelClosedException, ChannelNotConnectedException, TimeoutException {
        int NUM=1000;
        Address local_addr=channel.getLocalAddress();
        for(int i=1; i <= NUM; i++) {
            channel.send(new Message(local_addr, null, new Integer(i)));
            if(i % 100 == 0)
                System.out.println("-- sent " + i);
        }
        int received=0;
        while(received < NUM) {
            Object o=channel.receive(0);
            if(o instanceof Message) {
                Message m=(Message)o;
                Integer num=(Integer)m.getObject();
                received++;
                if(num.intValue() % 100 == 0)
                    System.out.println("-- received " + num);
            }
        }
        Assert.assertEquals(NUM, received);
    }


}
