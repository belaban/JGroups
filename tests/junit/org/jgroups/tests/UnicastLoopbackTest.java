package org.jgroups.tests;

import junit.framework.TestCase;
import org.jgroups.*;

/**
 * Tests unicasts to self (loopback of transport protocol)
 * @author Bela Ban Dec 31 2003
 * @version $Id: UnicastLoopbackTest.java,v 1.7 2005/01/28 22:08:07 belaban Exp $
 */
public class UnicastLoopbackTest extends TestCase {
    JChannel channel=null;


    public UnicastLoopbackTest(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        super.setUp();
        channel=new JChannel();
        channel.connect("demo-group");
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        if(channel != null) {
            channel.close();
            channel=null;
        }
    }


    public void testUnicastMsgs() throws ChannelClosedException, ChannelNotConnectedException, TimeoutException {
        int NUM=1000;
        Address local_addr=channel.getLocalAddress();
        for(int i=1; i <= NUM; i++) {
            channel.send(new Message(local_addr, null, new Integer(i)));
//            try {
//                Thread.sleep(1);
//            }
//            catch(InterruptedException e) {
//                e.printStackTrace();
//            }
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
        assertEquals(NUM, received);
    }

    public static void main(String[] args) {
        String[] testCaseName={UnicastLoopbackTest.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }
}
