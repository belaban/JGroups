package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.TP;
import org.jgroups.util.Promise;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.TimeoutException;

/**
 * Tests unicasts to self (loopback of transport protocol)
 * @author Richard Achmatowicz 12 May 2008
 * @author Bela Ban Dec 31 2003
 */
@Test(groups=Global.STACK_DEPENDENT,singleThreaded=true)
public class UnicastLoopbackTest extends ChannelTestBase {
    JChannel channel=null;

    @BeforeMethod protected void setUp()    throws Exception {channel=createChannel(true, 1);}
    @AfterMethod  protected void tearDown() throws Exception {Util.close(channel);}


    /**
     * Tests that when UNICAST messages are sent to self, the following conditions hold:
     * (i) no messages touch the network
     * (ii) all messages are correctly received
     */
    public void testUnicastMsgsWithLoopback() throws Exception {
        final long TIMEOUT = 60 * 1000;
        final int NUM=1000;
        long num_msgs_sent_before = 0 ;
        long num_msgs_sent_after = 0 ;

        Promise<Boolean> promise = new Promise<>() ;
        MyReceiver receiver = new MyReceiver(NUM, promise) ;
        channel.setReceiver(receiver) ;
        channel.connect("UnicastLoopbackTest") ;

        Address local_addr=channel.getAddress();

        num_msgs_sent_before = getNumMessagesSentViaNetwork(channel) ;

        // send NUM UNICAST messages to ourself
        for(int i=1; i <= NUM; i++) {
            channel.send(new Message(local_addr, null,i));
            if(i % 100 == 0)
                System.out.println("-- sent " + i);
        }

        num_msgs_sent_after = getNumMessagesSentViaNetwork(channel) ;

        // when sending msgs to self, messages should not touch the network
        System.out.println("num msgs before: " + num_msgs_sent_before + ", num msgs after: " + num_msgs_sent_after);
        assert num_msgs_sent_before <= num_msgs_sent_after;
        assert num_msgs_sent_after < NUM/10;

        try {
            // wait for all messages to be received
            promise.getResultWithTimeout(TIMEOUT) ;
        }
        catch(TimeoutException te) {
            // timeout exception occurred
            Assert.fail("Test timed out before all messages were received; received " + receiver.getNumMsgsReceived()) ;
        }

    }


    /**
     * Returns the number of messages sent across the network.
     * 
     * @param ch 
     * @return the number of messages sent across the network
     * @throws Exception
     */
    private static long getNumMessagesSentViaNetwork(JChannel ch) throws Exception {
        TP transport = ch.getProtocolStack().getTransport();
        if (transport == null)
            throw new Exception("transport layer is not present - check default stack configuration") ;
        return transport.getNumMessagesSent();
    }



    /**
     * A receiver which waits for all messages to be received and 
     * then sets a promise.
     */
    private static class MyReceiver extends ReceiverAdapter {

        private final int numExpected ;
        private int       numReceived;
        private final Promise<Boolean> p ;

        public MyReceiver(int numExpected, Promise<Boolean> p) {
            this.numExpected = numExpected ;
            this.numReceived = 0 ;
            this.p = p ;
        }

        // when we receive a Message, we update the count of messages received
        public void receive(Message msg) {

            Integer num=(Integer)msg.getObject();
            numReceived++;
            if(num != null && num % 100 == 0)
                System.out.println("-- received " + num);

            // if we have received NUM messages, set the result
            if (numReceived >= numExpected)
                p.setResult(Boolean.TRUE) ;
        }

        public int getNumMsgsReceived() {
            return numReceived ;
        }
    }


}
