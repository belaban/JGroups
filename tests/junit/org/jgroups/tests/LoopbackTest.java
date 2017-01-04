package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.TP;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Promise;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.TimeoutException;

/**
 * Tests unicast and multicast messages to self (loopback of transport protocol)
 * @author Richard Achmatowicz 12 May 2008
 * @author Bela Ban Dec 31 2003
 */
@Test(groups=Global.STACK_DEPENDENT,singleThreaded=true)
public class LoopbackTest extends ChannelTestBase {
    protected JChannel channel;

    @BeforeMethod protected void setUp()    throws Exception {channel=createChannel(true, 1).name("A");}
    @AfterMethod  protected void tearDown() throws Exception {Util.close(channel);}


    /**
     * Tests that when UNICAST messages are sent to self, the following conditions hold:
     * (i) no messages touch the network
     * (ii) all messages are correctly received
     */
    public void testUnicastMsgsWithLoopback() throws Exception {
        sendMessagesWithLoopback(true);
    }

    /**
     * Tests that when MULTICAST messages are sent to self, the following conditions hold:
     * (i) no messages touch the network
     * (ii) all messages are correctly received
     */
    public void testMulticastMsgsWithLoopback() throws Exception {
        sendMessagesWithLoopback(false);
    }

    protected void sendMessagesWithLoopback(boolean unicast) throws Exception {
        final long TIMEOUT = 60_0000;
        final int NUM=1000;
        long num_msgs_sent_before = 0;
        long num_msgs_sent_after = 0;

        Promise<Boolean> promise = new Promise<>() ;
        MyReceiver receiver = new MyReceiver(NUM, promise) ;
        channel.setReceiver(receiver) ;
        channel.connect("UnicastLoopbackTest") ;

        int largest_thread_pool_size=channel.getProtocolStack().getTransport().getThreadPoolSizeLargest();
        num_msgs_sent_before = getNumMessagesSentViaNetwork(channel) ;

        // send NUM messages to dest
        Address dest=unicast? channel.getAddress() : null;
        for(int i=1; i <= NUM; i++) {
            channel.send(new Message(dest, i));
            if(i % 100 == 0)
                System.out.printf("-- [%s] sent %d\n", Thread.currentThread().getName(), i);
        }

        num_msgs_sent_after = getNumMessagesSentViaNetwork(channel) ;

        System.out.printf("\nlargest pool size before: %d after: %d\n", largest_thread_pool_size,
                          largest_thread_pool_size=channel.getProtocolStack().getTransport().getThreadPoolSizeLargest());

        // when sending msgs to self, messages should not touch the network
        System.out.println("num msgs before: " + num_msgs_sent_before + ", num msgs after: " + num_msgs_sent_after);
        assert num_msgs_sent_before <= num_msgs_sent_after;
        if(unicast)
            assert num_msgs_sent_after < NUM/10;
        else
            assert num_msgs_sent_after <= NUM; // max of NUM single messages; probably some batches were sent
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
            Integer num=msg.getObject();
            numReceived++;
            if(num != null && num % 100 == 0)
                System.out.printf("-- [%s] received %d\n", Thread.currentThread().getName(), num);

            // if we have received NUM messages, set the result
            if (numReceived >= numExpected)
                p.setResult(Boolean.TRUE) ;
        }

        public void receive(MessageBatch batch) {
            int size=batch.size();
            numReceived+=size;
            // System.out.printf("received batch of %d msgs, total: %d\n", size, numReceived);
            for(Message msg: batch) {
                Integer num=msg != null? msg.getObject() : null;
                if(num != null && num % 100 == 0)
                    System.out.printf("-- [%s] received %d\n", Thread.currentThread().getName(), num);
            }

            if(numReceived >= numExpected)
                p.setResult(Boolean.TRUE);
        }

        public int getNumMsgsReceived() {
            return numReceived ;
        }
    }


}
