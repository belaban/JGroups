package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.BytesMessage;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.protocols.TP;
import org.jgroups.util.MyReceiver;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Tests unicast and multicast messages to self (loopback of transport protocol)
 * @author Richard Achmatowicz 12 May 2008
 * @author Bela Ban Dec 31 2003
 */
@Test(groups=Global.STACK_DEPENDENT,singleThreaded=true)
public class LoopbackTest extends ChannelTestBase {
    protected JChannel channel;

    @BeforeMethod protected void setUp() throws Exception {
        channel=createChannel().name("A");
        channel.stack().getTransport().getMessageStats().enable(true);
        makeUnique(channel);
    }

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
        final int NUM=1000;
        long num_msgs_sent_before=0, num_msgs_sent_after=0;

        MyReceiver<Integer> receiver = new MyReceiver<>() ;
        channel.setReceiver(receiver) ;
        channel.connect("UnicastLoopbackTest") ;

        int largest_thread_pool_size=channel.getProtocolStack().getTransport().getThreadPool().getLargestSize();
        num_msgs_sent_before=getNumMessagesSentViaNetwork(channel) ;

        // send NUM messages to dest
        Address dest=unicast? channel.getAddress() : null;
        for(int i=1; i <= NUM; i++) {
            channel.send(new BytesMessage(dest, i));
            if(i % 100 == 0)
                System.out.printf("-- [%s] sent %d\n", Thread.currentThread().getName(), i);
        }

        num_msgs_sent_after=getNumMessagesSentViaNetwork(channel) ;

        System.out.printf("\nlargest pool size before: %d after: %d\n", largest_thread_pool_size,
                          largest_thread_pool_size=channel.getProtocolStack().getTransport().getThreadPool().getLargestSize());

        // when sending msgs to self, messages should not touch the network
        System.out.println("num msgs sent before: " + num_msgs_sent_before + ", num msgs sent after: " + num_msgs_sent_after);
        assert num_msgs_sent_before <= num_msgs_sent_after;
        assert num_msgs_sent_after >= NUM; // max of NUM single messages; probably some batches were sent

        Util.waitUntil(5000, 100, () -> receiver.size() == NUM);
        List<Integer> actual=receiver.list();
        List<Integer> expected=IntStream.rangeClosed(1,1000).boxed().collect(Collectors.toList());
        assert actual.equals(expected);
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
        return transport.getMessageStats().getNumMsgsSent();
    }


}
