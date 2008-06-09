package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.TP;
import org.jgroups.util.Promise;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests unicasts to self (loopback of transport protocol)
 * @author Richard Achmatowicz 12 May 2008
 * @author Bela Ban Dec 31 2003
 * @version $Id: UnicastLoopbackTest.java,v 1.13 2008/06/09 14:32:43 belaban Exp $
 */
@Test(groups="temp",sequential=true)
public class UnicastLoopbackTest extends ChannelTestBase {
    JChannel channel=null;


    @BeforeMethod
    protected void setUp() throws Exception {
        channel=createChannel(true, 1);
    }

    @AfterMethod
    protected void tearDown() throws Exception {
        Util.close(channel);
    }


    /**
     * Tests that when UNICAST messages are sent with TP.loopback == true, the following 
     * conditions hold:
     * (i) no messages touch the network
     * (ii) all messages are correctly received
     * 
     * @throws ChannelException
     * @throws ChannelClosedException
     * @throws ChannelNotConnectedException
     * @throws TimeoutException
     * @throws Exception
     */
    public void testUnicastMsgsWithLoopback() throws Exception {
    	final long TIMEOUT = 2 * 1000 ;
    	final int NUM=1000;
    	long num_msgs_sent_before = 0 ;
    	long num_msgs_sent_after = 0 ;

    	Promise<Boolean> p = new Promise<Boolean>() ;
    	MyReceiver receiver = new MyReceiver(NUM, p) ;
    	channel.setReceiver(receiver) ;
    	channel.connect("demo-group") ;

    	Address local_addr=channel.getLocalAddress();

    	// set the loopback property on transport
    	setLoopbackProperty(channel, true) ;

    	num_msgs_sent_before = getNumMessagesSentViaNetwork(channel) ;

    	// send NUM UNICAST messages to ourself 
    	for(int i=1; i <= NUM; i++) {
    		channel.send(new Message(local_addr, null, new Integer(i)));
    		if(i % 100 == 0)
    			System.out.println("-- sent " + i);
    	}

    	num_msgs_sent_after = getNumMessagesSentViaNetwork(channel) ;

    	// when loopback == true, messages should not touch the network
        System.out.println("num msgs before: " + num_msgs_sent_before + ", num msgs after: " + num_msgs_sent_after);
        assert num_msgs_sent_before <= num_msgs_sent_after;
        assert num_msgs_sent_after < NUM/10;

        try {
    		// wait for all messages to be received
    		p.getResultWithTimeout(TIMEOUT) ;
    	}
    	catch(TimeoutException te) {
    		// timeout exception occurred 
    		Assert.fail("Test timed out before all messages were received") ;
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
    	if (transport == null) {
    		throw new Exception("transport layer is not present - check default stack configuration") ;
    	}

    	return transport.getNumMessagesSent();
    }


    /**
     * Set the value of the loopback property on the transport layer.
     * 
     * @param ch
     * @param loopback
     * @throws Exception
     */
    private static void setLoopbackProperty(JChannel ch, boolean loopback) throws Exception {

    	TP transport =ch.getProtocolStack().getTransport();
    	if (transport == null) {
    		throw new Exception("transport layer is not present - check default stack configuration") ;
    	}

    	// check if already set correctly
    	if ((loopback && transport.isLoopback()) || (!loopback && !transport.isLoopback()))
    		return ;

    	// otherwise, set it
    	transport.setLoopback(loopback);
    }

    /**
     * A receiver which waits for all messages to be received and 
     * then sets a promise.
     */
    private static class MyReceiver extends ReceiverAdapter {

    	private final int numExpected ;
    	private int numReceived;
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
    		if(num != null && num.intValue() % 100 == 0)
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
