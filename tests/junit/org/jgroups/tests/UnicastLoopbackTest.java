package org.jgroups.tests;

import junit.framework.TestCase;
import org.jgroups.*;
import org.jgroups.protocols.TP ;
import org.jgroups.util.Promise ;

/**
 * Tests unicasts to self (loopback of transport protocol)
 * @author Richard Achmatowicz 12 May 2008
 * @author Bela Ban Dec 31 2003
 * @version $Id: UnicastLoopbackTest.java,v 1.7.20.3 2008/06/02 07:57:51 belaban Exp $
 */
public class UnicastLoopbackTest extends TestCase {
    JChannel channel=null;


    public UnicastLoopbackTest(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        super.setUp();
        channel=new JChannel();
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        if(channel != null) {
            channel.close();
            channel=null;
        }
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
    	// assertEquals("Messages are (incorrectly) being sent via network", num_msgs_sent_before, num_msgs_sent_after) ;

        assertTrue("Messages are (incorrectly) being sent via network", num_msgs_sent_after < NUM);

        try {
    		// wait for all messages to be received
    		p.getResultWithTimeout(TIMEOUT) ;
    	}
    	catch(TimeoutException te) {
    		// timeout exception occurred 
    		fail("Test timed out before all messages were received") ;
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
    	TP transport=ch.getProtocolStack().getTransport();
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
    	TP transport=ch.getProtocolStack().getTransport();
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
    	private final Promise p ;

    	public MyReceiver(int numExpected, Promise p) {
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
