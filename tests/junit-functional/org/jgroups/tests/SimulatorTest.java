
//$Id: SimulatorTest.java,v 1.1 2008/10/23 16:59:39 rachmatowicz Exp $

package org.jgroups.tests;

import org.testng.annotations.Test ;
import org.testng.annotations.BeforeMethod ;
import org.testng.annotations.AfterMethod ;
import org.testng.Assert;

import org.jgroups.*;
import org.jgroups.debug.Simulator;
import org.jgroups.protocols.DELAY;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.Protocol;

import java.util.Properties;
import java.util.Vector;

/**
 * Tests for the fault-injection features of Simulator.
 * 
 * @author Richard Achmatowicz 
 */
@Test(groups=Global.FUNCTIONAL, sequential=true)
public class SimulatorTest {

	final static int NUM_PEERS = 3; 
	final static int NUM_MSGS = 5; 
	final static int WAIT_TIMEOUT = 5; // secs
	final static int MSGS_PER_STATUS_LINE = 1; 

	// convey assertion failure from thread to main framework
	static boolean allMsgsReceived = false ;

	IpAddress[] addresses = new IpAddress[NUM_PEERS] ;
	Vector<Address> members = null ;
	View view = null ;
	Simulator[] simulators = new Simulator[NUM_PEERS] ;
	DELAY[] layers = new DELAY[NUM_PEERS] ;
	Protocol[][] stacks = new Protocol[NUM_PEERS][] ;
	Thread[] threads = new Thread[NUM_PEERS] ;

	//define senders and receivers
	boolean[] isSender = new boolean[NUM_PEERS] ;

	// used to wait for signal that all messages received
	static Object all_msgs_recd = new Object() ;


	/**
	 * Set up a number of simulator instances wrapping NAKACK 
	 */
	@BeforeMethod(alwaysRun=true)
	public void setUp() throws Exception {

		System.out.println("calling setUp()") ;

		// define the senders and the receivers
		isSender[0] = false ;
		isSender[1] = true ;
		isSender[2] = true ;

		// dummy IP addresses and ports 
		addresses[0] = new IpAddress(1111);
		addresses[1] = new IpAddress(2222);
		addresses[2] = new IpAddress(3333);

		// dummy set of members which works for all three simulators
		members = new Vector<Address>();
		for (int i = 0 ; i < NUM_PEERS; i++) {
			members.add(addresses[i]);
		}

		// create a dummy View(creator, timestamp, member set) 
		view = new View(addresses[0], 1, members);

		// create new simulator instances
		for (int i = 0; i < NUM_PEERS; i++) {
			createSimulator(simulators, view, addresses, layers, stacks, i) ;
		}

		// describe the configuration of the three simulators
		for (int i = 0; i < NUM_PEERS; i++) {
			for (int j = 0; j < NUM_PEERS; j++) {
				if (i == j) 
					simulators[i].addMember(addresses[j]) ;
				else 
					simulators[i].addMember(addresses[j], simulators[j]) ;
			}
		}

		// set up the receiver callbacks for each simulator
		Simulator.Receiver[] receivers = new Simulator.Receiver[NUM_PEERS] ;

		// set up the sender and the receiver callbacks, according to whether
		// the peer is a sender or a receiver
		for (int i = 0; i < NUM_PEERS; i++) {

			if (isSender[i]) 
				receivers[i] = new SenderPeer(simulators[i]) ;
			else 
				receivers[i] = new ReceiverPeer(simulators[i]) ;

			simulators[i].setReceiver(receivers[i]);
		}

		// start the simulators
		for (int i = 0; i < NUM_PEERS; i++) 
			simulators[i].start();

		System.out.println("Ending setUp()") ;
	}

	@AfterMethod(alwaysRun=true)
	public void tearDown() throws Exception {

		System.out.println("Calling tearDown()") ;
		// reset messages received flag
		allMsgsReceived = false ;
		
		// stop the simulators
		for (int i = 0; i < NUM_PEERS; i++)
			simulators[i].stop();
		System.out.println("Ending tearDown()") ;
	}

	private void createSimulator(Simulator[] simulators, View view, Address[] addresses, DELAY[] layers, Protocol[][] stacks, int i) {

		// create the simulator instance
		simulators[i] = new Simulator();
		simulators[i].setLocalAddress(addresses[i]);
		simulators[i].setView(view);

		// set up a dummy passthrough layer using DELAY
		layers[i] = new DELAY();

		// set up its properties
		layers[i].setInDelay(0);
		layers[i].setOutDelay(0);

		// our protocol stack under test consists of one protocol
		stacks[i] = new Protocol[]{layers[i]};
		simulators[i].setProtocolStack(stacks[i]);
	}


	/*
	 * Drop messages from our address to destination address a
	 */
	class MyDropMessage implements Simulator.DropMessage {
		Address address = null ;

		MyDropMessage(Address a) {
			this.address = a ;
		}
		public boolean drop(Message msg, Address dest) {

			// multicast messages have their source address set, and drop
			// gets called |view| times, each with a different dest parameter
			
			// here we drop that part of a multicast which is headed for dest
			if (msg.getDest() == null && dest.equals(address)) {
				return true ;
			}

			// drop when sending specifically to address
			if (msg.getDest() != null && msg.getDest().equals(address)) {
				return true ;
			}
			return false ;
		}
	}


	/**
	 * Test dropped messages modelling.
	 */
	public void testDroppedMessages() {

		System.out.println("Starting testDroppedMessages") ;

		// set up a drop function which drops all messages from 2 to 0
		// (even those message instances which are part of a multicast) 
		Simulator.DropMessage d = new MyDropMessage(addresses[0]) ;
		simulators[2].registerDropMessage(d) ;

		// start the NAKACK peers and let them exchange messages
		for (int i = 0; i < NUM_PEERS; i++) {
			threads[i] = new MyPeer(simulators[i], isSender[i]) ;
			threads[i].start() ;
		}

		// wait for the receiver peer to signal that it has received messages, or timeout
		synchronized(all_msgs_recd) {
			try {
				all_msgs_recd.wait(WAIT_TIMEOUT * 1000) ;
			}
			catch(InterruptedException e) {
				System.out.println("main thread interrupted") ;
			}
		}

		// wait for the threads to terminate
		try {
			for (int i = 0; i < NUM_PEERS; i++) {
				threads[i].join() ;
			}
		}
		catch(InterruptedException e) {
		}


		int receiver = ((ReceiverPeer)simulators[0].getReceiver()).getNumberOfReceivedMessages() ;
		int sender1 = ((SenderPeer)simulators[1].getReceiver()).getNumberOfReceivedMessages() ;
		int sender2 = ((SenderPeer)simulators[2].getReceiver()).getNumberOfReceivedMessages() ;

		Assert.assertFalse(allMsgsReceived, "receiver received all messages from both peers") ;  
		Assert.assertTrue(receiver == NUM_MSGS, "receiver did not receive all messages from single peer: received " + receiver) ;  
		Assert.assertTrue(sender1 == 2 * NUM_MSGS, "sender1 did not receive messages from itself and other sender: received " + sender1) ;  
		Assert.assertTrue(sender2 == 2 * NUM_MSGS, "sender2 did not receive messages from itself and other sender: received " + sender2) ;  
	}

	/**
	 * Test crash failure modelling.
	 */
	public void testCrashFailure() {

		// simulate crash failure on sender
		System.out.println("Starting testCrashfailure") ;
		simulators[1].simulateCrashFailure() ;

		// start the NAKACK peers and let them exchange messages
		for (int i = 0; i < NUM_PEERS; i++) {
			threads[i] = new MyPeer(simulators[i], isSender[i]) ;
			threads[i].start() ;
		}

		// wait for the receiver peer to signal that it has received messages, or timeout
		synchronized(all_msgs_recd) {
			try {
				all_msgs_recd.wait(WAIT_TIMEOUT * 1000) ;
			}
			catch(InterruptedException e) {
				System.out.println("main thread interrupted") ;
			}
		}

		// wait for the threads to terminate
		try {
			for (int i = 0; i < NUM_PEERS; i++) {
				threads[i].join() ;
			}
		}
		catch(InterruptedException e) {
		}

		int receiver = ((ReceiverPeer)simulators[0].getReceiver()).getNumberOfReceivedMessages() ;
		int sender1 = ((SenderPeer)simulators[1].getReceiver()).getNumberOfReceivedMessages() ;
		int sender2 = ((SenderPeer)simulators[2].getReceiver()).getNumberOfReceivedMessages() ;

		Assert.assertFalse(allMsgsReceived, "receiver received all messages from both peers") ;  
		Assert.assertTrue(receiver == NUM_MSGS, "receiver did not receive all messages from single peer") ;  
		Assert.assertTrue(sender1 == 0, "sender1 received messages") ;  
		Assert.assertTrue(sender2 == NUM_MSGS, "sender2 did not receive messages only from itself") ;  
	}

	/**
	 * Test network partition modelling.
	 */
	public void testNetworkPartition() {

		System.out.println("Starting testNetworkPartition") ;

		Address[] part1 = {addresses[0], addresses[1]} ;
		Address[] part2 = {addresses[2]} ;

		simulators[0].simulatePartition(part1) ;
		simulators[1].simulatePartition(part1) ;
		simulators[2].simulatePartition(part2) ;

		// start the NAKACK peers and let them exchange messages
		for (int i = 0; i < NUM_PEERS; i++) {
			threads[i] = new MyPeer(simulators[i], isSender[i]) ;
			threads[i].start() ;
		}

		// wait for the receiver peer to signal that it has received messages, or timeout
		synchronized(all_msgs_recd) {
			try {
				all_msgs_recd.wait(WAIT_TIMEOUT * 1000) ;
			}
			catch(InterruptedException e) {
				System.out.println("main thread interrupted") ;
			}
		}

		// wait for the threads to terminate
		try {
			for (int i = 0; i < NUM_PEERS; i++) {
				threads[i].join() ;
			}
		}
		catch(InterruptedException e) {
		}


		int receiver = ((ReceiverPeer)simulators[0].getReceiver()).getNumberOfReceivedMessages() ;
		int sender1 = ((SenderPeer)simulators[1].getReceiver()).getNumberOfReceivedMessages() ;
		int sender2 = ((SenderPeer)simulators[2].getReceiver()).getNumberOfReceivedMessages() ;

		Assert.assertFalse(allMsgsReceived, "receiver received all messages from both peers") ;  
		Assert.assertTrue(receiver == NUM_MSGS, "receiver did not receive all messages from single peer") ;  
		Assert.assertTrue(sender1 == NUM_MSGS, "sender1 did not receive messages only from itself") ;  
		Assert.assertTrue(sender2 == NUM_MSGS, "sender2 did not receive messages only from itself") ;  
	}


	/**
	 * This is called by the Simulator when a message comes back up the stack.
	 * Used by message senders to simply display messages received from other peers.
	 */
	class SenderPeer implements Simulator.Receiver {
		Simulator simulator = null ;
		int num_mgs_received=0;

		SenderPeer(Simulator s) {
			this.simulator = s ;
		}

		// keep track of how many messages were received by this peer
		public void receive(Event evt) {
			if(evt.getType() == Event.MSG) {
				num_mgs_received++;
				if(num_mgs_received % MSGS_PER_STATUS_LINE == 0)
					System.out.println("<" + simulator.getLocalAddress() + ">:" + "<== " + num_mgs_received);
			}
		}

		public int getNumberOfReceivedMessages() {
			return num_mgs_received;
		}
	}

	/**
	 * This is called by the Simulator when a message comes back up the stack.
	 * This method should do the following:
	 * - receive messages from senders 
	 * - terminate when correct number of messages have been received
	 */
	class ReceiverPeer implements Simulator.Receiver {
		Simulator simulator = null ;
		int num_mgs_received=0;
		Message msg ;
		Address sender ;

		ReceiverPeer(Simulator s) {
			this.simulator = s ;
		}

		public synchronized void receive(Event evt) {

			if (evt.getType() == Event.MSG) {

				// keep track of seqno ordering of messages received
				msg=(Message)evt.getArg();
				sender=msg.getSrc();

				try {
					num_mgs_received++ ;

					Address address = simulator.getLocalAddress() ;

					if(num_mgs_received % MSGS_PER_STATUS_LINE == 0)
						System.out.println("<" + address + ">:" + "PASS: received msg #" + num_mgs_received + " from " + sender);


					// condition to terminate the test - all messages received (whether in 
					// correct order or not)
					if(num_mgs_received >= SimulatorTest.NUM_MSGS * (NUM_PEERS-1)) {

						// indicate that we have received the required number of messages
						// to differentiate between timeout and notifyAll cases on monitor
						allMsgsReceived = true ;

						// signal that all messages have been received - this will allow the receiver
						// thread to terminate normally
						synchronized(all_msgs_recd) {
							all_msgs_recd.notifyAll() ;
						}
					}   
				}
				catch(Exception ex) {
					System.out.println("SimulatorTest.receive()" + ex.toString());
				}
			}	
		}

		public int getNumberOfReceivedMessages() {
			return num_mgs_received;
		}
	}


	static class MyPeer extends Thread {

		Simulator s = null ;
		boolean sender = false ;

		public MyPeer(Simulator s, boolean sender) {
			this.s = s ;
			this.sender = sender ;
		}

		public void run() {

			// senders send NUM_MSGS messages to all peers, beginning with seqno 1
			if (sender) {

				Address address = s.getLocalAddress() ;

				// send a collection of dummy messages by mcast to the stack under test
				for(int i=1; i <= NUM_MSGS; i++) {

					Message msg=new Message(null, address, new Long(i));
					Event evt=new Event(Event.MSG, msg);

					// call Simulator.send() to introduce the event into the stack under test
					s.send(evt);

					// status indicator
					if(i % MSGS_PER_STATUS_LINE == 0)
						System.out.println("<" + address + ">:" + " ==> " + i);
				}	   
			}

			if (!sender) {
				// wait for the receiver callback to signal that it has received messages, or timeout
				// this just causes this thread to block until its receiver has finished 
				synchronized(all_msgs_recd) {
					try {
						all_msgs_recd.wait(WAIT_TIMEOUT * 1000) ;
					}
					catch(InterruptedException e) {
						System.out.println("thread interrupted") ;
					}
				}
			}
		}
	}

}



