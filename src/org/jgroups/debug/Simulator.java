package org.jgroups.debug;

import org.jgroups.*;
import org.jgroups.protocols.TP;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Queue;
import org.jgroups.util.QueueClosedException;
import org.jgroups.util.TimeScheduler;

import java.util.HashMap;
import java.util.Iterator;
import java.util.HashSet ;
import java.util.Set ;

/**
 * Tests one or more protocols independently. Look at org.jgroups.tests.FCTest for an example of how to use it.
 * @author Bela Ban
 * @version $Id: Simulator.java,v 1.16 2009/04/09 09:51:54 belaban Exp $
 */
public class Simulator {
	private Protocol[] protStack=null;
	private ProtocolAdapter ad=new ProtocolAdapter();
	ProtocolStack prot_stack=null;
	private Receiver r=null;
	private Protocol top=null, bottom=null;
	private Queue send_queue=new Queue();
	private Thread send_thread;
	private Queue recv_queue=new Queue();
	private Thread recv_thread;

	/** HashMap from Address to Simulator. */
	private final HashMap<Address, Simulator> addrTable=new HashMap<Address, Simulator>();
	private Address local_addr=null;
	private View view;

	/** fault-injection elements */
	private boolean crashFailureEnabled = false ;
	private boolean partitionEnabled = false ;
	private Set<Address> partition = new HashSet<Address>() ;
	private boolean slowProcessEnabled = false ;
	private long delay = 0 ; // in ms
	private boolean droppedMessagesEnabled = false ;
	private Set<DropMessage> droppedMessages = new HashSet<DropMessage>() ;

	public interface Receiver {
		void receive(Event evt);
	}

	public void setProtocolStack(Protocol[] stack) {
		this.protStack=stack;
		this.protStack[0].setUpProtocol(ad);
		this.protStack[this.protStack.length-1].setDownProtocol(ad);
		top=protStack[0];
		bottom=this.protStack[this.protStack.length-1];

		try {
			prot_stack=new ProtocolStack();
		} catch (ChannelException e) {           
			e.printStackTrace();
		}

		if(protStack.length > 1) {
			for(int i=0; i < protStack.length; i++) {
				Protocol p1=protStack[i];
				p1.setProtocolStack(prot_stack);
				Protocol p2=i+1 >= protStack.length? null : protStack[i+1];
				if(p2 != null) {
					p1.setDownProtocol(p2);
					p2.setUpProtocol(p1);
				}
			}
		}
	}

	public String dumpStats() {
		StringBuilder sb=new StringBuilder();
		for(int i=0; i < protStack.length; i++) {
			Protocol p1=protStack[i];
			sb.append(p1.getName()).append(":\n").append(p1.dumpStats()).append("\n");
		}
		return sb.toString();
	}

	public void addMember(Address addr) {
		addMember(addr, this);
	}

	public void addMember(Address addr, Simulator s) {
		addrTable.put(addr, s);
	}

	public void setLocalAddress(Address addr) {
		this.local_addr=addr;
	}

	public Address getLocalAddress() {
		return this.local_addr ;
	}

	public void setView(View v) {
		this.view=v;
	}

	public void setReceiver(Receiver r) {
		this.r=r;
	}

	public Receiver getReceiver() {
		return this.r ;
	}

	public Object send(Event evt) {
		return top.down(evt);
	}

	public void receive(Event evt) {
		try {
			Event copy;
			if(evt.getType() == Event.MSG && evt.getArg() != null) {
				copy=new Event(Event.MSG, ((Message)evt.getArg()).copy());
			}
			else
				copy=evt;

			recv_queue.add(copy);
		}
		catch(QueueClosedException e) {
		}
	}

	public void start() throws Exception {
		if(local_addr == null)
			throw new Exception("local_addr has to be non-null");
		if(protStack == null)
			throw new Exception("protocol stack is null");


		for(int i=0; i < protStack.length; i++) {
			Protocol p=protStack[i];
			p.setProtocolStack(prot_stack);
		}

		for(int i=0; i < protStack.length; i++) {
			Protocol p=protStack[i];
			p.init();
		}

        // bottom.up(new Event(Event.SET_LOCAL_ADDRESS, local_addr));
        protStack[0].down(new Event(Event.SET_LOCAL_ADDRESS, local_addr));

		for(int i=0; i < protStack.length; i++) {
			Protocol p=protStack[i];
			p.start();
		}

		// moved event processing to follow stack init (JGRP-843)

		if(view != null) {
			Event view_evt=new Event(Event.VIEW_CHANGE, view);
			bottom.up(view_evt);
			top.down(view_evt);
		}


		send_thread = new SendThread() ;
		send_thread.start();

		recv_thread = new ReceiveThread() ;
		recv_thread.start();
	}

	public void stop() {
		recv_thread=null;
		recv_queue.close(false);
		send_thread=null;
		send_queue.close(false);
		if(ad != null) {
			try {
				ad.getTimer().stop();
			}
			catch(InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	//
	// fault-injection methods
	//

	/*
	 * Simulate a crash failed process: no messages sent or received forever
	 */
	public void simulateCrashFailure() {
		crashFailureEnabled = true ;
		System.out.println("CRASH! at peer " + getLocalAddress());
	}

	/*
	 * Simulate a slow process.
	 * Received messages are delayed by delay factor.
	 */
	public void simulateSlowProcess(long delay) {
		slowProcessEnabled = true ;
		this.delay = delay ;
	}

	/*
	 * Simulate a network partition
	 * No messages are transferred between distinct partitions 
	 * Specify the partition we belong to.
	 */
	public void simulatePartition(Address[] partition) {

		partitionEnabled = true ;

		// clear out existing elements
		this.partition.clear();

		for (int i = 0; i < partition.length; i++) {
			this.partition.add(partition[i]) ;
		}

// 		Object[] elements = this.partition.toArray() ;
// 		System.out.print("<" + local_addr + ">: partition = {") ;
// 		for(int i = 0; i < elements.length; i++) {
// 		    System.out.print(" <" + (Address) elements[i] + "> ") ;
// 		}
// 		System.out.println("}") ;
	}

	/*
	 * Simulate a network partition merge.
	 */
	public void simulateMerge() {

		if (!partitionEnabled) 
			return ;

		partitionEnabled = false ;

		// clear out existing elements
		this.partition.clear();
	}


	/*
	 * Simulate dropped messages by registering a callback which determines
	 * if a message is to be dropped.   
	 */
	public void registerDropMessage(DropMessage d) {
		if (d != null) {
			droppedMessagesEnabled = true ;

			// add the DropMessage description to the list of callbacks
			droppedMessages.add(d) ;
		}
	}

	/*
	 * Remove the drop message rule.   
	 */
	public void deRegisterDropMessage(DropMessage d) {
		if (d != null) {
			// remove the DropMessage description from the list of callbacks
			droppedMessages.remove(d) ;

			if (droppedMessages.size() == 0) {
				droppedMessagesEnabled = false ;
			}
		}
	}

	/*
	 * Returns true if a message is to be dropped.   
	 */
	public boolean checkForDropMessage(Message msg, Address dest) {

		// iterate over the set of DropMessage callbacks and 
		// check if a message is to be dropped
		Address src = getLocalAddress() ;
			
		Iterator<DropMessage> it = droppedMessages.iterator();
		while (it.hasNext()) {
			DropMessage d = (DropMessage) it.next() ;
			if (d.drop(msg, dest))
				return true ;
		}
		return false ;
	}

	/*
	 * Method to determine if a message should be dropped before sending.
	 */
	public boolean senderDropFault(Message msg, Address dest) {

		Address a = getLocalAddress() ;

		// 1. crash failure - don't send messages
		if (crashFailureEnabled) {
			return true ;
		}

		// 2. partition - don't send messages to dest peers not in our partition
		if (partitionEnabled) {
			if (!partition.contains(dest)) {
				return true ;
			}
		}

		// 3. dropped messages - don't send if drop description exists
		if (droppedMessagesEnabled) {
			if (checkForDropMessage(msg, dest)) {
				return true ;
			}
		}

		return false ;
	}

	/*
	 * Method to determine if a message should be dropped before receiving.
	 */
	public boolean receiverDropFault(Message msg, Address src) {

		Address a = getLocalAddress() ;

		// 1. crash failure - don't receive messages
		if (crashFailureEnabled) { 
			return true ;
		}

		// 2. slow process - delay processing
		if (slowProcessEnabled) { 
			try {
				Thread.sleep(delay) ; 
				return false ;
			}
			catch(InterruptedException e) {
			}
		}

		// 3. partition - don't receive messages from src peers not in our partition
		if (partitionEnabled) {
			// look up message in partition table and drop if not present
			if (!partition.contains(src)) { 
				return true ;
			}
		}

		return false ;
	}


	class ProtocolAdapter extends TP {

		ProtocolAdapter() {
			timer=new TimeScheduler();
		}

		public TimeScheduler getTimer() {
			return timer;
		}

		public void setTimer(TimeScheduler timer) {
			this.timer=timer;
		}

		public String getName() {
			return "ProtocolAdapter";
		}

		public void sendMulticast(byte[] data, int offset, int length) throws Exception {
		}

		public void sendUnicast(PhysicalAddress dest, byte[] data, int offset, int length) throws Exception {
		}

		public String getInfo() {
			return null;
		}

        protected PhysicalAddress getPhysicalAddress() {
            throw new UnsupportedOperationException("not implemented");
        }

        public void init() throws Exception {
			super.init();
		}

		public Object up(Event evt) {
			if(r != null)
				r.receive(evt);
			return null;
		}

		/** send to unicast or multicast destination */
		public Object down(Event evt) {
			try {
				send_queue.add(evt);
			}
			catch(QueueClosedException e) {
			}
			return null;
		}
    }

	class SendThread extends Thread {

		public SendThread() {
			// System.out.println("send thread started") ;
		}

		public void run() {
			Event evt;
			while(send_thread != null) {
				try {
					// standard message processing
					evt=(Event)send_queue.remove();
					if(evt.getType() == Event.MSG) {
						Message msg=(Message)evt.getArg();
						Address dst=msg.getDest();
						Address src = msg.getSrc();
						// record the source address of the message
						if(src == null)
							((Message)evt.getArg()).setSrc(local_addr);

						Simulator s;
						Address d ;
						if(dst == null) {
							for(Iterator it=addrTable.values().iterator(); it.hasNext();) {
								s=(Simulator)it.next();

								// inject drop faults here
								d = s.getLocalAddress();
								if (!senderDropFault(msg, d)) {
									s.receive(evt);
								}
							}
						}
						else {
							s=(Simulator)addrTable.get(dst);
							if(s != null) {
								// inject drop faults here
								if (!senderDropFault(msg,dst)) {
									s.receive(evt);
								}
							}
						}
					}
				}
				catch(QueueClosedException e) {
					send_thread=null;
					break;
				}
			}
		}
	}

	class ReceiveThread extends Thread {

		ReceiveThread() {
			// System.out.println("receive thread started") ;    		
		}

		public void run() {
			Event evt;
			while(recv_thread != null) {
				try {
					evt=(Event)recv_queue.remove();

					Message msg=(Message)evt.getArg();
					Address dst=msg.getDest();
					Address src=msg.getSrc();

					// inject faults here
					if (!receiverDropFault(msg, src)) {
						bottom.up(evt);
					}
				}
				catch(QueueClosedException e) {
					recv_thread=null;
					break;
				}
			}
		}
	}

	/**
	 * Interface for a class which determines if a message should be
	 * dropped or not. Describes messages to be dropped. 
	 */
	public interface DropMessage {
		public boolean drop(Message msg, Address dest) ;
	}
}

