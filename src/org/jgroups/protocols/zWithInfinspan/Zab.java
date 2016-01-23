package org.jgroups.protocols.zWithInfinspan;


import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.jgroups.Address;
import org.jgroups.AnycastAddress;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.protocols.jzookeeper.ProtocolStats;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;


/*
 * It is orignal protocol of Apache Zookeeper. Also it has features of testing throuhput, latency (in Nano), ant etc. 
 * When using testing, it provides warm up test before starting real test.
 * @author Ibrahim EL-Sanosi
 */

public class Zab extends Protocol {
	private final static String ProtocolName = "Zab";
	private final static int numberOfSenderInEachClient = 20;
	private final AtomicLong zxid = new AtomicLong(0);
	private ExecutorService executor;
	private Address local_addr;
	private volatile Address leader;
	private volatile View view;
	private volatile boolean is_leader = false;
	private List<Address> zabMembers = Collections
			.synchronizedList(new ArrayList<Address>());
	private long lastZxidProposed = 0, lastZxidCommitted = 0;
	private final Set<MessageId> requestQueue = Collections
			.synchronizedSet(new HashSet<MessageId>());
	private Map<Long, ZabHeader> queuedCommitMessage = new HashMap<Long, ZabHeader>();
	private final Map<Long, ZabHeader> queuedProposalMessage = Collections
			.synchronizedMap(new HashMap<Long, ZabHeader>());
	private final LinkedBlockingQueue<ZabHeader> queuedMessages = new LinkedBlockingQueue<ZabHeader>();
	private ConcurrentMap<Long, Proposal> outstandingProposals = new ConcurrentHashMap<Long, Proposal>();
	private final Map<MessageId, Message> messageStore = Collections
			.synchronizedMap(new HashMap<MessageId, Message>());
	private Calendar cal = Calendar.getInstance();
	private int index = -1;
	private volatile boolean running = true;
	private volatile boolean startThroughput = false;
	private final static String outDir = "/home/pg/p13/a6915654/Zab/";
	private AtomicLong countMessageLeader = new AtomicLong(0);
	private long countMessageFollower = 0;
	private List<Address> clients = Collections
			.synchronizedList(new ArrayList<Address>());
	private AtomicLong local = new AtomicLong(0);
	private AtomicInteger indexs = new AtomicInteger(0);
    private final Map<Address, Long> orderStore = Collections.synchronizedMap(new HashMap<Address, Long>());
	private ProtocolStats stats;

	/*
	 * Empty constructor
	 */
	public Zab() {

	}

	@ManagedAttribute
	public boolean isleaderinator() {
		return is_leader;
	}

	public Address getleaderinator() {
		return leader;
	}

	public Address getLocalAddress() {
		return local_addr;
	}

	@ManagedOperation
	public String printStats() {
		return dumpStats().toString();
	}

	@Override
	public void start() throws Exception {
		super.start();
		running = true;
		executor = Executors.newSingleThreadExecutor();
		executor.execute(new FollowerMessageHandler(this.id));
		log.setLevel("trace");
		

	}
	/*
	 * reset all protocol fields, reset invokes after warm up has finished, then callback the clients to start 
	 * main test
	 */
	public void reset(Address client) {
		zxid.set(0);
		lastZxidProposed = 0;
		lastZxidCommitted = 0;
		requestQueue.clear();
		queuedCommitMessage.clear();
		queuedProposalMessage.clear();
		queuedMessages.clear();
		outstandingProposals.clear();
		messageStore.clear();
		startThroughput = false;
		countMessageLeader = new AtomicLong(0);// timer.cancel();
		countMessageFollower = 0;
		//is_warmUp = false;
		//this.stats = new ProtocolStats(ProtocolName, clients.size(), numberOfSenderInEachClient, outDir);
		log.info("Reset done");
		MessageId messageId = new MessageId(local_addr, -10,
				System.currentTimeMillis());
//		log.info("Start of run SystemStats");
//		SystemStats stats = new SystemStats();
//		Thread t = new Thread(new StatsCollector(stats,2));
//		t.start();
//		log.info("End of run SystemStats");
		//it only ensures one server (leader) call clients to start real test
		if (is_leader) {
			for (Address c : clients) {
				ZabHeader startTest = new ZabHeader(ZabHeader.STARTREALTEST,
						messageId);
				Message confirmClient = new Message(c).putHeader(this.id,
						startTest);
				down_prot.down(new Event(Event.MSG, confirmClient));
			}
		}

	}

	@Override
	public void stop() {
		running = false;
		executor.shutdown();
		super.stop();
	}

	public Object down(Event evt) {
		switch (evt.getType()) {
		case Event.MSG:
			Message m = (Message) evt.getArg();
			return null; // don't pass down
		case Event.SET_LOCAL_ADDRESS:
			local_addr = (Address) evt.getArg();
			break;
		}
		return down_prot.down(evt);
	}

	public Object up(Event evt) {
		Message msg = null;
		ZabHeader hdr;

		switch (evt.getType()) {
		case Event.MSG:
			msg = (Message) evt.getArg();
			hdr = (ZabHeader) msg.getHeader(this.id);
			log.info(" Receive Request header = " + hdr);
			if (hdr == null) {
				break; // pass up
			}
			switch (hdr.getType()) {
			case ZabHeader.START_SENDING:
				if (!zabMembers.contains(local_addr))
					return up_prot.up(new Event(Event.MSG, msg));
				break;
			case ZabHeader.REQUEST:
				forwardToLeader(msg);
				break;
			case ZabHeader.RESET:
				reset(msg.getSrc());
				break;
			case ZabHeader.FORWARD:
				//if (!is_warmUp){
					//stats.addLatencyProposalST(hdr.getMessageId(), System.nanoTime());
				//}
				queuedMessages.add(hdr);
				break;
			case ZabHeader.PROPOSAL:
				if (!is_leader) {
//					if (!is_warmUp && !startThroughput) {
//						startThroughput = true;
//						stats.setStartThroughputTime(System.currentTimeMillis());
//					}
					sendACK(msg, hdr);
				}
				break;
			case ZabHeader.ACK:
				if (is_leader) {
					processACK(msg, msg.getSrc());
				}
				break;
			case ZabHeader.COMMIT:
				deliver(hdr.getZxid());
				break;
			//case ZabHeader.STATS:
				//log.info(" " + clients);
				//stats.printProtocolStats(is_leader);
				//break;
			case ZabHeader.COUNTMESSAGE:
				sendTotalABMssages(hdr);
				log.info("Yes, I recieved count request");
				break;
			case ZabHeader.SENDMYADDRESS:
				if (!zabMembers.contains(msg.getSrc())) {
					clients.add(msg.getSrc());
					}
				break;
			case ZabHeader.STARTREALTEST:
				if (!zabMembers.contains(local_addr))
					return up_prot.up(new Event(Event.MSG, msg));
				break;
			}
			//s
			return null;
		case Event.VIEW_CHANGE:
			handleViewChange((View) evt.getArg());
			break;

		}

		return up_prot.up(evt);
	}

	
	public void up(MessageBatch batch) {
		for (Message msg : batch) {
			if (msg.isFlagSet(Message.Flag.NO_TOTAL_ORDER)
					|| msg.isFlagSet(Message.Flag.OOB)
					|| msg.getHeader(id) == null)
				continue;
			batch.remove(msg);

			try {
				up(new Event(Event.MSG, msg));
			} catch (Throwable t) {
				log.error("failed passing up message", t);
			}
		}

		if (!batch.isEmpty())
			up_prot.up(batch);
	}

	
	/*
	 * --------------------------------- Private Methods  --------------------------------
	 */

	private void handleViewChange(View v) {
		List<Address> mbrs = v.getMembers();
		leader = mbrs.get(0);
		if (leader.equals(local_addr)) {
			is_leader = true;
		}
		//make the first three joined server as ZK servers
		if (mbrs.size() == 3) {
			zabMembers.addAll(v.getMembers());
		}
		if (mbrs.size() > 3 && zabMembers.isEmpty()) {
			for (int i = 0; i < 3; i++) {
				zabMembers.add(mbrs.get(i));
			}

		}
		log.info("zabMembers size = " + zabMembers);
		if (mbrs.isEmpty())
			return;

		if (view == null || view.compareTo(v) < 0)
			view = v;
		else
			return;
	}

	private long getNewZxid() {
		return zxid.incrementAndGet();
	}

	/*
	 * If this server is a leader put the request in queue for processing it.
	 * otherwise forwards request to the leader
	 */
	private synchronized void forwardToLeader(Message msg) {
		ZabHeader hdrReq = (ZabHeader) msg.getHeader(this.id);
		requestQueue.add(hdrReq.getMessageOrderInfo().getId());
//		if (!is_warmUp && is_leader && !startThroughput) {
//			startThroughput = true;
//			stats.setStartThroughputTime(System.currentTimeMillis());
//		}

		if (is_leader) {
//			if (!is_warmUp){
//				long stp = System.nanoTime();
//				hdrReq.getMessageId().setStartLToFP(stp);
//				hdrReq.getMessageId().setStartTime(stp);
//				stats.addLatencyProposalForwardST(hdrReq.getMessageId(), System.nanoTime());
//			}
			queuedMessages.add(hdrReq);
		} else {
//			if (!is_warmUp){
//				long stf = System.nanoTime();
//				hdrReq.getMessageId().setStartFToLF(stf);
//				hdrReq.getMessageId().setStartTime(stf);
//			}
			forward(msg);
		}

	}
	/*
	 * Forward request to the leader
	 */
	private void forward(Message msg) {
		Address target = leader;
		if (target == null)
			return;
		ZabHeader hdrReq = (ZabHeader) msg.getHeader(this.id);
		try {
			ZabHeader hdr = new ZabHeader(ZabHeader.FORWARD, hdrReq.getMessageOrderInfo());
			Message forward_msg = new Message(target).putHeader(this.id, hdr);
			down_prot.down(new Event(Event.MSG, forward_msg));
		} catch (Exception ex) {
			log.error("failed forwarding message to " + msg, ex);
		}

	}

	/*
	 * This method is invoked by follower. 
	 * Follower receives a proposal. This method generates ACK message and send it to the leader.
	 */
	private void sendACK(Message msg, ZabHeader hdrAck) {
//		if (!is_warmUp){
//			stats.incNumRequest();
//		}
		if (msg == null)
			return;

		// ZABHeader hdr = (ZABHeader) msg.getHeader(this.id);
		if (hdrAck == null)
			return;

		// if (hdr.getZxid() != lastZxidProposed + 1){
		// log.info("Got zxid 0x"
		// + Long.toHexString(hdr.getZxid())
		// + " expected 0x"
		// + Long.toHexString(lastZxidProposed + 1));
		// }
		MessageOrderInfo messageOrderInfo = hdrAck.getMessageOrderInfo();
		lastZxidProposed = messageOrderInfo.getOrdering();
		queuedProposalMessage.put(messageOrderInfo.getOrdering(), hdrAck);
		ZabHeader hdrACK = new ZabHeader(ZabHeader.ACK, messageOrderInfo.getOrdering());
		Message ACKMessage = new Message(leader).putHeader(this.id, hdrACK);
//		if (!is_warmUp){
//			countMessageFollower++;
			//stats.incCountMessageFollower();
//			if (requestQueue.contains(hdrAck.getMessageId())) {
//				long stf = hdrAck.getMessageId().getStartFToLF();
//				stats.addLatencyFToLF((int) (System.nanoTime() - stf));
				//log.info("Latency for forward fro zxid=" +  dZxid + " start time=" + stf + " End Time=" + etf + " latency = "+((etf - stf)/1000000));
			//}
		//}
		try {
			down_prot.down(new Event(Event.MSG, ACKMessage));
		} catch (Exception ex) {
			log.error("failed sending ACK message to Leader");
		}

	}

	/*
	 * This method is invoked by leader. It receives ACK message from a follower
	 * and check if a majority is reached for particular proposal. 
	 */
	private synchronized void processACK(Message msgACK, Address sender) {

		ZabHeader hdr = (ZabHeader) msgACK.getHeader(this.id);
		long ackZxid = hdr.getZxid();
		if (lastZxidCommitted >= ackZxid) {
			return;
		}
		Proposal p = outstandingProposals.get(ackZxid);
		if (p == null) {
			return;
		}
		p.AckCount++;
		if (isQuorum(p.getAckCount())) {
			if (ackZxid == lastZxidCommitted + 1) {
				outstandingProposals.remove(ackZxid);
//				if(!is_warmUp && requestQueue.contains(hdr.getMessageId())){
//					long stf = hdr.getMessageId().getStartLToFP();
					//log.info("Latency for Prposal for zxid=" +  ackZxid + " start time=" + stf + " End Time=" + etf + " latency = "+((etf - stf)/1000000));
					//stats.addLatencyLToFP((int) (System.nanoTime() - stf));
				//}
				commit(ackZxid);
			} else
				System.out.println(">>> Can't commit >>>>>>>>>");
		}

	}

	/*
	 * This method is invoked by leader. It sends COMMIT message to all follower and itself.
	 */
	private void commit(long zxidd) {
		//ZABHeader hdrOrg = queuedProposalMessage.get(zxidd);
		synchronized (this) {
			lastZxidCommitted = zxidd;
		}
		ZabHeader hdrCommit = new ZabHeader(ZabHeader.COMMIT, zxidd);
		Message commitMessage = new Message().putHeader(this.id, hdrCommit);
		for (Address address : zabMembers) {
//			if (!address.equals(leader) && !is_warmUp){
//				countMessageLeader.incrementAndGet();
//				stats.incCountMessageLeader();
//			}
			Message cpy = commitMessage.copy();
			cpy.setDest(address);			
			down_prot.down(new Event(Event.MSG, cpy));

		}

	}
	
	/*
	 * Deliver the proposal locally and if the current server is the receiver of the request, 
	 * replay to the client.
	 */
	private void deliver(long dZxid) {
		ZabHeader hdrOrginal = queuedProposalMessage.remove(dZxid);
		MessageOrderInfo messageOrderInfo = hdrOrginal.getMessageOrderInfo();
		if(messageOrderInfo==null)
			log.info("messageOrderInfo equal null deliver");
		
        setLastOrderSequences(messageOrderInfo);
		queuedCommitMessage.put(dZxid, hdrOrginal);
//		if (!is_warmUp) {
//			stats.incnumReqDelivered();
//			stats.setEndThroughputTime(System.currentTimeMillis());
//		}

		if (log.isInfoEnabled())
			log.info("queuedCommitMessage size = " + queuedCommitMessage.size()
					+ " zxid " + dZxid);
		if (requestQueue.contains(messageOrderInfo.getId())) {
//			if (!is_warmUp){
//				long startTime = hdrOrginal.getMessageId().getStartTime();
//				stats.addLatency((int) (System.nanoTime() - startTime));
//			}
			sendOrderResponse(messageOrderInfo);
			requestQueue.remove((messageOrderInfo.getId()));
		}
	}
	
	private void sendOrderResponse(MessageOrderInfo messageOrderInfo){
		CSInteractionHeader hdrResponse = new CSInteractionHeader(CSInteractionHeader.RESPONSE, messageOrderInfo);
		Message msgResponse = new Message(messageOrderInfo.getId()
				.getOriginator()).putHeader((short) 78, hdrResponse);
		down_prot.down(new Event(Event.MSG, msgResponse));
	}
	
	
	private void setLastOrderSequences(MessageOrderInfo messageOrderInfo) {
        long[] clientLastOrder = new long[messageOrderInfo.getDestinations().length];
        List<Address> destinations = getAddresses(messageOrderInfo.getDestinations());
        //log.info("Order N# "+messageOrderInfo.getOrdering()+ " For "+destinations);
        for (int i = 0; i < destinations.size(); i++) {
            Address destination = destinations.get(i);
            if (!orderStore.containsKey(destination)) {
            	clientLastOrder[i] = -1;
                orderStore.put(destinations.get(i), messageOrderInfo.getOrdering());
            } else {
            	clientLastOrder[i] = orderStore.put(destination, messageOrderInfo.getOrdering());
            }
        }
        messageOrderInfo.setclientsLastOrder(clientLastOrder);
    }

	/*
	 * Check a majority
	 */
	private boolean isQuorum(int majority) {
		return majority >= ((zabMembers.size() / 2) + 1) ? true : false;
	}

	private String getCurrentTimeStamp() {
		long timestamp = new Date().getTime();
		cal.setTimeInMillis(timestamp);
		String timeString = new SimpleDateFormat("HH:mm:ss:SSS").format(cal
				.getTime());

		return timeString;
	}

	private void sendTotalABMssages(ZabHeader CarryCountMessageLeader) {
		if (!is_leader) {
			ZabHeader followerMsgCount = new ZabHeader(ZabHeader.COUNTMESSAGE,
					countMessageFollower);
			Message requestMessage = new Message(leader).putHeader(this.id,
					followerMsgCount);
			down_prot.down(new Event(Event.MSG, requestMessage));
		} else {
			long followerMsg = CarryCountMessageLeader.getZxid();
			//stats.addCountTotalMessagesFollowers((int)followerMsg);
		}

	}
	
	private List<Address> getAddresses(byte[] indexes) {
        if (view == null)
            throw new IllegalArgumentException("View cannot be null");

        List<Address> addresses = new ArrayList<Address>();
        for (byte index : indexes) {
            addresses.add(view.getMembers().get(index));
        }
        return addresses;
    }


	/*
	 * ----------------------------- End of Private Methods --------------------------------
	 */

	final class FollowerMessageHandler implements Runnable {

		private short id;

		public FollowerMessageHandler(short id) {
			this.id = id;
		}

		/**
		 * create a proposal and send it out to all the members
		 * 
		 * @param message
		 */
		@Override
		public void run() {
			handleRequests();
		}

		public void handleRequests() {
			ZabHeader hdrReq = null;
			while (running) {

				try {
					hdrReq = queuedMessages.take();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				long new_zxid = getNewZxid();
//				if (!is_warmUp){
//					stats.incNumRequest();
//				}
				MessageOrderInfo messageOrderInfo = hdrReq.getMessageOrderInfo();
				messageOrderInfo.setOrdering(new_zxid);
				ZabHeader hdrProposal = new ZabHeader(ZabHeader.PROPOSAL,
						hdrReq.getMessageOrderInfo());
				Message ProposalMessage = new Message().putHeader(this.id,
						hdrProposal);

				ProposalMessage.setSrc(local_addr);
				Proposal p = new Proposal();
				p.setMessageId(messageOrderInfo.getId());
				p.setMessageOrderInfo(hdrReq.getMessageOrderInfo());

				p.AckCount++;
				outstandingProposals.put(new_zxid, p);
				queuedProposalMessage.put(new_zxid, hdrProposal);
//				if (!is_warmUp){
//					//Long st = stats.getLatencyProposalST(hdrReq.getMessageId());
//					if (st!=null){
//						stats.removeLatencyProposalST(hdrReq.getMessageId());
//						stats.addLatencyProp((int) (System.nanoTime() - st));
//					}					
//					else{
//						Long stL = stats.getLatencyProposalForwardST(hdrReq.getMessageId());
//						if (stL!=null){
//							stats.removeLatencyProposalForwardST(hdrReq.getMessageId());
//							stats.addLatencyPropForward((int) (System.nanoTime() - stL));
//						}
//				    }
//				}

				try {

					for (Address address : zabMembers) {
						if (address.equals(leader))
							continue;
//						if (!is_warmUp){
//							countMessageLeader.incrementAndGet();
//							stats.incCountMessageLeader();
//						}
						Message cpy = ProposalMessage.copy();
						cpy.setDest(address);
						down_prot.down(new Event(Event.MSG, cpy));
					}
				} catch (Exception ex) {
					log.error("failed proposing message to members");
				}

			}

		}

	}

	public class StatsThread extends Thread {
		public void run() {
			int avg = 0, elementCount = 0;
//			for (int i = lastArrayIndex; i < latencies.size(); i++) {
//				avg += latencies.get(i);
//				elementCount++;
//			}

		//	lastArrayIndex = latencies.size() - 1;
			avg = avg / elementCount;
			//avgLatencies.add(avg);
		}
	}

	class ResubmitTimer extends TimerTask {

		@Override
		public void run() {
			//int finished = numReqDelivered.get();
			//currentCpuTime = System.currentTimeMillis();

			// Find average latency
			int avg = 0, elementCount = 0;

			// List<Integer> latCopy = new ArrayList<Integer>(latencies);
			//for (int i = lastArrayIndexUsingTime; i < latencies.size(); i++) {
				//if (latencies.get(i) > 50) {
				//	largeLatCount++;
					//largeLatencies.add((currentCpuTime - startThroughputTime)
							//+ "/" + latencies.get(i));
			//	}
				//avg += latencies.get(i);
				//elementCount++;
			//}

			//lastArrayIndexUsingTime = latencies.size() - 1;
			avg = avg / elementCount;

			//String mgsLat = (currentCpuTime - startThroughputTime) + "/"
					//+ ((finished - lastFinished) + "/" + avg);// (TimeUnit.MILLISECONDS.toSeconds(currentCpuTime
																// -
																// lastCpuTime)));
			//avgLatenciesTimer.add(mgsLat);
			//lastFinished = finished;
			//lastCpuTime = currentCpuTime;
		}

	}
}