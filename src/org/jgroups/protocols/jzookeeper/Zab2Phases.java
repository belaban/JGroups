package org.jgroups.protocols.jzookeeper;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
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

import java.util.concurrent.atomic.AtomicLong;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;

/* 
 * Zab_1 is the same implementation as Zab_0, but the follower will commit locally. 
 * This called Zab with 2 phases, can only work in 3 ZK servers.
 * Also it has features of testing throuhput, latency (in Nano), ant etc. 
 * When using testing, it provides warm up test before starting real test.
 */

public class Zab2Phases extends Protocol {

	private final static String ProtocolName = "Zab2Phases";
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
	private Map<Long, Zab2PhasesHeader> queuedCommitMessage = new HashMap<Long, Zab2PhasesHeader>();
	private List<Integer> avgLatencies = new ArrayList<Integer>();
	private List<String> avgLatenciesTimer = new ArrayList<String>();

	private final Map<Long, Zab2PhasesHeader> queuedProposalMessage = Collections
			.synchronizedMap(new HashMap<Long, Zab2PhasesHeader>());
	private final LinkedBlockingQueue<Zab2PhasesHeader> queuedMessages = new LinkedBlockingQueue<Zab2PhasesHeader>();
	private ConcurrentMap<Long, Proposal> outstandingProposals = new ConcurrentHashMap<Long, Proposal>();
	private final Map<MessageId, Message> messageStore = Collections
			.synchronizedMap(new HashMap<MessageId, Message>());
	private Calendar cal = Calendar.getInstance();
	private int index = -1;
	private volatile boolean running = true;
	private volatile boolean startThroughput = false;
	private final static String outDir = "/home/pg/p13/a6915654/Zab2Phases/";
	private AtomicLong countMessageLeader = new AtomicLong(0);
	private long countMessageFollower = 0;
	private boolean is_warmUp = true;
	private List<Address> clients = Collections
			.synchronizedList(new ArrayList<Address>());
	private ProtocolStats stats;

	public Zab2Phases() {

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
	 * reset all protocol fields, reset invokes after warm up has finished, then
	 * callback the clients to start main test
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
		avgLatencies.clear();
		avgLatenciesTimer.clear();
		startThroughput = false;
		countMessageLeader = new AtomicLong(0);
		countMessageFollower = 0;
		is_warmUp = false;
		this.stats = new ProtocolStats(ProtocolName, clients.size(),
				numberOfSenderInEachClient, outDir);
		log.info("Reset done");
		MessageId messageId = new MessageId(local_addr, -10,
				System.currentTimeMillis());
		// it only ensures one server (leader) call clients to start real test
		if (is_leader) {
			for (Address c : clients) {
				Zab2PhasesHeader startTest = new Zab2PhasesHeader(Zab2PhasesHeader.STARTREALTEST,
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
			handleClientRequest(m);
			return null; // don't pass down
		case Event.SET_LOCAL_ADDRESS:
			local_addr = (Address) evt.getArg();
			break;
		}
		return down_prot.down(evt);
	}

	public Object up(Event evt) {
		Message msg = null;
		Zab2PhasesHeader hdr;

		switch (evt.getType()) {
		case Event.MSG:
			msg = (Message) evt.getArg();
			hdr = (Zab2PhasesHeader) msg.getHeader(this.id);
			if (hdr == null) {
				break; // pass up
			}
			switch (hdr.getType()) {
			case Zab2PhasesHeader.START_SENDING:
				if (!zabMembers.contains(local_addr))
					return up_prot.up(new Event(Event.MSG, msg));
				break;
			case Zab2PhasesHeader.REQUEST:
				forwardToLeader(msg);
				break;
			case Zab2PhasesHeader.RESET:
				reset(msg.getSrc());
				break;
			case Zab2PhasesHeader.FORWARD:
				if (!is_warmUp){
					stats.addLatencyProposalST(hdr.getMessageId(), System.nanoTime());
				}
				queuedMessages.add(hdr);
				break;
			case Zab2PhasesHeader.PROPOSAL:
				if (!is_leader) {
					if (!is_warmUp && !startThroughput) {
						startThroughput = true;
						stats.setStartThroughputTime(System.currentTimeMillis());
					}
					sendACK(msg, hdr);
				}
				break;
			case Zab2PhasesHeader.ACK:
				if (is_leader) {
					processACK(msg, msg.getSrc());
				}
				break;
			case Zab2PhasesHeader.STATS:
				stats.printProtocolStats(is_leader);
				break;
			case Zab2PhasesHeader.COUNTMESSAGE:
				sendTotalABMwssages(hdr);
				log.info("Yes, I recieved count request");
				break;
			case Zab2PhasesHeader.SENDMYADDRESS:
				if (!zabMembers.contains(msg.getSrc())) {
					clients.add(msg.getSrc());
					System.out.println("Rceived clients address "
							+ msg.getSrc());
				}
				break;
			case Zab2PhasesHeader.STARTREALTEST:
				if (!zabMembers.contains(local_addr))
					return up_prot.up(new Event(Event.MSG, msg));
				break;
			case Zab2PhasesHeader.RESPONSE:
				handleOrderingResponse(hdr);
			}
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
	 * --------------------------------- Private Methods-----------------------------------
	 */

	/*
	 * Handling all client requests, processing them according to request type
	 */
	private void handleClientRequest(Message message) {
		Zab2PhasesHeader clientHeader = ((Zab2PhasesHeader) message.getHeader(this.id));

		if (clientHeader != null
				&& clientHeader.getType() == Zab2PhasesHeader.START_SENDING) {
			for (Address client : view.getMembers()) {
				if (log.isDebugEnabled())
					log.info("Address to check " + client);
				if (!zabMembers.contains(client)) {
					if (log.isDebugEnabled())
						log.info("Address to check is not zab Members, will send start request to"
								+ client + " " + getCurrentTimeStamp());
					message.setDest(client);
					down_prot.down(new Event(Event.MSG, message));
				}
			}
		}

		else if (clientHeader != null
				&& clientHeader.getType() == Zab2PhasesHeader.RESET) {

			for (Address server : zabMembers) {
				// message.setDest(server);
				Message resetMessage = new Message(server).putHeader(this.id,
						clientHeader);
//				log.info("Did it Reach handleClientRequest");
				resetMessage.setSrc(local_addr);
				down_prot.down(new Event(Event.MSG, resetMessage));
			}
		}

		else if (clientHeader != null
				&& clientHeader.getType() == Zab2PhasesHeader.STATS) {

			for (Address server : zabMembers) {
				// message.setDest(server);
				Message statsMessage = new Message(server).putHeader(this.id,
						clientHeader);
				down_prot.down(new Event(Event.MSG, statsMessage));
			}
		}

		else if (clientHeader != null
				&& clientHeader.getType() == Zab2PhasesHeader.COUNTMESSAGE) {
			for (Address server : zabMembers) {
				if (!server.equals(zabMembers.get(0))) {
					Message countMessages = new Message(server).putHeader(
							this.id, clientHeader);
					down_prot.down(new Event(Event.MSG, countMessages));
				}
			}

		}

		else if (!clientHeader.getMessageId().equals(null)
				&& clientHeader.getType() == Zab2PhasesHeader.REQUEST) {
			Address destination = null;
			messageStore.put(clientHeader.getMessageId(), message);
			Zab2PhasesHeader hdrReq = new Zab2PhasesHeader(Zab2PhasesHeader.REQUEST,
					clientHeader.getMessageId());
			++index;
			if (index > 2)
				index = 0;
			destination = zabMembers.get(index);
			Message requestMessage = new Message(destination).putHeader(
					this.id, hdrReq);
//			int diff = 1000 - hdrReq.size();
//			if (diff > 0)
//				requestMessage.setBuffer(new byte[diff]); // Necessary to ensure that
			down_prot.down(new Event(Event.MSG, requestMessage));
		}

		else if (!clientHeader.getMessageId().equals(null)
				&& clientHeader.getType() == Zab2PhasesHeader.SENDMYADDRESS) {
			Address destination = null;
			//destination = zabMembers.get(0);
			log.info("ZabMemberSize = " + zabMembers.size());
			for (Address server : zabMembers) {
				log.info("server address = " + server);
				message.dest(server);
				message.src(message.getSrc());
				down_prot.down(new Event(Event.MSG, message));
			}
		}

	}

	private void handleViewChange(View v) {
		List<Address> mbrs = v.getMembers();
		leader = mbrs.get(0);
		if (leader.equals(local_addr)) {
			is_leader = true;
		}
		// make the first three joined server as ZK servers
		if (mbrs.size() == 3) {
			zabMembers.addAll(v.getMembers());
		}
		if (mbrs.size() > 3 && zabMembers.isEmpty()) {
			for (int i = 0; i < 3; i++) {
				zabMembers.add(mbrs.get(i));
			}

		}
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
	private void forwardToLeader(Message msg) {
		Zab2PhasesHeader hdrReq = (Zab2PhasesHeader) msg.getHeader(this.id);
		requestQueue.add(hdrReq.getMessageId());
		if (!is_warmUp && is_leader && !startThroughput) {
			startThroughput = true;
			stats.setStartThroughputTime(System.currentTimeMillis());
			// timer = new Timer();
			// timer.schedule(new ResubmitTimer(), timeInterval, timeInterval);
		}

		if (is_leader) {
			if (!is_warmUp){
				long stp = System.nanoTime();
				hdrReq.getMessageId().setStartLToFP(stp);
				hdrReq.getMessageId().setStartTime(stp);
				stats.addLatencyProposalForwardST(hdrReq.getMessageId(), System.nanoTime());
			}
			queuedMessages.add(hdrReq);
		} else {
			if (!is_warmUp){
				long stf = System.nanoTime();
				hdrReq.getMessageId().setStartFToLF(stf);
				hdrReq.getMessageId().setStartTime(stf);
			}
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
		Zab2PhasesHeader hdrReq = (Zab2PhasesHeader) msg.getHeader(this.id);
		try {
			Zab2PhasesHeader hdr = new Zab2PhasesHeader(Zab2PhasesHeader.FORWARD,
					hdrReq.getMessageId());
			Message forward_msg = new Message(target).putHeader(this.id, hdr);
			down_prot.down(new Event(Event.MSG, forward_msg));
		} catch (Exception ex) {
			log.error("failed forwarding message to " + msg, ex);
		}

	}

	/*
	 * This method is invoked by follower. Follower receives a proposal. This
	 * method generates ACK message and send it to the leader and call commit
	 * message locally as it receives a majority.
	 */
	private void sendACK(Message msg, Zab2PhasesHeader hdrAck) {
		if(!is_warmUp){
			stats.incNumRequest();
		}
		if (msg == null)
			return;
		
		if (hdrAck == null)
			return;

		// if (hdr.getZxid() != lastZxidProposed + 1){
		// log.info("Got zxid 0x"
		// + Long.toHexString(hdr.getZxid())
		// + " expected 0x"
		// + Long.toHexString(lastZxidProposed + 1));
		// }

		lastZxidProposed = hdrAck.getZxid();
		queuedProposalMessage.put(hdrAck.getZxid(), hdrAck);
		Zab2PhasesHeader hdrACK = new Zab2PhasesHeader(Zab2PhasesHeader.ACK, hdrAck.getZxid(),
				hdrAck.getMessageId());
		Message ACKMessage = new Message(leader).putHeader(this.id, hdrACK);
		if (!is_warmUp){
			countMessageFollower++;
			stats.incCountMessageFollower();
			if (requestQueue.contains(hdrAck.getMessageId())) {
				long stf = hdrAck.getMessageId().getStartFToLF();
				stats.addLatencyFToLF((int) (System.nanoTime() - stf));
				//log.info("Latency for forward fro zxid=" +  dZxid + " start time=" + stf + " End Time=" + etf + " latency = "+((etf - stf)/1000000));
			}
		}
		commit(hdrAck.getZxid());
		try {
			down_prot.down(new Event(Event.MSG, ACKMessage));
		} catch (Exception ex) {
			log.error("failed sending ACK message to Leader");
		}
		
		//if (hdrAck.getZxid() == lastZxidCommitted + 1) {
		//outstandingProposals.remove(hdrAck.getZxid());
		//} else {
		//	System.out.println(">>> Can't commit >>>>>>>>>");
		//}

	}

	/*
	 * This method is invoked by leader. It receives ACK message from a follower
	 * and check if a majority is reached for particular proposal.
	 */
	private synchronized void processACK(Message msgACK, Address sender) {

		Zab2PhasesHeader hdr = (Zab2PhasesHeader) msgACK.getHeader(this.id);
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
				if(!is_warmUp && requestQueue.contains(hdr.getMessageId())){
					long stf = hdr.getMessageId().getStartLToFP();
					//log.info("Latency for Prposal for zxid=" +  ackZxid + " start time=" + stf + " End Time=" + etf + " latency = "+((etf - stf)/1000000));
					stats.addLatencyLToFP((int) (System.nanoTime() - stf));
				}
				commit(ackZxid);
			} else
				System.out.println(">>> Can't commit >>>>>>>>>");
		}

	}

	/*
	 * This method is invoked by leader and follower.
	 */
	private void commit(long zxidd) {
		//Zab2PhasesHeader hdrOrg = queuedProposalMessage.get(zxidd);
		synchronized (this) {
			lastZxidCommitted = zxidd;
		}
		deliver(zxidd);
//		if (hdrOrg == null) {
//			log.info("??????????????????????????? Header is null (commit)"
//					+ hdrOrg + " for zxid " + zxidd);
//			return;
//		}
		// log.info("Commiting commit >>>>>>>>>>>>>"+ zxidd);

	}

	/*
	 * Deliver the proposal locally and if the current server is the receiver of
	 * the request, replay to the client.
	 */
	private void deliver(long dZxid) {
		Zab2PhasesHeader hdrOrginal = queuedProposalMessage.remove(dZxid);
//		if (hdrOrginal == null) {
//			if (log.isInfoEnabled())
//				log.info("$$$$$$$$$$$$$$$$$$$$$ Header is null (deliver)"
//						+ hdrOrginal + " for zxid " + dZxid);
//			return;
//		}
		queuedCommitMessage.put(dZxid, hdrOrginal);
		if (!is_warmUp) {
			stats.incnumReqDelivered();
			stats.setEndThroughputTime(System.currentTimeMillis());
		}

		if (log.isInfoEnabled())
			log.info("queuedCommitMessage size = " + queuedCommitMessage.size()
					+ " zxid " + dZxid);
		if (requestQueue.contains(hdrOrginal.getMessageId())) {
			if (!is_warmUp) {
				long startTime = hdrOrginal.getMessageId().getStartTime();
				stats.addLatency((int) (System.nanoTime() - startTime));
			}
			Zab2PhasesHeader hdrResponse = new Zab2PhasesHeader(Zab2PhasesHeader.RESPONSE, dZxid,
					hdrOrginal.getMessageId());
			Message msgResponse = new Message(hdrOrginal.getMessageId()
					.getAddress()).putHeader(this.id, hdrResponse);
			down_prot.down(new Event(Event.MSG, msgResponse));

		}

	}

	/*
	 * Send replay to client
	 */
	private void handleOrderingResponse(Zab2PhasesHeader hdrResponse) {
		Message message = messageStore.get(hdrResponse.getMessageId());
		message.putHeader(this.id, hdrResponse);
		up_prot.up(new Event(Event.MSG, message));
		

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

	private void sendTotalABMwssages(Zab2PhasesHeader CarryCountMessageLeader) {
		if (!is_leader) {
			Zab2PhasesHeader followerMsgCount = new Zab2PhasesHeader(Zab2PhasesHeader.COUNTMESSAGE,
					countMessageFollower);
			Message requestMessage = new Message(leader).putHeader(this.id,
					followerMsgCount);
			down_prot.down(new Event(Event.MSG, requestMessage));
		} else {
			long followerMsg = CarryCountMessageLeader.getZxid();
			stats.addCountTotalMessagesFollowers((int) followerMsg);
		}

	}

	/*
	 * ----------------------------- End of Private Methods--------------------------------
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
			Zab2PhasesHeader hdrReq = null;
			while (running) {

				try {
					hdrReq = queuedMessages.take();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				long new_zxid = getNewZxid();
				if (!is_warmUp) {
					stats.incNumRequest();
				}

				Zab2PhasesHeader hdrProposal = new Zab2PhasesHeader(Zab2PhasesHeader.PROPOSAL,
						new_zxid, hdrReq.getMessageId());
				Message ProposalMessage = new Message().putHeader(this.id,
						hdrProposal);

				ProposalMessage.setSrc(local_addr);
				Proposal p = new Proposal();
				p.setMessageId(hdrReq.getMessageId());
				p.AckCount++;
				outstandingProposals.put(new_zxid, p);
				queuedProposalMessage.put(new_zxid, hdrProposal);
				//log.error("Yes I am Zab2Phases");
				if (!is_warmUp){
					Long st = stats.getLatencyProposalST(hdrReq.getMessageId());
					if (st!=null){
						stats.removeLatencyProposalST(hdrReq.getMessageId());
						stats.addLatencyProp((int) (System.nanoTime() - st));
					}					
					else{
						Long stL = stats.getLatencyProposalForwardST(hdrReq.getMessageId());
						if (stL!=null){
							stats.removeLatencyProposalForwardST(hdrReq.getMessageId());
							stats.addLatencyPropForward((int) (System.nanoTime() - stL));
						}
				    }
				}

				try {

					for (Address address : zabMembers) {
						if (address.equals(leader))
							continue;
						if (!is_warmUp) {
							countMessageLeader.incrementAndGet();
							stats.incCountMessageLeader();
						}
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
			//for (int i = lastArrayIndex; i < latencies.size(); i++) {
				//avg += latencies.get(i);
				//elementCount++;
			//}

			//lastArrayIndex = latencies.size() - 1;
			avg = avg / elementCount;
			avgLatencies.add(avg);
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
					//largeLatCount++;
					//largeLatencies.add((currentCpuTime - startThroughputTime)
						//	+ "/" + latencies.get(i));
				//}
				//avg += latencies.get(i);
				elementCount++;
			//}

			//lastArrayIndexUsingTime = latencies.size() - 1;
			avg = avg / elementCount;

			//String mgsLat = (currentCpuTime - startThroughputTime) + "/"
			//		+ ((finished - lastFinished) + "/" + avg);// (TimeUnit.MILLISECONDS.toSeconds(currentCpuTime
																// -
																// lastCpuTime)));
			//avgLatenciesTimer.add(mgsLat);
			//lastFinished = finished;
			//lastCpuTime = currentCpuTime;
		}

	}
}