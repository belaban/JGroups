package org.jgroups.protocols.jzookeeper;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
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
import java.util.SortedSet;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.protocols.jzookeeper.ZAB.ResubmitTimer;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;

/* 
 * Zab_3 (main approach ) is the same implementation as Zab_2.
 * Note that all the code and implementation are simaller to Zab_2, just we change probability 
 * parameter in ZUtil class from 1.0 10 0.5.
 * Also it has features of testing throughput, latency (in Nano), ant etc. 
 * When using testing, it provides warm up test before starting real test.
 */
public class ZAB3 extends Protocol {
	private final static String ProtocolName = "ZabTossCoin";
	private final static int numberOfSenderInEachClient = 20;
	protected final AtomicLong zxid = new AtomicLong(0);
	private ExecutorService executor;
	protected Address local_addr;
	protected volatile Address leader;
	protected volatile View view;
	protected volatile boolean is_leader = false;
	private List<Address> zabMembers = Collections
			.synchronizedList(new ArrayList<Address>());
	private long lastZxidProposed = 0, lastZxidCommitted = 0;
	private final Set<MessageId> requestQueue = Collections
			.synchronizedSet(new HashSet<MessageId>());
	private Map<Long, ZABHeader> queuedCommitMessage = new HashMap<Long, ZABHeader>();
	private final LinkedBlockingQueue<ZABHeader> queuedMessages = new LinkedBlockingQueue<ZABHeader>();
	private ConcurrentMap<Long, Proposal> outstandingProposals = new ConcurrentHashMap<Long, Proposal>();
	private final Map<Long, ZABHeader> queuedProposalMessage = Collections
			.synchronizedMap(new HashMap<Long, ZABHeader>());
	private final Map<MessageId, Message> messageStore = Collections
			.synchronizedMap(new HashMap<MessageId, Message>());
	Calendar cal = Calendar.getInstance();
	protected volatile boolean running = true;
	private int index = -1;
	// private Map<Long, Boolean> notACK = new HashMap<Long, Boolean>();
	SortedSet<Long> wantCommit = new TreeSet<Long>();
	private List<Integer> latencies = new ArrayList<Integer>();
	private List<Integer> avgLatencies = new ArrayList<Integer>();
	private List<String> avgLatenciesTimer = new ArrayList<String>();
	private long currentCpuTime = 0, rateCountTime = 0, lastTimer = 0,
			lastCpuTime = 0;

	private Timer _timer;
	private boolean startSending = false;
	private long lastRequestRecieved = 0;
	private long laslAckRecieved = 0;
	private boolean recievedFirstRequest = false;
	private long current = 0;
	private boolean startThroughput = false;
	private static PrintWriter outFile;
	private final static String outDir = "/home/pg/p13/a6915654/MMZAB/";

	private int lastArrayIndex = 0, lastArrayIndexUsingTime = 0;
	private long timeInterval = 500;
	private int lastFinished = 0;
	private Timer timer;
	private AtomicLong countMessageLeader = new AtomicLong(0);
	private long countMessageFollower = 0;
	private long countTotalMessagesFollowers = 0;
	private AtomicLong warmUpRequest = new AtomicLong(0);
	private static long warmUp = 10000;
	private int largeLatCount = 0;
	private List<String> largeLatencies = new ArrayList<String>();
	private AtomicInteger numReqDelivered = new AtomicInteger(0);
	private long rateInterval = 10000;
	private long rateCount = 0;
	private boolean is_warmUp = true;
	private int longWait = Integer.MIN_VALUE;
	private volatile boolean makeAllFollowersAck = false;
	private List<Address> clients = Collections
			.synchronizedList(new ArrayList<Address>());
	private ProtocolStats stats;

	/*
	 * Empty constructor
	 */
	public ZAB3() {

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

	@Override
	public void start() throws Exception {
		super.start();
		log.setLevel("trace");
		running = true;
		executor = Executors.newSingleThreadExecutor();
		executor.execute(new FollowerMessageHandler(this.id));
		this.outFile = new PrintWriter(new BufferedWriter(new FileWriter(outDir
				+ InetAddress.getLocalHost().getHostName() + "MZAB.log", true)));
	}
	
	/*
	 * reset all protocol fields, reset invokes after warm up has finished, then callback the  to start 
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
		startSending = false;
		wantCommit.clear();
		lastRequestRecieved = 0;
		laslAckRecieved = 0;
		recievedFirstRequest = false;
		latencies.clear();
		numReqDelivered = new AtomicInteger(0);
		rateCount = 0;
		rateInterval = 10000;
		rateCount = 0;
		largeLatCount = 0;
		largeLatencies.clear();
		lastArrayIndex = 0;
		lastArrayIndexUsingTime = 0;
		is_warmUp = false;// _timer.cancel();
		countMessageLeader = new AtomicLong(0);
		countMessageFollower = 0;
		countTotalMessagesFollowers = 0;
		currentCpuTime = 0;
		rateCountTime = 0;
		lastTimer = 0;
		lastCpuTime = 0;
		longWait = Integer.MIN_VALUE;
		this.stats = new ProtocolStats(ProtocolName, clients.size(), numberOfSenderInEachClient, outDir);
		try {
			this.outFile = new PrintWriter(new BufferedWriter(new FileWriter(
					outDir + InetAddress.getLocalHost().getHostName()
							+ "MZAB.log", true)));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		avgLatencies.clear();
		avgLatenciesTimer.clear();
		currentCpuTime = 0;
		MessageId messageId = new MessageId(local_addr, -10,
				System.currentTimeMillis());
		//it only ensures one server (leader) call clients to start real test
		if (is_leader) {
			for (Address c : clients) {
				ZABHeader startTest = new ZABHeader(ZABHeader.STARTREALTEST,
						messageId);
				Message confirmClient = new Message(c).putHeader(this.id,
						startTest);
				down_prot.down(new Event(Event.MSG, confirmClient));
			}
		}

	}
	
	/*
	 * For sending Dummy request
	 */
	class FinishTask extends TimerTask {
		private short idd;

		public FinishTask(short id) {
			this.idd = id;
		}

		@Override
		public void run() {
			current = System.currentTimeMillis();

			if ((current - laslAckRecieved) > 50
					&& (current - lastRequestRecieved) > 50
					&& !outstandingProposals.isEmpty()) {
				// if (log.isInfoEnabled()){
				// if (!outstandingProposals.isEmpty() && (currentTime -
				// lastRequestRecieved) >500) {
				this.cancel();
				if (log.isInfoEnabled())
					log.info("Comit Alllllllllllllllllllllllllllllllllll");
				ZABHeader commitPending = new ZABHeader(
						ZABHeader.COMMITOUTSTANDINGREQUESTS);
				for (Address address : zabMembers) {
					Message commitALL = new Message(address).putHeader(
							this.idd, commitPending);
					down_prot.down(new Event(Event.MSG, commitALL));
				}
				startSending = false;
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
		ZABHeader hdr;

		switch (evt.getType()) {
		case Event.MSG:
			Message msg = (Message) evt.getArg();
			// hdr=(ZABHeader)msg.getHeader(this.id);
			handleClientRequest(msg);
			return null; // don't pass down
		case Event.SET_LOCAL_ADDRESS:
			local_addr = (Address) evt.getArg();
			break;
		}
		return down_prot.down(evt);
	}

	public Object up(Event evt) {
		Message msg = null;
		ZABHeader hdr;

		switch (evt.getType()) {
		case Event.MSG:
			msg = (Message) evt.getArg();
			hdr = (ZABHeader) msg.getHeader(this.id);
			if (hdr == null) {
				break; // pass up
			}
			switch (hdr.getType()) {
			case ZABHeader.START_SENDING:
				return up_prot.up(new Event(Event.MSG, msg));
			case ZABHeader.RESET:
				reset(msg.getSrc());
				break;
			case ZABHeader.REQUEST:
				if (!is_warmUp && !is_leader && !startThroughput) {
					startThroughput = true;
					stats.setStartThroughputTime(System.currentTimeMillis());
					// timer = new Timer();
					// timer.schedule(new ResubmitTimer(), timeInterval,
					// timeInterval);

				}
				forwardToLeader(msg);
				break;
			case ZABHeader.FORWARD:
				lastRequestRecieved = System.currentTimeMillis();
				recievedFirstRequest = true;
				if (!is_warmUp && !startSending) {
					_timer = new Timer();
					_timer.scheduleAtFixedRate(new FinishTask(this.id), 200,
							200);
					startSending = true;
				}
				// lastRequestRecieved = System.currentTimeMillis();
				queuedMessages.add(hdr);
				break;
			case ZABHeader.PROPOSAL:
				if (!is_leader) {
					// hdr.getMessageId().setStartTime(System.currentTimeMillis());
					sendACK(msg, hdr);
				}
				break;
			case ZABHeader.ACK:
				processACK(msg, msg.getSrc());
				break;
			case ZABHeader.COMMITOUTSTANDINGREQUESTS:
				commitPendingRequest();
				break;
			case ZABHeader.STATS:
				stats.printProtocolStats();
				break;
			case ZABHeader.COUNTMESSAGE:
				sendTotalABMessages(hdr);
				log.info("Yes, I recieved count request");
				break;
			case ZABHeader.SENDMYADDRESS:
				if (!zabMembers.contains(msg.getSrc())) {
					clients.add(msg.getSrc());
					System.out.println("Rceived client;s address "
							+ msg.getSrc());
				}
				break;
			case ZABHeader.STARTREALTEST:
				if (!zabMembers.contains(local_addr))
					return up_prot.up(new Event(Event.MSG, msg));
				else
					break;
			case ZABHeader.RESPONSE:
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
	 * --------------------------------- Private Methods ---------------------------------
	 */
	private void handleClientRequest(Message message) {
		ZABHeader clientHeader = ((ZABHeader) message.getHeader(this.id));
		if (clientHeader != null
				&& clientHeader.getType() == ZABHeader.START_SENDING) {

			for (Address client : view.getMembers()) {
				if (log.isInfoEnabled())
					log.info("Address to check " + client);
				// if (!zabMembers.contains(client)){
				if (log.isInfoEnabled())
					log.info("Address to check is not zab Members, will send start request to"
							+ client + " " + getCurrentTimeStamp());
				message.setDest(client);
				down_prot.down(new Event(Event.MSG, message));
				// }

			}
		}

		else if (clientHeader != null
				&& clientHeader.getType() == ZABHeader.RESET) {

			for (Address server : zabMembers) {
				// message.setDest(server);
				Message resetMessage = new Message(server).putHeader(this.id,
						clientHeader);
				down_prot.down(new Event(Event.MSG, resetMessage));
			}
		}

		else if (clientHeader != null
				&& clientHeader.getType() == ZABHeader.STATS) {

			for (Address server : zabMembers) {
				// message.setDest(server);
				Message statsMessage = new Message(server).putHeader(this.id,
						clientHeader);
				down_prot.down(new Event(Event.MSG, statsMessage));
			}
		}

		else if (clientHeader != null
				&& clientHeader.getType() == ZABHeader.COUNTMESSAGE) {
			for (Address server : zabMembers) {
				if (!server.equals(zabMembers.get(0))) {
					Message countMessages = new Message(server).putHeader(
							this.id, clientHeader);
					down_prot.down(new Event(Event.MSG, countMessages));
				}
			}
		}

		else if (!clientHeader.getMessageId().equals(null)
				&& clientHeader.getType() == ZABHeader.REQUEST) {
			Address destination = null;
			messageStore.put(clientHeader.getMessageId(), message);
			ZABHeader hdrReq = new ZABHeader(ZABHeader.REQUEST,
					clientHeader.getMessageId());
			++index;
			if (index > 2)
				index = 0;
			destination = zabMembers.get(index);
			Message requestMessage = new Message(destination).putHeader(
					this.id, hdrReq);
			down_prot.down(new Event(Event.MSG, requestMessage));
		}

		else if (!clientHeader.getMessageId().equals(null)
				&& clientHeader.getType() == ZABHeader.SENDMYADDRESS) {
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
		this.view = v;
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
		ZABHeader hdrReq = (ZABHeader) msg.getHeader(this.id);
		requestQueue.add(hdrReq.getMessageId());
		if (!is_warmUp && is_leader && !startThroughput) {
			startThroughput = true;
			stats.setStartThroughputTime(System.currentTimeMillis());
			// timer = new Timer();
			// timer.schedule(new ResubmitTimer(), timeInterval, timeInterval);
			// numRequest.incrementAndGet();
			// queuedMessages.add(hdr);
		}
		if (is_leader) {
			hdrReq.getMessageId().setStartTime(System.nanoTime());
			queuedMessages.add((ZABHeader) msg.getHeader(this.id));
		} else {
			hdrReq.getMessageId().setStartTime(System.nanoTime());
			forward(msg);
		}

	}
	/*
	 * Forward request to the leader
	 */
	private void forward(Message msg) {
		Address target = leader;
		ZABHeader hdrReq = (ZABHeader) msg.getHeader(this.id);
		if (target == null)
			return;
		try {
			ZABHeader hdr = new ZABHeader(ZABHeader.FORWARD,
					hdrReq.getMessageId());
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
	private void sendACK(Message msg, ZABHeader hrdAck) {
		if (!is_warmUp){
			stats.incNumRequest();
		}
		Proposal p;
		if (msg == null)
			return;
		if (hrdAck == null)
			return;

		// if (hdr.getZxid() != lastZxidProposed + 1){
		// log.info("Got zxid 0x"
		// + Long.toHexString(hdr.getZxid())
		// + " expected 0x"
		// + Long.toHexString(lastZxidProposed + 1));
		// }
		p = new Proposal();
		p.AckCount++; // Ack from leader
		p.setZxid(hrdAck.getZxid());
		outstandingProposals.put(hrdAck.getZxid(), p);
		lastZxidProposed = hrdAck.getZxid();
		queuedProposalMessage.put(hrdAck.getZxid(), hrdAck);
		if (is_warmUp) {
			ZABHeader hdrACK = new ZABHeader(ZABHeader.ACK, hrdAck.getZxid());
			Message ackMessage = new Message().putHeader(this.id, hdrACK);
			try {
				for (Address address : zabMembers) {
					Message cpy = ackMessage.copy();
					cpy.setDest(address);
					down_prot.down(new Event(Event.MSG, cpy));
				}
			} catch (Exception ex) {
				log.error("failed proposing message to members");
			}
		}

		else if (ZUtil.SendAckOrNoSend() || makeAllFollowersAck) {
			ZABHeader hdrACK = new ZABHeader(ZABHeader.ACK, hrdAck.getZxid());
			Message ackMessage = new Message().putHeader(this.id, hdrACK);
			try {
				for (Address address : zabMembers) {
					if (!is_warmUp && !address.equals(local_addr)){
						countMessageFollower++;
						stats.incCountMessageFollower();
					}
					Message cpy = ackMessage.copy();
					cpy.setDest(address);
					down_prot.down(new Event(Event.MSG, cpy));
				}
			} catch (Exception ex) {
				log.error("failed proposing message to members");
			}
		}
	}
	
	/*
	 * This method is invoked by leader and follower. It receives ACK message from a follower
	 * and check if a majority is reached for particular proposal. 
	 */
	private synchronized void processACK(Message msgACK, Address sender) {
		laslAckRecieved = System.currentTimeMillis();
		Proposal p = null;
		ZABHeader hdr = (ZABHeader) msgACK.getHeader(this.id);
		long ackZxid = hdr.getZxid();
		
		// if (!(outstandingProposals.containsKey(hdr.getZxid())) &&
		// (lastZxidProposed < hdr.getZxid())){
		// p = new Proposal();
		// outstandingProposals.put(hdr.getZxid(), p);
		// queuedProposalMessage.put(hdr.getZxid(), hdr);
		// lastZxidProposed = hdr.getZxid();
		// }
		if (lastZxidCommitted >= ackZxid) {
			// if (log.isDebugEnabled()) {
			// log.info("proposal has already been committed, pzxid: 0x{} zxid: 0x{}",
			// lastZxidCommitted, ackZxid);
			// }
			return;
		}
		p = outstandingProposals.get(ackZxid);
		if (p == null) {
			return;
		}

		p.AckCount++;
		// if (log.isDebugEnabled()) {
		// log.debug("Count for zxid: " +
		// Long.toHexString(ackZxid)+" = "+ p.getAckCount());
		// }

		if (isQuorum(p.getAckCount())) {
			if (ackZxid == lastZxidCommitted + 1) {
				commit(ackZxid);
				outstandingProposals.remove(ackZxid);
				// }
				// if (isFirstZxid(ackZxid)) {
				// log.info(" if (isQuorum(p.getAckCount())) commiting " +
				// ackZxid);
				// commit(ackZxid);
				// outstandingProposals.remove(ackZxid);
			} else {
				long zxidCommiting = lastZxidCommitted + 1;
				if (longWait < (ackZxid - zxidCommiting))
					longWait = (int) (ackZxid - zxidCommiting);
				for (long z = zxidCommiting; z < ackZxid + 1; z++) {
					commit(z);
					outstandingProposals.remove(z);
				}
			}
			
		}
	}
	/*
	 * commit all pending requests
	 */
	private void commitPendingRequest() {
		if (!outstandingProposals.isEmpty()) {
			for (Proposal proposalPending : outstandingProposals.values()) {
				wantCommit.add(proposalPending.getZxid());
			}
			for (long zx : wantCommit) {
				commit(zx);
				outstandingProposals.remove(zx);
			}
			wantCommit.clear();
		}

	}
	/*
	 * This method is invoked by leader and follower. It sends COMMIT message to all follower and itself.
	 */
	private void commit(long zxidd) {
		ZABHeader hdrOrginal = null;
		synchronized (this) {
			lastZxidCommitted = zxidd;
		}

		hdrOrginal = queuedProposalMessage.get(zxidd);
		if (hdrOrginal == null) {
			// if (log.isInfoEnabled())
			// log.info("!!!!!!!!!!!!!!!!!!!!!!!!!!!! Header is null (commit)"+
			// hdrOrginal + " for zxid "+zxid);
			return;
		}
		deliver(zxidd);

	}
	/*
	 * Deliver the proposal locally and if the current server is the receiver of the request, 
	 * replay to the client.
	 */
	private void deliver(long committedZxid) {
		ZABHeader hdrOrginal = queuedProposalMessage.remove(committedZxid);
		if (hdrOrginal == null) {
			if (log.isInfoEnabled())
				log.info("$$$$$$$$$$$$$$$$$$$$$ Header is null (deliver)"
						+ hdrOrginal + " for zxid " + committedZxid);
			return;
		}
		queuedCommitMessage.put(committedZxid, hdrOrginal);
		if (!is_warmUp) {
			stats.setEndThroughputTime(System.currentTimeMillis());
			stats.incnumReqDelivered();

			if (numReqDelivered.get() > 9999000)
				makeAllFollowersAck = true;
		}
		if (log.isInfoEnabled())
			log.info("queuedCommitMessage size = " + queuedCommitMessage.size()
					+ " zxid " + committedZxid);

		if (requestQueue.contains(hdrOrginal.getMessageId())) {
			if (!is_warmUp){
				long startTime = hdrOrginal.getMessageId().getStartTime();
				latencies.add((int) (System.nanoTime() - startTime));
				stats.addLatency((int) (System.nanoTime() - startTime));
			}
			ZABHeader hdrResponse = new ZABHeader(ZABHeader.RESPONSE,
					committedZxid, hdrOrginal.getMessageId());
			Message msgResponse = new Message(hdrOrginal.getMessageId()
					.getAddress()).putHeader(this.id, hdrResponse);
			down_prot.down(new Event(Event.MSG, msgResponse));

		}

	}


	/*
	 * Send replay to client
	 */
	private void handleOrderingResponse(ZABHeader hdrResponse) {
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

	private boolean isFirstZxid(long zxid) {
		int i = 0;
		boolean find = true;
		for (long z : outstandingProposals.keySet()) {
			// log.info("Inside isFirstZxid loop" + z + " i =" + (++i));
			if (z < zxid) {
				find = false;
				break;
			}
		}

		return find;
	}

	private void sendTotalABMessages(ZABHeader carryCountMessageLeader) {
		if (!is_leader) {
			ZABHeader followerMsgCount = new ZABHeader(ZABHeader.COUNTMESSAGE,
					countMessageFollower);
			Message requestMessage = new Message(leader).putHeader(this.id,
					followerMsgCount);
			down_prot.down(new Event(Event.MSG, requestMessage));
		} else {
			long followerMsg = carryCountMessageLeader.getZxid();
			countTotalMessagesFollowers += followerMsg;
			stats.addCountTotalMessagesFollowers((int)followerMsg);
		}

	}

	private String getCurrentTimeStamp() {
		long timestamp = new Date().getTime();
		cal.setTimeInMillis(timestamp);
		String timeString = new SimpleDateFormat("HH:mm:ss:SSS").format(cal
				.getTime());

		return timeString;
	}

	/*
	 * ----------------------------- End of Private Methods------------------------------
	 */

	final class FollowerMessageHandler implements Runnable {

		private short id;

		public FollowerMessageHandler(short id) {
			this.id = id;
		}

		@Override
		public void run() {
			handleRequests();
		}

		/**
		 * create a proposal and send it out to all the members
		 * 
		 * @param message
		 */

		private void handleRequests() {
			ZABHeader hdrReq = null;
			// long currentTime = 0;
			while (running) {
				try {
					hdrReq = queuedMessages.take();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				long new_zxid = getNewZxid();
				ZABHeader hdrProposal = new ZABHeader(ZABHeader.PROPOSAL,
						new_zxid, hdrReq.getMessageId());
				Message ProposalMessage = new Message().putHeader(this.id,
						hdrProposal);

				ProposalMessage.setSrc(local_addr);
				Proposal p = new Proposal();
				p.setMessageId(hdrReq.getMessageId());
				p.setZxid(new_zxid);
				p.AckCount++;
				lastZxidProposed = new_zxid;
				outstandingProposals.put(new_zxid, p);
				queuedProposalMessage.put(new_zxid, hdrProposal);
				if (!is_warmUp){
					stats.incNumRequest();
				}

				try {
					for (Address address : zabMembers) {
						if (address.equals(leader))
							continue;
						if (!is_warmUp){
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
			List<Integer> copyLat = new ArrayList<Integer>(latencies);
			for (int i = lastArrayIndex; i < copyLat.size(); i++) {
				avg += copyLat.get(i);
				elementCount++;
			}

			lastArrayIndex = copyLat.size() - 1;
			avg = avg / elementCount;
			avgLatencies.add(avg);
		}
	}

	class ResubmitTimer extends TimerTask {

		@Override
		public void run() {
			int finished = numReqDelivered.get();
			currentCpuTime = System.currentTimeMillis();

			// Find average latency
			int avg = 0, elementCount = 0;

			// List<Integer> latCopy = new ArrayList<Integer>(latencies);
//			for (int i = lastArrayIndexUsingTime; i < latencies.size(); i++) {
//				if (latencies.get(i) > 50) {
//					largeLatCount++;
//					largeLatencies.add((currentCpuTime - startThroughputTime)
//							+ "/" + latencies.get(i));
//				}
//				avg += latencies.get(i);
//				elementCount++;
//			}

			lastArrayIndexUsingTime = latencies.size() - 1;
			avg = avg / elementCount;

//			String mgsLat = (currentCpuTime - startThroughputTime) + "/"
//					+ ((finished - lastFinished) + "/" + avg);// (TimeUnit.MILLISECONDS.toSeconds(currentCpuTime
																// -
																// lastCpuTime)));
			//avgLatenciesTimer.add(mgsLat);
			lastFinished = finished;
			lastCpuTime = currentCpuTime;
		}

	}
}
