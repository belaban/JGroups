package org.jgroups.protocols.zabPerf;

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
import org.jgroups.protocols.jzookeeper.MessageId;
import org.jgroups.protocols.jzookeeper.Proposal;
import org.jgroups.protocols.zabPerf.ProtocolStats;
import org.jgroups.protocols.jzookeeper.ZabHeader;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Util;

/*
 * It is orignal protocol of Apache Zookeeper. Also it has features of testing throuhput, latency (in Nano), ant etc. 
 * When using testing, it provides warm up test before starting real test.
 * @author Ibrahim EL-Sanosi
 */

public class Zab extends Protocol {
	private final static String ProtocolName = "Zab";
	private final static int numberOfSenderInEachClient = 20;
	private final AtomicLong zxid = new AtomicLong(0);
	private ExecutorService executor1;
	private ExecutorService executor2;
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
	private final LinkedBlockingQueue<ZabHeader> delivery = new LinkedBlockingQueue<ZabHeader>();
	private ConcurrentMap<Long, Proposal> outstandingProposals = new ConcurrentHashMap<Long, Proposal>();
	private final Map<MessageId, Message> messageStore = Collections
			.synchronizedMap(new HashMap<MessageId, Message>());
	private Calendar cal = Calendar.getInstance();
	private int index = -1;
	private int clientFinished = 0;
	private int numABRecieved = 0;
	private volatile boolean running = true;
	private volatile boolean startThroughput = false;
	private final static String outDir = "/home/pg/p13/a6915654/Zab/";
	private final static String outDirWork = "/home/pg/p13/a6915654/work/Zab/";
	private AtomicLong countMessageLeader = new AtomicLong(0);
	private long countMessageFollower = 0;
	private List<Address> clients = Collections
			.synchronizedList(new ArrayList<Address>());
	private ProtocolStats stats = new ProtocolStats();

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
		executor1 = Executors.newSingleThreadExecutor();
		executor1.execute(new FollowerMessageHandler(this.id));
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
		startThroughput = false;
		countMessageLeader = new AtomicLong(0);// timer.cancel();
		countMessageFollower = 0;
		this.stats = new ProtocolStats(ProtocolName, clients.size(),
				numberOfSenderInEachClient, outDir, outDirWork, false);
		log.info("Reset done");
		MessageId messageId = new MessageId(local_addr, -10,
				System.currentTimeMillis());

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
		executor1.shutdown();
		executor2.shutdown();

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
		ZabHeader hdr;

		switch (evt.getType()) {
		case Event.MSG:
			msg = (Message) evt.getArg();
			hdr = (ZabHeader) msg.getHeader(this.id);
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
				if (!stats.isWarmup()) {
					stats.addTempPPTForward(hdr.getMessageId(),
							System.nanoTime());
					long startTime = hdr.getMessageId().getStartTime();
					//log.info("startTime="+startTime);
					//long timeTemp = System.currentTimeMillis();
					//log.info("timeTemp="+timeTemp);
					//log.info("(int)(timeTemp-startTime)="+(timeTemp-startTime));
					stats.addOneRoundFToL((int)(System.nanoTime()-startTime));
					stats.addLatencyFToLFOneRound((int)(System.nanoTime()-startTime));
					//log.info("(int)(System.nanoTime()-startTime)="+(System.nanoTime()-startTime));

				}
				queuedMessages.add(hdr);
				break;
			case ZabHeader.PROPOSAL:
				if (!is_leader) {
					if (!stats.isWarmup() && !startThroughput) {
						startThroughput = true;
						stats.setStartThroughputTime(System.currentTimeMillis());
					}
					if (!stats.isWarmup()){
						//long timeTemp = System.nanoTime();
						//stats.addTempAckProcessTime(hdr.getMessageId(), timeTemp);
					}
					sendACK(msg, hdr);
				}
				break;
			case ZabHeader.ACK:
				if(!stats.isWarmup()){
					stats.addCommitProcessTime(hdr.getMessageId(), System.nanoTime());
				}				
				processACK(msg, msg.getSrc());
				break;
			case ZabHeader.COMMIT:
//				if(!stats.isWarmup()){
//					long st = System.nanoTime();
//					stats.addDeliveryProcessTime(hdr.getMessageId(), st);
//				}
				delivery.add(hdr);
				//deliver(hdr.getZxid());
				break;
			case ZabHeader.CLIENTFINISHED:
				clientFinished++;
				if(clientFinished==clients.size() && !is_leader)
					sendMyTotalBroadcast();
				break;
			case ZabHeader.STATS:
				stats.printProtocolStats(is_leader);
				break;
			case ZabHeader.COUNTMESSAGE:
				addTotalABMssages(hdr);
				log.info("Yes, I recieved count request");
				break;
			case ZabHeader.SENDMYADDRESS:
				if (!zabMembers.contains(msg.getSrc())) {
					clients.add(msg.getSrc());
					System.out.println("Rceived clients address "
							+ msg.getSrc());
				}
				break;
			case ZabHeader.STARTREALTEST:
				if (!zabMembers.contains(local_addr))
					return up_prot.up(new Event(Event.MSG, msg));
				break;
			case ZabHeader.RESPONSE:
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
	 * --------------------------------- Private Methods
	 * --------------------------------
	 */

	/*
	 * Handling all client requests, processing them according to request type
	 */
	private void handleClientRequest(Message message) {
		ZabHeader clientHeader = ((ZabHeader) message.getHeader(this.id));

		if (clientHeader != null
				&& clientHeader.getType() == ZabHeader.START_SENDING) {
			for (Address client : view.getMembers()) {
				if (log.isDebugEnabled())
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
				&& clientHeader.getType() == ZabHeader.RESET) {

			for (Address server : zabMembers) {
				// message.setDest(server);
				Message resetMessage = new Message(server).putHeader(this.id,
						clientHeader);
				resetMessage.setSrc(local_addr);
				down_prot.down(new Event(Event.MSG, resetMessage));
			}
		}

		else if (clientHeader != null
				&& clientHeader.getType() == ZabHeader.STATS) {

			for (Address server : zabMembers) {
				Message statsMessage = new Message(server).putHeader(this.id,
						clientHeader);
				down_prot.down(new Event(Event.MSG, statsMessage));
			}
		}

		else if (clientHeader != null
				&& clientHeader.getType() == ZabHeader.CLIENTFINISHED) {
			for (Address server : zabMembers) {
					Message countMessages = new Message(server).putHeader(
							this.id, clientHeader);
					down_prot.down(new Event(Event.MSG, countMessages));
			}

		}

		else if (!clientHeader.getMessageId().equals(null)
				&& clientHeader.getType() == ZabHeader.REQUEST) {
			Address destination = null;
			messageStore.put(clientHeader.getMessageId(), message);
			ZabHeader hdrReq = new ZabHeader(ZabHeader.REQUEST,
					clientHeader.getMessageId());
			++index;
			if (index > 2)
				index = 0;
			destination = zabMembers.get(index);
			Message requestMessage = new Message(destination).putHeader(
					this.id, hdrReq);
			// int diff = 1000 - hdrReq.size();
			// if (diff > 0)
			// requestMessage.setBuffer(new byte[diff]);
			down_prot.down(new Event(Event.MSG, requestMessage));
		}

		else if (!clientHeader.getMessageId().equals(null)
				&& clientHeader.getType() == ZabHeader.SENDMYADDRESS) {
			Address destination = null;
			destination = zabMembers.get(0);
			// destination = zabMembers.get(0);
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
			if (zabMembers.contains(local_addr)) {
				executor2 = Executors.newSingleThreadExecutor();
		        executor2.execute(new MessageHandler());
			}

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
	private synchronized void forwardToLeader(Message msg) {
		ZabHeader hdrReq = (ZabHeader) msg.getHeader(this.id);
		requestQueue.add(hdrReq.getMessageId());
		if (!stats.isWarmup() && is_leader && !startThroughput) {
			startThroughput = true;
			stats.setStartThroughputTime(System.currentTimeMillis());
		}

		if (is_leader) {
			if (!stats.isWarmup()) {
				long stp = System.nanoTime();
				hdrReq.getMessageId().setStartLToFP(stp);
				hdrReq.getMessageId().setStartTime(stp);
				stats.addtempPPTL(hdrReq.getMessageId(),
						System.nanoTime());
			}
			queuedMessages.add(hdrReq);
		} else {
			if (!stats.isWarmup()) {
				//long stf = System.nanoTime();
				hdrReq.getMessageId().setStartFToLFOneWay(System.nanoTime());
				hdrReq.getMessageId().setStartTime(System.nanoTime());
				//log.info("startTime="+stf);

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
		ZabHeader hdrReq = (ZabHeader) msg.getHeader(this.id);
		try {
			ZabHeader hdr = new ZabHeader(ZabHeader.FORWARD,
					hdrReq.getMessageId());
			Message forward_msg = new Message(target).putHeader(this.id, hdr);
			down_prot.down(new Event(Event.MSG, forward_msg));
		} catch (Exception ex) {
			log.error("failed forwarding message to " + msg, ex);
		}

	}

	/*
	 * This method is invoked by follower. Follower receives a proposal. This
	 * method generates ACK message and send it to the leader.
	 */
	private void sendACK(Message msg, ZabHeader hdrAck) {
		if (!stats.isWarmup()) {
			stats.incNumRequest();
		}
		if (msg == null)
			return;

		if (hdrAck == null)
			return;

		lastZxidProposed = hdrAck.getZxid();
		queuedProposalMessage.put(hdrAck.getZxid(), hdrAck);
		ZabHeader hdrACK = new ZabHeader(ZabHeader.ACK, hdrAck.getZxid(),
				hdrAck.getMessageId());
		Message ACKMessage = new Message(leader).putHeader(this.id, hdrACK);
		if (!stats.isWarmup()) {
			countMessageFollower++;
			stats.incCountMessageFollower();
			//long stAck = stats.getTempAckProcessTime(hdrAck.getMessageId());
			//long currentTime = System.nanoTime();
			//stats.removeTempAckProcessTime(hdrAck.getMessageId());
			//stats.addAckPT((int) (currentTime - stAck));
			if (requestQueue.contains(hdrAck.getMessageId())) {
				long stf = hdrAck.getMessageId().getStartFToLFOneWay();
				stats.addLatencyFToLF((int) (System.nanoTime() - stf));
				// log.info("Latency for forward fro zxid=" + dZxid +
				// " start time=" + stf + " End Time=" + etf +
				// " latency = "+((etf - stf)/1000000));
			}
		}
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
			//if (ackZxid == lastZxidCommitted + 1) {
				outstandingProposals.remove(ackZxid);
				if (!stats.isWarmup() && requestQueue.contains(hdr.getMessageId())) {
					long stf = hdr.getMessageId().getStartLToFP();
					// log.info("Latency for Prposal for zxid=" + ackZxid +
					// " start time=" + stf + " End Time=" + etf +
					// " latency = "+((etf - stf)/1000000));
					stats.addLatencyLToFP((int) (System.nanoTime() - stf));
				}
				commit(ackZxid, hdr);
//			} else
//				System.out.println(">>> Can't commit >>>>>>>>>");
		}

	}

	/*
	 * This method is invoked by leader. It sends COMMIT message to all follower
	 * and itself.
	 */
	private void commit(long zxidd, ZabHeader zhdr) {
		// ZABHeader hdrOrg = queuedProposalMessage.get(zxidd);
		synchronized (this) {
			lastZxidCommitted = zxidd;
		}
		ZabHeader hdrCommit = new ZabHeader(ZabHeader.COMMIT, zxidd, zhdr.getMessageId());
		Message commitMessage = new Message().putHeader(this.id, hdrCommit);
		if (!stats.isWarmup()){
			long cst = stats.getCommitProcessTime(zhdr.getMessageId());
			stats.removeCommitProcessTime(zhdr.getMessageId());
			stats.addCommitPTL((int) (System.nanoTime() - cst));
		}
		for (Address address : zabMembers) {
			if (!address.equals(leader) && !stats.isWarmup()) {
				countMessageLeader.incrementAndGet();
				stats.incCountMessageLeader();
			}
			Message cpy = commitMessage.copy();
			cpy.setDest(address);
			down_prot.down(new Event(Event.MSG, cpy));

		}

		// if (hdrOrg == null) {
		// log.info("??????????????????????????? Header is null (commit)"
		// + hdrOrg + " for zxid " + zxidd);
		// return;
		// }

	}

	/*
	 * Deliver the proposal locally and if the current server is the receiver of
	 * the request, replay to the client.
	 */
	private void deliver(long dZxid, ZabHeader zhdr) {
		ZabHeader hdrOrginal = queuedProposalMessage.remove(dZxid);
		// if (hdrOrginal == null) {
		// if (log.isInfoEnabled())
		// log.info("$$$$$$$$$$$$$$$$$$$$$ Header is null (deliver)"
		// + hdrOrginal + " for zxid " + dZxid);
		// return;
		// }

		queuedCommitMessage.put(dZxid, hdrOrginal);
		if (!stats.isWarmup()) {
			stats.incnumReqDelivered();
			stats.setEndThroughputTime(System.currentTimeMillis());
			//long deliveryStartTime = stats.getDeliveryProcessTime(zhdr.getMessageId());
			//stats.removeDeliveryProcessTime(zhdr.getMessageId());
			//stats.addDeliveryPT((int) (System.nanoTime() - deliveryStartTime));	
		}

		//if (log.isInfoEnabled())
			//log.info("queuedCommitMessage size = " + queuedCommitMessage.size()
					//+ " zxid " + dZxid);
		if (requestQueue.contains(hdrOrginal.getMessageId())) {
			if (!stats.isWarmup()) {
				long startTime = hdrOrginal.getMessageId().getStartTime();
				stats.addLatency((int) (System.nanoTime() - startTime));
			}
			ZabHeader hdrResponse = new ZabHeader(ZabHeader.RESPONSE, dZxid,
					hdrOrginal.getMessageId());
			Message msgResponse = new Message(hdrOrginal.getMessageId()
					.getOriginator()).putHeader(this.id, hdrResponse);
			down_prot.down(new Event(Event.MSG, msgResponse));

		}

	}

	/*
	 * Send replay to client
	 */
	private void handleOrderingResponse(ZabHeader hdrResponse) {
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

	private synchronized void addTotalABMssages(ZabHeader carryCountMessageLeader) {
		long followerMsg = carryCountMessageLeader.getZxid();
		stats.addCountTotalMessagesFollowers((int) followerMsg);
		numABRecieved++;
		if(numABRecieved==zabMembers.size()-1){
			ZabHeader headertStats = new ZabHeader(ZabHeader.STATS);
			for (Address zabServer:zabMembers){
				Message messageStats = new Message(zabServer).putHeader(this.id,
					headertStats);
				down_prot.down(new Event(Event.MSG, messageStats));
			}
		}
	}
	
	private void sendMyTotalBroadcast(){
		ZabHeader followerMsgCount = new ZabHeader(
				ZabHeader.COUNTMESSAGE, countMessageFollower);
		Message requestMessage = new Message(leader).putHeader(this.id,
				followerMsgCount);
		down_prot.down(new Event(Event.MSG, requestMessage));
	}

	/*
	 * ----------------------------- End of Private Methods
	 * --------------------------------
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
				if (!stats.isWarmup()) {
					stats.incNumRequest();
				}
				ZabHeader hdrProposal = new ZabHeader(ZabHeader.PROPOSAL,
						new_zxid, hdrReq.getMessageId());
				Message ProposalMessage = new Message().putHeader(this.id,
						hdrProposal);

				ProposalMessage.setSrc(local_addr);
				Proposal p = new Proposal();
				p.setMessageId(hdrReq.getMessageId());
				p.AckCount++;
				outstandingProposals.put(new_zxid, p);
				queuedProposalMessage.put(new_zxid, hdrProposal);
				if (!stats.isWarmup()) {
					if(hdrReq.getType()==ZabHeader.REQUEST){
						Long stPPTL = stats.getTempPPTL(hdrReq.getMessageId());
						stats.removeTempPPTL(hdrReq.getMessageId());
						stats.addPPTL((int) (System.nanoTime() - stPPTL));
					} else {
						Long stPPTFF = stats.getTempPPTForward(hdrReq
								.getMessageId());
							stats.removeTempPPTForward(hdrReq
									.getMessageId());
							stats.addPPTFF((int) (System
									.nanoTime() - stPPTFF));
						}
				}

				try {

					for (Address address : zabMembers) {
						if (address.equals(leader))
							continue;
						if (!stats.isWarmup()) {
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

	final class MessageHandler implements Runnable {
        @Override
        public void run() {
        	deliverMessages();

        }

        private void deliverMessages() {
			ZabHeader hdrDelivery= null;
            while (true) {
    				try {
    					hdrDelivery = delivery.take();
    					//log.info("(deliverMessages) deliver zxid = "+ hdrDelivery.getZxid());
    				} catch (InterruptedException e) {
    					// TODO Auto-generated catch block
    					e.printStackTrace();
    				}          
					//log.info("(going to call deliver zxid =  "+ hdrDelivery.getZxid());
                    deliver(hdrDelivery.getZxid(), hdrDelivery);
                        
            }
        }
  
    } 
}