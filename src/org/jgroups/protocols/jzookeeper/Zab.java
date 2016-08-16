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
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.protocols.jzookeeper.Zab2Phases.FollowerMessageHandler;
import org.jgroups.protocols.jzookeeper.Zab2Phases.MessageHandler;
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
	private final static String outDir = "/work/Zab/";
 	private final static String outDirWork = "/work/Zab/";
	private AtomicLong countMessageLeader = new AtomicLong(0);
	private long countMessageFollower = 0;
	private List<Address> clients = Collections
			.synchronizedList(new ArrayList<Address>());
	private ProtocolStats stats = new ProtocolStats();
	private Timer timer = new Timer();
	
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
		timer.schedule(new Throughput(), 1000, 1000);
		this.stats = new ProtocolStats(ProtocolName, clients.size(),
				numberOfSenderInEachClient, outDir, false);
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
//				if (!stats.isWarmup()) {
//					stats.addLatencyProposalST(hdr.getMessageId(),
//							System.nanoTime());
//				}
				queuedMessages.add(hdr);
				break;
			case ZabHeader.PROPOSAL:
				if (!is_leader) {
					if (!stats.isWarmup() && !startThroughput) {
						startThroughput = true;
						stats.setStartThroughputTime(System.currentTimeMillis());
						stats.setLastNumReqDeliveredBefore(0);
						stats.setLastThroughputTime(System.currentTimeMillis());
					}
					sendACK(msg, hdr);
				}
				break;
			case ZabHeader.ACK:
				if (is_leader) {
					processACK(msg, msg.getSrc());
				}
				break;
			case ZabHeader.COMMIT:
				delivery.add(hdr);
				//deliver(hdr.getZxid());
				break;
			case ZabHeader.CLIENTFINISHED:
				clientFinished++;
				if(clientFinished==clients.size() && !is_leader){
					running = false;	
					timer.cancel();
					sendMyTotalBroadcast();			
				}
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
	private synchronized void handleClientRequest(Message message) {
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
				log.info("(going to executor2, handleViewChange  ");
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
			//log.info("requestQueue size ="+requestQueue.size());

		}

		if (is_leader) {
			if (!stats.isWarmup()) {
				long stp = System.nanoTime();
				//log.info("stl="+stp+" For req id="+hdrReq.getMessageId());
				//log.info("requestQueue size ="+requestQueue.size());


				//hdrReq.getMessageId().setStartLToFP(stp);
				hdrReq.getMessageId().setStartTime(stp);
				//stats.addLatencyProposalForwardST(hdrReq.getMessageId(),
						//System.nanoTime());
			}
			queuedMessages.add(hdrReq);
		} else {
			if (!stats.isWarmup()) {
				long stf = System.nanoTime();
				//log.info("stl="+stf+" For req id="+hdrReq.getMessageId());
			//	hdrReq.getMessageId().setStartFToLFOneWay(stf);
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
//			if (requestQueue.contains(hdrAck.getMessageId())) {
//				long stf = hdrAck.getMessageId().getStartFToLFOneWay();
//				stats.addLatencyFToLF((int) (System.nanoTime() - stf));
				// log.info("Latency for forward fro zxid=" + dZxid +
				// " start time=" + stf + " End Time=" + etf +
				// " latency = "+((etf - stf)/1000000));
			//}
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
				//if (!stats.isWarmup() && requestQueue.contains(hdr.getMessageId())) {
					//long stf = hdr.getMessageId().getStartLToFP();
					// log.info("Latency for Prposal for zxid=" + ackZxid +
					// " start time=" + stf + " End Time=" + etf +
					// " latency = "+((etf - stf)/1000000));
					//stats.addLatencyLToFP((int) (System.nanoTime() - stf));
				//}
				commit(ackZxid);
//			} else
//				System.out.println(">>> Can't commit >>>>>>>>>");
		}

	}

	/*
	 * This method is invoked by leader. It sends COMMIT message to all follower
	 * and itself.
	 */
	private void commit(long zxidd) {
		// ZABHeader hdrOrg = queuedProposalMessage.get(zxidd);
    if (zxidd != lastZxidCommitted + 1) {
			if (log.isDebugEnabled()){
				 log.debug("delivering Zxid out of order "+zxidd + " should be "
						 + lastZxidCommitted + 1);
			}
		}
		synchronized (this) {
			lastZxidCommitted = zxidd;
		}
		ZabHeader hdrCommit = new ZabHeader(ZabHeader.COMMIT, zxidd);
		Message commitMessage = new Message().putHeader(this.id, hdrCommit);
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
	private void deliver(long dZxid) {
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
		}

		//if (log.isInfoEnabled())
		//log.info("queuedCommitMessage size = " + queuedCommitMessage.size()
				//	+ " zxid " + dZxid);
		if(hdrOrginal==null)
			log.info("****hdrOrginal is null ****");
		if (requestQueue.contains(hdrOrginal.getMessageId())) {
			if (!stats.isWarmup()) {
				long startTime = hdrOrginal.getMessageId().getStartTime();
				long endTime = System.nanoTime();
//				log.info("deliver st="+startTime+" For req id="+hdrOrginal.getMessageId());
//				log.info("deliver et="+endTime+" For req id="+hdrOrginal.getMessageId());
//				log.info("deliver latency="+( (endTime-startTime))+" For req id="+hdrOrginal.getMessageId());
//				log.info("deliver latency="+( (int) (endTime-startTime))+" For req id="+hdrOrginal.getMessageId());
//				latency= endTime - startTime;
//				if(latency<0){
//					log.info("deliver st="+startTime+" For req id="+hdrOrginal.getMessageId());
//					log.info("deliver et="+endTime+" For req id="+hdrOrginal.getMessageId());
//				}
				stats.addLatency((endTime - startTime));
				//stats.addKVLatencies(((System.currentTimeMillis()-stats.getStartThroughputTime())), (endTime - startTime));
				
			}
			ZabHeader hdrResponse = new ZabHeader(ZabHeader.RESPONSE, dZxid,
					hdrOrginal.getMessageId());
			Message msgResponse = new Message(hdrOrginal.getMessageId()
					.getOriginator()).putHeader(this.id, hdrResponse);
			down_prot.down(new Event(Event.MSG, msgResponse));
			requestQueue.remove(hdrOrginal.getMessageId());

		}

	}

	/*
	 * Send replay to client
	 */
	private void handleOrderingResponse(ZabHeader hdrResponse) {
		Message message = messageStore.get(hdrResponse.getMessageId());
		message.putHeader(this.id, hdrResponse);
		up_prot.up(new Event(Event.MSG, message));
		messageStore.remove(hdrResponse.getMessageId());

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
//				if (!stats.isWarmup()) {
//					Long st = stats.getLatencyProposalST(hdrReq.getMessageId());
//					if (st != null) {
//						stats.removeLatencyProposalST(hdrReq.getMessageId());
//						stats.addLatencyProp((int) (System.nanoTime() - st));
//					} else {
//						Long stL = stats.getLatencyProposalForwardST(hdrReq
//								.getMessageId());
//						if (stL != null) {
//							stats.removeLatencyProposalForwardST(hdrReq
//									.getMessageId());
//							stats.addLatencyPropForward((int) (System
//									.nanoTime() - stL));
//						}
//					}
//				}

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
        	//if(is_leader)
			log.info("call deliverMessages()");
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
                    deliver(hdrDelivery.getZxid());
                        
            }
        }
  
    } 
	
	class Throughput extends TimerTask {

		public Throughput() {

		}

		private long startTime = 0;
		private long currentTime = 0;
		private double currentThroughput = 0;
		private int finishedThroughput = 0;

		@Override
		public void run() {
			//if (running){
				startTime = stats.getLastThroughputTime();
				currentTime = System.currentTimeMillis();
				finishedThroughput=stats.getnumReqDelivered();
				//log.info("Start Time="+startTime);
				//log.info("currentTime Time="+currentTime);
				//elpasedThroughputTimeInSecond = (int) (TimeUnit.MILLISECONDS
						//.toSeconds(currentThroughput - startTime));
				//log.info("elpasedThroughputTimeInSecond Time="+elpasedThroughputTimeInSecond);
				//log.info("finishedThroughput="+finishedThroughput);
				//log.info("stats.getLastNumReqDeliveredBefore()="+stats
						//.getLastNumReqDeliveredBefore());
				currentThroughput = (((double)finishedThroughput - stats
						.getLastNumReqDeliveredBefore()) / ((double)(currentTime - startTime)/1000.0));
				stats.setLastNumReqDeliveredBefore(finishedThroughput);
				stats.setLastThroughputTime(currentTime);
				stats.addThroughput(convertLongToTimeFormat(currentTime),
					currentThroughput);
			//}
		}

		public String convertLongToTimeFormat(long time) {
			Date date = new Date(time);
			SimpleDateFormat longToTime = new SimpleDateFormat("HH:mm:ss.SSSZ");
			return longToTime.format(date);
		}
	}

}