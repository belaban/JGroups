package org.jgroups.protocols.jzookeeper;
	import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
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
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;

	
	/* 
	 * Zab_3 (main approach ) is the same implementation as Zab_2.
	 * Note that all the code and implementation are simaller to Zab_2, just we change probability 
	 * parameter in ZUtil class from 1.0 10 0.5.
	 * Also it has features of testing throughput, latency (in Nano), ant etc. 
	 * When using testing, it provides warm up test before starting real test.
	 */
	public class ZabCoinTossing extends Protocol {
		private final static String ProtocolName = "ZabCoinTossing";
		private final static int numberOfSenderInEachClient = 20;
		protected final AtomicLong        zxid=new AtomicLong(0);
	    private ExecutorService executor;
	    protected Address                           local_addr;
	    protected volatile Address                  leader;
	    protected volatile View                     view;
	    protected volatile boolean                  is_leader=false;
	    private List<Address> zabMembers = Collections.synchronizedList(new ArrayList<Address>());
		private long lastZxidProposed=0, lastZxidCommitted=0;
	    private final Set<MessageId> requestQueue =Collections.synchronizedSet(new HashSet<MessageId>());
		private Map<Long, ZabCoinTossingHeader> queuedCommitMessage = new HashMap<Long, ZabCoinTossingHeader>();
	    private final LinkedBlockingQueue<ZabCoinTossingHeader> queuedMessages =
		        new LinkedBlockingQueue<ZabCoinTossingHeader>();
		private ConcurrentMap<Long, Proposal> outstandingProposals = new ConcurrentHashMap<Long, Proposal>();
	    private final Map<Long, ZabCoinTossingHeader> queuedProposalMessage = Collections.synchronizedMap(new HashMap<Long, ZabCoinTossingHeader>());
	    private final Map<MessageId, Message> messageStore = Collections.synchronizedMap(new HashMap<MessageId, Message>());
		Calendar cal = Calendar.getInstance();
	    protected volatile boolean                  running=true;
	    private int index=-1;
	    //private Map<Long, Boolean> notACK = new HashMap<Long, Boolean>();
	    SortedSet<Long> wantCommit = new TreeSet<Long>();
		private long currentCpuTime = 0, rateCountTime = 0, lastTimer = 0, lastCpuTime = 0;
	    private Timer _timer;
	    private boolean startSending = false;
	    private long lastRequestRecieved=0;
	    private long laslAckRecieved=0;
	    private boolean recievedFirstRequest = false;
	    private long current = 0;
	    private long startThroughputTime = 0;
	    private long endThroughputTime = 0;
	    private boolean startThroughput = false;
	    private final static String outDir="/home/pg/p13/a6915654/ZabCoinTossing/";
	    private int lastArrayIndex = 0, lastArrayIndexUsingTime = 0 ;
		private long timeInterval = 500;
		private int lastFinished = 0;
		private AtomicLong countMessageLeader = new AtomicLong(0);
		private long countMessageFollower = 0;
	    private int largeLatCount = 0;
	    private List<String> largeLatencies = new ArrayList<String>();
	    private  AtomicInteger                  numReqDeviverd=new AtomicInteger(0);
	    private boolean is_warmUp=true;
		private int longWait = Integer.MIN_VALUE;
		private volatile boolean makeAllFollowersAck=false;
		private List<Address>  clients = Collections.synchronizedList(new ArrayList<Address>());
		private ProtocolStats stats;
		public ZabCoinTossing(){
	    	
	    }
	    
	    @ManagedAttribute
	    public boolean isleaderinator() {return is_leader;}
	    public Address getleaderinator() {return leader;}
	    public Address getLocalAddress() {return local_addr;}
	    
	    @Override
	    public void start() throws Exception {
	        super.start();
		    log.setLevel("trace");
	        //if (zabMembers.contains(local_addr)){
		        running=true;        
			    executor = Executors.newSingleThreadExecutor();
			    executor.execute(new FollowerMessageHandler(this.id));	   
	    }
	    
	    
	    public void reset(Address client) {
	    	zxid.set(0);     	lastZxidProposed=0;
	    	lastZxidCommitted=0;         requestQueue.clear();
	    	queuedCommitMessage.clear();queuedProposalMessage.clear();        
	        queuedMessages.clear(); outstandingProposals.clear();       
	        messageStore.clear();startSending=false;        
	        wantCommit.clear();lastRequestRecieved=0;        
	        laslAckRecieved=0;recievedFirstRequest = false;        
	        numReqDeviverd= new AtomicInteger(0);       
	        startThroughputTime = 0;       
	        endThroughputTime = 0;       
	    	largeLatCount = 0;largeLatencies.clear();   	
	    	lastArrayIndex = 0;lastArrayIndexUsingTime = 0 ;  	
	        is_warmUp=false;//_timer.cancel();        
	        countMessageLeader = new AtomicLong(0);        
	        countMessageFollower = 0;      
	    	currentCpuTime = 0; rateCountTime = 0;
	    	lastTimer = 0; lastCpuTime = 0;
	    	longWait = Integer.MIN_VALUE;
	    	currentCpuTime=0;
	    	this.stats = new ProtocolStats(ProtocolName, clients.size(),
					numberOfSenderInEachClient, outDir);
			log.info("Reset done");
	    	MessageId messageId = new MessageId(local_addr,
					-10, System.currentTimeMillis());
	    	if (is_leader){
	    		for(Address c: clients){
			    	ZabCoinTossingHeader startTest = new ZabCoinTossingHeader(ZabCoinTossingHeader.STARTREALTEST, messageId);
			        Message confirmClient=new Message(c).putHeader(this.id, startTest);
					down_prot.down(new Event(Event.MSG, confirmClient));  
	    		}
	    	}
		    
	    }  
		   
	    // For sending Dummy request
	    
	    class FinishTask extends TimerTask {
	    	private short idd;
	    	public FinishTask(short id){
	    		this.idd = id;
	    	}
			@Override
			public void run() {
				//this can be used to measure rate of each thread
				//at this moment, it is not necessary
				//log.info("cccccccall FinishTask diff time "+(currentTime - timeDiff));
//				if (outstandingProposals.isEmpty()){
//					this.cancel();
//					return;
//				}
//				if ((currentTime - lastRequestRecieved) > 1000){
//					this.cancel();
//					return;
//				}
				
				 current = System.currentTimeMillis();

	        	if ((current -laslAckRecieved) > 50
	                     && (current - lastRequestRecieved) > 50
	                     && !outstandingProposals.isEmpty()){
	        		//if (log.isInfoEnabled()){
				//if (!outstandingProposals.isEmpty() && (currentTime - lastRequestRecieved) >500) {
	        		this.cancel();
	        		if (log.isInfoEnabled())
	        			log.info("Comit Alllllllllllllllllllllllllllllllllll");
	    			ZabCoinTossingHeader commitPending = new ZabCoinTossingHeader(ZabCoinTossingHeader.COMMITOUTSTANDINGREQUESTS);
					for (Address address : zabMembers) {
	                    Message commitALL = new Message(address).putHeader(this.idd, commitPending);
	            		down_prot.down(new Event(Event.MSG, commitALL));     
	                }
					//makeAllFollowersAck=true;
					
					startSending=false;
				}
				
			}
		}
	    @Override
	    public void stop() {
	        running=false;
	        executor.shutdown();
	        super.stop();
	    }

	    public Object down(Event evt) {
	        ZabCoinTossingHeader hdr;

	        switch(evt.getType()) {
	            case Event.MSG:
	                Message msg=(Message)evt.getArg();
	               // hdr=(ZabCoinTossingHeader)msg.getHeader(this.id);
	                handleClientRequest(msg);               
	                return null; // don't pass down
	            case Event.SET_LOCAL_ADDRESS:
	                local_addr=(Address)evt.getArg();
	                break;
	        }
	        return down_prot.down(evt);
	    }

	    public Object up(Event evt) {
	        Message msg = null;
	        ZabCoinTossingHeader hdr;

	        switch(evt.getType()) {
	            case Event.MSG:            	
	                msg=(Message)evt.getArg();
	                hdr=(ZabCoinTossingHeader)msg.getHeader(this.id);
	                if(hdr == null){
	                    break; // pass up
	                }
	                switch(hdr.getType()) {                
	                    case ZabCoinTossingHeader.START_SENDING:
	            			return up_prot.up(new Event(Event.MSG, msg));
	                    case ZabCoinTossingHeader.RESET:
	  	                    reset(msg.getSrc());
		                	break;
	                	case ZabCoinTossingHeader.REQUEST:
	                		if (!is_warmUp && !is_leader && !startThroughput){
	                			startThroughput = true;
	                			stats.setStartThroughputTime(System.currentTimeMillis());
	                		}
	                		forwardToLeader(msg);
	                		break;
	                    case ZabCoinTossingHeader.FORWARD:
	                    	lastRequestRecieved=System.currentTimeMillis();
	                    	recievedFirstRequest = true;
	                    	//log.info("Start--------------------------------------------------- _timer");
	                    	if (!is_warmUp && !startSending){
		                    	_timer = new Timer();
		        				_timer.scheduleAtFixedRate(new FinishTask(this.id), 200, 200);
		                    	startSending=true;
	                    	}
	                    	//lastRequestRecieved = System.currentTimeMillis();
	                		queuedMessages.add(hdr);
	                		break;
	                    case ZabCoinTossingHeader.PROPOSAL:
		                   	if (!is_leader){
		                   		//hdr.getMessageId().setStartTime(System.currentTimeMillis());
		            			sendACK(msg, hdr);
		            		}
		                   	break;           		
	                    case ZabCoinTossingHeader.ACK:
	                			processACK(msg, msg.getSrc());
	                		break;
	                    case ZabCoinTossingHeader.COMMITOUTSTANDINGREQUESTS:
	                    	//makeAllFollowersAck=true;
	            			commitPendingRequest();
	            			//startSending = false;
	            		break;
	                    case ZabCoinTossingHeader.STATS:
	        				stats.printProtocolStats(is_leader);
	                    	break;
	                    case ZabCoinTossingHeader.COUNTMESSAGE:
	                		sendTotalABMessages(hdr);  
	                		log.info("Yes, I recieved count request");
	                	break;
	                    case ZabCoinTossingHeader.SENDMYADDRESS:
	                    	if (!zabMembers.contains(msg.getSrc())) {
	        					clients.add(msg.getSrc());
	        					System.out.println("Rceived clients address "
	        							+ msg.getSrc());
	        				}
	        				break;
	                    case ZabCoinTossingHeader.STARTREALTEST:
	                    	if(!zabMembers.contains(local_addr))
	                			return up_prot.up(new Event(Event.MSG, msg));
	                    case ZabCoinTossingHeader.RESPONSE:
	                    	handleOrderingResponse(hdr);
	                    	
	                    	
	            }                
	                return null;
	                
	              case Event.VIEW_CHANGE:
	              handleViewChange((View)evt.getArg());
	              break;

	        }

	        return up_prot.up(evt);
	    }

	    public void up(MessageBatch batch) {
	        for(Message msg: batch) {
	            if(msg.isFlagSet(Message.Flag.NO_TOTAL_ORDER) || msg.isFlagSet(Message.Flag.OOB) || msg.getHeader(id) == null)
	                continue;
	            batch.remove(msg);

	            try {
	                up(new Event(Event.MSG, msg));
	            }
	            catch(Throwable t) {
	                log.error("failed passing up message", t);
	            }
	        }

	        if(!batch.isEmpty())
	            up_prot.up(batch);
	    }

	    /* --------------------------------- Private Methods ----------------------------------- */

	    
	    private void handleClientRequest(Message message){
	    	ZabCoinTossingHeader clientHeader = ((ZabCoinTossingHeader) message.getHeader(this.id));
	    	if (clientHeader!=null && clientHeader.getType() == ZabCoinTossingHeader.START_SENDING){

	    		for (Address client : view.getMembers()){
	 			   if (log.isInfoEnabled())
	 				   log.info("Address to check " + client);
	        	  // if (!zabMembers.contains(client)){
					   if (log.isInfoEnabled())
		        	 	    log.info("Address to check is not zab Members, will send start request to" + client+ " "+ getCurrentTimeStamp());
		        		message.setDest(client);
		        	    down_prot.down(new Event(Event.MSG, message));    	
	        		//}
	        		
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
	    	
	    	else if (clientHeader!=null && clientHeader.getType() == ZabCoinTossingHeader.STATS){

	    		for (Address server : zabMembers){
	        			//message.setDest(server);
	        	        Message statsMessage = new Message(server).putHeader(this.id, clientHeader);
	        	        down_prot.down(new Event(Event.MSG, statsMessage));    	
	        	}
	    	}
	    	

			else if (clientHeader != null
					&& clientHeader.getType() == ZabCoinTossingHeader.COUNTMESSAGE) {
				for (Address server : zabMembers) {
					if(!server.equals(zabMembers.get(0))){
						Message countMessages = new Message(server).putHeader(this.id,
								clientHeader);
						down_prot.down(new Event(Event.MSG, countMessages));
					}
				}
			}
	    	
	    	else if(!clientHeader.getMessageId().equals(null) && clientHeader.getType() == ZabCoinTossingHeader.REQUEST){
		    	 Address destination = null;
		         messageStore.put(clientHeader.getMessageId(), message);
		        ZabCoinTossingHeader hdrReq=new ZabCoinTossingHeader(ZabCoinTossingHeader.REQUEST, clientHeader.getMessageId());  
		        ++index;
		        if (index>2)
		        	index=0;
		        destination = zabMembers.get(index);
		        Message requestMessage = new Message(destination).putHeader(this.id, hdrReq);
		       down_prot.down(new Event(Event.MSG, requestMessage));    
	    	}
	    	
	    	else if (!clientHeader.getMessageId().equals(null)
					&& clientHeader.getType() == ZabCoinTossingHeader.SENDMYADDRESS) {
				Address destination = null;
				destination = zabMembers.get(0);
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
	        List<Address> mbrs=v.getMembers();
	        leader=mbrs.get(0);
	        if (leader.equals(local_addr)){
	        	is_leader = true;
	        }
	        if (mbrs.size() == 3){
	        	zabMembers.addAll(v.getMembers());        	
	        	
	        }
	        if (mbrs.size() > 3 && zabMembers.isEmpty()){
	        	for (int i = 0; i < 3; i++) {
	            	zabMembers.add(mbrs.get(i));
				}

	        }
	        if(mbrs.isEmpty()) return;

	        if(view == null || view.compareTo(v) < 0)
	            view=v;
	        else
	            return; 
	    }

	    private long getNewZxid(){
	    	return zxid.incrementAndGet();
	    }

	    private void forwardToLeader(Message msg) {
		   ZabCoinTossingHeader hdrReq = (ZabCoinTossingHeader) msg.getHeader(this.id);
		   requestQueue.add(hdrReq.getMessageId());
		   if (!is_warmUp && is_leader && !startThroughput){
				startThroughput = true;
				stats.setStartThroughputTime(System.currentTimeMillis());
			}
		   if (is_leader){
			   hdrReq.getMessageId().setStartTime(System.nanoTime());
			   queuedMessages.add((ZabCoinTossingHeader)msg.getHeader(this.id));
	       }	   
		   else{
			   hdrReq.getMessageId().setStartTime(System.nanoTime());
			   forward(msg);
		   }
	            
	           
	   }

	    private void forward(Message msg) {
	        Address target=leader;
	 	    ZabCoinTossingHeader hdrReq = (ZabCoinTossingHeader) msg.getHeader(this.id);
	        if(target == null)
	            return;
		    try {
		        ZabCoinTossingHeader hdr=new ZabCoinTossingHeader(ZabCoinTossingHeader.FORWARD, hdrReq.getMessageId());
		        Message forward_msg=new Message(target).putHeader(this.id,hdr);
		        down_prot.down(new Event(Event.MSG, forward_msg));
		     }
		    catch(Exception ex) {
		      log.error("failed forwarding message to " + msg, ex);
		    }
	      
	    }
	    

	    private void sendACK(Message msg, ZabCoinTossingHeader hrdAck){
	    	if(!is_warmUp){
				stats.incNumRequest();
			}
	    	Proposal p;
	    	if (msg == null )
	    		return;
	    	
	    	//ZabCoinTossingHeader hdr = (ZabCoinTossingHeader) msg.getHeader(this.id);
	    	
	    	if (hrdAck == null)
	    		return;
	    	
//	    	if (hdr.getZxid() != lastZxidProposed + 1){
//	            log.info("Got zxid 0x"
//	                    + Long.toHexString(hdr.getZxid())
//	                    + " expected 0x"
//	                    + Long.toHexString(lastZxidProposed + 1));
//	        }
	      	

			//log.info("[" + local_addr + "] " + "follower, sending ack (sendAck) at "+getCurrentTimeStamp());
//			if (!(outstandingProposals.containsKey(hdr.getZxid()))){
				p = new Proposal();
				p.AckCount++; // Ack from leader
				p.setZxid(hrdAck.getZxid());
				outstandingProposals.put(hrdAck.getZxid(), p);
				lastZxidProposed = hrdAck.getZxid();
				queuedProposalMessage.put(hrdAck.getZxid(), hrdAck);
//			}
//			else{
//				p = outstandingProposals.get(hdr.getZxid());
//				p.AckCount++; // Ack from leader
//				queuedProposalMessage.put(hdr.getZxid(), hdr);
//				lastZxidProposed = hdr.getZxid();

			//}
			if (is_warmUp){
				ZabCoinTossingHeader hdrACK = new ZabCoinTossingHeader(ZabCoinTossingHeader.ACK, hrdAck.getZxid());
				Message ackMessage = new Message().putHeader(this.id, hdrACK);
				//log.info("Sending ACK for " + hdr.getZxid()+" "+getCurrentTimeStamp()+ " " +getCurrentTimeStamp());
				//notACK.put(hdr.getZxid(), true);

				try{
				for (Address address : zabMembers) {
	                Message cpy = ackMessage.copy();
	                cpy.setDest(address);
	        		down_prot.down(new Event(Event.MSG, cpy));     
	            }
	         }catch(Exception ex) {
	    		log.error("failed proposing message to members");
	    	}    
			}
			
			else if (ZUtil.SendAckOrNoSend()|| makeAllFollowersAck) {

//				log.info("["
//						+ local_addr
//						+ "] "
//						+ "follower, sending ack if (ZUtil.SendAckOrNoSend()) (sendAck) at "+getCurrentTimeStamp());
				//if(makeAllFollowersAck)
					//log.info("*********** makeAllFollowersAck Probability not working "+(++countACKNoProb));
				ZabCoinTossingHeader hdrACK = new ZabCoinTossingHeader(ZabCoinTossingHeader.ACK, hrdAck.getZxid());
				Message ackMessage = new Message().putHeader(this.id, hdrACK);
				//log.info("Sending ACK for " + hdr.getZxid()+" "+getCurrentTimeStamp()+ " " +getCurrentTimeStamp());
				//notACK.put(hdr.getZxid(), true);

				try{
				for (Address address : zabMembers) {
					if (!is_warmUp && !address.equals(local_addr)) {
						countMessageFollower++;
						stats.incCountMessageFollower();
					}
	                Message cpy = ackMessage.copy();
	                cpy.setDest(address);
	        		down_prot.down(new Event(Event.MSG, cpy));     
	            }
	         }catch(Exception ex) {
	    		log.error("failed proposing message to members");
	    	}    
			}
			//else{
				//log.info("Not Sending ACK for " + hdr.getZxid()+" "+getCurrentTimeStamp()+ " " +getCurrentTimeStamp());
				//notACK.put(hdr.getZxid(), false);
			//}
		
	    	}
	    
	    
	    private synchronized void processACK(Message msgACK, Address sender){
	    	laslAckRecieved = System.currentTimeMillis();
		    Proposal p = null;
	    	ZabCoinTossingHeader hdr = (ZabCoinTossingHeader) msgACK.getHeader(this.id);	
	    	long ackZxid = hdr.getZxid();
	    	//log.info("Reciving Ack zxid " + ackZxid + " sender " + sender+ " " +getCurrentTimeStamp());

//	    	if (!(outstandingProposals.containsKey(hdr.getZxid())) && (lastZxidProposed < hdr.getZxid())){
//				p = new Proposal();
//		        outstandingProposals.put(hdr.getZxid(), p); 
//				queuedProposalMessage.put(hdr.getZxid(), hdr);
//		        lastZxidProposed = hdr.getZxid();
//		}
		
	 	    //log.info("recieved ack "+ackZxid+" "+ sender + " "+getCurrentTimeStamp());

			if (lastZxidCommitted >= ackZxid) {
	            //if (log.isDebugEnabled()) {
	                //log.info("proposal has already been committed, pzxid: 0x{} zxid: 0x{}",
	                        //lastZxidCommitted, ackZxid);
	            //}
	            return;
	        }
	        p = outstandingProposals.get(ackZxid);
	        if (p == null) {
//	            log.info("*********************Trying to commit future proposal: zxid 0x{} from {}",
//	                    Long.toHexString(ackZxid), sender);
	            return;
	        }
			
			p.AckCount++;
			//if (log.isDebugEnabled()) {
//	            log.debug("Count for zxid: " +
//	                    Long.toHexString(ackZxid)+" = "+ p.getAckCount());
	        //}
			
			if (isQuorum(p.getAckCount())) {
				if (ackZxid == lastZxidCommitted+1){
					commit(ackZxid);
					outstandingProposals.remove(ackZxid);				
				//}
				//if (isFirstZxid(ackZxid)) {
					//log.info(" if (isQuorum(p.getAckCount())) commiting " + ackZxid);
					//commit(ackZxid);
					//outstandingProposals.remove(ackZxid);
				} else {
					long zxidCommiting = lastZxidCommitted +1;
					if(longWait<(ackZxid-zxidCommiting))
						longWait =(int)(ackZxid-zxidCommiting);
						
					//log.info("committing !!!!!!!!!!!!!!!!!!!!!!!!! from "+ (lastZxidCommitted +1)
							//+ " to "+ ackZxid);
					for (long z = zxidCommiting; z < ackZxid+1; z++){
						commit(z);
						outstandingProposals.remove(z);
					}
				}
					//for (Proposal proposalPending : outstandingProposals.values()) {
						//if (proposalPending.getZxid() < p.getZxid()) {
							//log.info(" inside proposalPending.getZxid() < p.getZxid() "
									//+ proposalPending.getZxid() + " " + p.getZxid());
							//wantCommit.add(proposalPending.getZxid());
							//log.info(" wantCommit size " + wantCommit.size());
						//}
					//}
					//wantCommit.add(ackZxid);
					
//					log.info(" processAck Commiting allwantCommit) commiting " + wantCommit + " before "+ackZxid);
//					for (long zx : wantCommit) {
//						if (isFirstZxid(zx)) {
//							commit(zx);
//							//log.info(" for (long zx : wantCommit) commiting " + zx);
//							outstandingProposals.remove(zx);
//						} else
//							break;
//					}
//					wantCommit.clear();
//				}
			}
			
			// }

		}
	    
	    private void commitPendingRequest(){
	    	
	    	if (!outstandingProposals.isEmpty()){
		    	for (Proposal proposalPending : outstandingProposals.values()){
		    		wantCommit.add(proposalPending.getZxid());
		    	}
				//log.info("Before Finished outstandingProposals "+outstandingProposals.keySet());
				//log.info("Before Finished wantCommit "+wantCommit);
		
			
		    	//log.info("Commiting all  "+wantCommit);
		    	for (long zx:wantCommit){
					//log.info("Commiting "+outstandingProposals.keySet());
						commit(zx);
						outstandingProposals.remove(zx);
					}
				
				
//				log.info("After Finished outstandingProposals "+outstandingProposals.keySet());
//				log.info("After Finished wantCommit "+wantCommit);
				wantCommit.clear();
	    	}
		
	    	
	    }
			
	    private void commit(long zxidd){
				
		       	//log.info("[" + local_addr + "] "+"About to commit the request (commit) for zxid="+zxid+" "+getCurrentTimeStamp());

			    ZabCoinTossingHeader hdrOrginal = null;
		    	   synchronized(this){
		    	       lastZxidCommitted = zxidd;
		    	   }
		    	   
			   hdrOrginal = queuedProposalMessage.get(zxidd);
			   if (hdrOrginal == null){
				   //if (log.isInfoEnabled())
					   //log.info("!!!!!!!!!!!!!!!!!!!!!!!!!!!! Header is null (commit)"+ hdrOrginal + " for zxid "+zxid);
				   return;
			   }
	   	      //MessageId mid = hdrOrginal.getMessageId();
//			   if (mid == null){
//				   log.info("!!!!!!!!!!!!!!!!!!!!!!!!!!!! Message is null (commit)"+ mid + " for zxid "+zxid);
//				   return;
//			   }
		       //ZabCoinTossingHeader hdrCommit = new ZabCoinTossingHeader(ZabCoinTossingHeader.COMMIT, zxid, mid);
		       //Message commitMessage = new Message().putHeader(this.id, hdrCommit);
		       //commitMessage.src(local_addr);
			   deliver(zxidd);

		    }
	    
			
	    private void deliver(long committedZxid){
		    	//ZabCoinTossingHeader hdr = (ZabCoinTossingHeader) toDeliver.getHeader(this.id);
		    	//long zxid = hdr.getZxid();
		    	//log.info("[" + local_addr + "] "+ " delivering message (deliver) for zxid=" + hdr.getZxid()+" "+getCurrentTimeStamp());

		    	ZabCoinTossingHeader hdrOrginal = queuedProposalMessage.remove(committedZxid);
		    	if (hdrOrginal == null) {
					 if (log.isInfoEnabled())
						 log.info("$$$$$$$$$$$$$$$$$$$$$ Header is null (deliver)"
							+ hdrOrginal + " for zxid " + committedZxid);
					return;
				}
		    	queuedCommitMessage.put(committedZxid, hdrOrginal);
		    	if (!is_warmUp) {
					stats.incnumReqDelivered();
					stats.setEndThroughputTime(System.currentTimeMillis());
				}
					
					if (numReqDeviverd.get() > 9999000)
						makeAllFollowersAck=true;
					
					//long startTime  = hdrOrginal.getMessageId().getStartTime();
					//latencies.add((int)(System.currentTimeMillis() - startTime));
					//rateCount++;
				//if (rateCount == rateInterval){
					//new StatsThread().start();
					//rateCount=0;
				//}		
					//if (numReqDeviverd.get()>=1000000){
						//timer.cancel();
						//_timer.cancel();
					//}
			
		//}			endThroughputTime = System.currentTimeMillis();
				//long startTimes  = hdrOrginal.getMessageId().getStartTime();
				//latencies.add((int)(System.currentTimeMillis() - startTimes));
				   if (log.isInfoEnabled())
						log.info("queuedCommitMessage size = " + queuedCommitMessage.size() + " zxid "+committedZxid);

		    	if (requestQueue.contains(hdrOrginal.getMessageId())){
		    		if (!is_warmUp) {
						long startTime = hdrOrginal.getMessageId().getStartTime();
						stats.addLatency((int) (System.nanoTime() - startTime));
					}
			    	ZabCoinTossingHeader hdrResponse = new ZabCoinTossingHeader(ZabCoinTossingHeader.RESPONSE, committedZxid,  hdrOrginal.getMessageId());
			    	Message msgResponse = new Message(hdrOrginal.getMessageId().getAddress()).putHeader(this.id, hdrResponse);
		       		down_prot.down(new Event(Event.MSG, msgResponse));     

		    	}
		    	
		    	   
		   }
			
			
	    private void handleOrderingResponse(ZabCoinTossingHeader hdrResponse) {
				
		        Message message = messageStore.get(hdrResponse.getMessageId());
		        message.putHeader(this.id, hdrResponse);
		        up_prot.up(new Event(Event.MSG, message));

		 }
		    	

			private boolean isQuorum(int majority){
				//log.info(" acks =  " + majority + " majority "+ ((zabMembers.size()/2)+1));

		    	return majority >= ((zabMembers.size()/2) + 1)? true : false;
		    }
			
			private boolean isFirstZxid(long zxid){
				int i =0;
				boolean find = true;
				for (long z : outstandingProposals.keySet()){
					//log.info("Inside isFirstZxid loop" + z + " i =" + (++i));
					if (z < zxid){
						find = false;
						break;
					}
				}       		
				
				return find;
			}
			
			private void sendTotalABMessages(ZabCoinTossingHeader carryCountMessageLeader){
				if(!is_leader){
				    ZabCoinTossingHeader followerMsgCount = new ZabCoinTossingHeader(ZabCoinTossingHeader.COUNTMESSAGE, countMessageFollower);
			   	    Message requestMessage = new Message(leader).putHeader(this.id, followerMsgCount);
			        down_prot.down(new Event(Event.MSG, requestMessage)); 
				}
				else{
					long followerMsg = carryCountMessageLeader.getZxid();
					stats.addCountTotalMessagesFollowers((int) followerMsg);

				}
					
			}
			
			private String getCurrentTimeStamp(){
				long timestamp = new Date().getTime();  
				cal.setTimeInMillis(timestamp);
				String timeString =
					   new SimpleDateFormat("HH:mm:ss:SSS").format(cal.getTime());

				return timeString;
			}

	    

	/* ----------------------------- End of Private Methods -------------------------------- */
	   

	 final class FollowerMessageHandler implements Runnable {
	    	
	    	private short id;
	    	public FollowerMessageHandler(short id){
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
	        	ZabCoinTossingHeader hdrReq = null;
	        	//long currentTime = 0;
	            while (running) {
	            	//if (queuedMessages.peek()!=null)
	            		//lastTimeQueueChecked = System.currentTimeMillis();
	            	
	            	//currentTime = System.currentTimeMillis();

//	            	if (queuedMessages.peek()==null){
//		    			log.info("lastTimeQueueChecked = " + lastTimeQueueChecked + 
//		    					" diff = " + (currentTime - lastTimeQueueChecked) );
//		    			log.info("laslAckRecieved = " + laslAckRecieved + 
//		    					" diff = " + (currentTime - laslAckRecieved) );
//		    			log.info("lastRequestRecieved = " + lastRequestRecieved + 
//		    					" diff = " + (currentTime - lastRequestRecieved) );
//		    			log.info("outstandingProposals size = " + outstandingProposals.size());
//		    			log.info("queuedMessages size = " + queuedMessages.size());
//	            	}



//	            	if ((currentTime - lastTimeQueueChecked) > 20 
//	            		   	 && (currentTime -laslAckRecieved) > 20
//	                         && (currentTime - lastRequestRecieved) > 20
//	                         && !outstandingProposals.isEmpty()){
//	            		//if (log.isInfoEnabled()){
//	            			log.info("Dummy is sending ************ " + outstandingProposals.size());
//	            		//}
//	            		ZabCoinTossingHeader commitPending = new ZabCoinTossingHeader(ZabCoinTossingHeader.COMMITOUTSTANDINGREQUESTS);
//	    				for (Address address : zabMembers) {
//	                        Message commitALL = new Message(address).putHeader(this.id, commitPending);
//	                		down_prot.down(new Event(Event.MSG, commitALL));     
//	                    }
//	            	}
	            		
	                	 try {
	                		
	                		 hdrReq=queuedMessages.take();
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
	                 
	            	
	                
	            	long new_zxid = getNewZxid();
	            	if (!is_warmUp) {
						stats.incNumRequest();
					}
	    			 //log.info("Queue Size------> "+ queuedMessages.size());

	            	ZabCoinTossingHeader hdrProposal = new ZabCoinTossingHeader(ZabCoinTossingHeader.PROPOSAL, new_zxid, hdrReq.getMessageId()); 
	            	//hdrProposal.getMessageId().setStartTime(System.currentTimeMillis());
	                Message ProposalMessage=new Message().putHeader(this.id, hdrProposal);

	                ProposalMessage.setSrc(local_addr);
	            	Proposal p = new Proposal();
	            	p.setMessageId(hdrReq.getMessageId());
	            	p.setZxid(new_zxid);
	            	p.AckCount++;
	            	lastZxidProposed=new_zxid;
	            	

	            	//log.info("Zxid count for zxid = " + new_zxid + " count = "  +p.AckCount+" "+getCurrentTimeStamp());
	            	outstandingProposals.put(new_zxid, p);
	            	queuedProposalMessage.put(new_zxid, hdrProposal);
	            	
	            	try{
	            		
	            		//log.info("[" + local_addr + "] "+" prepar for proposal (run) for zxid="+new_zxid+" "+getCurrentTimeStamp());

	                 	//log.info("Leader is about to sent a proposal " + ProposalMessage);
	                 	for (Address address : zabMembers) {
	                        if(address.equals(leader))
	                        	continue; 
	                        if (!is_warmUp) {
								countMessageLeader.incrementAndGet();
								stats.incCountMessageLeader();
							}
		                     Message cpy = ProposalMessage.copy();
		                     cpy.setDest(address);
		                     down_prot.down(new Event(Event.MSG, cpy));     
	                    }
	                 }catch(Exception ex) {
	            		log.error("failed proposing message to members");
	            	}    
	            	
	            
	            }
	            
	        }

	       
	    }

	 public class StatsThread extends Thread{
		 
		 public void run(){
			 int avg = 0, elementCount = 0;
			//List<Integer> copyLat = new ArrayList<Integer>(latencies);
			 //for (int i =  lastArrayIndex; i < copyLat.size(); i++){
				// avg+=copyLat.get(i);
				// elementCount++;
			 //}
			 
			// lastArrayIndex = copyLat.size() - 1;
			 avg= avg/elementCount;
			// avgLatencies.add(avg);
		 }
	 }
	 
	 class ResubmitTimer extends TimerTask{

		@Override
		public void run() {
			int finished = numReqDeviverd.get();
			currentCpuTime = System.currentTimeMillis();
			
			//Find average latency
			 int avg = 0, elementCount = 0;
			 
			 //List<Integer> latCopy = new ArrayList<Integer>(latencies);
			// for (int i =  lastArrayIndexUsingTime; i < latencies.size(); i++){
//				 if (latencies.get(i)>50){
//					 largeLatCount++;
//					 largeLatencies.add((currentCpuTime - startThroughputTime) + "/" + latencies.get(i));
//				 }
//				 avg+=latencies.get(i);
				// elementCount++;
			// }
			 
			 //lastArrayIndexUsingTime = latencies.size()-1;
			 avg= avg/elementCount;
		 
			String mgsLat = (currentCpuTime - startThroughputTime) + "/" +
			                ((finished - lastFinished) + "/" + avg);// (TimeUnit.MILLISECONDS.toSeconds(currentCpuTime - lastCpuTime)));
			//avgLatenciesTimer.add(mgsLat);
			lastFinished = finished;
			lastCpuTime = currentCpuTime;
		}
		 
	 }
	}
