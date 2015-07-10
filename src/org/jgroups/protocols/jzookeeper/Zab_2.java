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
import org.jgroups.protocols.jzookeeper.ZAB.ResubmitTimer;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;


/* 
 * Zab_2 is the same implementation as Zab_3, but the follower will commit locally
 * and send ack to everybody within ensemble. As soon as any replicas (follower or leader)
 * receives a majority of acks, then it commits locally, so that leader doesn’t need to send commit.
 * Also it has features of testing throughput, latency (in Nano), ant etc. 
 * When using testing, it provides warm up test before starting real test.
 * Note that all the code and implementation are samiller to Zab_4, just we change probability 
 * parameter in ZUtil class from 0.5 10 1.0.
 */
public class Zab_2 extends Protocol {
	protected final AtomicLong        zxid=new AtomicLong(0);
    private ExecutorService executor;
    protected Address                           local_addr;
    protected volatile Address                  leader;
    protected volatile View                     view;
    protected volatile boolean                  is_leader=false;
    private List<Address> zabMembers = Collections.synchronizedList(new ArrayList<Address>());
	private long lastZxidProposed=0, lastZxidCommitted=0;
    private final Set<MessageId> requestQueue =Collections.synchronizedSet(new HashSet<MessageId>());
	private Map<Long, ZABHeader> queuedCommitMessage = new HashMap<Long, ZABHeader>();
    private final LinkedBlockingQueue<ZABHeader> queuedMessages =
	        new LinkedBlockingQueue<ZABHeader>();
	private ConcurrentMap<Long, Proposal> outstandingProposals = new ConcurrentHashMap<Long, Proposal>();
    private final Map<Long, ZABHeader> queuedProposalMessage = Collections.synchronizedMap(new HashMap<Long, ZABHeader>());
    private final Map<MessageId, Message> messageStore = Collections.synchronizedMap(new HashMap<MessageId, Message>());
	Calendar cal = Calendar.getInstance();
    protected volatile boolean                  running=true;
    private int index=-1;
    //private Map<Long, Boolean> notACK = new HashMap<Long, Boolean>();
    SortedSet<Long> wantCommit = new TreeSet<Long>();
    private List<Integer> latencies = new ArrayList<Integer>();
	private List<Integer> avgLatencies = new ArrayList<Integer>();
	private List<String> avgLatenciesTimer = new ArrayList<String>();
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
    private static PrintWriter outFile;
    private final static String outDir="/home/pg/p13/a6915654/MMZAB/";
    
    private int lastArrayIndex = 0, lastArrayIndexUsingTime = 0 ;
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
    private  AtomicInteger                  numReqDeviverd=new AtomicInteger(0);
	private  AtomicInteger                  numRequest=new AtomicInteger(0);
	private long rateInterval = 10000;
	private long rateCount = 0;
    private boolean is_warmUp=true;
	private int longWait = Integer.MIN_VALUE;



    public Zab_2(){
    	
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
		    this.outFile = new PrintWriter(new BufferedWriter(new FileWriter
					(outDir+InetAddress.getLocalHost().getHostName()+"MZAB.log",true)));
    }
    
    
    public void reset(Address client) {
    	zxid.set(0);     	lastZxidProposed=0;
    	lastZxidCommitted=0;         requestQueue.clear();
    	queuedCommitMessage.clear();queuedProposalMessage.clear();        
        queuedMessages.clear(); outstandingProposals.clear();       
        messageStore.clear();startSending=false;        
        wantCommit.clear();lastRequestRecieved=0;        
        laslAckRecieved=0;recievedFirstRequest = false;        
        latencies.clear(); numReqDeviverd= new AtomicInteger(0);       
        numRequest= new AtomicInteger(0);startThroughputTime = 0;       
        endThroughputTime = 0;rateCount = 0;       
        rateInterval = 10000;rateCount = 0;   	
    	largeLatCount = 0;largeLatencies.clear();   	
    	lastArrayIndex = 0;lastArrayIndexUsingTime = 0 ;  	
        is_warmUp=false;//_timer.cancel();        
        countMessageLeader = new AtomicLong(0);        
        countMessageFollower = 0;countTotalMessagesFollowers = 0;        
    	currentCpuTime = 0; rateCountTime = 0;
    	lastTimer = 0; lastCpuTime = 0;
    	longWait = Integer.MIN_VALUE;
    	try {
			this.outFile = new PrintWriter(new BufferedWriter(new FileWriter
					(outDir+InetAddress.getLocalHost().getHostName()+"MZAB.log",true)));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	avgLatencies.clear();avgLatenciesTimer.clear();   	
    	currentCpuTime=0;
    	MessageId messageId = new MessageId(local_addr,
				-10, System.currentTimeMillis());
    	ZABHeader startTest = new ZABHeader(ZABHeader.STARTREALTEST, messageId);
        Message confirmClient=new Message(client).putHeader(this.id, startTest);
		down_prot.down(new Event(Event.MSG, confirmClient));  
	    
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
//			if (outstandingProposals.isEmpty()){
//				this.cancel();
//				return;
//			}
//			if ((currentTime - lastRequestRecieved) > 1000){
//				this.cancel();
//				return;
//			}
			
			 current = System.currentTimeMillis();

        	if ((current -laslAckRecieved) > 50
                     && (current - lastRequestRecieved) > 50
                     && !outstandingProposals.isEmpty()){
        		//if (log.isInfoEnabled()){
			//if (!outstandingProposals.isEmpty() && (currentTime - lastRequestRecieved) >500) {
        		this.cancel();
        		if (log.isInfoEnabled())
        			log.info("Comit Alllllllllllllllllllllllllllllllllll");
    			ZABHeader commitPending = new ZABHeader(ZABHeader.COMMITOUTSTANDINGREQUESTS);
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
        ZABHeader hdr;

        switch(evt.getType()) {
            case Event.MSG:
                Message msg=(Message)evt.getArg();
               // hdr=(ZABHeader)msg.getHeader(this.id);
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
        ZABHeader hdr;

        switch(evt.getType()) {
            case Event.MSG:            	
                msg=(Message)evt.getArg();
                hdr=(ZABHeader)msg.getHeader(this.id);
                if(hdr == null){
                    break; // pass up
                }
                switch(hdr.getType()) {                
                    case ZABHeader.START_SENDING:
            			return up_prot.up(new Event(Event.MSG, msg));
                    case ZABHeader.RESET:
  	                    reset(msg.getSrc());
	                	break;
                	case ZABHeader.REQUEST:
                		if (!is_warmUp && !is_leader && !startThroughput){
                			startThroughput = true;
                			startThroughputTime = System.currentTimeMillis();
                		   // timer = new Timer();
                			//timer.schedule(new ResubmitTimer(), timeInterval, timeInterval);

                		}
                		forwardToLeader(msg);
                		break;
                    case ZABHeader.FORWARD:
                    	lastRequestRecieved=System.currentTimeMillis();
                    	recievedFirstRequest = true;
                    	//log.info("Start--------------------------------------------------- _timer");
                    	if (!is_warmUp && !startSending){
	                    	//_timer = new Timer();
	        				//_timer.scheduleAtFixedRate(new FinishTask(this.id), 200, 200);
	                    	startSending=true;
                    	}
                    	//lastRequestRecieved = System.currentTimeMillis();
                		queuedMessages.add(hdr);
                		break;
                    case ZABHeader.PROPOSAL:
	                   	if (!is_leader){
	                   		//hdr.getMessageId().setStartTime(System.currentTimeMillis());
	            			sendACK(msg, hdr);
	            		}
	                   	break;           		
                    case ZABHeader.ACK:
                			processACK(msg, msg.getSrc());
                		break;
                    case ZABHeader.COMMITOUTSTANDINGREQUESTS:
                    	//makeAllFollowersAck=true;
            			commitPendingRequest();
            			//startSending = false;
            		break;
                    case ZABHeader.STATS:
                    	printMZabStats();
                    	break;
                    case ZABHeader.COUNTMESSAGE:
                		sendTotalABMessages(hdr);  
                		log.info("Yes, I recieved count request");
                	break;
                    case ZABHeader.STARTREALTEST:
                    	if(!zabMembers.contains(local_addr))
                			return up_prot.up(new Event(Event.MSG, msg));
                    	else
                    		break;
                    case ZABHeader.RESPONSE:
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
    	ZABHeader clientHeader = ((ZABHeader) message.getHeader(this.id));
    	if (clientHeader!=null && clientHeader.getType() == ZABHeader.START_SENDING){

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
    	

    	else if (clientHeader!=null && clientHeader.getType() == ZABHeader.RESET){

    		for (Address server : zabMembers){
        			//message.setDest(server);
        	        Message resetMessage = new Message(server).putHeader(this.id, clientHeader);
        	        down_prot.down(new Event(Event.MSG, resetMessage));    	
        	}
    	}
    	
    	else if (clientHeader!=null && clientHeader.getType() == ZABHeader.STATS){

    		for (Address server : zabMembers){
        			//message.setDest(server);
        	        Message statsMessage = new Message(server).putHeader(this.id, clientHeader);
        	        down_prot.down(new Event(Event.MSG, statsMessage));    	
        	}
    	}
    	

		else if (clientHeader != null
				&& clientHeader.getType() == ZABHeader.COUNTMESSAGE) {
			for (Address server : zabMembers) {
				if(!server.equals(zabMembers.get(0))){
					Message countMessages = new Message(server).putHeader(this.id,
							clientHeader);
					down_prot.down(new Event(Event.MSG, countMessages));
				}
			}
		}
    	
    	else if(!clientHeader.getMessageId().equals(null) && clientHeader.getType() == ZABHeader.REQUEST){
	    	 Address destination = null;
	         messageStore.put(clientHeader.getMessageId(), message);
	        ZABHeader hdrReq=new ZABHeader(ZABHeader.REQUEST, clientHeader.getMessageId());  
	        ++index;
	        if (index>2)
	        	index=0;
	        destination = zabMembers.get(index);
	        Message requestMessage = new Message(destination).putHeader(this.id, hdrReq);
	       down_prot.down(new Event(Event.MSG, requestMessage));    
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
	   ZABHeader hdrReq = (ZABHeader) msg.getHeader(this.id);
	   requestQueue.add(hdrReq.getMessageId());
	   if (!is_warmUp && is_leader && !startThroughput){
			startThroughput = true;
			startThroughputTime = System.currentTimeMillis();
		     // timer = new Timer();
			 // timer.schedule(new ResubmitTimer(), timeInterval, timeInterval);
   	    //numRequest.incrementAndGet();
   		//queuedMessages.add(hdr);
		}
	   if (is_leader){
		   hdrReq.getMessageId().setStartTime(System.nanoTime());
		   queuedMessages.add((ZABHeader)msg.getHeader(this.id));
       }	   
	   else{
		   hdrReq.getMessageId().setStartTime(System.nanoTime());
		   forward(msg);
	   }
            
           
   }

    private void forward(Message msg) {
        Address target=leader;
 	    ZABHeader hdrReq = (ZABHeader) msg.getHeader(this.id);
        if(target == null)
            return;
	    try {
	        ZABHeader hdr=new ZABHeader(ZABHeader.FORWARD, hdrReq.getMessageId());
	        Message forward_msg=new Message(target).putHeader(this.id,hdr);
	        down_prot.down(new Event(Event.MSG, forward_msg));
	     }
	    catch(Exception ex) {
	      log.error("failed forwarding message to " + msg, ex);
	    }
      
    }
    

    private void sendACK(Message msg, ZABHeader hrdAck){
   		numRequest.incrementAndGet();
    	Proposal p;
    	if (msg == null )
    		return;
    	
    	//ZABHeader hdr = (ZABHeader) msg.getHeader(this.id);
    	
    	if (hrdAck == null)
    		return;
    	
//    	if (hdr.getZxid() != lastZxidProposed + 1){
//            log.info("Got zxid 0x"
//                    + Long.toHexString(hdr.getZxid())
//                    + " expected 0x"
//                    + Long.toHexString(lastZxidProposed + 1));
//        }
      	

		//log.info("[" + local_addr + "] " + "follower, sending ack (sendAck) at "+getCurrentTimeStamp());
//		if (!(outstandingProposals.containsKey(hdr.getZxid()))){
			p = new Proposal();
			p.AckCount++; // Ack from leader
			p.setZxid(hrdAck.getZxid());
			outstandingProposals.put(hrdAck.getZxid(), p);
			lastZxidProposed = hrdAck.getZxid();
			queuedProposalMessage.put(hrdAck.getZxid(), hrdAck);
//		}
//		else{
//			p = outstandingProposals.get(hdr.getZxid());
//			p.AckCount++; // Ack from leader
//			queuedProposalMessage.put(hdr.getZxid(), hdr);
//			lastZxidProposed = hdr.getZxid();

		//}
		if (is_warmUp){
			ZABHeader hdrACK = new ZABHeader(ZABHeader.ACK, hrdAck.getZxid());
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
		
		else if (ZUtil.SendAckOrNoSend()){// || makeAllFollowersAck) {

//			log.info("["
//					+ local_addr
//					+ "] "
//					+ "follower, sending ack if (ZUtil.SendAckOrNoSend()) (sendAck) at "+getCurrentTimeStamp());
			//if(makeAllFollowersAck)
				//log.info("*********** makeAllFollowersAck Probability not working "+(++countACKNoProb));
			ZABHeader hdrACK = new ZABHeader(ZABHeader.ACK, hrdAck.getZxid());
			Message ackMessage = new Message().putHeader(this.id, hdrACK);
			//log.info("Sending ACK for " + hdr.getZxid()+" "+getCurrentTimeStamp()+ " " +getCurrentTimeStamp());
			//notACK.put(hdr.getZxid(), true);

			try{
			for (Address address : zabMembers) {
				if (!is_warmUp && !address.equals(local_addr))
					countMessageFollower++;
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
    	ZABHeader hdr = (ZABHeader) msgACK.getHeader(this.id);	
    	long ackZxid = hdr.getZxid();
    	//log.info("Reciving Ack zxid " + ackZxid + " sender " + sender+ " " +getCurrentTimeStamp());

//    	if (!(outstandingProposals.containsKey(hdr.getZxid())) && (lastZxidProposed < hdr.getZxid())){
//			p = new Proposal();
//	        outstandingProposals.put(hdr.getZxid(), p); 
//			queuedProposalMessage.put(hdr.getZxid(), hdr);
//	        lastZxidProposed = hdr.getZxid();
//	}
	
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
//            log.info("*********************Trying to commit future proposal: zxid 0x{} from {}",
//                    Long.toHexString(ackZxid), sender);
            return;
        }
		
		p.AckCount++;
		//if (log.isDebugEnabled()) {
//            log.debug("Count for zxid: " +
//                    Long.toHexString(ackZxid)+" = "+ p.getAckCount());
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
				
//				log.info(" processAck Commiting allwantCommit) commiting " + wantCommit + " before "+ackZxid);
//				for (long zx : wantCommit) {
//					if (isFirstZxid(zx)) {
//						commit(zx);
//						//log.info(" for (long zx : wantCommit) commiting " + zx);
//						outstandingProposals.remove(zx);
//					} else
//						break;
//				}
//				wantCommit.clear();
//			}
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
			
			
//			log.info("After Finished outstandingProposals "+outstandingProposals.keySet());
//			log.info("After Finished wantCommit "+wantCommit);
			wantCommit.clear();
    	}
	
    	
    }
		
    private void commit(long zxidd){
			
	       	//log.info("[" + local_addr + "] "+"About to commit the request (commit) for zxid="+zxid+" "+getCurrentTimeStamp());

		    ZABHeader hdrOrginal = null;
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
//		   if (mid == null){
//			   log.info("!!!!!!!!!!!!!!!!!!!!!!!!!!!! Message is null (commit)"+ mid + " for zxid "+zxid);
//			   return;
//		   }
	       //ZABHeader hdrCommit = new ZABHeader(ZABHeader.COMMIT, zxid, mid);
	       //Message commitMessage = new Message().putHeader(this.id, hdrCommit);
	       //commitMessage.src(local_addr);
		   deliver(zxidd);

	    }
    
		
    private void deliver(long committedZxid){
	    	//ZABHeader hdr = (ZABHeader) toDeliver.getHeader(this.id);
	    	//long zxid = hdr.getZxid();
	    	//log.info("[" + local_addr + "] "+ " delivering message (deliver) for zxid=" + hdr.getZxid()+" "+getCurrentTimeStamp());

	    	ZABHeader hdrOrginal = queuedProposalMessage.remove(committedZxid);
	    	if (hdrOrginal == null) {
				 if (log.isInfoEnabled())
					 log.info("$$$$$$$$$$$$$$$$$$$$$ Header is null (deliver)"
						+ hdrOrginal + " for zxid " + committedZxid);
				return;
			}
	    	queuedCommitMessage.put(committedZxid, hdrOrginal);
	    	if(!is_warmUp){
		    	numReqDeviverd.incrementAndGet();
				endThroughputTime = System.currentTimeMillis();
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
		}
	//}			endThroughputTime = System.currentTimeMillis();
			//long startTimes  = hdrOrginal.getMessageId().getStartTime();
			//latencies.add((int)(System.currentTimeMillis() - startTimes));
			   if (log.isInfoEnabled())
					log.info("queuedCommitMessage size = " + queuedCommitMessage.size() + " zxid "+committedZxid);

	    	if (requestQueue.contains(hdrOrginal.getMessageId())){
	    		long startTime  = hdrOrginal.getMessageId().getStartTime();
				latencies.add((int)(System.nanoTime() - startTime));
	    		//log.info("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!I am the zab request receiver, going to send response back to " + hdrOrginal.getMessageId().getAddress());
		    	ZABHeader hdrResponse = new ZABHeader(ZABHeader.RESPONSE, committedZxid,  hdrOrginal.getMessageId());
		    	Message msgResponse = new Message(hdrOrginal.getMessageId().getAddress()).putHeader(this.id, hdrResponse);
	       		down_prot.down(new Event(Event.MSG, msgResponse));     

	    	}
	    	
	    	   
	   }
		
		
    private void handleOrderingResponse(ZABHeader hdrResponse) {
			
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
		
		private void sendTotalABMessages(ZABHeader carryCountMessageLeader){
			if(!is_leader){
			    ZABHeader followerMsgCount = new ZABHeader(ZABHeader.COUNTMESSAGE, countMessageFollower);
		   	    Message requestMessage = new Message(leader).putHeader(this.id, followerMsgCount);
		        down_prot.down(new Event(Event.MSG, requestMessage)); 
			}
			else{
				long followerMsg = carryCountMessageLeader.getZxid();
				countTotalMessagesFollowers += followerMsg;
			}
				
		}
		
		private void printMZabStats(){	
			// print Min, Avg, and Max latency
			List<Long> latAvg = new ArrayList<Long>();
			int count =0;
			long avgTemp =0;
			long min = Long.MAX_VALUE, avg =0, max = Long.MIN_VALUE;
			for (long lat : latencies){
				if (lat < min){
					min = lat;
				}
				if (lat > max){
					max = lat;
				}
				avg+=lat;
				avgTemp+=lat;
				count++;
				if(count>10000){
					latAvg.add(avgTemp/count);
					count=0;
					avgTemp=0;
				}
				
			}

			outFile.println("Number of Request Recieved = "+(numRequest));
			outFile.println("Number of Request Deliever = " + numReqDeviverd);
			outFile.println("Total ZAB Messages = " + (countMessageLeader.get() + countTotalMessagesFollowers));
			outFile.println("Throughput = " + (numReqDeviverd.get()/(TimeUnit.MILLISECONDS.toSeconds(endThroughputTime-startThroughputTime)))+ " ops/sec");
			outFile.println("Large Latencies count " + largeLatCount);	
			outFile.println("Large Latencies " + largeLatencies);	
			outFile.println("Latency /Min= " + min + " /Avg= "+ (avg/latencies.size())+
			        " /Max= " +max);	
			//outFile.println("Latency average rate with interval 100000 = " + 
			       // avgLatencies + " numbers avg = " + avgLatencies.size());
			//outFile.println("Latency average rate with interval 200 MillSec = " + 
			        //avgLatenciesTimer);
			//outFile.println("Longest wait time for commit = " + longWait);
			//Collections.sort(latencies);
			outFile.println("Latency average rate with No intervel = " + 
					latAvg + " numbers avg = " + latAvg.size());
			
			Collections.sort(latencies);
			int x50=0, x100=0, x150=0, x200=0, x250=0, x300=0, x350=0,x400=0, x450=0, x500=0, x550=0, x600=0, x650=0, x700=0, xLager700=0;
			
			for (Integer l:latencies){
				if (l<51)
					x50++;
				else if(l>50 && l<101)
					x100++;
				else if(l>100 && l<151)
					x150++;
				else if(l>150 && l<201)
					x200++;
				else if(l>200 && l<251)
					x250++;
				else if(l>250 && l<301)
					x300++;
				else if(l>300 && l<351)
					x350++;
				else if(l>350 && l<401)
					x400++;
				else if(l>400 && l<451)
					x450++;
				else if(l>450 && l<501)
					x500++;
				else if(l>500 && l<551)
					x550++;
				else if(l>560 && l<601)
					x600++;
				else if(l>600 && l<651)
					x650++;
				else if(l>650 && l<701)
					x700++;
				else
					xLager700++;
			}
				outFile.println("Distribution contains latencies form (0-50) " + x50);
			    outFile.println("Distribution contains latencies form (51-100) " + x100);
			    outFile.println("Distribution contains latencies form (101-150) " + x150);
			    outFile.println("Distribution contains latencies form (151-200) " + x200);
			    outFile.println("Distribution contains latencies form (201-250) " + x250);
			    outFile.println("Distribution contains latencies form (251-300) " + x300);
			    outFile.println("Distribution contains latencies form (301-350) " + x350);
			    outFile.println("Distribution contains latencies form (351-400) " + x400);
			    outFile.println("Distribution contains latencies form (401-450) " + x450);
			    outFile.println("Distribution contains latencies form (451-500) " + x500);
			    outFile.println("Distribution contains latencies form (501-550) " + x550);
			    outFile.println("Distribution contains latencies form (551-600) " + x600);
			    outFile.println("Distribution contains latencies form (601-650) " + x650);
			    outFile.println("Distribution contains latencies form (651-700) " + x700);
			    outFile.println("Distribution contains latencies form (  > 700) " + xLager700);
		
			
//			final int groupRange = 50;
//
//			ListMultimap<Integer, Integer> map = Multimaps.index(latencies, new Function<Integer, Integer>() {
//			    public Integer apply(Integer i) {
//			        //work out which group the value belongs in
//			        return (i / groupRange) + (i % groupRange == 0 ? 0 : 1);
//			    }
//			});
//			for (Integer key : map.keySet()) {
//			    List<Integer> value = map.get(key);
//			    outFile.println("Distribution contains " + value.size() + " items from " + value.get(0) + " to " + value.get(value.size() - 1));
//			}
			
			    outFile.println("Test Generated at "+ new Date() + " /Lasted for = "
					 	+ TimeUnit.MILLISECONDS.toSeconds((endThroughputTime-startThroughputTime)));	
			 outFile.println();	

			outFile.close();	
		
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
        	ZABHeader hdrReq = null;
        	//long currentTime = 0;
            while (running) {
            	//if (queuedMessages.peek()!=null)
            		//lastTimeQueueChecked = System.currentTimeMillis();
            	
            	//currentTime = System.currentTimeMillis();

//            	if (queuedMessages.peek()==null){
//	    			log.info("lastTimeQueueChecked = " + lastTimeQueueChecked + 
//	    					" diff = " + (currentTime - lastTimeQueueChecked) );
//	    			log.info("laslAckRecieved = " + laslAckRecieved + 
//	    					" diff = " + (currentTime - laslAckRecieved) );
//	    			log.info("lastRequestRecieved = " + lastRequestRecieved + 
//	    					" diff = " + (currentTime - lastRequestRecieved) );
//	    			log.info("outstandingProposals size = " + outstandingProposals.size());
//	    			log.info("queuedMessages size = " + queuedMessages.size());
//            	}



//            	if ((currentTime - lastTimeQueueChecked) > 20 
//            		   	 && (currentTime -laslAckRecieved) > 20
//                         && (currentTime - lastRequestRecieved) > 20
//                         && !outstandingProposals.isEmpty()){
//            		//if (log.isInfoEnabled()){
//            			log.info("Dummy is sending ************ " + outstandingProposals.size());
//            		//}
//            		ZABHeader commitPending = new ZABHeader(ZABHeader.COMMITOUTSTANDINGREQUESTS);
//    				for (Address address : zabMembers) {
//                        Message commitALL = new Message(address).putHeader(this.id, commitPending);
//                		down_prot.down(new Event(Event.MSG, commitALL));     
//                    }
//            	}
            		
                	 try {
                		
                		 hdrReq=queuedMessages.take();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
                 
            	
                
            	long new_zxid = getNewZxid();
    			 //log.info("Queue Size------> "+ queuedMessages.size());

            	ZABHeader hdrProposal = new ZABHeader(ZABHeader.PROPOSAL, new_zxid, hdrReq.getMessageId()); 
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
            	numRequest.incrementAndGet();       	
            	
            	try{
            		
            		//log.info("[" + local_addr + "] "+" prepar for proposal (run) for zxid="+new_zxid+" "+getCurrentTimeStamp());

                 	//log.info("Leader is about to sent a proposal " + ProposalMessage);
                 	for (Address address : zabMembers) {
                        if(address.equals(leader))
                        	continue; 
                    	if (!is_warmUp)
	                		countMessageLeader.incrementAndGet();
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
		 List<Integer> copyLat = new ArrayList<Integer>(latencies);
		 for (int i =  lastArrayIndex; i < copyLat.size(); i++){
			 avg+=copyLat.get(i);
			 elementCount++;
		 }
		 
		 lastArrayIndex = copyLat.size() - 1;
		 avg= avg/elementCount;
		 avgLatencies.add(avg);
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
		 for (int i =  lastArrayIndexUsingTime; i < latencies.size(); i++){
			 if (latencies.get(i)>50){
				 largeLatCount++;
				 largeLatencies.add((currentCpuTime - startThroughputTime) + "/" + latencies.get(i));
			 }
			 avg+=latencies.get(i);
			 elementCount++;
		 }
		 
		 lastArrayIndexUsingTime = latencies.size()-1;
		 avg= avg/elementCount;
	 
		String mgsLat = (currentCpuTime - startThroughputTime) + "/" +
		                ((finished - lastFinished) + "/" + avg);// (TimeUnit.MILLISECONDS.toSeconds(currentCpuTime - lastCpuTime)));
		avgLatenciesTimer.add(mgsLat);
		lastFinished = finished;
		lastCpuTime = currentCpuTime;
	}
	 
 }
}
