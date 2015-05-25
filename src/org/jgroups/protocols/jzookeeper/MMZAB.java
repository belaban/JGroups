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

public class MMZAB extends Protocol {
	// d
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
   
    private Timer _timer;
    private boolean startSending = false;
    private boolean makeAllFollowersAck = false;
    private long lastRequestRecieved=0;
    private long laslAckRecieved=0;
    private volatile long lastTimeQueueChecked=0;
    private boolean recievedFirstRequest = false;
    private long current = 0;






    public MMZAB(){
    	
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
			 current = System.currentTimeMillis();
			//log.info("cccccccall FinishTask diff time "+(currentTime - timeDiff));
//			if (outstandingProposals.isEmpty()){
//				this.cancel();
//				return;
//			}
//			if ((currentTime - lastRequestRecieved) > 1000){
//				this.cancel();
//				return;
//			}
			

        	if ((current - lastTimeQueueChecked) > 20 
        		   	 && (current -laslAckRecieved) > 20
                     && (current - lastRequestRecieved) > 20
                     && !outstandingProposals.isEmpty()){
        		//if (log.isInfoEnabled()){
			//if (!outstandingProposals.isEmpty() && (currentTime - lastRequestRecieved) >500) {
        		this.cancel();
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
//                    	if(zabMembers.contains(local_addr)){
//                			//log.info("$$$$$$$$$$$$$$$$$ initiale startSending and makeAllFollowersAck = false");
//                			startSending=false;
//                			makeAllFollowersAck=false;
//                    	}
//                    	else
                    		return up_prot.up(new Event(Event.MSG, msg));
                	case ZABHeader.REQUEST:
                		forwardToLeader(msg);
                		break;
                    case ZABHeader.FORWARD:
                    	lastRequestRecieved=System.currentTimeMillis();
                    	recievedFirstRequest = true;
                    	//log.info("Start--------------------------------------------------- _timer");
                    	if (!startSending){
	                    	_timer = new Timer();
	        				_timer.scheduleAtFixedRate(new FinishTask(this.id), 200, 200);
	                    	startSending=true;
                    	}
                    	//lastRequestRecieved = System.currentTimeMillis();
                		queuedMessages.add(hdr);
                		break;
                    case ZABHeader.PROPOSAL:
	                   	if (!is_leader){
	            			sendACK(msg);
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
        	   if (!zabMembers.contains(client)){
				   if (log.isInfoEnabled())
	        	 	    log.info("Address to check is not zab Members, will send start request to" + client+ " "+ getCurrentTimeStamp());
	        		message.setDest(client);
	        	    down_prot.down(new Event(Event.MSG, message));    	
        		}
        		
    	}
    	}
    	else if(!clientHeader.getMessageId().equals(null)){
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
	   if (is_leader){
		  queuedMessages.add((ZABHeader)msg.getHeader(this.id));
       }	   
	   else{
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
    

    private void sendACK(Message msg){
    	Proposal p;
    	if (msg == null )
    		return;
    	
    	ZABHeader hdr = (ZABHeader) msg.getHeader(this.id);
    	
    	if (hdr == null)
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
			p.setZxid(hdr.getZxid());
			outstandingProposals.put(hdr.getZxid(), p);
			lastZxidProposed = hdr.getZxid();
			queuedProposalMessage.put(hdr.getZxid(), hdr);
//		}
//		else{
//			p = outstandingProposals.get(hdr.getZxid());
//			p.AckCount++; // Ack from leader
//			queuedProposalMessage.put(hdr.getZxid(), hdr);
//			lastZxidProposed = hdr.getZxid();

		//}
		if (ZUtil.SendAckOrNoSend()){// || makeAllFollowersAck) {
//			log.info("["
//					+ local_addr
//					+ "] "
//					+ "follower, sending ack if (ZUtil.SendAckOrNoSend()) (sendAck) at "+getCurrentTimeStamp());
			//if(makeAllFollowersAck)
				//log.info("*********** makeAllFollowersAck Probability not working "+(++countACKNoProb));
			ZABHeader hdrACK = new ZABHeader(ZABHeader.ACK, hdr.getZxid());
			Message ackMessage = new Message().putHeader(this.id, hdrACK);
			log.info("Sending ACK for " + hdr.getZxid()+" "+getCurrentTimeStamp()+ " " +getCurrentTimeStamp());
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
		else{
			log.info("Not Sending ACK for " + hdr.getZxid()+" "+getCurrentTimeStamp()+ " " +getCurrentTimeStamp());
			//notACK.put(hdr.getZxid(), false);
		}
	
    	}
    
    
    private synchronized void processACK(Message msgACK, Address sender){
    	laslAckRecieved = System.currentTimeMillis();
	    Proposal p = null;
    	ZABHeader hdr = (ZABHeader) msgACK.getHeader(this.id);	
    	long ackZxid = hdr.getZxid();
    	log.info("Reciving Ack zxid " + ackZxid + " sender " + sender+ " " +getCurrentTimeStamp());

//    	if (!(outstandingProposals.containsKey(hdr.getZxid())) && (lastZxidProposed < hdr.getZxid())){
//			p = new Proposal();
//	        outstandingProposals.put(hdr.getZxid(), p); 
//			queuedProposalMessage.put(hdr.getZxid(), hdr);
//	        lastZxidProposed = hdr.getZxid();
//	}
	
 	    //log.info("recieved ack "+ackZxid+" "+ sender + " "+getCurrentTimeStamp());

		if (lastZxidCommitted >= ackZxid) {
            if (log.isDebugEnabled()) {
                log.info("proposal has already been committed, pzxid: 0x{} zxid: 0x{}",
                        lastZxidCommitted, ackZxid);
            }
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
	
		
	    	log.info("Commiting all  "+wantCommit);
	    	for (long zx:wantCommit){
				log.info("Commiting "+outstandingProposals.keySet());
					commit(zx);
					outstandingProposals.remove(zx);
				}
			
			
//			log.info("After Finished outstandingProposals "+outstandingProposals.keySet());
//			log.info("After Finished wantCommit "+wantCommit);
			wantCommit.clear();
    	}
	
    	
    }
		
    private void commit(long zxid){
			
	       	//log.info("[" + local_addr + "] "+"About to commit the request (commit) for zxid="+zxid+" "+getCurrentTimeStamp());

		    ZABHeader hdrOrginal = null;
	    	   synchronized(this){
	    	       lastZxidCommitted = zxid;
	    	   }
	    	   
		   hdrOrginal = queuedProposalMessage.get(zxid);
		   if (hdrOrginal == null){
			   if (log.isInfoEnabled())
				   log.info("!!!!!!!!!!!!!!!!!!!!!!!!!!!! Header is null (commit)"+ hdrOrginal + " for zxid "+zxid);
			   return;
		   }
   	       MessageId mid = hdrOrginal.getMessageId();
//		   if (mid == null){
//			   log.info("!!!!!!!!!!!!!!!!!!!!!!!!!!!! Message is null (commit)"+ mid + " for zxid "+zxid);
//			   return;
//		   }
	       ZABHeader hdrCommit = new ZABHeader(ZABHeader.COMMIT, zxid, mid);
	       Message commitMessage = new Message().putHeader(this.id, hdrCommit);
	       commitMessage.src(local_addr);
		   deliver(commitMessage);

	    }
    
		
    private void deliver(Message toDeliver){
	    	ZABHeader hdr = (ZABHeader) toDeliver.getHeader(this.id);
	    	long zxid = hdr.getZxid();
	    	//log.info("[" + local_addr + "] "+ " delivering message (deliver) for zxid=" + hdr.getZxid()+" "+getCurrentTimeStamp());

	    	ZABHeader hdrOrginal = queuedProposalMessage.remove(zxid);
	    	if (hdrOrginal == null) {
				 if (log.isInfoEnabled())
					 log.info("$$$$$$$$$$$$$$$$$$$$$ Header is null (deliver)"
						+ hdrOrginal + " for zxid " + hdr.getZxid());
				return;
			}
	    	queuedCommitMessage.put(zxid, hdrOrginal);
			   //if (log.isInfoEnabled())
					log.info("queuedCommitMessage size = " + queuedCommitMessage.size() + " zxid "+zxid);

	    	if (requestQueue.contains(hdrOrginal.getMessageId())){
	    		//log.info("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!I am the zab request receiver, going to send response back to " + hdrOrginal.getMessageId().getAddress());
		    	ZABHeader hdrResponse = new ZABHeader(ZABHeader.RESPONSE, zxid,  hdrOrginal.getMessageId());
		    	Message msgResponse = new Message(hdrOrginal.getMessageId().getAddress()).putHeader(this.id, hdrResponse);
	       		down_prot.down(new Event(Event.MSG, msgResponse));     

	    	}
	    	
	    	   
	   }
		
		
    private void handleOrderingResponse(ZABHeader hdrResponse) {
			
	    	//log.info("[" + local_addr + "] "+ "recieved response message (handleOrderingResponse) for zxid=" + hdrResponse.getZxid()+" "+getCurrentTimeStamp());

	        Message message = messageStore.get(hdrResponse.getMessageId());
	        message.putHeader(this.id, hdrResponse);

	        //log.info("[ " + local_addr + "] " + "Received zab ordered for request " + message + " its zxid = " + hdrResponse);
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

}
