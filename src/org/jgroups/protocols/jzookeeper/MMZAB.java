package org.jgroups.protocols.jzookeeper;

import java.io.DataInput;
import java.io.DataOutput;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.jgroups.Address;
import org.jgroups.AnycastAddress;
import org.jgroups.Event;
import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Bits;
import org.jgroups.util.BoundedHashMap;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Promise;
import org.jgroups.util.Util;

public class MMZAB extends Protocol {
	
	protected final AtomicLong        zxid=new AtomicLong(0);
    private ExecutorService executor;
    private int check =0;
    protected Address                           local_addr;
    protected volatile Address                  coord;
    protected volatile View                     view;
    protected volatile boolean                  is_coord=false;
    private List<Address> zabMembers = Collections.synchronizedList(new ArrayList<Address>());
	private long lastZxidProposed=0, zxidACK=0, lastZxidCommitted=0;
    private Random random = new Random(); // Random object for selecting which box member to use
    private final Set<MessageId> requestQueue =Collections.synchronizedSet(new HashSet<MessageId>());
	private Map<Long, ZABHeader> queuedCommitMessage = new HashMap<Long, ZABHeader>();
    private final LinkedBlockingQueue<ZABHeader> queuedMessages =
	        new LinkedBlockingQueue<ZABHeader>();
	private ConcurrentMap<Long, Proposal> outstandingProposals = new ConcurrentHashMap<Long, Proposal>();
	private Map<Long, ZABHeader> queuedProposalMessage = new HashMap<Long, ZABHeader>();
    private AtomicInteger localSequence = new AtomicInteger(); // This nodes sequence number
    private final Map<MessageId, Message> messageStore = Collections.synchronizedMap(new HashMap<MessageId, Message>());
	Calendar cal = Calendar.getInstance();

    protected volatile boolean                  running=true;
 
    public MMZAB(){
    	
    }
    
    @ManagedAttribute
    public boolean isCoordinator() {return is_coord;}
    public Address getCoordinator() {return coord;}
    public Address getLocalAddress() {return local_addr;}
    
    @Override
    public void start() throws Exception {
        super.start();
        running=true;
        
	    executor = Executors.newSingleThreadExecutor();
	    executor.execute(new FollowerMessageHandler(this.id));
	    log.setLevel("trace");
	    
    }
    @Override
    public void stop() {
        running=false;
        executor.shutdown();
        super.stop();
    }

    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                Message msg=(Message)evt.getArg();
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
                
                	case ZABHeader.REQUEST:
                		forwardToLeader(msg);
                		break;
                    case ZABHeader.FORWARD:
                    	
                    	if(!is_coord) {
                			if(log.isErrorEnabled())
                				//log.error("[" + local_addr + "] "+ ": non-Leader; dropping FORWARD request from " + msg.getSrc());
                			break;
                		 }
                    	 

                		//log.info("[" + local_addr + "] "+"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!Leader, puting request in queue at "+getCurrentTimeStamp());
                		queuedMessages.add(hdr);
                		break;
                    case ZABHeader.PROPOSAL:
	                   	if (!is_coord){
	            			sendACK(msg);
	            		}
	            		else 
	                		//log.info("[" + local_addr + "] "+"Leader, proposal message received ignoring it (up, proposal) at "+getCurrentTimeStamp());

	                   	break;           		
                    case ZABHeader.ACK:
                     		//log.info("Leader, ack message received, call processACK(up, ACK) at "+ getCurrentTimeStamp());
                			processACK(msg, msg.getSrc());
                		break;
//                    case ZABHeader.COMMIT:
//                		 deliver(msg);
//                		 break;
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
 	    //log.info("[" + local_addr + "] "+" recieved request from application (handleClientRequest) from "+message.getSrc()+ " "+getCurrentTimeStamp());

    	 Address destination = null;
    	 MessageId messageId = new MessageId(local_addr, localSequence.getAndIncrement()); // Increment localSequence
         messageStore.put(messageId, message);
         
        ZABHeader hdrReq=new ZABHeader(ZABHeader.REQUEST, messageId);        
        destination = Util.pickRandomElement(zabMembers); // Select box at random;
        
//        if (log.isTraceEnabled())
//            log.info("Send ordering request | " + message + " | dest " + destination);
        Message requestMessage = new Message(destination).putHeader(this.id, hdrReq);
        //requestMessage.setSrc(local_addr);
        down_prot.down(new Event(Event.MSG, requestMessage));    	
    	
    }
    
    private void handleViewChange(View v) {
        List<Address> mbrs=v.getMembers();
        coord=mbrs.get(0);
        if (coord.equals(local_addr)){
        	is_coord = true;
        }
        if (mbrs.size() == 3){
        	zabMembers.addAll(v.getMembers());
        	//log.info("Zab box view = " + zabMembers);
        	
        }
        if (mbrs.size() > 3 && zabMembers.isEmpty()){
        	for (int i = 0; i < v.getMembers().size()-1; i++) {
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
	
	   //log.info("[" + local_addr + "] "+" recieved request (forwardToLeader) from "+msg.getSrc() + " at "+getCurrentTimeStamp());

	   ZABHeader hdrReq = (ZABHeader) msg.getHeader(this.id);
	   requestQueue.add(hdrReq.getMessageId());
	   if (is_coord){
		  queuedMessages.add((ZABHeader)msg.getHeader(this.id));
	      //log.info("[" + local_addr + "] "+" I am a leader received new request no forward msg (forward) "+msg+" "+getCurrentTimeStamp());
       }	   
	   else{
		   forward(msg);
	   }
            
           
   }

    private void forward(Message msg) {
 	   //log.info("[" + local_addr + "] "+"forward request to the leader (forward) at "+getCurrentTimeStamp());

        Address target=coord;
 	    ZABHeader hdrReq = (ZABHeader) msg.getHeader(this.id);
        if(target == null)
            return;
       
	   // log.info("[" + local_addr + "] "+"recieved msg (forward) "+msg+" "+getCurrentTimeStamp());
	
	    try {
	        ZABHeader hdr=new ZABHeader(ZABHeader.FORWARD, hdrReq.getMessageId());
	        Message forward_msg=new Message(target).putHeader(this.id,hdr);
	        down_prot.down(new Event(Event.MSG, forward_msg));
	     }
	    catch(Exception ex) {
	    //  log.error("failed forwarding message to " + msg, ex);
	    }
      
    }
    

    private void sendACK(Message msg){
    	Proposal p;
		//log.info("follower, sending ack (sendAck) at "+getCurrentTimeStamp());

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
		if (ZUtil.SendAckOrNoSend()) {
//			log.info("["
//					+ local_addr
//					+ "] "
//					+ "follower, sending ack if (ZUtil.SendAckOrNoSend()) (sendAck) at "+getCurrentTimeStamp());

			ZABHeader hdrACK = new ZABHeader(ZABHeader.ACK, hdr.getZxid(), hdr.getMessageId());
			Message ackMessage = new Message(null).putHeader(id, hdrACK);
			try{
			int count = 0;
			for (Address address : zabMembers) {
                count ++;
                if (count > 3)
                	break;
                Message cpy = ackMessage.copy();
                cpy.setDest(address);
        		down_prot.down(new Event(Event.MSG, cpy));     
            }
         }catch(Exception ex) {
    		log.error("failed proposing message to members");
    	}    
		}
		// }
	     	
		
    	}
    
    
synchronized private void processACK(Message msgACK, Address sender){
	    Proposal p = null;
    	ZABHeader hdr = (ZABHeader) msgACK.getHeader(this.id);	
    	long ackZxid = hdr.getZxid();
    	
//    	if (!(outstandingProposals.containsKey(hdr.getZxid())) && (lastZxidProposed < hdr.getZxid())){
//		p = new Proposal();
//        outstandingProposals.put(hdr.getZxid(), p); 
//		queuedProposalMessage.put(hdr.getZxid(), hdr);
//        lastZxidProposed = hdr.getZxid();
//	}
	
 	  // log.info("[" + local_addr + "] "+"recieved ack from (processACK) "+sender +" for zxid="+ackZxid+" "+getCurrentTimeStamp()+" "+getCurrentTimeStamp());

		if (lastZxidCommitted >= ackZxid) {
            if (log.isDebugEnabled()) {
                log.info("proposal has already been committed, pzxid: 0x{} zxid: 0x{}",
                        lastZxidCommitted, ackZxid);
            }
            return;
        }
        p = outstandingProposals.get(ackZxid);
        if (p == null) {
            //log.info("Trying to commit future proposal: zxid 0x{} from {}",
             //       Long.toHexString(ackZxid), sender);
            return;
        }
		
		p.AckCount++;
		if (log.isDebugEnabled()) {
//            log.debug("Count for zxid: " +
//                    Long.toHexString(ackZxid)+" = "+ p.getAckCount());
        }
		
		if(isQuorum(p.getAckCount()) && isFirstZxid(ackZxid)){
			
			if (ackZxid != lastZxidCommitted+1) {
//                log.info("Commiting zxid 0x{} from {} not first! "+
//                        ackZxid+" "+ sender);
            }
           
	       	//log.info("[" + local_addr + "] "+"About to call commit() (ProcessACK) for zxid="+ackZxid);
            outstandingProposals.remove(ackZxid);

            commit(ackZxid);	
		}
			
			
		}
		
private void commit(long zxid){
			
	       	//log.info("[" + local_addr + "] "+"About to commit the request (commit) for zxid="+zxid+" "+getCurrentTimeStamp());

		    ZABHeader hdrOrginal = null;
	    	   synchronized(this){
	    	       lastZxidCommitted = zxid;
	    	   }
		   hdrOrginal = queuedProposalMessage.get(zxid);
   	       MessageId mid = hdrOrginal.getMessageId();
	       ZABHeader hdrCommit = new ZABHeader(ZABHeader.COMMIT, zxid, mid);
	       Message commitMessage = new Message().putHeader(this.id, hdrCommit);
	       commitMessage.src(local_addr);
		   deliver(commitMessage);

//	       int count = 0;
//           for (Address address : view.getMembers()) {
//               count++;
//        	   if (address.equals(coord)){
//        		   deliver(commitMessage);
//        		   continue;
//        	   }   		   
//              if (count > 3)
//            	 break;
//              Message cpy = commitMessage.copy();
//              cpy.setDest(address);
//              down_prot.down(new Event(Event.MSG, cpy));     
//           }

	    }
		
private void deliver(Message toDeliver){
				    	ZABHeader hdrOrginal = null;
	    	ZABHeader hdr = (ZABHeader) toDeliver.getHeader(this.id);
	    	long zxid = hdr.getZxid();
	    	//log.info("[" + local_addr + "] "+ " delivering message (deliver) for zxid=" + hdr.getZxid()+" "+getCurrentTimeStamp());

	    	hdrOrginal = queuedProposalMessage.remove(zxid);


	    	queuedCommitMessage.put(zxid, hdrOrginal);

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

	        //log.info("[ " + local_addr + "] " + "Received zab ordered for request " + message + " its zxid = " + hdrResponse);
	        up_prot.up(new Event(Event.MSG, message));

	    }
	    	

		private boolean isQuorum(int majority){
			//log.info(" acks =  " + majority + " majority "+ ((zabMembers.size()/2)+1));

	    	return majority >= ((zabMembers.size()/2) + 1)? true : false;
	    }
		
		private boolean isFirstZxid(long zxid){
			
			boolean find = true;
			for (long z : outstandingProposals.keySet()){
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
            while (running) {
            	
                	 try {
                		 hdrReq = queuedMessages.take();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
                 
            	
                
            	long new_zxid = getNewZxid();
            	ZABHeader hdrProposal = new ZABHeader(ZABHeader.PROPOSAL, new_zxid, hdrReq.getMessageId());                
                Message ProposalMessage=new Message().putHeader(this.id, hdrProposal);

                ProposalMessage.setSrc(local_addr);
            	Proposal p = new Proposal();
            	p.setMessageId(hdrReq.getMessageId());
            	p.AckCount++;
            	
            	
            	//log.info("Zxid count for zxid = " + new_zxid + " count = "  +p.AckCount+" "+getCurrentTimeStamp());
            	outstandingProposals.put(new_zxid, p);
            	queuedProposalMessage.put(new_zxid, hdrProposal);
            	
            	
            	
            	try{
            		
            		//log.info("[" + local_addr + "] "+" prepar for proposal (run) for zxid="+new_zxid+" "+getCurrentTimeStamp());

                 	//log.info("Leader is about to sent a proposal " + ProposalMessage);
                 	int count = 0;
                 	for (Address address : zabMembers) {
                        count ++;
                        if(address.equals(coord))
                        	continue;
                        if (count > 3)
                        	break;
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
