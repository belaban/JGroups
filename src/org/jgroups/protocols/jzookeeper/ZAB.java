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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
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

public class ZAB extends Protocol {
	
	protected final AtomicLong                  zxid=new AtomicLong(0);
    private ExecutorService                     executor;
    protected Address                           local_addr;
    protected volatile Address                  leader;
    private int QUEUE_CAPACITY = 500;
    protected volatile View                     view;
    protected volatile boolean                  is_leader=false;
    private List<Address>                       zabMembers = Collections.synchronizedList(new ArrayList<Address>());
	private long                                lastZxidProposed=0, lastZxidCommitted=0;
    private final Set<MessageId>                requestQueue =Collections.synchronizedSet(new HashSet<MessageId>());
	private Map<Long, ZABHeader>                queuedCommitMessage = Collections.synchronizedMap(new HashMap<Long, ZABHeader>());
    private final Map<Long, ZABHeader> queuedProposalMessage = Collections.synchronizedMap(new HashMap<Long, ZABHeader>());
   // private final LinkedBlockingQueue<ZABHeader> queuedMessages =
	       // new LinkedBlockingQueue<ZABHeader>();
    private final BlockingQueue<ZABHeader> queuedMessages = new ArrayBlockingQueue<ZABHeader>(QUEUE_CAPACITY);

	private ConcurrentMap<Long, Proposal> outstandingProposals = new ConcurrentHashMap<Long, Proposal>();
    private final Map<MessageId, Message> messageStore = Collections.synchronizedMap(new HashMap<MessageId, Message>());
    private Calendar cal = Calendar.getInstance();
    private int index=-1;
    protected volatile boolean                  running=true;
 
    public ZAB(){
    	
    }
    
    @ManagedAttribute
    public boolean isleaderinator() {return is_leader;}
    public Address getleaderinator() {return leader;}
    public Address getLocalAddress() {return local_addr;}

    @ManagedOperation
    public String printStats() {
        return dumpStats().toString();
    }

    @Override
    public void start() throws Exception {
        super.start();
        running=true;    
	    executor = Executors.newSingleThreadExecutor();
	    executor.execute(new FollowerMessageHandler(this.id));
	    log.setLevel("trace");
	    
    }
    
    public void reset() {
       
    	lastZxidProposed=0;
    	lastZxidCommitted=0;
        requestQueue.clear();
    	queuedCommitMessage.clear();
        queuedProposalMessage.clear(); 
        queuedMessages.clear();
        outstandingProposals.clear();
        messageStore.clear();
        if(log.isInfoEnabled())
        	log.info("Reset Done");
	    
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
            	Message m = (Message) evt.getArg();
                handleClientRequest(m);
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
	                    reset();
	                	break;
                	case ZABHeader.REQUEST:
                		forwardToLeader(msg);
                		break;
                    case ZABHeader.FORWARD:                    	
//                    	if(!is_leader) {
//                			if(log.isErrorEnabled())
//                				//log.error("[" + local_addr + "] "+ ": non-Leader; dropping FORWARD request from " + msg.getSrc());
//                			break;
//                		 }                    	 
                		queuedMessages.add(hdr);
                		break;
                    case ZABHeader.PROPOSAL:
	                   	if (!is_leader){
	            			sendACK(msg);
	            		}
	                   	break;           		
                    case ZABHeader.ACK:
                		if (is_leader){
                			processACK(msg, msg.getSrc());
                		}
                		break;
                    case ZABHeader.COMMIT:
                		 deliver(msg);
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
    	
    	else if (clientHeader!=null && clientHeader.getType() == ZABHeader.RESET){
    		for (Address server : view.getMembers()){
        		if (zabMembers.contains(server)){
        			message.setDest(server);
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
    	 
    	if (msg == null )
    		return;
    	
    	ZABHeader hdr = (ZABHeader) msg.getHeader(this.id);
    	if (hdr == null)
    		return;
    	
    	if (hdr.getZxid() != lastZxidProposed + 1){
//            log.info("Got zxid 0x"
//                    + Long.toHexString(hdr.getZxid())
//                    + " expected 0x"
//                    + Long.toHexString(lastZxidProposed + 1));
        }
    	
    	lastZxidProposed = hdr.getZxid();
    	queuedProposalMessage.put(hdr.getZxid(), hdr);
		ZABHeader hdrACK = new ZABHeader(ZABHeader.ACK, hdr.getZxid(), hdr.getMessageId());
		Message ACKMessage = new Message(leader).putHeader(this.id, hdrACK);
		
		try{
    		down_prot.down(new Event(Event.MSG, ACKMessage));     
         }catch(Exception ex) {
    		log.error("failed sending ACK message to Leader");
    	} 
		
		
    }
    
    
    private synchronized void processACK(Message msgACK, Address sender){
	   
    	ZABHeader hdr = (ZABHeader) msgACK.getHeader(this.id);	
    	long ackZxid = hdr.getZxid();
		if (lastZxidCommitted >= ackZxid) {
            return;
        }
        Proposal p = outstandingProposals.get(ackZxid);
        if (p == null) {
            return;
        }		
		p.AckCount++;
		//if (log.isDebugEnabled()) {
//            log.debug("Count for zxid: " +
//                    Long.toHexString(ackZxid)+" = "+ p.getAckCount());
        //}
		
		if(isQuorum(p.getAckCount())){ 	
            outstandingProposals.remove(ackZxid);
            commit(ackZxid);	
		}
			
			
		}
		
    private synchronized void commit(long zxidd){
	    ZABHeader hdrOrg = queuedProposalMessage.get(zxidd);
   	    //log.info("Czxid = "+ hdrOrg.getZxid() + " " + getCurrentTimeStamp());
		   ZABHeader hdrOrginal = null;
	       synchronized(this){
	    	   lastZxidCommitted = zxidd;
	       }

		   hdrOrginal = queuedProposalMessage.get(zxidd);
		   if (hdrOrginal == null){
			   //if (log.isInfoEnabled())
				   //log.info("??????????????????????????? Header is null (commit)"+ hdrOrginal + " for zxid "+zxidd);
			   return;
		   }
   	       MessageId mid = hdrOrginal.getMessageId();
//		   if (mid == null){
//			   log.info("%%%%%%%%%%%%%%%%%%%%%%%%%%% Message is null (commit)"+ mid + " for zxid "+zxidd);
//			   return;
//		   }
	       ZABHeader hdrCommit = new ZABHeader(ZABHeader.COMMIT, zxidd, mid);
	       Message commitMessage = new Message().putHeader(this.id, hdrCommit);	       
           for (Address address : zabMembers) {
//        	   if (address.equals(leader)){
//        		   deliver(commitMessage);
//        		   continue;
//        	   }   		   
              Message cpy = commitMessage.copy();
              cpy.setDest(address);
   		      //log.info("[" + local_addr + "] "+ "YYYYYYYY sending comit message zxid to = "+zxidd+":"+address);
              down_prot.down(new Event(Event.MSG, cpy));     
           }

	    }
		
    private synchronized void deliver(Message toDeliver){
		//ZABHeader hdrOrginal = null;
		ZABHeader hdr = (ZABHeader) toDeliver.getHeader(this.id);
		long zxid = hdr.getZxid();

//		log.info("[" + local_addr + "] "
//				+ " recieved commit message (deliver) for zxid="
//				+ hdr.getZxid() + " " + getCurrentTimeStamp());
		//log.info("Dzxid = "+ hdr.getZxid() + " " + getCurrentTimeStamp());
		ZABHeader hdrOrginal = queuedProposalMessage.remove(zxid);
		if (hdrOrginal == null) {
			if (log.isInfoEnabled())
				log.info("$$$$$$$$$$$$$$$$$$$$$ Header is null (deliver)"
					+ hdrOrginal + " for zxid " + hdr.getZxid());
			return;
		}
//		log.info("!!!!!!!!!!!! check if (requestQueue.contains(hdrOrginal.getMessageId() (deliver)"
//				+ requestQueue.contains(hdrOrginal.getMessageId()));

		queuedCommitMessage.put(zxid, hdrOrginal);
		 if (log.isInfoEnabled())
			 log.info("queuedCommitMessage size = " + queuedCommitMessage.size() + " zxid "+zxid);
		if (requestQueue.contains(hdrOrginal.getMessageId())) {
			//log.info("I am the zab request receiver, "+ hdr.getZxid());
					//+ hdrOrginal.getMessageId().getAddress());
			ZABHeader hdrResponse = new ZABHeader(ZABHeader.RESPONSE, hdr.getZxid(),
					hdrOrginal.getMessageId());
			Message msgResponse = new Message(hdrOrginal.getMessageId()
					.getAddress()).putHeader(this.id, hdrResponse);
			down_prot.down(new Event(Event.MSG, msgResponse));

		}

	}
		
		
    private void handleOrderingResponse(ZABHeader hdrResponse) {
	        Message message = messageStore.get(hdrResponse.getMessageId());
	        message.putHeader(this.id, hdrResponse);
	        up_prot.up(new Event(Event.MSG, message));

	    }
	    	
		private boolean isQuorum(int majority){
	    	return majority >= ((zabMembers.size()/2) + 1)? true : false;
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
            	outstandingProposals.put(new_zxid, p);
            	queuedProposalMessage.put(new_zxid, hdrProposal);
            	
            	          	
            	try{
       
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
