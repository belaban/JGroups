package org.jgroups.protocols;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.MBean;
import org.jgroups.protocols.jzookeeper.CommitProcessor;
import org.jgroups.protocols.jzookeeper.Proposal;
import org.jgroups.protocols.jzookeeper.SyncRequestProcessor;
import org.jgroups.protocols.jzookeeper.ZUtil;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Bits;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Util;

@MBean(description="Implementation of total order protocol using a sequencer")
public class MZAB extends Protocol {

	private List<Address> members=new ArrayList<Address>();
	private Address                           local_addr;
	private volatile Address                  leader;
	private volatile View                     view;
	private boolean                           isLeader;
	private boolean running        =          false;
    protected final AtomicLong        zxid=new AtomicLong(0);

    private ExecutorService executor;
    
	private long lastZxidProposed=-1, zxidACK=-1, lastZxidCommitted=-1;
	
	private ConcurrentMap<Long, Proposal> outstandingProposals = new ConcurrentHashMap<Long, Proposal>();
	
	private Map<Long, Message> queuedProposalMessage = new HashMap<Long, Message>();
	private Map<Long, Message> queuedCommitMessage = new HashMap<Long, Message>();
	private final LinkedBlockingQueue<Message> queuedMessages =
	        new LinkedBlockingQueue<Message>();

	CommitProcessor commitProcessor = new CommitProcessor();
	
	SyncRequestProcessor syncProcessor = new SyncRequestProcessor();

	 public void start() throws Exception {
	        super.start();
	        running=true;
	        executor = Executors.newSingleThreadExecutor();
	        executor.execute(new FollowerMessageHandler(this.id));
	        log.setLevel("trace");
	    }
	
    /**
     * Just remove if you don't need to reset any state
     */
    public static void reset() {
    }

    
    public boolean isLeader(){
    	return isLeader;
    }
    
    public long getNewZxid(){
    	return zxid.incrementAndGet();
    }
    

    public Object up(Event evt) {
        log.info("UP[" + local_addr + "] " + "recieved from "+ evt.getType());

        switch(evt.getType()) {
            case Event.MSG:

                Message msg=(Message)evt.getArg();
                MZABHeader hdr = (MZABHeader)msg.getHeader(this.id);
                if (hdr==null)
                	break;
                switch(hdr.type) {
                	case MZABHeader.FORWARD:
                		
                		if(!isLeader) {
                			if(log.isErrorEnabled())
                            log.error(local_addr + ": non-Leader; dropping FORWARD request from " + msg.getSrc());
                			return null;
                		 }
                		try {
                    		log.info("Leader, Forward Requests received puting in queure (forwardToLeader)");
                			queuedMessages.add(msg);
                		}
                		catch(Exception ex) {
                			log.error("failed forwarding message to " + msg.getDest(), ex);
                		}
                		return null;
                   
                	case MZABHeader.PROPOSAL:
                    		log.info("Members, proposal message received, call senAck (up, proposal)");
                			sendACK(msg);
                		
                		return null;
                    
                	 case MZABHeader.ACK:
                		
                     		log.info("Ack message received, call processACK(up, ACK)");
                			processACK(msg, msg.getSrc());
                			return null;
                		
                	 
                }
            case Event.VIEW_CHANGE:
                handleViewChange((View)evt.getArg());
                break;
        }

        return up_prot.up(evt);            // Pass up to the layer above us
    }

    public void up(MessageBatch batch) {
        for(Message msg: batch) {
            // do something; perhaps check for the presence of a header
        }

        if(!batch.isEmpty())
            up_prot.up(batch);
    }

    public Object down(Event evt) {
    			
        switch(evt.getType()) {
           case Event.MSG:

	            Message msg=(Message)evt.getArg();
	            
                if(msg.getDest() != null || msg.isFlagSet(Message.Flag.NO_TOTAL_ORDER) || msg.isFlagSet(Message.Flag.OOB))
                    break;

                if(msg.getSrc() == null)
                    msg.setSrc(local_addr);
              
		            log.info("New request are received (down) " + msg);

	
	
		            forwardToLeader(msg);
	          
            return null;
        
            
            case Event.TMP_VIEW:
            case Event.VIEW_CHANGE:          	
            	handleViewChange((View)evt.getArg());
                break;
            case Event.SET_LOCAL_ADDRESS:
            	local_addr = (Address) evt.getArg();
                break;
        }
        return down_prot.down(evt);
    }
    
    
    public void forwardToLeader(Message msg){
    	Address target = leader;
    	
        if(target == null)
            return;
        
        if (msg == null)
    		return;
        
        log.info("[" + local_addr + "] "+"recieved msg (forwardToLeader) "+msg);

        if (!isLeader()){
        try {
        	MZABHeader forwardMsg = new MZABHeader(MZABHeader.FORWARD);
    		msg.putHeader(this.id, forwardMsg);
    		msg.setDest(target);
    		
            //Message forward_msg=new Message(target, Util.objectToByteBuffer(msg)).putHeader(this.id,hdr);
            down_prot.down(new Event(Event.MSG, msg));
        }catch (Exception e){
        	
        }
        }	
        
        else{
    		log.info("Leader, therefore Requests puting in queury (forwardToLeader) "+msg);
    		queuedMessages.add(msg);   
    	}
  
    		//msg.sDest(leader);
//            log.info("Not leader, Requests forwarding to the leader(forward
    		log.info("send it to " + leader);
       		//down_prot.down(new Event(Event.MSG, forwardMsg));  
    	
    	
    }  
    	
    	
    
//    public void sendProposal(Message msg){
//    	
//    	Message ProposalMessage = null;
//    	
//    	if (msg == null)
//    		return;
//    	
//    	Address sender=msg.getSrc();
//        if(view != null && !view.containsMember(sender)) {
//            if(log.isErrorEnabled())
//                log.error(local_addr + ": dropping FORWARD request from non-member " + sender +
//                            "; view=" + view);
//            return;
//        }
//        
//
//    	long newZxid = getNewZxid();
//    	MZABHeader hdrProposal = new MZABHeader(MZABHeader.PROPOSAL, newZxid);
//        byte [] bufMs = msg.getBuffer();
//        String updateMsg = new String(bufMs);
//        updateMsg = updateMsg + " zxid= " + zxid;
//        msg = msg.setBuffer(updateMsg.getBytes());
//        ProposalMessage=new Message(null, msg.getRawBuffer(), msg.getOffset(), msg.getLength()).putHeader(this.id, hdrProposal);
//    	
//    	Proposal p = new Proposal();
//    	p.setMessage(ProposalMessage);
//    	p.setMessageSrc(msg.getSrc());
//    	outstandingProposals.put(newZxid, p);
//    	try{
//    		//Message forwardMsg = new Message(null, Util.objectToByteBuffer(msg));
//    		down_prot.down(new Event(Event.MSG, ProposalMessage));     
//         }catch(Exception ex) {
//    		log.error("failed proposing message to members");
//    	}    	
//    }
    
    
    /**
     * create a ACK and send it out to all the leader
     * 
     * @param message
     */
    public void sendACK(Message msg){
    	Proposal p;
		log.info("follower, sending ack (sendAck)");

    	if (msg == null )
    		return;
    	
    	MZABHeader hdr = (MZABHeader) msg.getHeader(this.id);
    	
    	if (hdr == null)
    		return;
    	
    	if (hdr.getZxid() != lastZxidProposed + 1){
            log.warn("Got zxid 0x"
                    + Long.toHexString(hdr.getZxid())
                    + " expected 0x"
                    + Long.toHexString(lastZxidProposed + 1));
        }
    	

	    	
			if (!isLeader()){
				p = new Proposal();
		       	p.setMessage(new Message(null, msg.getObject()));
		    	p.setMessageSrc(hdr.getSrc());
		    	p.AckCount++; //Ack from leader
		    	outstandingProposals.put(zxid.get(), p);
		    	lastZxidProposed = hdr.getZxid();
		    	commitProcessor.processMessage(msg);
				syncProcessor.processMessage(msg);       
				queuedProposalMessage.put(hdr.getZxid(), msg);
				if (ZUtil.SendAckOrNoSend()){
		
					MZABHeader hdrACK = new MZABHeader(MZABHeader.ACK, hdr.getZxid());
					Message ACKMessage = new Message(null).putHeader(id, hdrACK);
					try{
			    		down_prot.down(new Event(Event.MSG, ACKMessage));     
			         }catch(Exception ex) {
			    		log.error("failed sending ACK message to Leader");
			    	} 
	    	    }
    	     }
	     	else{
	     		p = outstandingProposals.get(hdr.getZxid());
	     		p.AckCount++;				
			}
		
		
    	}
		
    
    /**
     * Keep a count of acks that are received by the leader for a particular
     * proposal
     * 
     * @param ,msgACK
     *                the proposal message sent out
     * @param sender
     */
    
    synchronized void processACK(Message msgACK, Address sender){
    	
    	log.info("Received ACK from " + sender);
    	MZABHeader hdr = (MZABHeader) msgACK.getHeader(this.id);	
    	long ackZxid = hdr.getZxid();

		if (lastZxidCommitted >= ackZxid) {
            if (log.isDebugEnabled()) {
                log.debug("proposal has already been committed, pzxid: 0x{} zxid: 0x{}",
                        Long.toHexString(lastZxidCommitted), Long.toHexString(ackZxid));
            }
            // The proposal has already been committed
            return;
        }
        Proposal p = outstandingProposals.get(ackZxid);
        if (p == null) {
            log.warn("Trying to commit future proposal: zxid 0x{} from {}",
                    Long.toHexString(ackZxid), sender);
            return;
        }
		
		p.AckCount++;
		if (log.isDebugEnabled()) {
            log.debug("Count for zxid: 0x{} is {}",
                    Long.toHexString(ackZxid), p.getAckCount());
        }
		
		if(isQuorum(p.getAckCount()) && isFirstZxid(zxid.get())){
			
			if (zxid.get() != lastZxidCommitted+1) {
                log.warn("Commiting zxid 0x{} from {} not first!",
                        Long.toHexString(ackZxid), sender);
                log.warn("First is 0x{}", Long.toHexString(lastZxidCommitted + 1));
            }
            outstandingProposals.remove(ackZxid);
           

            if (p.getMessage() == null) {
                log.warn("Going to commmit null request for proposal: {}", p);
            }
            
            commit(ackZxid);
            commitProcessor.commit(p.getMessage());
			
			
			
		}
    	
    }
    
    
    public boolean isFirstZxid(long zxid){
		
 		boolean find = true;
 		for (long z : outstandingProposals.keySet()){
 			if (z < zxid){
 				find = false;
 				break;
 			}
 		}       		
 		
 		return find;
 	}
    
    
    /**
     * Create a commit message and send it to all the members
     * 
     * @param zxid
     */
    public void commit(long zxid){
    	   synchronized(this){
    	       lastZxidCommitted = zxid;
    	   }
    	   
    	   deliver(zxid);

    }
    
    public void deliver(long zxid){
    	Message msg = null;
		msg = queuedProposalMessage.remove(zxid);

    		
    	if (!isLeader() && msg == null)
           	log.warn("No message pending for zxid" + zxid);
    		
    	if (queuedCommitMessage.containsKey(zxid)){
           	log.warn("message is already delivered for zxid" + zxid);
           	return;
    	}
    		
    	queuedCommitMessage.put(zxid, msg);
    	log.info("[" + local_addr + "] "+ " commitet request with zxid = "+zxid);

    }
    	
    

    
    public void handleViewChange(View v) {
    	if(view == null || view.compareTo(v) < 0)
            view=v;
        else
            return;
    	
    	List<Address> new_members=(v.getMembers());
        synchronized(members) {
            members.clear();
            if(new_members != null && !new_members.isEmpty())
                members.addAll(new_members);
        }

        

        Address new_leader=Util.getCoordinator(v);
        boolean leader_changed=leader == null || !leader.equals(new_leader);
        if(leader_changed) {
            leader=new_leader;
        	//isLeader = true;      	
        }
        
        isLeader=local_addr != null && local_addr.equals(leader);


    }
    
    public boolean isQuorum(int majority){
    	return majority >= (view.size()/2) + 1? true : false;
    }

    public void stop() {
    	if (log.isDebugEnabled())
            log.debug("The protocot are therminated");
    	running=false;
        executor.shutdown();
        
        super.stop();
    }


    public static class MZABHeader extends Header {
        // your variables
    	 private static final byte FORWARD       = 1;
         private static final byte PROPOSAL      = 2;
         private static final byte ACK           = 3;
         private static final byte COMMIT        = 4;
         
         private int type = -1;
         private long zxid=-1;   
         private Address src = null;
         
    	 public MZABHeader() {
         }
    	

         public MZABHeader(byte type) {
             this.type=type;
         }

         public MZABHeader(byte type, long seqno) {
             this(type);
             this.zxid=zxid;
         }
         
         public MZABHeader(byte type, Address src, long seqno) {
             this(type);
             this.src = src;
             this.zxid=zxid;
         }

         public long getZxid() {
             return zxid;
         }
         
         public Address getSrc() {
             return src;
         }

         public String toString() {
             StringBuilder sb=new StringBuilder(64);
             sb.append(printType());
             if(zxid >= 0)
                 sb.append(" seqno=" + zxid);
         
             return sb.toString();

    }

         protected final String printType() {
             switch(type) {
                 case FORWARD:        return "FORWARD";
                 case PROPOSAL:          return "PROPOSAL";
                 case COMMIT:  return "WRAPPED_BCAST";
                 default:             return "n/a";
             }
         }


         public void writeTo(DataOutput out) throws Exception {
             out.writeByte(type);
             Bits.writeLong(zxid,out);
         }

         public void readFrom(DataInput in) throws Exception {
             type=in.readByte();
             zxid=Bits.readLong(in);
         }

         public int size() {
             return Global.BYTE_SIZE + Bits.size(zxid) + Global.BYTE_SIZE; // type + zxid + flush_ack
         }

     }
    
    
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
    
            while (running) {
            	
            	Message messgae = null;
            	messgae = queuedMessages.poll();
                 if (messgae == null) {
                	 try {
						messgae = queuedMessages.take();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
                 }
            	
            	Address sender = messgae.getSrc();
            	log.info("view is = "+view.containsMember(sender));
             	log.info("view is = "+view);
                if(view != null && !view.containsMember(sender)) {
                	log.info("Sender is not included");
                    if(log.isErrorEnabled())
                        log.error(local_addr + ": dropping FORWARD request from non-member " + sender +
                                    "; view=" + view);
                    return;
                }
                

            	long new_zxid = getNewZxid();
            	MZABHeader hdrProposal = new MZABHeader(MZABHeader.PROPOSAL, messgae.getSrc(), new_zxid);                
                Message ProposalMessage=new Message(null, messgae.getRawBuffer(), messgae.getOffset(), messgae.getLength()).putHeader(this.id, hdrProposal);
            	
            	Proposal p = new Proposal();
            	p.setMessage(messgae);
            	p.setMessageSrc(messgae.getSrc());
            	//p.AckCount++;
            	outstandingProposals.put(zxid.get(), p);
            	
            	try{
            		down_prot.down(new Event(Event.MSG, ProposalMessage));     
                 }catch(Exception ex) {
            		log.error("failed proposing message to members");
            	}    
            	
            }
            
        }

       
    }

}

