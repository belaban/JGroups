package org.jgroups.protocols.jzookeeper;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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

public class ZAB extends Protocol {
	
	protected final AtomicLong        zxid=new AtomicLong(0);
    private ExecutorService executor;

    protected Address                           local_addr;
    protected volatile Address                  coord;
    protected volatile View                     view;
    protected volatile boolean                  is_coord=false;
    protected final AtomicLong                  seqno=new AtomicLong(0);
    protected ArrayList<Address>                zabMembers=new ArrayList<Address>();    
	private long lastZxidProposed=0, zxidACK=0, lastZxidCommitted=0;
    private Random random = new Random(); // Random object for selecting which box member to use
    private final Set<MessageID> requestCache =Collections.synchronizedSet(new HashSet<MessageID>());
	private Map<Long, MessageInfo> queuedCommitMessage = new HashMap<Long, MessageInfo>();
    private final LinkedBlockingQueue<MessageInfo> queuedMessages =
	        new LinkedBlockingQueue<MessageInfo>();
	private ConcurrentMap<Long, Proposal> outstandingProposals = new ConcurrentHashMap<Long, Proposal>();
	private Map<Long, MessageInfo> queuedProposalMessage = new HashMap<Long, MessageInfo>();
	private Map<Long, Address> messageSource = new HashMap<Long, Address>();
    private AtomicInteger localSequence = new AtomicInteger(); // This nodes sequence number
    private final Map<MessageID, Message> messageStore = Collections.synchronizedMap(new HashMap<MessageID, Message>());





    /** Maintains messages forwarded to the coord which which no ack has been received yet.
     *  Needs to be sorted so we resend them in the right order
     */
    protected final NavigableMap<Long,Message>  forward_table=new ConcurrentSkipListMap<Long,Message>();

    
    protected final Lock                        send_lock=new ReentrantLock();

    protected final Condition                   send_cond=send_lock.newCondition();

    /** When ack_mode is set, we need to wait for an ack for each forwarded message until we can send the next one */
    protected volatile boolean                  ack_mode=true;

    /** Set when we block all sending threads to resend all messages from forward_table */
    protected volatile boolean                  flushing=false;

    protected volatile boolean                  running=true;

    /** Keeps track of the threads sending messages */
    protected final AtomicInteger               in_flight_sends=new AtomicInteger(0);

    // Maintains received seqnos, so we can weed out dupes
    protected final ConcurrentMap<Address,BoundedHashMap<Long,Long>> delivery_table=Util.createConcurrentMap();

    protected volatile Flusher                  flusher;
        

    /** Used for each resent message to wait until the message has been received */
    protected final Promise<Long>               ack_promise=new Promise<Long>();



    @Property(description="Size of the set to store received seqnos (for duplicate checking)")
    protected int  delivery_table_max_size=2000;

    @Property(description="Number of acks needed before going from ack-mode to normal mode. " +
      "0 disables this, which means that ack-mode is always on")
    protected int  threshold=10;

    protected int  num_acks=0;

    protected long forwarded_msgs=0;
    protected long bcast_msgs=0;
    protected long received_forwards=0;
    protected long received_bcasts=0;
    protected long delivered_bcasts=0;

    @ManagedAttribute
    public boolean isCoordinator() {return is_coord;}
    public Address getCoordinator() {return coord;}
    public Address getLocalAddress() {return local_addr;}
    @ManagedAttribute
    public long getForwarded() {return forwarded_msgs;}
    @ManagedAttribute
    public long getBroadcast() {return bcast_msgs;}
    @ManagedAttribute
    public long getReceivedForwards() {return received_forwards;}
    @ManagedAttribute
    public long getReceivedBroadcasts() {return received_bcasts;}

    @ManagedAttribute(description="Number of messages in the forward-table")
    public int getForwardTableSize() {return forward_table.size();}

    public void setThreshold(int new_threshold) {this.threshold=new_threshold;}

    public void setDeliveryTableMaxSize(int size) {delivery_table_max_size=size;}

    @ManagedOperation
    public void resetStats() {
        forwarded_msgs=bcast_msgs=received_forwards=received_bcasts=delivered_bcasts=0L;
    }

    @ManagedOperation
    public Map<String,Object> dumpStats() {
        Map<String,Object> m=super.dumpStats();
        m.put("forwarded",forwarded_msgs);
        m.put("broadcast",bcast_msgs);
        m.put("received_forwards", received_forwards);
        m.put("received_bcasts",   received_bcasts);
        m.put("delivered_bcasts",  delivered_bcasts);
        return m;
    }

    @ManagedOperation
    public String printStats() {
        return dumpStats().toString();
    }

    public void start() throws Exception {
        super.start();
        running=true;
        ack_mode=true;
        
	    executor = Executors.newSingleThreadExecutor();
	    executor.execute(new FollowerMessageHandler(this.id));
	    log.setLevel("trace");
	    
    }

    public void stop() {
        running=false;
        executor.shutdown();
        unblockAll();
        stopFlusher();
        super.stop();
    }

    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
            	log.info("[" + local_addr + "] "+"received request (down)");
            	log.info("[" + local_addr + "] "+ "print view (down) " +view.getMembers());

                Message msg=(Message)evt.getArg();

                if(msg.getSrc() == null){
                	log.info("[" + local_addr + "] "+"inside if(msg.getSrc() == null) (down)");
                      // msg.setSrc(local_addr);
                }
                if(flushing){
                	log.info("invoking block() method (down)");

                    block();
                    
                }
               
                try {
                    log.info("[" + local_addr + "] "+"invloking handleClientRequest method  (down)"+msg);
                    handleClientRequest(msg);
                }
                catch(Exception ex) {
                    log.error("failed sending message", ex);
                }
                finally {
                    in_flight_sends.decrementAndGet();
                }
                return null; // don't pass down

            case Event.VIEW_CHANGE:
                handleViewChange((View)evt.getArg());
                break;

            case Event.TMP_VIEW:
                handleTmpView((View)evt.getArg());
                break;

            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;
        }
        return down_prot.down(evt);
    }




    public Object up(Event evt) {
        Message msg;
        ZABHeader hdr;

        switch(evt.getType()) {
            case Event.MSG:
                msg=(Message)evt.getArg();
                //if(msg.isFlagSet(Message.Flag.NO_TOTAL_ORDER) || msg.isFlagSet(Message.Flag.OOB))
                    //break;
                hdr=(ZABHeader)msg.getHeader(this.id);
                if(hdr == null)
                    break; // pass up
                log.info("[" + local_addr + "] "+ " received message from (up) " + msg.getSrc() + " type "+ hdr.type);

                switch(hdr.type) {
                	case ZABHeader.REQUEST:
                		forwardToCoord(hdr.getMessageInfo());
                		break;
                    case ZABHeader.FORWARD:
                    	
                    	if(!is_coord) {
                			if(log.isErrorEnabled())
                            log.error("[" + local_addr + "] "+ ": non-Leader; dropping FORWARD request from " + msg.getSrc());
                			break;
                		 }
                		try {
                    		log.info("[" + local_addr + "] "+"Leader, puting in queuy");
                			queuedMessages.add(hdr.getMessageInfo());
                		}
                		catch(Exception ex) {
                			log.error("failed forwarding message to " + msg.getDest(), ex);
                		}
                		break;

                    case ZABHeader.PROPOSAL:
                   	 	log.info("[" + local_addr + "] "+"(up) inside PROPOSAL");

	                   	if (!is_coord){
	                		log.info("[" + local_addr + "] "+"follower, proposal message received, call senAck (up, proposal)");
	                    	log.info("[" + local_addr + "]"+"Proposal source  "+ msg.getSrc());

	            			sendACK(msg);
	            		}
	            		else 
	                		log.info("[" + local_addr + "] "+"Leader, proposal message received ignoring it (up, proposal)");

	                   	break;           		
                    case ZABHeader.ACK:
                		log.info("["+local_addr+"] "+"follower, ACK message received, call senAck (up, proposal)");
                		if (is_coord){
                     		log.info("Leader, ack message received, call processACK(up, ACK)");
                			processACK(msg, msg.getSrc());
                			
                		}
                		break;
                    case ZABHeader.COMMIT:
                		log.info("["+local_addr+"] "+"follower, commit message received, call deliver (up, COMMIT)");
                	     msg.getHeaders();
                		log.info("["+local_addr+"] "+"check source address, print headers (up, COMMIT) " +msg.getHeaders().toString());


                		 deliver(msg);
                		 break;
                    case ZABHeader.RESPONSE:
                    	handleOrderingResponse(hdr);
                    	
            }                
                return null;
            case Event.VIEW_CHANGE:
                Object retval=up_prot.up(evt);
                handleViewChange((View)evt.getArg());
                return retval;

            case Event.TMP_VIEW:
                handleTmpView((View)evt.getArg());
                break;
        }

        return up_prot.up(evt);
    }

    public void up(MessageBatch batch) {
        for(Message msg: batch) {
            if(msg.isFlagSet(Message.Flag.NO_TOTAL_ORDER) || msg.isFlagSet(Message.Flag.OOB) || msg.getHeader(id) == null)
                continue;
            batch.remove(msg);

            // simplistic implementation
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

    
    public void handleClientRequest(Message message){    	
    
    	 MessageID messageId = new MessageID(local_addr, localSequence.getAndIncrement()); // Increment localSequence
         messageStore.put(messageId, message);

         MessageInfo messageInfo = new MessageInfo(messageId);
         
        ZABHeader hdrReq=new ZABHeader(ZABHeader.REQUEST, messageInfo);
        log.trace("Send ordering request zab memebers box | " + zabMembers);

        Address destination = zabMembers.get(random.nextInt(zabMembers.size())); // Select box at random;
        
        if (log.isTraceEnabled())
            log.trace("Send ordering request | " + message + " | dest " + destination);

        Message requestMessage = new Message(destination).src(local_addr).putHeader(id, hdrReq);
        down_prot.down(new Event(Event.MSG, requestMessage));    	
    	
    }
    protected void handleViewChange(View v) {
        List<Address> mbrs=v.getMembers();
        if (mbrs.size() == 3){
        	zabMembers.addAll(mbrs);
        	log.info("Zab box size = " + zabMembers.size());
        	log.info("Zab box view = " + zabMembers);
        	
        }
        if(mbrs.isEmpty()) return;

        if(view == null || view.compareTo(v) < 0)
            view=v;
        else
            return;

        delivery_table.keySet().retainAll(mbrs);

        Address existing_coord=coord, new_coord=mbrs.get(0);
        boolean coord_changed=existing_coord == null || !existing_coord.equals(new_coord);
        if(coord_changed && new_coord != null) {
            stopFlusher();
            startFlusher(new_coord); // needs to be done in the background, to prevent blocking if down() would block
        }
    }

    public long getNewZxid(){
    	return zxid.incrementAndGet();
    }
    protected void flush(final Address new_coord) throws InterruptedException {
        // wait until all threads currently sending messages have returned (new threads after flushing=true) will block
        // flushing is set to true in startFlusher()
        while(flushing && running) {
            if(in_flight_sends.get() == 0)
                break;
            Thread.sleep(100);
        }

        send_lock.lockInterruptibly();
        try {
            if(log.isTraceEnabled())
                log.trace(local_addr + ": coord changed from " + coord + " to " + new_coord);
            coord=new_coord;
            is_coord=local_addr != null && local_addr.equals(coord);
            flushMessagesInForwardTable();
        }
        finally {
            if(log.isTraceEnabled())
                log.trace(local_addr + ": flushing completed");
            flushing=false;
            ack_mode=true; // go to ack-mode after flushing
            num_acks=0;
            send_cond.signalAll();
            send_lock.unlock();
        }
    }


    // If we're becoming coordinator, we need to handle TMP_VIEW as
    // an immediate change of view. See JGRP-1452.
    private void handleTmpView(View v) {
        List<Address> mbrs=v.getMembers();
        if(mbrs.isEmpty()) return;

        Address new_coord=mbrs.get(0);
        if(!new_coord.equals(coord) && local_addr != null && local_addr.equals(new_coord))
            handleViewChange(v);
    }


    /**
     * Sends all messages currently in forward_table to the new coordinator (changing the dest field).
     * This needs to be done, so the underlying reliable unicast protocol (e.g. UNICAST) adds these messages
     * to its retransmission mechanism<br/>
     * Note that we need to resend the messages in order of their seqnos ! We also need to prevent other message
     * from being inserted until we're done, that's why there's synchronization.<br/>
     * Access to the forward_table doesn't need to be synchronized as there won't be any insertions during flushing
     * (all down-threads are blocked)
     */
    protected void flushMessagesInForwardTable() {
        if(is_coord) {
            for(Map.Entry<Long,Message> entry: forward_table.entrySet()) {
                Long key=entry.getKey();
                Message msg=entry.getValue();
                byte[] val;
                try {
                    val=Util.objectToByteBuffer(msg);
                }
                catch(Exception e) {
                    log.error("flushing (broadcasting) failed", e);
                    continue;
                }

                ZABHeader hdr=new ZABHeader(ZABHeader.PROPOSAL, key);
                Message forward_msg=new Message(null, val).putHeader(this.id, hdr);
                if(log.isTraceEnabled())
                    log.trace(local_addr + ": flushing (broadcasting) " + local_addr + "::" + key);
                down_prot.down(new Event(Event.MSG, forward_msg));
            }
            return;
        }

        while(flushing && running && !forward_table.isEmpty()) {
            Map.Entry<Long,Message> entry=forward_table.firstEntry();
            final Long key=entry.getKey();
            Message    msg=entry.getValue();
            byte[]     val;

            try {
                val=Util.objectToByteBuffer(msg);
            }
            catch(Exception e) {
                log.error("flushing (broadcasting) failed", e);
                continue;
            }

            while(flushing && running && !forward_table.isEmpty()) {
                ZABHeader hdr=new ZABHeader(ZABHeader.FLUSH, key);
                Message forward_msg=new Message(coord, val).putHeader(this.id,hdr).setFlag(Message.Flag.DONT_BUNDLE);
                if(log.isTraceEnabled())
                    log.trace(local_addr + ": flushing (forwarding) " + local_addr + "::" + key + " to coord " + coord);
                ack_promise.reset();
                down_prot.down(new Event(Event.MSG, forward_msg));
                Long ack=ack_promise.getResult(500);
                if((ack != null && ack.equals(key)) || !forward_table.containsKey(key))
                    break;
            }
        }
    }


   protected void forwardToCoord(MessageInfo msgInfo) {
	   requestCache.add(msgInfo.getId());
	   if (is_coord){
		  queuedMessages.add(msgInfo);
	      log.info("[" + local_addr + "] "+" I am a leader received new request no forward msg (forward) "+msgInfo);

       }	   
	   else{
		   forward(msgInfo);
	   }
            
           
   }

    protected void forward(MessageInfo msgInfo) {
        Address target=coord;
        if(target == null)
            return;
       
	    log.info("[" + local_addr + "] "+"recieved msg (forward) "+msgInfo);
	
	    try {
	        ZABHeader hdr=new ZABHeader(ZABHeader.FORWARD, msgInfo);
	        Message forward_msg=new Message(target).putHeader(this.id,hdr);
	        down_prot.down(new Event(Event.MSG, forward_msg));
	     }
	    catch(Exception ex) {
	      log.error("failed forwarding message to " + msgInfo.getId(), ex);
	    }
      
    }
    
    public void sendACK(Message msg){
		log.info("follower, sending ack (sendAck)");

    	if (msg == null )
    		return;
    	
    	ZABHeader hdr = (ZABHeader) msg.getHeader(this.id);
    	
    	if (hdr == null)
    		return;
    	
    	if (hdr.getZxid() != lastZxidProposed + 1){
            log.warn("Got zxid 0x"
                    + Long.toHexString(hdr.getZxid())
                    + " expected 0x"
                    + Long.toHexString(lastZxidProposed + 1));
        }
    	
    	lastZxidProposed = hdr.getZxid();
		queuedProposalMessage.put(hdr.getZxid(), hdr.getMessageInfo());
		
		//send Ack to the leader
		
		ZABHeader hdrACK = new ZABHeader(ZABHeader.ACK, hdr.getZxid());
		Message ACKMessage = new Message(coord).putHeader(this.id, hdrACK);
		
		try{
    		//Message forwardMsg = new Message(null, Util.objectToByteBuffer(msg));
    		down_prot.down(new Event(Event.MSG, ACKMessage));     
         }catch(Exception ex) {
    		log.error("failed sending ACK message to Leader");
    	} 
		
		
    }
    
    
synchronized void processACK(Message msgACK, Address sender){
    	
    	log.info("Received ACK from " + sender);
    	ZABHeader hdr = (ZABHeader) msgACK.getHeader(this.id);	
    	long ackZxid = hdr.getZxid();

		if (lastZxidCommitted >= ackZxid) {
            if (log.isDebugEnabled()) {
                log.debug("proposal has already been committed, pzxid: 0x{} zxid: 0x{}",
                        Long.toHexString(lastZxidCommitted), Long.toHexString(ackZxid));
                log.info(Long.toHexString(lastZxidCommitted) + " " + Long.toHexString(ackZxid));
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
            log.debug("Count for zxid: " +
                    Long.toHexString(ackZxid)+" = "+ p.getAckCount());
        }
		
		log.info("quorum for msg " + ackZxid + "="+  isQuorum(p.getAckCount()));
		if(isQuorum(p.getAckCount())){
			
			if (ackZxid != lastZxidCommitted+1) {
                log.warn("Commiting zxid 0x{} from {} not first! "+
                        Long.toHexString(ackZxid)+" "+ sender);
                log.warn("First is 0x{}"+ Long.toHexString(lastZxidCommitted + 1));
            }
            outstandingProposals.remove(ackZxid);
           

            if (p.getMessageInfo() == null) {
                log.warn("Going to commmit null request for proposal: {}", p);
            }
            
            commit(ackZxid);	
		}
			
			
		}
		
		public void commit(long zxid){
			Address msgSrc = null;
	    	   synchronized(this){
	    	       lastZxidCommitted = zxid;
	    	   }
	    	   
	    	   //msgSrc = messageSource.get(zxid);
	       	   //log.info("message commit its sourse address is "+msgSrc);

	    	   ZABHeader hdrCommit = new ZABHeader(ZABHeader.COMMIT, zxid);
	    	   Message commitMessage = new Message(null).putHeader(id, hdrCommit);
	    	   
	    	   try{
	       		down_prot.down(new Event(Event.MSG, commitMessage));     
	            }catch(Exception ex) {
	       		log.error("failed sending commit message to members");
	       	} 

	    }
		
		public void deliver(Message toDeliver){
	    	MessageInfo msgInfo = null;
	    	Address src = null;
	    	ZABHeader hdr = (ZABHeader) toDeliver.getHeader(this.id);
	    	long zxid = hdr.getZxid();
	    	
	    	msgInfo = queuedProposalMessage.remove(zxid);
	    	log.info("[" + local_addr + "] "+ " Check proposal message object = " + msgInfo);

	    	queuedCommitMessage.put(zxid, msgInfo);
	    	log.info("[" + local_addr + "] "+ " commit request with MSG and zxid = " + " "+ msgInfo + " " + zxid);

	    	if (requestCache.contains(msgInfo.getId())){
	    		log.info("I am the zab request receiver, going to send response back to " + msgInfo.getId().getOriginator());
		    	ZABHeader hdrResponse = new ZABHeader(ZABHeader.RESPONSE, zxid,  msgInfo);
		    	Message msg = new Message(msgInfo.getId().getOriginator()).putHeader(this.id, hdrResponse);
	       		down_prot.down(new Event(Event.MSG, msg));     

	    	}
	    	
	    	   
	   }
		
		
		private void handleOrderingResponse(ZABHeader hdrResponse) {
	        if (log.isTraceEnabled())
	            log.trace("ZAB Ordering response received | " + hdrResponse);

	        MessageInfo messageInfo = hdrResponse.getMessageInfo();
	        Message message = messageStore.get(messageInfo.getId());
	        message.setDest(local_addr);
	        log.info("[ " + local_addr + "] " + "Received zab ordered for request " + message + " its zxid = " + hdrResponse);
	        up_prot.up(new Event(Event.MSG, message));

	        //broadcastMessage(viewManager.getDestinations(messageInfo), message);
	    }
	    	

		public boolean isQuorum(int majority){
			log.info(" acks =  " + majority + " majority "+ ((view.size()/2)+1));

	    	return majority >= ((view.size()/2) + 1)? true : false;
	    }

    protected void broadcast(final Message msg, boolean copy, Address original_sender, long seqno, boolean resend) {
        log.info("[ " + local_addr + "] " + "inside broadcast method");

    	Message bcast_msg=null;

        if(!copy) {
            log.info("[ " + local_addr + "]" + "inside broadcast method if(!copy)");
            bcast_msg=msg; // no need to add a header, message already has one
        }
        else {
            log.info("[ " + local_addr + "]" + "inside broadcast method making ZABHeader.WRAPPED_BCAST");
            ZABHeader new_hdr=new ZABHeader(ZABHeader.ACK, seqno);
            bcast_msg=new Message(null, msg.getRawBuffer(), msg.getOffset(), msg.getLength()).putHeader(this.id, new_hdr);

            if(resend) {
                new_hdr.flush_ack=true;
                bcast_msg.setFlag(Message.Flag.DONT_BUNDLE);
            }
        }

        if(log.isTraceEnabled())
            log.trace(local_addr + ": broadcasting " + original_sender + "::" + seqno);

        down_prot.down(new Event(Event.MSG,bcast_msg));
        bcast_msgs++;
    }




    protected void block() {
        send_lock.lock();
        try {
            while(flushing && running) {
                try {
                    send_cond.await();
                }
                catch(InterruptedException e) {
                }
            }
        }
        finally {
            send_lock.unlock();
        }
    }

    protected void unblockAll() {
        flushing=false;
        send_lock.lock();
        try {
            send_cond.signalAll();
            ack_promise.setResult(null);
        }
        finally {
            send_lock.unlock();
        }
    }

    protected synchronized void startFlusher(final Address new_coord) {
        if(flusher == null || !flusher.isAlive()) {
            if(log.isTraceEnabled())
                log.trace(local_addr + ": flushing started");
            // causes subsequent message sends (broadcasts and forwards) to block (https://issues.jboss.org/browse/JGRP-1495)
            flushing=true;
            
            flusher=new Flusher(new_coord);
            flusher.setName("Flusher");
            flusher.start();
        }
    }

    protected void stopFlusher() {
        flushing=false;
        Thread tmp=flusher;

        while(tmp != null && tmp.isAlive()) {
            tmp.interrupt();
            ack_promise.setResult(null);
            try {
                tmp.join();
            }
            catch(InterruptedException e) {
            }
        }
    }

/* ----------------------------- End of Private Methods -------------------------------- */

    protected class Flusher extends Thread {
        protected final Address new_coord;

        public Flusher(Address new_coord) {
            this.new_coord=new_coord;
        }

        public void run() {
            try {
                flush(new_coord);
            }
            catch (InterruptedException e) {
            }
        }
    }




    public static class ZABHeader extends Header {
         private static final byte REQUEST       = 1;
    	 private static final byte FORWARD       = 2;
         private static final byte PROPOSAL      = 3;
         private static final byte ACK           = 4;
         private static final byte COMMIT        = 5;
         private static final byte RESPONSE      = 6;
         private static final byte DELIVER       = 7;
         protected static final byte FLUSH       = 8;
         

        protected byte        type=-1;
        protected long        seqno=-1;
        protected Address     messageSource=null;
        protected MessageInfo   messageInfo=null;
        protected boolean     flush_ack;

        public ZABHeader() {
        }

        public ZABHeader( byte type, MessageInfo info) {
            this.type=type;
            this.messageInfo=info;
        }
        
        public ZABHeader(byte type) {
            this.type=type;
        }

        public ZABHeader(byte type, long seqno) {
            this(type);
            this.seqno=seqno;
        }
        public ZABHeader(byte type, long seqno, MessageInfo messageInfo) {
            this(type);
            this.seqno=seqno;
            this.messageInfo=messageInfo;
        }
        
        public ZABHeader(byte type, long seqno, Address messageSource) {
            this(type);
            this.seqno=seqno;
            this.messageSource=messageSource;
        }

     
        public String toString() {
            StringBuilder sb=new StringBuilder(64);
            sb.append(printType());
            if(seqno >= 0)
                sb.append(" seqno=" + seqno);
            if(flush_ack)
                sb.append(" (flush_ack)");
            return sb.toString();
        }

        protected final String printType() {
        	
        	switch(type) {
            case REQUEST:        return "REQUEST";
            case FORWARD:        return "FORWARD";
            case PROPOSAL:       return "PROPOSAL";
            case ACK:            return "ACK";
            case COMMIT:         return "COMMIT";
            case RESPONSE:         return "RESPONSE";
            case DELIVER:         return "DELIVER";
            default:             return "n/a";
        }
           
        }
        
        public long getZxid() {
            return seqno;
        }
        
        public MessageInfo getMessageInfo(){
        	return messageInfo;
        }
        
        public Address getSrc() {
            return messageSource;
        }
        
        public void setSrc(Address messageSource) {
            this.messageSource = messageSource;
        }


        public void writeTo(DataOutput out) throws Exception {
            out.writeByte(type);
            Bits.writeLong(seqno,out);
            Util.writeAddress(messageSource, out);
            messageInfo.writeTo(out);
            out.writeBoolean(flush_ack);
        }

        public void readFrom(DataInput in) throws Exception {
            type=in.readByte();
            seqno=Bits.readLong(in);
            messageSource = Util.readAddress(in);
            messageInfo.readFrom(in);
            flush_ack=in.readBoolean();
        }

        public int size() {
            return Global.BYTE_SIZE + (messageInfo != null ? messageInfo.size() : 0) + Bits.size(seqno) + (messageSource != null ? Util.size(messageSource) : 0); // type + seqno + flush_ack
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
            	
            	MessageInfo messageInfo = null;
            	messageInfo = queuedMessages.poll();
                 if (messageInfo == null) {
                	 try {
                		 messageInfo = queuedMessages.take();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
                 }
            	//Address sender = messgae.getSrc();
//                if(view != null && !view.containsMember(sender)) {
//                	log.info("Sender is not included");
//                    if(log.isErrorEnabled())
//                        log.error(local_addr + ": dropping FORWARD request from non-member " + sender +
//                                    "; view=" + view);
//                    return;
//                }
                

            	long new_zxid = getNewZxid();
            	ZABHeader hdrProposal = new ZABHeader(ZABHeader.PROPOSAL, new_zxid, messageInfo);                
                Message ProposalMessage=new Message(null).putHeader(this.id, hdrProposal);

                ProposalMessage.setSrc(local_addr);
            	Proposal p = new Proposal();
            	p.setMessageInfo(messageInfo);
            	p.AckCount++;
            	log.info("Zxid count for zxid = " + new_zxid + " count = "  +p.AckCount);
            	outstandingProposals.put(new_zxid, p);
            	queuedProposalMessage.put(new_zxid, messageInfo);
            	
            	try{
                 	log.info("Leader is about to sent a proposal " + ProposalMessage);
            		down_prot.down(new Event(Event.MSG, ProposalMessage));     
                 }catch(Exception ex) {
            		log.error("failed proposing message to members");
            	}    
            	
            }
            
        }

       
    }

}
