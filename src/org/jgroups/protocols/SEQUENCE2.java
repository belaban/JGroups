
package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.protocols.ZAB.ZABHeader;
import org.jgroups.protocols.jzookeeper.Proposal;
import org.jgroups.stack.Protocol;
import org.jgroups.util.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
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


/**
 * Implementation of total order protocol using a sequencer.
 * Consult <a href="https://github.com/belaban/JGroups/blob/master/doc/design/SEQUENCER.txt">SEQUENCER.txt</a> for details
 * @author Bela Ban
 */
@MBean(description="Implementation of total order protocol using a sequencer")
public class SEQUENCE2 extends Protocol {
    protected final AtomicLong        zxid=new AtomicLong(0);
    private ExecutorService executor;

    protected Address                           local_addr;
    protected volatile Address                  coord;
    protected volatile View                     view;
    protected volatile boolean                  is_coord=false;
    protected final AtomicLong                  seqno=new AtomicLong(0);

	private long lastZxidProposed=0, zxidACK=0, lastZxidCommitted=0;

	private Map<Long, Message> queuedCommitMessage = new HashMap<Long, Message>();
    private final LinkedBlockingQueue<Message> queuedMessages =
	        new LinkedBlockingQueue<Message>();
	private ConcurrentMap<Long, Proposal> outstandingProposals = new ConcurrentHashMap<Long, Proposal>();
	private Map<Long, Message> queuedProposalMessage = new HashMap<Long, Message>();


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
        unblockAll();
        stopFlusher();
        super.stop();
    }

    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
            	log.info("[" + local_addr + "] "+"received request (down) SEQUENCE2");
                Message msg=(Message)evt.getArg();
                if(msg.getDest() != null || msg.isFlagSet(Message.Flag.NO_TOTAL_ORDER) || msg.isFlagSet(Message.Flag.OOB))
                    break;

                if(msg.getSrc() == null)
                    msg.setSrc(local_addr);

                if(flushing){
                	log.info("invoking block() method (down)");

                    block();
                    
                }
                
                // A seqno is not used to establish ordering, but only to weed out duplicates; next_seqno doesn't need
                // to increase monotonically, but only to be unique (https://issues.jboss.org/browse/JGRP-1461) !
               // long next_seqno=seqno.incrementAndGet();
                try {
//                    SequencerHeader hdr=new SequencerHeader(is_coord? SequencerHeader.PROPOSAL : SequencerHeader.FORWARD);
//                    log.info("put new header (down) " + hdr.type);
//                    msg.putHeader(this.id, hdr);
//                    if(log.isTraceEnabled())
//                        log.trace("[" + local_addr + "]: forwarding " + local_addr + "::" + seqno + " to coord " + coord);

                    // We always forward messages to the coordinator, even if we're the coordinator. Having the coord
                    // send its messages directly led to starvation of messages from other members. MPerf perf went up
                    // from 20MB/sec/node to 50MB/sec/node with this change !
                    log.info("[" + local_addr + "] "+"invloking forwardToCoord method  (down)"+msg);

                    forwardToCoord(msg);
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
        SequencerHeader hdr;

        switch(evt.getType()) {
            case Event.MSG:
                msg=(Message)evt.getArg();
                if(msg.isFlagSet(Message.Flag.NO_TOTAL_ORDER) || msg.isFlagSet(Message.Flag.OOB))
                    break;
                hdr=(SequencerHeader)msg.getHeader(this.id);
                if(hdr == null)
                    break; // pass up
                log.info("[" + local_addr + "] "+ " received message from (up) " + msg.getSrc() + " type "+ hdr.type);

                switch(hdr.type) {
                    case SequencerHeader.FORWARD:
                    	
                    	if(!is_coord) {
                			if(log.isErrorEnabled())
                            log.error("[" + local_addr + "] "+ ": non-Leader; dropping FORWARD request from " + msg.getSrc());
                			return null;
                		 }
                		try {
                    		log.info("[" + local_addr + "] "+"Leader, puting in queuy");
                			queuedMessages.add(msg);
                		}
                		catch(Exception ex) {
                			log.error("failed forwarding message to " + msg.getDest(), ex);
                		}
                		return null;

                    case SequencerHeader.PROPOSAL:
                   	 log.info("[" + local_addr + "] "+"(up) inside PROPOSAL");

                   	if (!is_coord){
                		log.info("[" + local_addr + "] "+"follower, proposal message received, call senAck (up, proposal)");
            			sendACK(msg);
            		}
            		else 
                		log.info("[" + local_addr + "] "+"Leader, proposal message received ignoring it (up, proposal)");

            		return null;
            		
            		
                    case SequencerHeader.ACK:
                		log.info("["+local_addr+"] "+"follower, ACK message received, call senAck (up, proposal)");
                		if (is_coord){
                     		log.info("Leader, ack message received, call processACK(up, ACK)");
                			processACK(msg, msg.getSrc());
                			return null;
                		}
                    case SequencerHeader.COMMIT:
                		log.info("["+local_addr+"] "+"follower, commit message received, call deliver (up, COMMIT)");

                		 deliver(msg);
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

    protected void handleViewChange(View v) {
        List<Address> mbrs=v.getMembers();
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

                SequencerHeader hdr=new SequencerHeader(SequencerHeader.PROPOSAL, key);
                Message forward_msg=new Message(null, val).putHeader(this.id, hdr);
                if(log.isTraceEnabled())
                    log.trace(local_addr + ": flushing (broadcasting) " + local_addr + "::" + key);
                down_prot.down(new Event(Event.MSG, forward_msg));
            }
            return;
        }

        // for forwarded messages, we need to receive the forwarded message from the coordinator, to prevent this case:
        // - V1={A,B,C}
        // - A crashes
        // - C installs V2={B,C}
        // - C forwards messages 3 and 4 to B (the new coord)
        // - B drops 3 because its view is still V1
        // - B installs V2
        // - B receives message 4 and broadcasts it
        // ==> C's message 4 is delivered *before* message 3 !
        // ==> By resending 3 until it is received, then resending 4 until it is received, we make sure this won't happen
        // (see https://issues.jboss.org/browse/JGRP-1449)
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
                SequencerHeader hdr=new SequencerHeader(SequencerHeader.FLUSH, key);
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


   protected void forwardToCoord(Message msg) {
        //if(is_coord) {
        	//next_seqno=seqno.incrementAndGet();
            log.info("[ " + local_addr + "] "+"recieved msg (forwardToCoord) (if (is_coord) "+msg);
            forward(msg);
           // return;
        //}
        //else{
          //  forward(msg);
            //return;
        }

//        if(!running || flushing) {
//            log.info("[ " + local_addr + "] "+"recieved msg (forwardToCoord) if(!running || flushing) "+msg);
//
//            forward_table.put(next_seqno, msg);
//            return;
//        }
   
//        if(!ack_mode) {
//            forward_table.put(next_seqno, msg);
//            log.info("[ " + local_addr + "] "+"recieved msg (forwardToCoord) if(!ack_mode) "+msg);
//            forward(msg, next_seqno, false);
//
//            return;
//        }

//        /*send_lock.lock();
//        try {
//            forward_table.put(next_seqno, msg);
//            while(running && !flushing) {
//                log.info("[ " + local_addr + "] "+"recieved msg (forwardToCoord) while(running && !flushing) "+msg);
//
//                ack_promise.reset();
//                forward(msg, next_seqno, true);
//                if(!ack_mode || !running || flushing)
//                    break;
//                Long ack=ack_promise.getResult(500);
//                if((ack != null && ack.equals(next_seqno)) || !forward_table.containsKey(next_seqno))
//                    break;
//            }
//        }
//        finally {
//            send_lock.unlock();
//        }*/
   // }

    protected void forward(final Message msg) {
        Address target=coord;
        if(target == null)
            return;
        byte type=SequencerHeader.FORWARD;
        log.info("[" + local_addr + "] "+"recieved msg (forward) "+msg + " type " + type);

        try {
            SequencerHeader hdr=new SequencerHeader(type);
            Message forward_msg=new Message(target, Util.objectToByteBuffer(msg)).putHeader(this.id,hdr);
            down_prot.down(new Event(Event.MSG, forward_msg));
            forwarded_msgs++;
        }
        catch(Exception ex) {
            log.error("failed forwarding message to " + msg.getDest(), ex);
        }
    }
    
    public void sendACK(Message msg){
		log.info("follower, sending ack (sendAck)");

    	if (msg == null )
    		return;
    	
    	SequencerHeader hdr = (SequencerHeader) msg.getHeader(this.id);
    	
    	if (hdr == null)
    		return;
    	
    	if (hdr.getZxid() != lastZxidProposed + 1){
            log.warn("Got zxid 0x"
                    + Long.toHexString(hdr.getZxid())
                    + " expected 0x"
                    + Long.toHexString(lastZxidProposed + 1));
        }
    	
    	lastZxidProposed = hdr.getZxid();
		queuedProposalMessage.put(hdr.getZxid(), msg);
		
		//send Ack to the leader
		
		SequencerHeader hdrACK = new SequencerHeader(SequencerHeader.ACK, hdr.getZxid());
		Message ACKMessage = new Message(coord).putHeader(this.id, hdrACK);
		
		try{
    		//Message forwardMsg = new Message(null, Util.objectToByteBuffer(msg));
    		down_prot.down(new Event(Event.MSG, ACKMessage));     
         }catch(Exception ex) {
    		log.error("failed sending ACK message to Leader");
    	} 
		
		
		//Proposal p = outstandingProposal.get(hdr.getZxid());
		//p.ackSet.add(hdr.getZxid());
		
    	
    }
    
    
synchronized void processACK(Message msgACK, Address sender){
    	
    	log.info("Received ACK from " + sender);
    	SequencerHeader hdr = (SequencerHeader) msgACK.getHeader(this.id);	
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
            log.debug("Count for zxid: 0x{} is {}" +
                    Long.toHexString(ackZxid)+" "+ p.getAckCount());
        }
		
		log.info("quorum for msg " + ackZxid + "="+  isQuorum(p.getAckCount()));
		if(isQuorum(p.getAckCount())){
			
			if (zxid.get() != lastZxidCommitted+1) {
                log.warn("Commiting zxid 0x{} from {} not first! "+
                        Long.toHexString(ackZxid)+" "+ sender);
                log.warn("First is 0x{}"+ Long.toHexString(lastZxidCommitted + 1));
            }
            outstandingProposals.remove(ackZxid);
           

            if (p.getMessage() == null) {
                log.warn("Going to commmit null request for proposal: {}", p);
            }
            
            commit(ackZxid);	
		}
			
			
		}
		
		public void commit(long zxid){
	    	   synchronized(this){
	    	       lastZxidCommitted = zxid;
	    	   }
	    	   
	    	   SequencerHeader hdrCommit = new SequencerHeader(SequencerHeader.COMMIT, zxid);
	    	   Message commitMessage = new Message(null).putHeader(id, hdrCommit);
	    	   
	    	   try{
	       		down_prot.down(new Event(Event.MSG, commitMessage));     
	            }catch(Exception ex) {
	       		log.error("failed sending commit message to members");
	       	} 

	    }
		
		public void deliver(Message toDeliver){
	    	Message msg = null;
	    	SequencerHeader hdr = (SequencerHeader) toDeliver.getHeader(this.id);
	    	long zxid = hdr.getZxid();
	    	
	    //	if (!is_coord){
	    		msg = queuedProposalMessage.remove(zxid);
	    		
		    	//if (!is_coord && msg == null)
		          // 	log.warn("No message pending for zxid" + zxid);
		    		
//		    	if (queuedCommitMessage.containsKey(zxid)){
//		           	log.warn("message is already delivered for zxid" + zxid);
//		           	return;
//		    	}

	    	queuedCommitMessage.put(zxid, msg);
	    	log.info("[" + local_addr + "] "+ " commitet request with zxid = "+zxid);
	    	   
	    	}
	    	//log.info("about to send responce back to client");
	        //down_prot.up(new Event(Event.MSG,msg));

	    	
	    

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
            log.info("[ " + local_addr + "]" + "inside broadcast method making SequencerHeader.WRAPPED_BCAST");
            SequencerHeader new_hdr=new SequencerHeader(SequencerHeader.ACK, seqno);
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




    public static class SequencerHeader extends Header {
    	
    	 private static final byte FORWARD       = 1;
         private static final byte PROPOSAL      = 2;
         private static final byte ACK           = 3;
         private static final byte COMMIT        = 4;
         protected static final byte FLUSH       = 5;
         
    	
//        protected static final byte FORWARD       = 1;
//        protected static final byte FLUSH         = 2;
//        protected static final byte BCAST         = 3;
//        protected static final byte WRAPPED_BCAST = 4;

        protected byte    type=-1;
        protected long    seqno=-1;
        protected boolean flush_ack;

        public SequencerHeader() {
        }

        public SequencerHeader(byte type) {
            this.type=type;
        }

        public SequencerHeader(byte type, long seqno) {
            this(type);
            this.seqno=seqno;
        }

        public long getSeqno() {
            return seqno;
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
            case FORWARD:        return "FORWARD";
            case PROPOSAL:       return "PROPOSAL";
            case ACK:            return "ACK";
            case COMMIT:         return "COMMIT";
            default:             return "n/a";
        }
           
        }
        
        public long getZxid() {
            return seqno;
        }


        public void writeTo(DataOutput out) throws Exception {
            out.writeByte(type);
            Bits.writeLong(seqno,out);
            out.writeBoolean(flush_ack);
        }

        public void readFrom(DataInput in) throws Exception {
            type=in.readByte();
            seqno=Bits.readLong(in);
            flush_ack=in.readBoolean();
        }

        public int size() {
            return Global.BYTE_SIZE + Bits.size(seqno) + Global.BYTE_SIZE; // type + seqno + flush_ack
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
            	//Message msg = (Message)Util.objectFromByteBuffer(messgae.getRawBuffer(), messgae.getOffset(), messgae.getLength());
            	Address sender = messgae.getSrc();
            	//log.info("view is = "+view.containsMember(sender));
             	//log.info("view is = "+view);
                if(view != null && !view.containsMember(sender)) {
                	log.info("Sender is not included");
                    if(log.isErrorEnabled())
                        log.error(local_addr + ": dropping FORWARD request from non-member " + sender +
                                    "; view=" + view);
                    return;
                }
                

            	long new_zxid = getNewZxid();
            	SequencerHeader hdrProposal = new SequencerHeader(SequencerHeader.PROPOSAL, new_zxid);                
                Message ProposalMessage=new Message(null, messgae.getRawBuffer(), messgae.getOffset(), messgae.getLength()).putHeader(this.id, hdrProposal);
                ProposalMessage.setSrc(local_addr);
            	Proposal p = new Proposal();
            	p.setMessage(messgae);
            	p.setMessageSrc(messgae.getSrc());
            	p.AckCount++;
            	log.info("ACK nums = "+p.AckCount);
            	outstandingProposals.put(zxid.get(), p);
            	queuedProposalMessage.put(new_zxid, messgae);
            	
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