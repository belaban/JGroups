package org.jgroups.protocols.jzookeeper;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
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
import org.jgroups.protocols.SEQUENCER.SequencerHeader;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Bits;
import org.jgroups.util.BoundedHashMap;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Promise;
import org.jgroups.util.Util;

/**
 * Implementation of Zookeeper Atmoic Broadcast protocol using JGroups.
 * @author Bela Ban and Ibrahim EL-Sanosi
 */
public class ZABEx extends Protocol {
	
	private Address                           local_addr;
	private volatile Address                  leader;
	private volatile View                     view;
	private volatile boolean                  isLeader=false;
	private long                        zxid=0;
    private long                              seqForwardMessage = 0;
    private int 					            Quorum = (view.size()/2) + 1;
    private ConcurrentMap<Long,Proposal>  outstandingProposal = new ConcurrentHashMap<Long,Proposal>();

    /** Maintains messages forwarded to the Leader which which no ack has been received yet.
     *  Needs to be sorted so we resend them in the right order
     */
    private final ConcurrentMap<Long,Message>  forwardMessage = new ConcurrentHashMap<Long,Message>();

    private volatile boolean                  running=true;

    // Maintains received seqnos, so we can weed out dupes
    private final ConcurrentMap<Long,Message> deliveriedMessage= Util.createConcurrentMap();
    
    private ArrayList<Address> follower = new ArrayList<Address>();

    private long forwardedMsgsNum=0;
    private long proposalMsgNum=0;
    private long commitMsgNum=0;

    public boolean isLeader() {return isLeader;}
    public Address getCoordinator() {return leader;}
    public Address getLocalAddress() {return local_addr;}
    
    public int getForwardTableSize() {return forwardMessage.size();}

    public void resetStats() {
    	forwardedMsgsNum=proposalMsgNum=commitMsgNum=0L;
    }
    
    public Map<String,Object> dumpStats() {
        Map<String,Object> m=super.dumpStats();
        m.put("forwarded",forwardedMsgsNum);
        m.put("proposal",proposalMsgNum);
        m.put("commit", commitMsgNum);
        return m;
    }
    
    public String printStats() {
        return dumpStats().toString();

    }

    public void start() throws Exception {
        super.start();
        running=true;
    }

    public void stop() {
        running=false;
        super.stop();
    }

    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                Message msg=(Message)evt.getArg();
                if(msg.getDest() != null || msg.isFlagSet(Message.Flag.NO_TOTAL_ORDER) || msg.isFlagSet(Message.Flag.OOB))
                    break;

                if(msg.getSrc() == null)
                    msg.setSrc(local_addr);

                if (isLeader())
                	zxid++;
                try {
                    ZookeeperHeader hdr=new ZookeeperHeader(isLeader()? ZookeeperHeader.BCAST : ZookeeperHeader.WRAPPED_BCAST, zxid);
                    msg.putHeader(this.id, hdr);
                    if(log.isTraceEnabled())
                        //log.trace("[" + local_addr + "]: forwarding " + local_addr + "::" + seqno + " to coord " + coord);

                    // We always forward messages to the coordinator, even if we're the coordinator. Having the coord
                    // send its messages directly led to starvation of messages from other members. MPerf perf went up
                    // from 20MB/sec/node to 50MB/sec/node with this change !
                    forwardToCoord(zxid, msg);
                }
                catch(Exception ex) {
                    log.error("failed sending message", ex);
                }
                finally {
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
        ZookeeperHeader hdr;

        switch(evt.getType()) {
            case Event.MSG:
                msg=(Message)evt.getArg();
                if(msg.isFlagSet(Message.Flag.NO_TOTAL_ORDER) || msg.isFlagSet(Message.Flag.OOB))
                    break;
                hdr=(ZookeeperHeader)msg.getHeader(this.id);
                if(hdr == null)
                    break; // pass up

                switch(hdr.type) {
                    case ZookeeperHeader.FORWARD:
                    case ZookeeperHeader.FLUSH:
                        if(!isLeader) {
                            if(log.isErrorEnabled())
                                log.error(local_addr + ": non-coord; dropping FORWARD request from " + msg.getSrc());
                            return null;
                        }
                        Address sender=msg.getSrc();
                        if(view != null && !view.containsMember(sender)) {
                            if(log.isErrorEnabled())
                                log.error(local_addr + ": dropping FORWARD request from non-member " + sender +
                                            "; view=" + view);
                            return null;
                        }

                        broadcast(msg, true, msg.getSrc(), hdr.seqno, false); // do copy the message
                        forwardedMsgsNum++;                        break;

                    case ZookeeperHeader.BCAST:
                        deliver(msg, evt, hdr);
                        proposalMsgNum++;
                        break;

                    case ZookeeperHeader.WRAPPED_BCAST:
                        unwrapAndDeliver(msg, hdr.flush_ack);  // unwrap the original message (in the payload) and deliver it
                        proposalMsgNum++;
                        break;
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

        deliveriedMessage.keySet().retainAll(mbrs);

        Address existing_coord=leader, new_coord=mbrs.get(0);
        boolean coord_changed=existing_coord == null || !existing_coord.equals(new_coord);
        if(coord_changed && new_coord != null) {
            startFlusher(new_coord); // needs to be done in the background, to prevent blocking if down() would block
        }
    }

    protected void flush(final Address new_coord) throws InterruptedException {
        // wait until all threads currently sending messages have returned (new threads after flushing=true) will block
        // flushing is set to true in startFlusher()
        

        try {
            if(log.isTraceEnabled())
                log.trace(local_addr + ": coord changed from " + leader + " to " + new_coord);
            leader=new_coord;
            isLeader=local_addr != null && local_addr.equals(leader);
        }
        finally {
            if(log.isTraceEnabled())
                log.trace(local_addr + ": flushing completed");
           
        }
    }


    // If we're becoming coordinator, we need to handle TMP_VIEW as
    // an immediate change of view. See JGRP-1452.
    private void handleTmpView(View v) {
        List<Address> mbrs=v.getMembers();
        if(mbrs.isEmpty()) return;

        Address new_coord=mbrs.get(0);
        if(!new_coord.equals(leader) && local_addr != null && local_addr.equals(new_coord))
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
    
   protected void forwardToCoord(long seqno, Message msg) {
        if(isLeader) {
            forward(msg, seqno, false);
            return;
        }


    }

    protected void forward(final Message msg, long seqno, boolean flush) {
        Address target=leader;
        if(target == null)
            return;
        byte type=flush? ZookeeperHeader.FLUSH : ZookeeperHeader.FORWARD;
        try {
            SequencerHeader hdr=new SequencerHeader(type, seqno);
            Message forward_msg=new Message(target, Util.objectToByteBuffer(msg)).putHeader(this.id,hdr);
            down_prot.down(new Event(Event.MSG, forward_msg));
            forwardedMsgsNum++;
        }
        catch(Exception ex) {
            log.error("failed forwarding message to " + msg.getDest(), ex);
        }
    }

    protected void broadcast(final Message msg, boolean copy, Address original_sender, long seqno, boolean resend) {
        Message bcast_msg=null;

        if(!copy) {
            bcast_msg=msg; // no need to add a header, message already has one
        }
        else {
            ZookeeperHeader new_hdr=new ZookeeperHeader(ZookeeperHeader.WRAPPED_BCAST, seqno);
            bcast_msg=new Message(null, msg.getRawBuffer(), msg.getOffset(), msg.getLength()).putHeader(this.id, new_hdr);
            if(resend) {
                bcast_msg.setFlag(Message.Flag.DONT_BUNDLE);
            }
        }

        if(log.isTraceEnabled())
            log.trace(local_addr + ": broadcasting " + original_sender + "::" + seqno);

        down_prot.down(new Event(Event.MSG,bcast_msg));
        proposalMsgNum++;
    }



    /**
     * Unmarshal the original message (in the payload) and then pass it up (unless already delivered)
     * @param msg
     */
    protected void unwrapAndDeliver(final Message msg, boolean flush_ack) {
        try {
            Message msg_to_deliver=(Message)Util.objectFromByteBuffer(msg.getRawBuffer(), msg.getOffset(), msg.getLength());
            ZookeeperHeader hdr=(ZookeeperHeader)msg_to_deliver.getHeader(this.id);
            if(flush_ack)
                hdr.flush_ack=true;
            deliver(msg_to_deliver, new Event(Event.MSG, msg_to_deliver), hdr);
        }
        catch(Exception ex) {
            log.error("failure unmarshalling buffer", ex);
        }
    }


    protected void deliver(Message msg, Event evt, ZookeeperHeader hdr) {
        Address sender=msg.getSrc();
        if(sender == null) {
            if(log.isErrorEnabled())
                log.error(local_addr + ": sender is null, cannot deliver " + "::" + hdr.getSeqno());
            return;
        }
        long msg_seqno=hdr.getSeqno();
        if(sender.equals(local_addr)) {
        	forwardMessage.remove(msg_seqno);
            
        }
        if(!canDeliver(sender, msg_seqno)) {
            if(log.isWarnEnabled())
                log.warn(local_addr + ": dropped duplicate message " + sender + "::" + msg_seqno);
            return;
        }
        if(log.isTraceEnabled())
            log.trace(local_addr + ": delivering " + sender + "::" + msg_seqno);
        up_prot.up(evt);
        proposalMsgNum++;
    }


    /**
     * Checks if seqno has already been received from sender. This weeds out duplicates.
     * Note that this method is never called concurrently for the same sender, as the sender in NAKACK will always be
     * the coordinator.
     */
    protected boolean canDeliver(Address sender, long seqno) {
        
        return true;
    }

    protected void block() {
       
    }

    protected void unblockAll() {
        
    }

    protected synchronized void startFlusher(final Address new_coord) {
        
    }

   

    public static class ZookeeperHeader extends Header {
        protected static final byte FORWARD       = 1;
        protected static final byte FLUSH         = 2;
        protected static final byte BCAST         = 3;
        protected static final byte WRAPPED_BCAST = 4;

        protected byte    type=-1;
        protected long    seqno=-1;
        protected boolean flush_ack;

        public ZookeeperHeader() {
        }

        public ZookeeperHeader(byte type) {
            this.type=type;
        }

        public ZookeeperHeader(byte type, long seqno) {
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
                case FLUSH:          return "FLUSH";
                case BCAST:          return "BCAST";
                case WRAPPED_BCAST:  return "WRAPPED_BCAST";
                default:             return "n/a";
            }
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

}