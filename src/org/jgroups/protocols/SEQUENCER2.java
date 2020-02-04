
package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.Experimental;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.stack.Protocol;
import org.jgroups.util.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Supplier;


/**
 * Implementation of total order protocol using a sequencer_uum.
 * 
 * Todo 1: on a sequencer change, the new coordinator needs to determine the highest seqno from all members
 * Todo 2: on a sequencer change, if a member has pendindg messages in the forward-queue, they need to be resent
 * Todo 3: this protocol is currently broken, as a new member doesn't get the highest seqno and thus creates its table
 *         at offset=0, which means it will queue all messages higher than 0, and eventually run out of memory!!!
 * 
 * @author Bela Ban
 * @edited Andrei Palade
 */
@Experimental
@MBean(description="Implementation of total order protocol using a sequencer (unicast-unicast-multicast)")
public class SEQUENCER2 extends Protocol {
    protected Address                           local_addr;
    protected volatile Address                  coord;
    protected volatile View                     view;
    protected volatile boolean                  is_coord=false;
    protected final AtomicLong                  seqno=new AtomicLong(0); // only used by the sequencer

    // messages to be multicast are added to this queue; when seqnos are received from the sequencer, we remove and
    // send messages from the queue
    protected final BlockingQueue<Message>      fwd_queue=new LinkedBlockingQueue<>(20000); // make this configurable

    // the number of seqno requests sent to the sequencer
    protected final AtomicInteger               seqno_reqs=new AtomicInteger(0);


    protected volatile boolean                  running=true;

    protected static final BiConsumer<MessageBatch,Message> BATCH_ACCUMULATOR=MessageBatch::add;


    @ManagedAttribute protected long request_msgs;
    @ManagedAttribute protected long response_msgs;

    @ManagedAttribute protected long bcasts_sent;
    @ManagedAttribute protected long bcasts_received;
    @ManagedAttribute protected long bcasts_delivered;

    @ManagedAttribute protected long sent_requests;
    @ManagedAttribute protected long received_requests;
    @ManagedAttribute protected long sent_responses;
    @ManagedAttribute protected long received_responses;

    protected Table<Message>  received_msgs = new Table<>();

    @ManagedAttribute
    public boolean isCoordinator()   {return is_coord;}
    public Address getCoordinator()  {return coord;}
    public Address getLocalAddress() {return local_addr;}

    @ManagedAttribute(description="Number of messages in the forward-queue")
    public int getFwdQueueSize() {return fwd_queue.size();}
    

    @ManagedOperation
    public void resetStats() {
        request_msgs=response_msgs=bcasts_sent=bcasts_received=bcasts_delivered=0L;
        sent_requests=received_requests=sent_responses=received_responses=0L; // reset number of sent and received requests and responses
    }


	public void start() throws Exception {
		super.start();
		running = true;
	}

    public void stop() {
        running=false;
        super.stop();
    }

    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.VIEW_CHANGE:
                handleViewChange(evt.getArg());
                break;

            case Event.TMP_VIEW:
                handleTmpView(evt.getArg());
                break;

            case Event.SET_LOCAL_ADDRESS:
                local_addr=evt.getArg();
                break;
        }
        return down_prot.down(evt);
    }


    public Object down(Message msg) {
        if(msg.getDest() != null || msg.isFlagSet(Message.Flag.NO_TOTAL_ORDER) || msg.isFlagSet(Message.Flag.OOB))
            return down_prot.down(msg);

        if(msg.getSrc() == null)
            msg.setSrc(local_addr);

        try {
            fwd_queue.put(msg);
            if(seqno_reqs.getAndIncrement() == 0) {
                int num_reqs=seqno_reqs.get();
                sendSeqnoRequest(num_reqs);
            }
        }
        catch(InterruptedException e) {
            if(!running)
                return null;
            throw new RuntimeException(e);
        }
        return null; // don't pass down
    }

    public Object up(Event evt) {
        switch(evt.getType()) {
            case Event.VIEW_CHANGE:
                Object retval=up_prot.up(evt);
                handleViewChange(evt.getArg());
                return retval;

            case Event.TMP_VIEW:
                handleTmpView(evt.getArg());
                break;
        }
        return up_prot.up(evt);
    }

    public Object up(Message msg) {
        SequencerHeader hdr;

        if(msg.isFlagSet(Message.Flag.NO_TOTAL_ORDER) || msg.isFlagSet(Message.Flag.OOB))
            return up_prot.up(msg);
        hdr=msg.getHeader(this.id);
        if(hdr == null)
            return up_prot.up(msg); // pass up
                                
        switch(hdr.type) {
            case SequencerHeader.REQUEST:
                if(!is_coord) {
                    log.error("%s: non-coord; dropping REQUEST request from %s", local_addr, msg.getSrc());
                    return null;
                }
                Address sender=msg.getSrc();
                if(view != null && !view.containsMember(sender)) {
                    log.error("%s : dropping REQUEST from non-member %s; view=%s" + view, local_addr, sender, view);
                    return null;
                }

                long new_seqno=seqno.getAndAdd(hdr.num_seqnos) +1;
                sendSeqnoResponse(sender, new_seqno, hdr.num_seqnos);
                        
                received_requests++;
                break;
                        
            case SequencerHeader.RESPONSE:
                Address coordinator=msg.getSrc();
                if(view != null && !view.containsMember(coordinator)) {
                    log.error(local_addr + "%s: dropping RESPONSE from non-coordinator %s; view=%s", local_addr, coordinator, view);
                    return null;
                }

                long send_seqno=hdr.seqno;
                for(int i=0; i < hdr.num_seqnos; i++) {
                    Message bcast_msg=fwd_queue.poll();
                    if(bcast_msg == null) {
                        log.error(Util.getMessage("Received%DSeqnosButFwdqueueIsEmpty"), hdr.num_seqnos);
                        break;
                    }

                    if(log.isTraceEnabled())
                        log.trace("%s: broadcasting %d", local_addr, send_seqno);
                    broadcast(bcast_msg, send_seqno++);
                }
                int num_reqs=0;
                if((num_reqs=seqno_reqs.addAndGet(-hdr.num_seqnos)) > 0 && num_reqs > 0)
                    sendSeqnoRequest(num_reqs);
                break;
                    
            case SequencerHeader.BCAST:
                deliver(msg, hdr);
                bcasts_received++;
                break;
        }
        return null;
    }

    /* public void up(MessageBatch batch) { // todo: better impl: add seq messages into the table in 1 op
        List<Tuple<Long,Message>> msgs=null;
        for(Iterator<Message> it=batch.iterator(); it.hasNext();) {
            final Message msg=it.next();
            if(msg == null || msg.isFlagSet(Message.Flag.NO_TOTAL_ORDER) || msg.isFlagSet(Message.Flag.OOB))
                continue;
            SequencerHeader hdr=(SequencerHeader)msg.getHeader(id);
            if(hdr == null)
                continue;

            it.remove(); // we have a header; remove the message from the batch, so it won't be passed up the stack

            switch(hdr.type) {
                case SequencerHeader.REQUEST:
                case SequencerHeader.RESPONSE:
                    up(msg);
                    break;

                case SequencerHeader.BCAST:
                    if(msgs == null)
                        msgs=new ArrayList<Tuple<Long,Message>>(batch.size());
                    msgs.add(new Tuple<Long,Message>(hdr.seqno, msg));
                    break;

                default:
                    log.error(Util.getMessage("HeaderTypeNotKnown"), local_addr, hdr.type);
            }
        }

        if(msgs != null) {
            Address sender=batch.sender();
            if(sender == null) {
                log.error(local_addr + ": sender is null, cannot deliver batch " + "::" + batch);
                return;
            }

            final Table<Message> win=received_msgs;

            System.out.println("<--B " + batch.sender() + "::" + batch);

            win.add(msgs);

            final AtomicBoolean processing=win.getProcessing();
            if(processing.compareAndSet(false, true))
                removeAndDeliver(processing, win, sender);
        }


        if(!batch.isEmpty())
            up_prot.up(batch);
    }*/

    public void up(MessageBatch batch) {
        MessageIterator it=batch.iterator();
        while(it.hasNext()) {
            Message msg=it.next();
            if(msg.isFlagSet(Message.Flag.NO_TOTAL_ORDER) || msg.isFlagSet(Message.Flag.OOB) || msg.getHeader(id) == null)
                continue;
            it.remove();

            // simplistic implementation
            try {
                up(msg);
            }
            catch(Throwable t) {
                log.error(Util.getMessage("FailedPassingUpMessage"), t);
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

        Address existing_coord=coord, new_coord=mbrs.get(0);
        boolean coord_changed=!Objects.equals(existing_coord, new_coord);
        if(coord_changed && new_coord != null) {
            coord=new_coord;

            // todo: if I'm the new coord, get the highest seqno from all members. If not, re-send my pending seqno reqs
        }
        if(new_coord != null)
            is_coord=new_coord.equals(local_addr);
    }


    // If we're becoming coordinator, we need to handle TMP_VIEW as
    // an immediate change of view. See JGRP-1452.
    private void handleTmpView(View v) {
        Address new_coord=v.getCoord();
        if(new_coord != null && !new_coord.equals(coord) && local_addr != null && local_addr.equals(new_coord))
            handleViewChange(v);
    }


    
    protected void sendSeqnoRequest(int num_seqnos) {
        Address target=coord;
        if(target == null)
            return;
        SequencerHeader hdr=new SequencerHeader(SequencerHeader.REQUEST, 0, num_seqnos);
        Message forward_msg=new EmptyMessage(target).putHeader(this.id, hdr);
        down_prot.down(forward_msg);
        sent_requests++;
    }
    
	protected void sendSeqnoResponse(Address original_sender,long seqno, int num_seqnos) {
		SequencerHeader hdr = new SequencerHeader(SequencerHeader.RESPONSE, seqno, num_seqnos);
		Message ucast_msg = new EmptyMessage(original_sender).putHeader(this.id, hdr);
		
        if (log.isTraceEnabled())
            log.trace(local_addr + ": sending seqno response to " + original_sender + ":: new_seqno=" + seqno + ", num_seqnos=" + num_seqnos);

        down_prot.down(ucast_msg);
        sent_responses++;
	}
    
    protected void broadcast(final Message msg, long seqno) {
        msg.putHeader(this.id, new SequencerHeader(SequencerHeader.BCAST, seqno));
 
        if(log.isTraceEnabled())
            log.trace(local_addr + ": broadcasting ::" + seqno);

        down_prot.down(msg);
        bcasts_sent++;
    }



    protected void deliver(Message msg, SequencerHeader hdr) {
        Address sender=msg.getSrc();
        if(sender == null) {
            if(log.isErrorEnabled())
                log.error(local_addr + ": sender is null, cannot deliver " + "::" + hdr.getSeqno());
            return;
        }
        
        final Table<Message> win=received_msgs;
        win.add(hdr.seqno, msg);
        removeAndDeliver(win, sender);
    }
    
    
    protected void removeAndDeliver(Table<Message> win, Address sender) {
        AtomicInteger adders=win.getAdders();
        if(adders.getAndIncrement() != 0)
            return;

        final MessageBatch     batch=new MessageBatch(win.size()).dest(local_addr).sender(sender).multicast(false);
        Supplier<MessageBatch> batch_creator=() -> batch;
        do {
            try {
                batch.reset();
                win.removeMany(true, 0, null, batch_creator, BATCH_ACCUMULATOR);
            }
            catch(Throwable t) {
                log.error("failed removing messages from table for " + sender, t);
            }
            if(!batch.isEmpty()) {
                // batch is guaranteed to NOT contain any OOB messages as the drop_oob_msgs_filter removed them
                deliverBatch(batch);
            }
        }
        while(adders.decrementAndGet() != 0);
    }


    protected void deliverBatch(MessageBatch batch) {
        try {
            if(batch.isEmpty())
                return;
            if(log.isTraceEnabled()) {
                Message first=batch.first(), last=batch.last();
                StringBuilder sb=new StringBuilder(local_addr + ": delivering");
                if(first != null && last != null) {
                    SequencerHeader hdr1=first.getHeader(id), hdr2=last.getHeader(id);
                    sb.append(" #").append(hdr1.seqno).append(" - #").append(hdr2.seqno);
                }
                sb.append(" (" + batch.size()).append(" messages)");
                log.trace(sb);
            }
            up_prot.up(batch);
        }
        catch(Throwable t) {
            log.error(Util.getMessage("FailedToDeliverMsg"), local_addr, "batch", batch, t);
        }
    }



    /* ----------------------------- End of Private Methods -------------------------------- */
    

    public static class SequencerHeader extends Header {
        protected static final byte REQUEST       = 1;
        protected static final byte BCAST         = 2;
        protected static final byte RESPONSE      = 3;

        protected byte    type;
        protected long    seqno;
        protected int     num_seqnos=1; // the number of seqnos requested (REQUEST) or returned (on a RESPONSE)

        public SequencerHeader() {}

        public SequencerHeader(byte type) {this.type=type;}

        public SequencerHeader(byte type, long seqno) {
            this(type, seqno, 1);
        }

        public SequencerHeader(byte type, long seqno, int num_seqnos) {
            this(type);
            this.seqno=seqno;
            this.num_seqnos=num_seqnos;
        }
        public short getMagicId() {return 86;}
        public Supplier<? extends Header> create() {
            return SequencerHeader::new;
        }

        public long getSeqno() {return seqno;}
        
        public String toString() {
            StringBuilder sb=new StringBuilder(64);
            sb.append(printType());
            if(seqno >= 0)
                sb.append(" seqno=" + seqno);
            if(num_seqnos > 1)
                sb.append(", num_seqnos=" + num_seqnos);
            return sb.toString();
        }

        protected final String printType() {
            switch(type) {
                case REQUEST:        return "REQUEST";
                case BCAST:          return "BCAST";
                case RESPONSE:	     return "RESPONSE";
                default:             return "n/a";
            }
        }

        @Override
        public void writeTo(DataOutput out) throws IOException {
            out.writeByte(type);
            Bits.writeLong(seqno,out);
            out.writeShort(num_seqnos);
        }

        @Override
        public void readFrom(DataInput in) throws IOException {
            type=in.readByte();
            seqno=Bits.readLong(in);
            num_seqnos=in.readUnsignedShort();
        }

        // type + seqno + localSeqno + flush_ack
        @Override
        public int serializedSize() {
            return Global.BYTE_SIZE + Bits.size(seqno) + Global.SHORT_SIZE;
        }
    }

}
