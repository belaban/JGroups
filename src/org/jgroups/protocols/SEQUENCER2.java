
package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.Experimental;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Bits;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Table;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Implementation of total order protocol using a sequencer_uum.
 * 
 * Todo 1: on a sequencer change, the new coordinator needs to determine the highest seqno from all members
 * Todo 2: on a sequencer change, if a member has pendindg messages in the forward-queue, they need to be resent
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



    protected long request_msgs=0;
    protected long response_msgs=0;
    protected long bcast_msgs=0;
    
    protected long received_bcasts=0;
    protected long delivered_bcasts=0;
    protected long broadcasts_sent=0;
    
    protected long sent_requests=0;    
    protected long received_requests=0;
    protected long sent_responses=0;
    protected long received_responses=0;

    protected Table<Message>  received_msgs = new Table<>();
    private int max_msg_batch_size = 100;

    @ManagedAttribute
    public boolean isCoordinator() {return is_coord;}
    public Address getCoordinator() {return coord;}
    public Address getLocalAddress() {return local_addr;}
    @ManagedAttribute
    public long getBroadcast() {return bcast_msgs;}
    @ManagedAttribute
    public long getReceivedRequests() {return received_requests;}
    @ManagedAttribute
    public long getReceivedBroadcasts() {return received_bcasts;}

    @ManagedAttribute(description="Number of messages in the forward-queue")
    public int getFwdQueueSize() {return fwd_queue.size();}
    

    @ManagedOperation
    public void resetStats() {
        request_msgs=response_msgs=bcast_msgs=received_bcasts=delivered_bcasts=broadcasts_sent=0L;
        sent_requests=received_requests=sent_responses=received_responses=0L; // reset number of sent and received requests and responses
    }

    @ManagedOperation
    public Map<String,Object> dumpStats() {
        Map<String,Object> m=super.dumpStats();
        m.put("requests", request_msgs);
        m.put("responses", response_msgs);
        m.put("broadcast",bcast_msgs);
        
        m.put("sent_requests", sent_requests);
        m.put("received_requests", received_requests);
        m.put("sent_responses", sent_responses);
        m.put("received_responses", received_responses);
        
        m.put("received_bcasts",   received_bcasts);
        m.put("delivered_bcasts",  delivered_bcasts);
        m.put("broadcasts_sent",   broadcasts_sent);
        return m;
    }

    @ManagedOperation
    public String printStats() {
        return dumpStats().toString();
    }


	public void start() throws Exception {
		super.start();
		running = true;
	}

    public void stop() {
        running=false;
        super.stop();
    }

    public long getBroadcastsSent(){
    	return broadcasts_sent;
    }
    
    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
            	
                Message msg=(Message)evt.getArg();
                if(msg.getDest() != null || msg.isFlagSet(Message.Flag.NO_TOTAL_ORDER) || msg.isFlagSet(Message.Flag.OOB))
                    break;
                
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
                        if((num_reqs=seqno_reqs.addAndGet(-hdr.num_seqnos)) > 0) {
                            if(num_reqs > 0)
                                sendSeqnoRequest(num_reqs);
                        }
	                    break;
                    
                    case SequencerHeader.BCAST:
                        deliver(msg, evt, hdr);
                        received_bcasts++;
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
                    up(new Event(Event.MSG, msg));
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
        for(Message msg: batch) {
            if(msg.isFlagSet(Message.Flag.NO_TOTAL_ORDER) || msg.isFlagSet(Message.Flag.OOB) || msg.getHeader(id) == null)
                continue;
            batch.remove(msg);

            // simplistic implementation
            try {
                up(new Event(Event.MSG, msg));
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
        boolean coord_changed=existing_coord == null || !existing_coord.equals(new_coord);
        if(coord_changed && new_coord != null) {
            coord=new_coord;

            // todo: if I'm the new coord, get the highest seqno from all members. If not, re-send my pending seqno reqs
        }
        is_coord=new_coord.equals(local_addr);
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


    
    protected void sendSeqnoRequest(int num_seqnos) {
        Address target=coord;
        if(target == null)
            return;
        SequencerHeader hdr=new SequencerHeader(SequencerHeader.REQUEST, 0, num_seqnos);
        Message forward_msg=new Message(target).putHeader(this.id, hdr);
        down_prot.down(new Event(Event.MSG, forward_msg));
        sent_requests++;
    }
    
	protected void sendSeqnoResponse(Address original_sender,long seqno, int num_seqnos) {
		SequencerHeader hdr = new SequencerHeader(SequencerHeader.RESPONSE, seqno, num_seqnos);
		Message ucast_msg = new Message(original_sender).putHeader(this.id, hdr);
		
        if (log.isTraceEnabled())
            log.trace(local_addr + ": sending seqno response to " + original_sender + ":: new_seqno=" + seqno + ", num_seqnos=" + num_seqnos);

        down_prot.down(new Event(Event.MSG, ucast_msg));
        sent_responses++;
	}
    
    protected void broadcast(final Message msg, long seqno) {
        msg.putHeader(this.id, new SequencerHeader(SequencerHeader.BCAST, seqno));
 
        if(log.isTraceEnabled())
            log.trace(local_addr + ": broadcasting ::" + seqno);

        down_prot.down(new Event(Event.MSG, msg));
        bcast_msgs++;
    }



    protected void deliver(Message msg, Event evt, SequencerHeader hdr) {
        Address sender=msg.getSrc();
        if(sender == null) {
            if(log.isErrorEnabled())
                log.error(local_addr + ": sender is null, cannot deliver " + "::" + hdr.getSeqno());
            return;
        }
        
        final Table<Message> win=received_msgs;
        win.add(hdr.seqno, msg);

        final AtomicBoolean processing=win.getProcessing();
        if(processing.compareAndSet(false, true)) 
            removeAndDeliver(processing, win, sender);
    }
    
    
    protected void removeAndDeliver(final AtomicBoolean processing, Table<Message> win, Address sender) {
        boolean released_processing=false;
        try {
            while(true) {
                List<Message> list=win.removeMany(processing, true, max_msg_batch_size);
                if(list != null) // list is guaranteed to NOT contain any OOB messages as the drop_oob_msgs_filter removed them
                    deliverBatch(new MessageBatch(local_addr, sender, null, false, list));
                else {
                    released_processing=true;
                    return;
                }
            }
        }
        finally {
            // processing is always set in win.remove(processing) above and never here ! This code is just a
            // 2nd line of defense should there be an exception before win.removeMany(processing) sets processing
            if(!released_processing)
                processing.set(false);
        }
    }
    
    protected void deliverBatch(MessageBatch batch) {
        try {
            if(batch.isEmpty())
                return;
            if(log.isTraceEnabled()) {
                Message first=batch.first(), last=batch.last();
                StringBuilder sb=new StringBuilder(local_addr + ": delivering");
                if(first != null && last != null) {
                    SequencerHeader hdr1=(SequencerHeader)first.getHeader(id), hdr2=(SequencerHeader)last.getHeader(id);
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

        public void writeTo(DataOutput out) throws Exception {
            out.writeByte(type);
            Bits.writeLong(seqno,out);
            out.writeShort(num_seqnos);
        }

        public void readFrom(DataInput in) throws Exception {
            type=in.readByte();
            seqno=Bits.readLong(in);
            num_seqnos=in.readUnsignedShort();
        }

        // type + seqno + localSeqno + flush_ack
        public int size() {
            return Global.BYTE_SIZE + Bits.size(seqno) + Global.SHORT_SIZE;
        }
    }

}