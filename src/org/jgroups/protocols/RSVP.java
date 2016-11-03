package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;
import org.jgroups.util.AckCollector;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Supplier;

/**
 * Protocol which implements synchronous messages (https://issues.jboss.org/browse/JGRP-1389). A send of a message M
 * with flag RSVP set will block until all non-faulty recipients (one for unicasts, N for multicasts) have acked M, or
 * until a timeout kicks in.
 * @author Bela Ban
 * @since  3.1
 */
@MBean(description="Implements synchronous acks for messages which have their RSVP flag set)")
public class RSVP extends Protocol {

    /* -----------------------------------------    Properties     -------------------------------------------------- */
    @Property(description="Max time in milliseconds to block for an RSVP'ed message (0 blocks forever).")
    protected long    timeout=10000;

    @Property(description="Whether an exception should be thrown when the timeout kicks in, and we haven't yet received " +
      "all acks. An exception would be thrown all the way up to JChannel.send(). If we use RSVP_NB, this will be ignored.")
    protected boolean throw_exception_on_timeout=true;

    @Property(description="When true, we pass the message up to the application and only then send an ack. When false, " +
      "we send an ack first and only then pass the message up to the application.")
    protected boolean ack_on_delivery=true;

    @Property(description="Interval (in milliseconds) at which we resend the RSVP request. Needs to be < timeout. 0 disables it.")
    protected long    resend_interval=2000;
    /* --------------------------------------------- Fields ------------------------------------------------------ */
    /** ID to be used to identify messages. Short.MAX_VALUE (ca 32K plus 32K negative) should be enough, and wrap-around
     * shouldn't be an issue. Using Message.Flag.RSVP should be the exception, not the rule... */
    protected short                            current_id;

    protected TimeScheduler                    timer;

    protected volatile List<Address>           members=new ArrayList<>();

    protected Address                          local_addr;

    /** Used to store IDs and their acks */
    protected final ConcurrentMap<Short,Entry> ids=new ConcurrentHashMap<>();

    protected Future<?>                        resend_task;

    @ManagedAttribute(description="If we have UNICAST or UNICAST3 in the stack, we don't need to handle unicast messages " +
      "as they're retransmitted anyway")
    protected boolean                          handle_unicasts=true;


    @ManagedAttribute(description="Number of pending RSVP requests")
    public int getPendingRsvpRequests() {return ids.size();}


    public void init() throws Exception {
        super.init();
        timer=getTransport().getTimer();
        if(timeout > 0 && resend_interval > 0 && resend_interval >= timeout) {
            log.warn(Util.getMessage("RSVP_Misconfig"), resend_interval, timeout);
            resend_interval=timeout / 3;
        }
        handle_unicasts=stack.findProtocol(UNICAST3.class) == null;
    }


    public void start() throws Exception {
        super.start();
        startResendTask();
    }

    public void stop() {
        stopResendTask();
        ids.values().forEach(Entry::destroy);
        ids.clear();
        super.stop();
    }


    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.VIEW_CHANGE:
                handleView(evt.getArg());
                break;

            case Event.SET_LOCAL_ADDRESS:
                local_addr=evt.getArg();
                break;
        }
        return down_prot.down(evt);
    }

    public Object down(Message msg) {
        Address target=msg.getDest();
        if((target != null && !handle_unicasts) || !(msg.isFlagSet(Message.Flag.RSVP) || msg.isFlagSet(Message.Flag.RSVP_NB)))
            return down_prot.down(msg);

        short next_id=getNextId();
        RsvpHeader hdr=new RsvpHeader(RsvpHeader.REQ, next_id);
        msg.putHeader(id, hdr);
        boolean block=msg.isFlagSet(Message.Flag.RSVP);

        Entry entry=target != null? new Entry(target) : new Entry(members); // volatile read of members
        Object retval=null;
        try {
            ids.put(next_id, entry);

            // sync members again - if a view was received after reading members intro Entry, but
            // before adding Entry to ids (https://issues.jboss.org/browse/JGRP-1503)
            entry.retainAll(members);

            // Send the message
            if(log.isTraceEnabled())
                log.trace(local_addr + ": " + hdr.typeToString() + " --> " + (target == null? "cluster" : target));
            retval=down_prot.down(msg);

            if(msg.isTransientFlagSet(Message.TransientFlag.DONT_LOOPBACK))
                entry.ack(local_addr);

            // Block on AckCollector (if we need to block)
            if(block)
                entry.block(timeout);
        }
        catch(TimeoutException e) {
            if(throw_exception_on_timeout)
                throw new RuntimeException(e);
            else if(log.isWarnEnabled())
                log.warn(Util.getMessage("RSVP_Timeout"), entry);
        }
        finally {
            if(block) {
                Entry tmp=ids.remove(next_id);
                if(tmp != null)
                    tmp.destroy();
            }
        }
        return retval;
    }

    public Object up(Event evt) {
        switch(evt.getType()) {
            case Event.VIEW_CHANGE:
                handleView(evt.getArg());
                break;
        }
        return up_prot.up(evt);
    }

    public Object up(Message msg) {
        if(!(msg.isFlagSet(Message.Flag.RSVP) || msg.isFlagSet(Message.Flag.RSVP_NB)))
            return up_prot.up(msg);

        Address dest=msg.getDest();
        RsvpHeader hdr=msg.getHeader(id);
        if(hdr == null) {
            if(dest == null || handle_unicasts)
                log.error(Util.getMessage("MessageWithRSVPFlagNeedsToHaveAnRsvpHeader"));
            return up_prot.up(msg);
        }
        Address sender=msg.getSrc();
        if(log.isTraceEnabled())
            log.trace(local_addr + ": " + hdr.typeToString() + " <-- " + sender);
        switch(hdr.type) {
            case RsvpHeader.REQ:
                if(this.ack_on_delivery) {
                    try {
                        return up_prot.up(msg);
                    }
                    finally {
                        sendResponse(sender, hdr.id);
                    }
                }
                else {
                    sendResponse(sender, hdr.id);
                    return up_prot.up(msg);
                }

            case RsvpHeader.REQ_ONLY:
                return null;

            case RsvpHeader.RSP:
                handleResponse(msg.getSrc(), hdr.id);
                return null;
        }
        return up_prot.up(msg);
    }

    public void up(MessageBatch batch) {
        List<Short> response_ids=null;
        Address dest=batch.dest();
        for(Message msg: batch) {
            if(!(msg.isFlagSet(Message.Flag.RSVP) || msg.isFlagSet(Message.Flag.RSVP_NB)))
                continue;
            RsvpHeader hdr=msg.getHeader(id);
            if(hdr == null) {
                if(dest == null || handle_unicasts)
                    log.error(Util.getMessage("MessageWithRSVPFlagNeedsToHaveAnRsvpHeader"));
                continue;
            }
            switch(hdr.type) {
                case RsvpHeader.REQ:
                    if(!ack_on_delivery) // send ack on *reception*
                        sendResponse(batch.sender(), hdr.id);
                    else {
                        if(response_ids == null)
                            response_ids=new ArrayList<>();
                        response_ids.add(hdr.id);
                    }
                    break;

                case RsvpHeader.REQ_ONLY:
                case RsvpHeader.RSP:
                    if(hdr.type == RsvpHeader.RSP)
                        handleResponse(msg.getSrc(), hdr.id);
                    batch.remove(msg);
                    break;
            }
        }

        if(!batch.isEmpty())
            up_prot.up(batch);

        // we're sending RSVP responses if ack_on_delivery is true. Unfortunately, this is done after the entire
        // *batch* was delivered, not after each message that was delivered
        if(response_ids != null)
            for(short rsp_id: response_ids)
                sendResponse(batch.sender(), rsp_id);
    }

    protected void handleView(View view) {
        members=view.getMembers();
        for(Iterator<Map.Entry<Short,Entry>> it=ids.entrySet().iterator(); it.hasNext();) {
            Entry entry=it.next().getValue();
            if(entry != null && entry.retainAll(view.getMembers()) && entry.size() == 0) {
                entry.destroy();
                it.remove();
            }
        }
    }


    protected void handleResponse(Address member, short id) {
        Entry entry=ids.get(id);
        if(entry != null) {
            entry.ack(member);
            if(entry.size() == 0) {
                entry.destroy();
                ids.remove(id);
            }
        }
    }

    protected void sendResponse(Address dest, short id) {
        try {
            RsvpHeader hdr=new RsvpHeader(RsvpHeader.RSP,id);
            Message msg=new Message(dest) .putHeader(this.id, hdr)
              .setFlag(Message.Flag.RSVP, Message.Flag.INTERNAL, Message.Flag.DONT_BUNDLE, Message.Flag.OOB);

            if(log.isTraceEnabled())
                log.trace(local_addr + ": " + hdr.typeToString() + " --> " + dest);
            down_prot.down(msg);
        }
        catch(Throwable t) {
            log.error(Util.getMessage("FailedSendingResponse"), t);
        }
    }

    protected synchronized short getNextId() {
        return current_id++;
    }

    protected synchronized void startResendTask() {
        if(resend_task == null || resend_task.isDone())
            resend_task=timer.scheduleWithFixedDelay(new ResendTask(), resend_interval, resend_interval, TimeUnit.MILLISECONDS,
                                                     getTransport() instanceof TCP);
    }

    protected synchronized void stopResendTask() {
        if(resend_task != null)
            resend_task.cancel(false);
        resend_task=null;
    }

    @ManagedAttribute(description="Is the resend task running")
    protected synchronized boolean isResendTaskRunning() {
        return resend_task != null && !resend_task.isDone();
    }

    protected static class Entry {
        protected final AckCollector ack_collector;
        protected final Address      target; // if null --> multicast, else --> unicast
        protected final long         timestamp; // creation time (ns)

        /** Unicast entry */
        protected Entry(Address member) {
            this.target=member;
            this.ack_collector=new AckCollector(member);
            this.timestamp=System.nanoTime();
        }

        /** Multicast entry */
        protected Entry(Collection<Address> members) {
            this.target=null;
            this.ack_collector=new AckCollector(members);
            this.timestamp=System.nanoTime();
        }

        protected void    ack(Address member)                         {ack_collector.ack(member);}
        protected boolean retainAll(Collection<Address> members)      {return ack_collector.retainAll(members);}
        protected int     size()                                      {return ack_collector.size();}
        protected void    block(long timeout) throws TimeoutException {ack_collector.waitForAllAcks(timeout);}
        protected void    destroy()                                   {ack_collector.destroy();}
        public String     toString()                                  {return ack_collector.toString();}
    }


    protected class ResendTask implements Runnable {

        public void run() {
            Set<Address> sent=new HashSet<>(); // list of all unicast dests we already sent a beacon msg
            boolean      mcast_sent=false;

            for(Map.Entry<Short,Entry> entry: ids.entrySet()) {
                Short rsvp_id=entry.getKey();
                Entry val=entry.getValue();
                long age=TimeUnit.MILLISECONDS.convert(System.nanoTime() - val.timestamp, TimeUnit.NANOSECONDS);
                if(age >= timeout || val.ack_collector.size() == 0) {
                    if(age >= timeout)
                        log.warn(Util.getMessage("RSVP_Timeout"), entry);
                    val.destroy();
                    ids.remove(rsvp_id);
                    continue;
                }

                Address dest=val.target;
                // make sure we only send 1 mcast per resend cycle
                if(dest == null) {
                    if(mcast_sent)
                        continue;
                    else
                        mcast_sent=true;
                }
                else if(!sent.add(dest)) // only send a unicast beacon once for each target dest
                    continue;

                RsvpHeader hdr=new RsvpHeader(RsvpHeader.REQ_ONLY, rsvp_id);
                Message msg=new Message(dest).setFlag(Message.Flag.RSVP).putHeader(id,hdr);
                if(log.isTraceEnabled())
                    log.trace(local_addr + ": " + hdr.typeToString() + " --> " + (val.target == null? "cluster" : val.target));
                down_prot.down(msg);
            }
        }
    }

    
    protected static class RsvpHeader extends Header {
        protected static final byte REQ      = 1;
        protected static final byte REQ_ONLY = 2;
        protected static final byte RSP      = 3;

        protected byte    type;
        protected short   id;


        public RsvpHeader() {
        }

        public RsvpHeader(byte type, short id) {
            this.type=type;
            this.id=id;
        }
        public short getMagicId() {return 76;}
        public Supplier<? extends Header> create() {return RsvpHeader::new;}

        public int serializedSize() {
            return Global.BYTE_SIZE + Global.SHORT_SIZE;
        }

        public void writeTo(DataOutput out) throws Exception {
            out.writeByte(type);
            out.writeShort(id);
        }

        public void readFrom(DataInput in) throws Exception {
            type=in.readByte();
            id=in.readShort();
        }

        public String toString() {return typeToString() + "(" + id + ")";}

        protected String typeToString() {
            switch(type) {
                case REQ :     return "REQ";
                case REQ_ONLY: return "REQ-ONLY";
                case RSP:      return "RSP";
                default:       return "unknown";
            }
        }
    }


}
