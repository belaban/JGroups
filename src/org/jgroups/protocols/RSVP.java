package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.Experimental;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;
import org.jgroups.util.AckCollector;
import org.jgroups.util.TimeScheduler;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Protocol which implements synchronous messages (https://issues.jboss.org/browse/JGRP-1389). A send of a message M
 * with flag RSVP set will block until all non-faulty recipients (one for unicasts, N for multicasts) have acked M, or
 * until a timeout kicks in.
 * @author Bela Ban
 * @since 3.1
 */
@Experimental
@MBean(description="Implements synchronous acks for messages which have their RSVP flag set)")
public class RSVP extends Protocol {

    /* -----------------------------------------    Properties     -------------------------------------------------- */
    @Property(description="Max time in milliseconds to block for an RSVP'ed message (0 blocks forever).")
    protected long timeout=10000;

    @Property(description="Whether an exception should be thrown when the timeout kicks in, and we haven't yet received " +
      "all acks. An exception would be thrown all the way up to JChannel.send()")
    protected boolean throw_exception_on_timeout=true;

    @Property(description="When true, we pass the message up to the application and only then send an ack. When false, " +
      "we send an ack first and only then pass the message up to the application.")
    protected boolean ack_on_delivery=true;

    @Property(description="Interval (in milliseconds) at which we resend the RSVP request. Needs to be < timeout. 0 disables it.")
    protected long resend_interval=2000;
    /* --------------------------------------------- Fields ------------------------------------------------------ */
    /** ID to be used to identify messages. Short.MAX_VALUE (ca 32K plus 32K negative) should be enough, and wrap-around
     * shouldn't be an issue. Using Message.Flag.RSVP should be the exception, not the rule... */
    protected short current_id=0;

    protected TimeScheduler timer;

    protected volatile List<Address> members=new ArrayList<Address>();

    protected Address local_addr;

    /** Used to store IDs and their acks */
    protected final Map<Short,Entry> ids=new HashMap<Short,Entry>();


    @ManagedAttribute(description="Number of pending RSVP requests")
    public int getPendingRsvpRequests() {synchronized(ids) {return ids.size();}}




    public void init() throws Exception {
        super.init();
        timer=getTransport().getTimer();
        if(timeout > 0 && resend_interval > 0 && resend_interval >= timeout) {
            log.warn("resend_interval (" + resend_interval + ") is >= timeout (" + timeout + "); setting " +
                       "resend_interval to timeout / 3");
            resend_interval=timeout / 3;
        }
    }


    public void stop() {
        synchronized(ids) {
            for(Entry entry: ids.values())
                entry.destroy();
            ids.clear();
        }
        super.destroy();
    }


    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                Message msg=(Message)evt.getArg();
                if(!msg.isFlagSet(Message.Flag.RSVP))
                    break;

                short next_id=getNextId();
                RsvpHeader hdr=new RsvpHeader(RsvpHeader.REQ, next_id);
                msg.putHeader(id, hdr);

                // 1. put into hashmap
                Address target=msg.getDest();
                Entry entry=target != null? new Entry(target) : new Entry(members); // volatile read of members
                Object retval=null;
                try {
                    synchronized(ids) {
                        ids.put(next_id, entry);
                    }

                    // sync members again - if a view was received after reading members intro Entry, but
                    // before adding Entry to ids (https://issues.jboss.org/browse/JGRP-1503)
                    entry.retainAll(members);

                    // 2. start timer task
                    entry.startTask(next_id);

                    // 3. Send the message
                    if(log.isTraceEnabled())
                        log.trace(local_addr + ": " + hdr.typeToString() + " --> " + target);
                    retval=down_prot.down(evt);

                    // 4. Block on AckCollector
                    entry.block(timeout);
                }
                catch(TimeoutException e) {
                    if(throw_exception_on_timeout)
                        throw e;
                    else if(log.isWarnEnabled())
                        log.warn("message ran into a timeout, missing acks: " + entry);
                }
                finally {
                    synchronized(ids) {
                        Entry tmp=ids.remove(next_id);
                        if(tmp != null)
                            tmp.destroy();
                    }
                }
                return retval;

            case Event.VIEW_CHANGE:
                handleView((View)evt.getArg());
                break;

            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;
        }
        return down_prot.down(evt);
    }


    public Object up(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                Message msg=(Message)evt.getArg();
                if(msg.isFlagSet(Message.Flag.RSVP)) {
                    RsvpHeader hdr=(RsvpHeader)msg.getHeader(id);
                    if(hdr == null) {
                        log.error("message with RSVP flag needs to have an RsvpHeader");
                        break;
                    }
                    Address sender=msg.getSrc();
                    if(log.isTraceEnabled())
                        log.trace(local_addr + ": " + hdr.typeToString() + " <-- " + sender);
                    switch(hdr.type) {
                        case RsvpHeader.REQ:
                            if(this.ack_on_delivery) {
                                try {
                                    return up_prot.up(evt);
                                }
                                finally {
                                    sendResponse(sender, hdr.id);
                                }
                            }
                            else {
                                sendResponse(sender, hdr.id);
                                return up_prot.up(evt);
                            }

                        case RsvpHeader.REQ_ONLY:
                            return null;

                        case RsvpHeader.RSP:
                            handleResponse(msg.getSrc(), hdr.id);
                            return null;
                    }
                }
                break;
            case Event.VIEW_CHANGE:
                handleView((View)evt.getArg());
                break;
        }
        return up_prot.up(evt);
    }


    protected void handleView(View view) {
        members=view.getMembers();

        synchronized(ids) {
            for(Iterator<Map.Entry<Short,Entry>> it=ids.entrySet().iterator(); it.hasNext();) {
                Entry entry=it.next().getValue();
                if(entry != null && entry.retainAll(view.getMembers()) && entry.size() == 0) {
                    entry.destroy();
                    it.remove();
                }
            }
        }
    }


    protected void handleResponse(Address member, short id) {
        synchronized(ids) {
            Entry entry=ids.get(id);
            if(entry != null) {
                entry.ack(member);
                if(entry.size() == 0) {
                    entry.destroy();
                    ids.remove(id);
                }
            }
        }
    }

    protected void sendResponse(Address dest, short id) {
        try {
            Message msg=new Message(dest);
            msg.setFlag(Message.Flag.RSVP, Message.Flag.OOB);
            RsvpHeader hdr=new RsvpHeader(RsvpHeader.RSP,id);
            msg.putHeader(this.id, hdr);
            if(log.isTraceEnabled())
                log.trace(local_addr + ": " + hdr.typeToString() + " --> " + dest);
            down_prot.down(new Event(Event.MSG, msg));
        }
        catch(Throwable t) {
            log.error("failed sending response", t);
        }
    }

    protected synchronized short getNextId() {
        return current_id++;
    }


    protected class Entry {
        protected final AckCollector ack_collector;
        protected final Address      target; // if null --> multicast, else --> unicast
        protected Future<?>          resend_task;

        /** Unicast entry */
        protected Entry(Address member) {
            this.target=member;
            this.ack_collector=new AckCollector(member);
        }

        /** Multicast entry */
        protected Entry(Collection<Address> members) {
            this.target=null;
            this.ack_collector=new AckCollector(members);
        }

        protected void startTask(final short rsvp_id) {
            if(resend_task != null && !resend_task.isDone())
                resend_task.cancel(false);

            resend_task=timer.scheduleWithFixedDelay(new Runnable() {
                public void run() {
                    if(ack_collector.size() == 0) {
                        cancelTask();
                        return;
                    }
                    Message msg=new Message(target);
                    msg.setFlag(Message.Flag.RSVP);
                    RsvpHeader hdr=new RsvpHeader(RsvpHeader.REQ_ONLY, rsvp_id);
                    msg.putHeader(id, hdr);
                    if(log.isTraceEnabled())
                        log.trace(local_addr + ": " + hdr.typeToString() + " --> " + target);
                    down_prot.down(new Event(Event.MSG, msg));
                }
            }, resend_interval, resend_interval, TimeUnit.MILLISECONDS);
        }

        protected void cancelTask() {
            if(resend_task != null)
                resend_task.cancel(false);
            ack_collector.destroy();
        }

        protected void    ack(Address member)                         {ack_collector.ack(member);}
        protected boolean retainAll(Collection<Address> members)      {return ack_collector.retainAll(members);}
        protected int     size()                                      {return ack_collector.size();}
        protected void    block(long timeout) throws TimeoutException {ack_collector.waitForAllAcks(timeout);}
        protected void    destroy()                                   {cancelTask();}
        public String     toString()                                  {return ack_collector.toString();}
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

        public int size() {
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

        public String toString() {
            String tmp=typeToString();
            return tmp + "(" + id + ")";
        }

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
