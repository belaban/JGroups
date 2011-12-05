package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.Experimental;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;
import org.jgroups.util.AckCollector;
import org.jgroups.util.TimeScheduler;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.*;

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
    protected boolean throw_exception_on_timeout;

    @Property(description="When true, we pass the message up to the application and only then send an ack. When false, " +
      "we send an ack first and only then pass the message up to the application.")
    protected boolean ack_on_delivery=true;
    
    /* --------------------------------------------- Fields ------------------------------------------------------ */
    /** ID to be used to identify messages. Short.MAX_VALUE (ca 32K plus 32K negative) should be enough, and wrap-around
     * shouldn't be an issue. Using Message.Flag.RSVP should be the exception, not the rule... */
    protected short current_id=0;

    protected TimeScheduler timer;

    protected final List<Address> members=new ArrayList<Address>();

    protected Address local_addr;

    /** Used to store IDs and their acks */
    protected final Map<Short,Entry> ids=new HashMap<Short,Entry>();

    



    public void init() throws Exception {
        super.init();
        timer=getTransport().getTimer();
    }

    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                Message msg=(Message)evt.getArg();
                if(!msg.isFlagSet(Message.Flag.RSVP))
                    break;

                System.out.println(local_addr + ": msg to " + msg.getDest() + " has flag RSVP");


                short next_id=getNextId();
                RsvpHeader hdr=new RsvpHeader(RsvpHeader.REQ, next_id);
                msg.putHeader(id, hdr);

                // 1. put into hashmap
                Address target=msg.getDest();
                Entry entry;
                if(target != null)
                    entry=new Entry(false, target);
                else {
                    synchronized(members) {
                        entry=new Entry(true, members);
                    }
                }

                synchronized(ids) {
                    ids.put(next_id, entry);
                }

                // 2. start timer task


                // 3. Send the message
                Object retval=down_prot.down(evt);


                // 4. Block on AckCollector
                try {
                    entry.block(timeout);
                }
                catch(TimeoutException e) {
                    if(throw_exception_on_timeout)
                        throw e;
                    else if(log.isWarnEnabled())
                        log.warn("message ran into a timeout, missing acks: " + entry.ack_collector);
                }
                return retval;

            case Event.VIEW_CHANGE:
                View view=(View)evt.getArg();
                synchronized(members) {
                    members.clear();
                    members.addAll(view.getMembers());
                }
                synchronized(ids) {
                    for(Iterator<Map.Entry<Short,Entry>> it=ids.entrySet().iterator(); it.hasNext();) {
                        entry=it.next().getValue();
                        if(entry != null) {
                            entry.ack_collector.retainAll(view.getMembers());
                            if(entry.ack_collector.size() == 0)
                                it.remove();
                        }
                    }
                }
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

                    switch(hdr.type) {
                        case RsvpHeader.REQ:
                            Address sender=msg.getSrc();
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

                        case RsvpHeader.RSP:
                            handleResponse(msg.getSrc(), hdr.id);
                            return null;
                    }
                }
                break;
        }
        return up_prot.up(evt);
    }

    protected void handleResponse(Address member, short id) {
        synchronized(ids) {
            Entry entry=ids.get(id);
            if(entry != null) {
                entry.ack_collector.ack(member);
                if(entry.ack_collector.size() == 0)
                    ids.remove(id);
            }
        }
    }

    protected void sendResponse(Address dest, short id) {
        try {
            Message msg=new Message(dest);
            msg.setFlag(Message.Flag.RSVP);
            msg.putHeader(this.id, new RsvpHeader(RsvpHeader.RSP, id));
            down_prot.down(new Event(Event.MSG, msg));
        }
        catch(Throwable t) {
            log.error("failed sending response", t);
        }
    }

    protected synchronized short getNextId() {
        return current_id++;
    }


    protected static class Entry {
        protected final AckCollector ack_collector;
        protected final boolean      multicast;

        protected Entry(boolean multicast, Address ... members) {
            this.multicast=multicast;
            this.ack_collector=new AckCollector(members);
        }

        protected Entry(boolean multicast, Collection<Address> members) {
            this.multicast=multicast;
            this.ack_collector=new AckCollector(members);
        }

        protected void block(long timeout) throws TimeoutException {
            ack_collector.waitForAllAcks(timeout);
        }
    }

    
    protected static class RsvpHeader extends Header {
        protected static final byte REQ = 1;
        protected static final byte RSP = 2;

        protected byte  type;
        protected short id;


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
            return (type == REQ? "REQ(" : "RSP(") + id + ")";
        }
    }


}
