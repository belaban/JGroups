package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.Experimental;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.conf.AttributeType;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.function.Supplier;

/**
 * Implementation of daisy chaining. Multicast messages are sent to our neighbor, which sends them to its neighbor etc.
 * A TTL restricts the number of times a message is forwarded. The advantage of daisy chaining is that - for
 * point-to-point transports such as TCP - we can avoid the N-1 issue: when A sends a multicast message to 10
 * members, it needs to send it 9 times. With daisy chaining, it sends it 1 time, and in the next round, can already
 * send another message. This leads to much better throughput, see the ref in the JIRA.<p/>
 * Should be inserted just above MERGE3, in TCP based configurations.
 * JIRA: https://jira.jboss.org/browse/JGRP-1021
 * @author Bela Ban
 * @since 2.11
 */
@Experimental
@MBean(description="Protocol just above the transport which disseminates multicasts via daisy chaining")
public class DAISYCHAIN extends Protocol {

    @Property(description="Loop back multicast messages")
    boolean loopback=true;

    @ManagedAttribute(description="Local address")
    protected volatile Address       local_addr;
    @ManagedAttribute(description="The member to which all multicasts are forwarded")
    protected volatile Address       next;
    @ManagedAttribute(description="The current view")

    protected volatile View          view;

    @ManagedAttribute(type=AttributeType.SCALAR)
    protected int                    msgs_forwarded;
    @ManagedAttribute(type=AttributeType.SCALAR)
    protected int                    msgs_sent;
    protected TP                     transport;



    public void resetStats() {
        super.resetStats();
        msgs_forwarded=msgs_sent=0;
    }

    public void init() throws Exception {
        transport=getTransport();
    }

    public Object down(final Event evt) {
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
        if(msg.getDest() != null)
            return down_prot.down(msg); // only process multicast messages

        if(next == null) // view hasn't been received yet, use the normal transport
            return down_prot.down(msg);

        if(loopback) {
            if(msg.getSrc() == null && local_addr != null)
                msg.setSrc(local_addr);
            if(log.isTraceEnabled())
                log.trace("%s: looping back message %s", local_addr, msg);
            transport.loopback(msg, true);
        }

        short hdr_ttl=(short)(view.size()-1);
        // we need to copy the message, as we cannot do a msg.setSrc(next): the next retransmission
        // would use 'next' as destination  !
        Message copy=msg.copy(true, true).setDest(next)
          .putHeader(getId(), new DaisyHeader(hdr_ttl));
        msgs_sent++;
        if(log.isTraceEnabled())
            log.trace("%s: forwarding multicast message %s (hdrs: %s) to %s", local_addr, msg, msg.getHeaders(), next);
        return down_prot.down(copy);
    }

    public Object up(Message msg) {
        DaisyHeader hdr=msg.getHeader(getId());
        if(hdr == null)
            return up_prot.up(msg);

        // 1. forward the message to the next in line if ttl > 0
        short ttl=hdr.getTTL();
        if(log.isTraceEnabled())
            log.trace("%s: received message from %s with ttl=%d", local_addr, msg.getSrc(), ttl);
        if(--ttl > 0) {
            Message copy=msg.copy(true, true)
              .setDest(next).putHeader(getId(), new DaisyHeader(ttl));
            msgs_forwarded++;
            if(log.isTraceEnabled())
                log.trace("%s: forwarding message to %s with ttl=%d", local_addr, next, ttl);
            down_prot.down(copy);
        }

        // 2. Pass up
        msg.setDest(null);
        return up_prot.up(msg);
    }

    public void up(MessageBatch batch) {
        for(Message msg: batch) {
            DaisyHeader hdr=msg.getHeader(getId());
            if(hdr != null) {
                // 1. forward the message to the next in line if ttl > 0
                short ttl=hdr.getTTL();
                if(log.isTraceEnabled())
                    log.trace("%s: received message from %s with ttl=%d", local_addr, msg.getSrc(), ttl);
                if(--ttl > 0) {
                    Message copy=msg.copy(true, true)
                      .setDest(next).putHeader(getId(), new DaisyHeader(ttl));
                    msgs_forwarded++;
                    if(log.isTraceEnabled())
                        log.trace("%s: forwarding message to %s with ttl=%d", local_addr, next, ttl);
                    down_prot.down(copy);
                }

                // 2. Pass up
                msg.setDest(null);
            }
        }

        if(!batch.isEmpty())
            up_prot.up(batch);
    }

    protected void handleView(View view) {
        this.view=view;
        Address tmp=Util.pickNext(view.getMembers(), local_addr);
        if(tmp != null && !tmp.equals(local_addr)) {
            next=tmp;
            log.debug("%s: next=%s", local_addr, next);
        }
        else
            next=null;
    }


    public static class DaisyHeader extends Header {
        private short   ttl;

        public DaisyHeader() {
        }

        public DaisyHeader(short ttl) {
            this.ttl=ttl;
        }

        public short getMagicId()      {return 69;}
        public short getTTL()          {return ttl;}
        public void  setTTL(short ttl) {this.ttl=ttl;}

        public Supplier<? extends Header> create() {return DaisyHeader::new;}

        @Override public int  serializedSize()                           {return Global.SHORT_SIZE;}
        @Override public void writeTo(DataOutput out) throws IOException {out.writeShort(ttl);}
        @Override public void readFrom(DataInput in) throws IOException  {ttl=in.readShort();}
        public String         toString()                                 {return "ttl=" + ttl;}
    }

}
