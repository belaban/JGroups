package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.*;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;

/**
 * Implementation of daisy chaining. Multicast messages are sent to our neighbor, which sends them to its neighbor etc.
 * A TTL restricts the number of times a message is forwarded. The advantage of daisy chaining is that - for
 * point-to-point transports such as TCP - we can avoid the N-1 issue: when A sends a multicast message to 10
 * members, it needs to send it 9 times. With daisy chaining, it sends it 1 time, and in the next round, can already
 * send another message. This leads to much better throughput, see the ref in the JIRA.<p/> 
 * JIRA: https://jira.jboss.org/browse/JGRP-1021
 * @author Bela Ban
 * @version $Id: DAISYCHAIN.java,v 1.9 2010/08/24 10:07:53 belaban Exp $
 */
@Experimental @Unsupported
@MBean(description="Protocol just above the transport which disseminates multicasts via daisy chaining")
public class DAISYCHAIN extends Protocol {


    /* -----------------------------------------    Properties     -------------------------------------------------- */
    @Property(description="Loop back multicast messages")
    boolean loopback=true;

    @Property(description="Max number of messages in the send queue. The adder will block until more space is available")
    int send_queue_max_size=1000;

    @Property(description="Max number of messages in the forward queue. The adder will block until more space is available")
    int forward_queue_max_size=1000;

    /* --------------------------------------------- Fields ------------------------------------------------------ */
    protected Address              local_addr, next;
    protected int                  view_size=0;
    protected final Queue<Message> send_queue=new ConcurrentLinkedQueue<Message>();
    protected final Queue<Message> forward_queue=new ConcurrentLinkedQueue<Message>();
    protected volatile boolean     forward=false; // flipped between true and false, to ensure fairness
    protected Executor             default_pool=null;
    protected Executor             oob_pool=null;


    @ManagedAttribute
    public int msgs_forwarded=0;

    @ManagedAttribute
    public int msgs_sent=0;

    @ManagedAttribute
    public int getForwardQueueSize() {return forward_queue.size();}

    @ManagedAttribute
    public int getSendQueueSize() {return send_queue.size();}


    public void init() throws Exception {
        default_pool=getTransport().getDefaultThreadPool();
        oob_pool=getTransport().getOOBThreadPool();
    }

    public Object down(final Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                final Message msg=(Message)evt.getArg();
                Address dest=msg.getDest();
                if(dest != null && !dest.isMulticastAddress())
                    break; // only process multicast messages

                if(next == null) // view hasn't been received yet, use the normal transport
                    break;

                // we need to copy the message, as we cannot do a msg.setSrc(next): the next retransmission
                // would use 'next' as destination  !
                Message copy=msg.copy(true);
                short hdr_ttl=(short)(loopback? view_size -1 : view_size);
                DaisyHeader hdr=new DaisyHeader(hdr_ttl);
                copy.setDest(next);
                copy.putHeader(getId(), hdr);
                send_queue.offer(copy);

                if(loopback) {
                    if(log.isTraceEnabled()) log.trace(new StringBuilder("looping back message ").append(msg));
                    msg.setSrc(local_addr);

                    Executor pool=msg.isFlagSet(Message.OOB)? oob_pool : default_pool;
                    pool.execute(new Runnable() {
                        public void run() {
                            up_prot.up(evt);
                        }
                    });
                }

                return forward();


            case Event.VIEW_CHANGE:
                handleView((View)evt.getArg());
                break;

            case Event.TMP_VIEW:
                view_size=((View)evt.getArg()).size();
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
                DaisyHeader hdr=(DaisyHeader)msg.getHeader(getId());
                if(hdr == null)
                    break;

                // 1. forward the message to the next in line if ttl > 0
                short ttl=hdr.getTTL();
                if(log.isTraceEnabled())
                    log.trace(local_addr + ": received message from " + msg.getSrc() + " with ttl=" + ttl);
                if(--ttl > 0) {
                    Message copy=msg.copy(true);
                    copy.setDest(next);
                    copy.putHeader(getId(), new DaisyHeader(ttl));
                    forward_queue.offer(copy);
                    forward();
                }

                // 2. Pass up
                msg.setDest(null);
                break;
        }
        return up_prot.up(evt);
    }


    protected Object forward() {
        Message        msg=null;
        Queue<Message> queue=null;
        String         tmp=null;

        while(!(send_queue.isEmpty() && forward_queue.isEmpty())) {
            tmp=forward? " forwarding" : " sending";
            queue=forward? forward_queue : send_queue;
            if(queue.isEmpty()) {
                queue=forward? send_queue : forward_queue;
                msgs_sent++;
            }
            else {
                msgs_forwarded++;
            }

            msg=queue.poll();
            if(msg != null)
                break;
            forward=!forward;
        }

        if(msg == null)
            return null;

        if(log.isTraceEnabled()) {
            DaisyHeader hdr=(DaisyHeader)msg.getHeader(getId());
            log.trace(local_addr + ": " + tmp + " message with ttl=" + hdr.getTTL() + " to " + next);
        }
        return down_prot.down(new Event(Event.MSG, msg));
    }
    

    protected void handleView(View view) {
        view_size=view.size();
        next=Util.pickNext(view.getMembers(), local_addr);
        if(log.isDebugEnabled())
            log.debug("next=" + next);
    }


    public static class DaisyHeader extends Header {
        private short   ttl;

        public DaisyHeader() {
        }

        public DaisyHeader(short ttl) {
            this.ttl=ttl;
        }

        public short getTTL() {return ttl;}

        public void setTTL(short ttl) {
            this.ttl=ttl;
        }

        public int size() {
            return Global.SHORT_SIZE;
        }

        public void writeTo(DataOutputStream out) throws IOException {
            out.writeShort(ttl);
        }

        public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
            ttl=in.readShort();
        }

        public String toString() {
            return "ttl=" + ttl;
        }
    }

}
