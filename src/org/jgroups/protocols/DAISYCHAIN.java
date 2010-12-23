package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.*;
import org.jgroups.stack.Protocol;
import org.jgroups.util.ConcurrentLinkedBlockingQueue;
import org.jgroups.util.Util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;

/**
 * Implementation of daisy chaining. Multicast messages are sent to our neighbor, which sends them to its neighbor etc.
 * A TTL restricts the number of times a message is forwarded. The advantage of daisy chaining is that - for
 * point-to-point transports such as TCP - we can avoid the N-1 issue: when A sends a multicast message to 10
 * members, it needs to send it 9 times. With daisy chaining, it sends it 1 time, and in the next round, can already
 * send another message. This leads to much better throughput, see the ref in the JIRA.<p/>
 * Should be inserted just above MERGE2, in TCP based configurations.
 * JIRA: https://jira.jboss.org/browse/JGRP-1021
 * @author Bela Ban
 * @since 2.11
 */
@Experimental
@MBean(description="Protocol just above the transport which disseminates multicasts via daisy chaining")
public class DAISYCHAIN extends Protocol {


    /* -----------------------------------------    Properties     -------------------------------------------------- */
    @Property(description="Loop back multicast messages")
    boolean loopback=true;

    @Property(description="The number of messages in the forward queue. This queue is used to host messages that " +
      "need to be forwarded by us on behalf of our neighbor")
    int forward_queue_size=10000;

    @Property(description="The number of messages in the send queue. This queue is used to host messages that need " +
      "to be sent")
    int send_queue_size=10000;

    /* --------------------------------------------- Fields ------------------------------------------------------ */
    protected Address                local_addr, next;
    protected int                    view_size=0;
    protected Executor               default_pool=null;
    protected Executor               oob_pool=null;
    protected BlockingQueue<Message> send_queue;
    protected BlockingQueue<Message> forward_queue;
    protected volatile boolean       forward=false; // flipped between true and false, to ensure fairness
    protected volatile boolean       running=true;

    @ManagedAttribute
    public int msgs_forwarded=0;

    @ManagedAttribute
    public int msgs_sent=0;

    @ManagedAttribute
    public int getElementsInForwardQueue() {return forward_queue.size();}

    @ManagedAttribute
    public int getElementsInSendQueue() {return send_queue.size();}

    public void init() throws Exception {
        default_pool=getTransport().getDefaultThreadPool();
        oob_pool=getTransport().getOOBThreadPool();
        send_queue=new ConcurrentLinkedBlockingQueue<Message>(send_queue_size);
        forward_queue=new ConcurrentLinkedBlockingQueue<Message>(forward_queue_size);
    }

    public void start() throws Exception {
        super.start();
        running=true;
    }

    public void stop() {
        super.stop();
        running=false;
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

                try {
                    msgs_sent++;
                    send_queue.put(copy);
                }
                catch(InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return null;
                }

                if(loopback) {
                    if(log.isTraceEnabled()) log.trace(new StringBuilder("looping back message ").append(msg));
                    if(msg.getSrc() == null)
                        msg.setSrc(local_addr);

                    Executor pool=msg.isFlagSet(Message.OOB)? oob_pool : default_pool;
                    pool.execute(new Runnable() {
                        public void run() {
                            up_prot.up(evt);
                        }
                    });
                }

                return processQueues();


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
                    msgs_forwarded++;
                    if(forward_queue.offer(copy)) // we don't want incoming threads to block
                        processQueues();
                }

                // 2. Pass up
                msg.setDest(null);
                break;
        }
        return up_prot.up(evt);
    }


    protected Object processQueues() {
        int cnt=0;
        while(running && cnt++ < 10000) { // cnt is a second line of defense against loops and should never be used !
            try {
                Message msg=forward? forward_queue.poll() : send_queue.poll();
                if(msg == null) {
                    msg=forward? send_queue.poll() : forward_queue.poll();
                    if(msg == null)
                        continue;
                }
                if(log.isTraceEnabled()) {
                    DaisyHeader hdr=(DaisyHeader)msg.getHeader(getId());
                    log.trace(local_addr + ": " + (forward? " forwarding" : " sending") + " message with ttl=" + hdr.getTTL() + " to " + next);
                }
                return down_prot.down(new Event(Event.MSG, msg));
            }
            catch(Throwable t) {
                log.error("failed sending message down", t);
                return null;
            }
            finally {
                forward=!forward;
            }
        }
        return null;
    }


    protected void handleView(View view) {
        view_size=view.size();
        Address tmp=Util.pickNext(view.getMembers(), local_addr);
        if(tmp != null && !tmp.equals(local_addr)) {
            next=tmp;
            if(log.isDebugEnabled())
                log.debug("next=" + next);
        }
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
