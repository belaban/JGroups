package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.Property;
import org.jgroups.stack.AckReceiverWindow;
import org.jgroups.stack.Protocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Implements https://jira.jboss.org/jira/browse/JGRP-822, which allows for concurrent delivery of messages from the
 * same sender based on scopes. Similar to using OOB messages, but messages within the same scope are ordered.
 * @author Bela Ban
 * @version $Id: SCOPE.java,v 1.2 2010/03/23 15:11:20 belaban Exp $
 */
public class SCOPE extends Protocol {

    @Property(description="Max number of messages to be removed from the AckReceiverWindow. This property might " +
            "get removed anytime, so don't use it !")
    private int max_msg_batch_size=50000;

    /** Used to grab the seqno for a message to be sent down the stack */
    protected final ConcurrentMap<Address,ConcurrentMap<Short, AtomicLong>> seqno_table
            =new ConcurrentHashMap<Address,ConcurrentMap<Short,AtomicLong>>();

    /** Used to find the correct AckReceiverWindow on message reception and deliver it in the right order */
    protected final ConcurrentMap<Address,ConcurrentMap<Short,AckReceiverWindow>> receiver_table
            =new ConcurrentHashMap<Address,ConcurrentMap<Short,AckReceiverWindow>>();


    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                Message msg=(Message)evt.getArg();
                if(msg.isFlagSet(Message.SCOPED)) {

                    ScopeHeader hdr=(ScopeHeader)msg.getHeader(id);
                    if(hdr == null)
                        throw new IllegalStateException("message doesn't have a ScopeHeader attached");

                    // 1. Make the message OOB, so it will get delivered concurrently on the receiver side
                    msg.setFlag(Message.OOB);

                    // 2. Add a header with a scope and a scope-seqno
                    Address dest=msg.getDest();
                    if(dest == null)
                        dest=Global.NULL_ADDR;
                    ConcurrentMap<Short,AtomicLong> val=seqno_table.get(dest);
                    if(val == null) {
                        val=new ConcurrentHashMap<Short,AtomicLong>();
                        ConcurrentMap<Short,AtomicLong> tmp=seqno_table.putIfAbsent(dest,val);
                        if(tmp != null)
                            val=tmp;
                    }

                    AtomicLong seqno_generator=val.get(hdr.scope);
                    if(seqno_generator == null) {
                        seqno_generator=new AtomicLong(0);
                        AtomicLong old=val.putIfAbsent(hdr.scope, seqno_generator);
                        if(old != null)
                            seqno_generator=old;
                    }
                    hdr.seqno=seqno_generator.incrementAndGet();
                }
                break;

            case Event.VIEW_CHANGE:
                handleView((View)evt.getArg());
                break;
        }

        return down_prot.down(evt);
    }


    public Object up(Event evt) {

        switch(evt.getType()) {
            case Event.MSG:
                Message msg=(Message)evt.getArg();
                if(!msg.isFlagSet(Message.SCOPED))
                    break;
                
                ScopeHeader hdr=(ScopeHeader)msg.getHeader(id);
                if(hdr == null)
                    throw new IllegalStateException("message doesn't have a ScopeHeader attached");
                Address sender=msg.getSrc();
                ConcurrentMap<Short,AckReceiverWindow> val=receiver_table.get(sender);
                if(val == null) {
                    val=new ConcurrentHashMap<Short,AckReceiverWindow>();
                    ConcurrentMap<Short, AckReceiverWindow> tmp=receiver_table.putIfAbsent(sender, val);
                    if(tmp != null)
                        val=tmp;
                }
                AckReceiverWindow win=val.get(hdr.scope);
                if(win == null) {
                    win=new AckReceiverWindow(1);
                    AckReceiverWindow old=val.putIfAbsent(hdr.scope, win);
                    if(old != null)
                        win=old;
                }
                win.add(hdr.seqno, msg);

                AtomicBoolean processing=win.getProcessing();
                if(!processing.compareAndSet(false, true))
                    return null;

                // try to remove (from the AckReceiverWindow) as many messages as possible as pass them up
                try {
                    while(true) {
                        List<Message> msgs=win.removeManyAsList(max_msg_batch_size);
                        if(msgs == null || msgs.isEmpty())
                            break;

                        for(Message m: msgs) {
                            try {
                                up_prot.up(new Event(Event.MSG, m));
                            }
                            catch(Throwable t) {
                                log.error("couldn't deliver message " + m, t);
                            }
                        }
                    }
                }
                finally {
                    processing.set(false);
                }

                return null;

            
            case Event.VIEW_CHANGE:
                handleView((View)evt.getArg());
                break;
        }

        return super.up(evt);
    }

    private void handleView(View view) {
        
    }



    public static class ScopeHeader extends Header {
        public static final byte MSG    = 1;
        public static final byte EXPIRE = 2;

        byte  type;
        short scope=0;   // starts with 1
        long  seqno=0;   // starts with 1

        public static ScopeHeader createMessageHeader(short scope, long seqno) {
            return new ScopeHeader(MSG, scope, seqno);
        }

        public static ScopeHeader createExpireHeader(short scope) {
            return new ScopeHeader(EXPIRE, scope, 0);
        }

        public ScopeHeader() {
            
        }

        private ScopeHeader(byte type, short scope, long seqno) {
            this.type=type;
            this.scope=scope;
            this.seqno=seqno;
        }

        public short getScope() {
            return scope;
        }

        public long getSeqno() {
            return seqno;
        }

        public int size() {
            switch(type) {
                case MSG:     return Global.BYTE_SIZE + Global.SHORT_SIZE + Global.LONG_SIZE;
                case EXPIRE:  return Global.BYTE_SIZE + Global.SHORT_SIZE;
                default:      throw new IllegalStateException("type has to be MSG or EXPIRE");
            }
        }

        public void writeTo(DataOutputStream out) throws IOException {
            out.writeByte(type);
            switch(type) {
                case MSG:
                    out.writeShort(scope);
                    out.writeLong(seqno);
                    break;
                case EXPIRE:
                    out.writeShort(scope);
                    break;
                default:
                    throw new IllegalStateException("type has to be MSG or EXPIRE");
            }
        }

        public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
            type=in.readByte();
            switch(type) {
                case MSG:
                    scope=in.readShort();
                    seqno=in.readLong();
                    break;
                case EXPIRE:
                    scope=in.readShort();
                    break;
                default:
                    throw new IllegalStateException("type has to be MSG or EXPIRE");
            }
        }

        public String toString() {
            StringBuilder sb=new StringBuilder(typeToString(type));
            switch(type) {
                case MSG:
                    sb.append(": scope=").append(scope).append(", seqno=").append(seqno);
                    break;
                case EXPIRE:
                    sb.append(": scope=").append(scope);
                    break;
                default:
                    sb.append("n/a");
            }
            return sb.toString();
        }

        public static String typeToString(byte type) {
            switch(type) {
                case MSG: return "MSG";
                case EXPIRE: return "EXPIRE";
                default: return "n/a";
            }
        }
    }
}
