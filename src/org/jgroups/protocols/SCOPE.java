package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Implements https://jira.jboss.org/jira/browse/JGRP-822, which allows for concurrent delivery of messages from the
 * same sender based on scopes. Similar to using OOB messages, but messages within the same scope are ordered.
 * @author Bela Ban
 * @version $Id: SCOPE.java,v 1.5 2010/03/25 17:03:30 belaban Exp $
 */
public class SCOPE extends Protocol {

    @Property(description="Max number of messages to be removed from the AckReceiverWindow. This property might " +
            "get removed anytime, so don't use it !")
    private int max_msg_batch_size=50000;

    /** Used to find the correct AckReceiverWindow on message reception and deliver it in the right order */
    protected final ConcurrentMap<Address,ConcurrentMap<Short,MessageQueue>> receiver_table
            =new ConcurrentHashMap<Address,ConcurrentMap<Short,MessageQueue>>();


    protected final Executor thread_pool
            =new ThreadPoolExecutor(2, 10, 5000, TimeUnit.MILLISECONDS, new SynchronousQueue()); // todo: replace with custom config'ed pool


    @ManagedAttribute(description="Number of scopes in receiver_table")
    public int getNumberOfReceiverScopes() {
        int retval=0;
        for(ConcurrentMap<Short,MessageQueue> map: receiver_table.values())
            retval+=map.keySet().size();
        return retval;
    }


    public Object down(Event evt) {
        switch(evt.getType()) {
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

                // we don't handle unscoped or OOB messages
                if(!msg.isFlagSet(Message.SCOPED) || msg.isFlagSet(Message.OOB))
                    break;
                
                ScopeHeader hdr=(ScopeHeader)msg.getHeader(id);
                if(hdr == null)
                    throw new IllegalStateException("message doesn't have a ScopeHeader attached");
                Address sender=msg.getSrc();
                ConcurrentMap<Short,MessageQueue> val=receiver_table.get(sender);
                if(val == null) {
                    val=new ConcurrentHashMap<Short,MessageQueue>();
                    ConcurrentMap<Short, MessageQueue> tmp=receiver_table.putIfAbsent(sender, val);
                    if(tmp != null)
                        val=tmp;
                }
                MessageQueue queue=val.get(hdr.scope);
                if(queue == null) {
                    queue=new MessageQueue();
                    MessageQueue old=val.putIfAbsent(hdr.scope, queue);
                    if(old != null)
                        queue=old;
                }
                queue.add(msg);

                // System.out.println("queue for " + hdr.scope + ": " + queue);

                if(!queue.acquire())
                    return null;

                QueueThread thread=new QueueThread(queue);
                thread_pool.execute(thread);

                // try to remove (from the AckReceiverWindow) as many messages as possible as pass them up
                /*try {
                    Message msg_to_deliver;
                    while((msg_to_deliver=queue.remove()) != null) {
                        try {
                            up_prot.up(new Event(Event.MSG, msg_to_deliver));
                        }
                        catch(Throwable t) {
                            log.error("couldn't deliver message " + msg_to_deliver, t);
                        }
                    }
                }
                finally {
                    queue.release();
                }*/

                return null;

            
            case Event.VIEW_CHANGE:
                handleView((View)evt.getArg());
                break;
        }

        return up_prot.up(evt);
    }

    private void handleView(View view) {
        Vector<Address> members=view.getMembers();

        // Remove all non members from receiver_table
        Set<Address> keys=new HashSet<Address>(receiver_table.keySet());
        keys.removeAll(members);
        for(Address key: keys) {
            ConcurrentMap<Short,MessageQueue> val=receiver_table.remove(key);
            if(val != null) {
                Collection<MessageQueue> values=val.values();
                for(MessageQueue queue: values)
                    queue.clear();
            }
            if(log.isTraceEnabled())
                log.trace("removed " + key + " from receiver_table");
        }
    }

    protected static class MessageQueue {
        private final Queue<Message> queue=new ConcurrentLinkedQueue<Message>();
        private final AtomicBoolean  processing=new AtomicBoolean(false);

        public void add(Message msg) {
            queue.add(msg);
        }

        public Message remove() {
            return queue.poll();
        }

        public void clear() {
            queue.clear();
        }

        public boolean acquire() {
            return processing.compareAndSet(false, true);
        }

        public boolean release() {
            return processing.compareAndSet(true, false);
        }
    }

    protected class QueueThread implements Runnable {
        protected final MessageQueue queue;

        public QueueThread(MessageQueue queue) {
            this.queue=queue;
        }

        public void run() {
            // try to remove (from the AckReceiverWindow) as many messages as possible as pass them up
            try {
                Message msg_to_deliver;
                while((msg_to_deliver=queue.remove()) != null) {
                    try {
                        up_prot.up(new Event(Event.MSG, msg_to_deliver));
                    }
                    catch(Throwable t) {
                        log.error("couldn't deliver message " + msg_to_deliver, t);
                    }
                }
            }
            finally {
                queue.release();
            }
        }
    }



    public static class ScopeHeader extends Header {
        public static final byte MSG    = 1;
        public static final byte EXPIRE = 2;

        byte  type;
        short scope=0;   // starts with 1

        public static ScopeHeader createMessageHeader(short scope) {
            return new ScopeHeader(MSG, scope);
        }

        public static ScopeHeader createExpireHeader(short scope) {
            return new ScopeHeader(EXPIRE, scope);
        }

        public ScopeHeader() {
            
        }

        private ScopeHeader(byte type, short scope) {
            this.type=type;
            this.scope=scope;
        }

        public short getScope() {
            return scope;
        }

        public int size() {
            switch(type) {
                case MSG:
                case EXPIRE:
                    return Global.BYTE_SIZE + Global.SHORT_SIZE;
                default:
                    throw new IllegalStateException("type has to be MSG or EXPIRE");
            }
        }

        public void writeTo(DataOutputStream out) throws IOException {
            out.writeByte(type);
            switch(type) {
                case MSG:
                    out.writeShort(scope);
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
                case MSG:    return "MSG";
                case EXPIRE: return "EXPIRE";
                default:     return "n/a";
            }
        }
    }
}
