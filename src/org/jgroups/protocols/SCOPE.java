package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.util.*;
import org.jgroups.util.ThreadFactory;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.stack.Protocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Implements https://jira.jboss.org/jira/browse/JGRP-822, which allows for concurrent delivery of messages from the
 * same sender based on scopes. Similar to using OOB messages, but messages within the same scope are ordered.
 * @author Bela Ban
 * @version $Id: SCOPE.java,v 1.8 2010/03/26 09:45:45 belaban Exp $
 */
public class SCOPE extends Protocol {

    protected int thread_pool_min_threads=2;

    protected int thread_pool_max_threads=10;

    protected long thread_pool_keep_alive_time=30000;

    @Property(description="Thread naming pattern for threads in this channel. Default is cl")
    protected String thread_naming_pattern="cl";


    /** Used to find the correct AckReceiverWindow on message reception and deliver it in the right order */
    protected final ConcurrentMap<Address,ConcurrentMap<Short,MessageQueue>> queues
            =new ConcurrentHashMap<Address,ConcurrentMap<Short,MessageQueue>>();


    protected String cluster_name;

    protected Address local_addr;

    protected Executor thread_pool;

    protected ThreadGroup thread_group;
    
    protected ThreadFactory thread_factory;


    @ManagedAttribute(description="Number of scopes in queues")
    public int getNumberOfReceiverScopes() {
        int retval=0;
        for(ConcurrentMap<Short,MessageQueue> map: queues.values())
            retval+=map.keySet().size();
        return retval;
    }

    @ManagedAttribute(description="Total number of messages in all queues")
    public int getNumberOfMessages() {
        int retval=0;
        for(ConcurrentMap<Short,MessageQueue> map: queues.values()) {
            for(MessageQueue queue: map.values())
                retval+=queue.size();
        }

        return retval;
    }


    @Property(name="thread_pool.min_threads",description="Minimum thread pool size for the regular thread pool")
    public void setThreadPoolMinThreads(int size) {
        thread_pool_min_threads=size;
        if(thread_pool instanceof ThreadPoolExecutor)
            ((ThreadPoolExecutor)thread_pool).setCorePoolSize(size);
    }

    public int getThreadPoolMinThreads() {return thread_pool_min_threads;}


    @Property(name="thread_pool.max_threads",description="Maximum thread pool size for the regular thread pool")
    public void setThreadPoolMaxThreads(int size) {
        thread_pool_max_threads=size;
        if(thread_pool instanceof ThreadPoolExecutor)
            ((ThreadPoolExecutor)thread_pool).setMaximumPoolSize(size);
    }

    public int getThreadPoolMaxThreads() {return thread_pool_max_threads;}


    @Property(name="thread_pool.keep_alive_time",description="Timeout in milliseconds to remove idle thread from regular pool")
    public void setThreadPoolKeepAliveTime(long time) {
        thread_pool_keep_alive_time=time;
        if(thread_pool instanceof ThreadPoolExecutor)
            ((ThreadPoolExecutor)thread_pool).setKeepAliveTime(time, TimeUnit.MILLISECONDS);
    }

    public long getThreadPoolKeepAliveTime() {return thread_pool_keep_alive_time;}

    @ManagedOperation(description="Removes all queues and scopes")
    public void removeAllQueues() {
        queues.clear();
    }

    @ManagedOperation(description="Dumps all scopes associated with members")
    public String dumpScopes() {
        StringBuilder sb=new StringBuilder();
        for(Map.Entry<Address,ConcurrentMap<Short,MessageQueue>> entry: queues.entrySet()) {
            sb.append(entry.getKey()).append(": ").append(new TreeSet<Short>(entry.getValue().keySet())).append("\n");
        }
        return sb.toString();
    }


    public void init() throws Exception {
        super.init();
        thread_group=new ThreadGroup(getTransport().getPoolThreadGroup(), "SCOPE Threads");
        thread_factory=new DefaultThreadFactory(thread_group, "SCOPE", false, true);
        setInAllThreadFactories(cluster_name, local_addr, thread_naming_pattern);
    }

    public void start() throws Exception {
        super.start();
        thread_pool=createThreadPool(thread_pool_min_threads, thread_pool_max_threads,
                                     thread_pool_keep_alive_time, thread_factory);
    }

    public void stop() {
        super.stop();
        shutdownThreadPool(thread_pool);
        for(ConcurrentMap<Short,MessageQueue> map: queues.values()) {
            for(MessageQueue queue: map.values())
                queue.release(); // to prevent a thread killed on shutdown() from holding on to the lock
        }
    }

    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;

            case Event.VIEW_CHANGE:
                handleView((View)evt.getArg());
                break;

            case Event.CONNECT:
            case Event.CONNECT_WITH_STATE_TRANSFER:
            case Event.CONNECT_USE_FLUSH:
            case Event.CONNECT_WITH_STATE_TRANSFER_USE_FLUSH:
                cluster_name=(String)evt.getArg();
                setInAllThreadFactories(cluster_name, local_addr, thread_naming_pattern);
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

                MessageQueue queue=getOrCreateQueue(msg.getSrc(), hdr.scope);
                queue.add(msg);

                if(!queue.acquire())
                    return null;

                QueueThread thread=new QueueThread(queue);
                thread_pool.execute(thread);
                return null;
            
            case Event.VIEW_CHANGE:
                handleView((View)evt.getArg());
                break;
        }

        return up_prot.up(evt);
    }


    protected MessageQueue getOrCreateQueue(Address sender, short scope) {
        ConcurrentMap<Short,MessageQueue> val=queues.get(sender);
        if(val == null) {
            val=new ConcurrentHashMap<Short,MessageQueue>();
            ConcurrentMap<Short, MessageQueue> tmp=queues.putIfAbsent(sender, val);
            if(tmp != null)
                val=tmp;
        }
        MessageQueue queue=val.get(scope);
        if(queue == null) {
            queue=new MessageQueue();
            MessageQueue old=val.putIfAbsent(scope, queue);
            if(old != null)
                queue=old;
        }

        return queue;
    }

    protected static ExecutorService createThreadPool(int min_threads, int max_threads, long keep_alive_time,
                                                      final org.jgroups.util.ThreadFactory factory) {

        ThreadPoolExecutor pool=new ThreadManagerThreadPoolExecutor(min_threads, max_threads, keep_alive_time,
                                                                    TimeUnit.MILLISECONDS, new SynchronousQueue<Runnable>());
        pool.setThreadFactory(factory);
        pool.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        return pool;
    }

    protected static void shutdownThreadPool(Executor thread_pool) {
        if(thread_pool instanceof ExecutorService) {
            ExecutorService service=(ExecutorService)thread_pool;
            service.shutdownNow();
            try {
                service.awaitTermination(Global.THREADPOOL_SHUTDOWN_WAIT_TIME, TimeUnit.MILLISECONDS);
            }
            catch(InterruptedException e) {
            }
        }
    }

    private void setInAllThreadFactories(String cluster_name, Address local_address, String pattern) {
        ThreadFactory[] factories= {thread_factory};

        for(ThreadFactory factory:factories) {
            if(pattern != null)
                factory.setPattern(pattern);
            if(cluster_name != null)
                factory.setClusterName(cluster_name);
            if(local_address != null)
                factory.setAddress(local_address.toString());
        }
    }

    private void handleView(View view) {
        Vector<Address> members=view.getMembers();

        // Remove all non members from receiver_table
        Set<Address> keys=new HashSet<Address>(queues.keySet());
        keys.removeAll(members);
        for(Address key: keys)
            clearQueue(key);
    }

    public void clearQueue(Address member) {
        ConcurrentMap<Short,MessageQueue> val=queues.remove(member);
        if(val != null) {
            Collection<MessageQueue> values=val.values();
            for(MessageQueue queue: values)
                queue.clear();
        }
        if(log.isTraceEnabled())
            log.trace("removed " + member + " from receiver_table");
    }



    protected static class MessageQueue {
        private final Queue<Message> queue=new ConcurrentLinkedQueue<Message>();
        private final AtomicBoolean  processing=new AtomicBoolean(false);

        public boolean acquire() {
            return processing.compareAndSet(false, true);
        }

        public boolean release() {
            return processing.compareAndSet(true, false);
        }

        public void add(Message msg) {
            queue.add(msg);
        }

        public Message remove() {
            return queue.poll();
        }

        public void clear() {
            queue.clear();
        }

        public int size() {
            return queue.size();
        }
    }

    
    protected class QueueThread implements Runnable {
        protected final MessageQueue queue;

        public QueueThread(MessageQueue queue) {
            this.queue=queue;
        }

        public void run() {
            // try to remove as many messages as possible from the queue and pass them up
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
