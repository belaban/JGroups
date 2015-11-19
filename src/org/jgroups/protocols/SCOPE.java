package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.util.*;
import org.jgroups.util.ThreadFactory;
import org.jgroups.annotations.*;
import org.jgroups.stack.Protocol;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.*;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Implements https://jira.jboss.org/jira/browse/JGRP-822, which allows for concurrent delivery of messages from the
 * same sender based on scopes. Similar to using OOB messages, but messages within the same scope are ordered.
 * @author Bela Ban
 * @since 2.10
 * @deprecated Use the async invocation API instead:
 * http://www.jgroups.org/manual-3.x/html/user-building-blocks.html#AsyncInvocation
 */
@MBean(description="Implementation of scopes (concurrent delivery of messages from the same sender)")
@Deprecated
public class SCOPE extends Protocol {

    protected int thread_pool_min_threads=2;

    protected int thread_pool_max_threads=10;

    protected long thread_pool_keep_alive_time=30000;

    @Property(description="Thread naming pattern for threads in this channel. Default is cl")
    protected String thread_naming_pattern="cl";

    @Property(description="Time in milliseconds after which an expired scope will get removed. An expired scope is one " +
            "to which no messages have been added in max_expiration_time milliseconds. 0 never expires scopes")
    protected long expiration_time=30000;

    @Property(description="Interval in milliseconds at which the expiry task tries to remove expired scopes")
    protected long expiration_interval=60000;

    protected Future<?> expiry_task=null;


    /**
     * Used to find the correct AckReceiverWindow on message reception and deliver it in the right order
     */
    protected final ConcurrentMap<Address,ConcurrentMap<Short,MessageQueue>> queues=Util.createConcurrentMap();


    protected String cluster_name;

    protected Address local_addr;

    protected Executor thread_pool;

    protected ThreadFactory thread_factory;

    protected TimeScheduler timer;


    public SCOPE() {
    }

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

    @Experimental
    @ManagedOperation(description="Removes all queues and scopes - only used for testing, might get removed any time !")
    public void removeAllQueues() {
        queues.clear();
    }

    /**
     * Multicasts an EXPIRE message to all members, and - on reception - each member removes the scope locally
     * @param scope
     */
    @ManagedOperation(description="Expires the given scope around the cluster")
    public void expire(short scope) {
        ScopeHeader hdr=ScopeHeader.createExpireHeader(scope);
        Message expiry_msg=new Message();
        expiry_msg.putHeader(Global.SCOPE_ID, hdr);
        expiry_msg.setFlag(Message.SCOPED);
        down_prot.down(new Event(Event.MSG, expiry_msg));
    }

    public void removeScope(Address member, short scope) {
        if(member == null) return;
        ConcurrentMap<Short, MessageQueue> val=queues.get(member);
        if(val != null) {
            MessageQueue queue=val.remove(scope);
            if(queue != null)
                queue.clear();
        }
    }

    @ManagedOperation(description="Dumps all scopes associated with members")
    public String dumpScopes() {
        StringBuilder sb=new StringBuilder();
        for(Map.Entry<Address,ConcurrentMap<Short,MessageQueue>> entry: queues.entrySet()) {
            sb.append(entry.getKey()).append(": ").append(new TreeSet<>(entry.getValue().keySet())).append("\n");
        }
        return sb.toString();
    }

    @ManagedAttribute(description="Number of active thread in the pool")
    public int getNumActiveThreads() {
        if(thread_pool instanceof ThreadPoolExecutor)
            return ((ThreadPoolExecutor)thread_pool).getActiveCount();
        return 0;
    }


    public void init() throws Exception {
        super.init();
        timer=getTransport().getTimer();
        thread_factory=new DefaultThreadFactory("SCOPE", false, true);
        setInAllThreadFactories(cluster_name, local_addr, thread_naming_pattern);

        // sanity check for expiration
        if((expiration_interval > 0 && expiration_time <= 0) || (expiration_interval <= 0 && expiration_time > 0))
            throw new IllegalArgumentException("expiration_interval (" + expiration_interval + ") and expiration_time (" +
                    expiration_time + ") don't match");
    }

    public void start() throws Exception {
        super.start();
        thread_pool=createThreadPool(thread_pool_min_threads, thread_pool_max_threads,
                                     thread_pool_keep_alive_time, thread_factory);
        if(expiration_interval > 0 && expiration_time > 0)
            startExpiryTask();
    }

    public void stop() {
        super.stop();
        stopExpiryTask();
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
                if(!msg.isFlagSet(Message.SCOPED) || msg.isFlagSet(Message.Flag.OOB))
                    break;
                
                ScopeHeader hdr=(ScopeHeader)msg.getHeader(id);
                if(hdr == null)
                    throw new IllegalStateException(Util.getMessage("MessageDoesnTHaveAScopeHeaderAttached"));

                if(hdr.type == ScopeHeader.EXPIRE) {
                    removeScope(msg.getSrc(), hdr.scope);
                    return null;
                }

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

    public void up(MessageBatch batch) {
        for(Message msg: batch) {
            if(!msg.isFlagSet(Message.SCOPED) || msg.isFlagSet(Message.Flag.OOB)) // we don't handle unscoped or OOB messages
                continue;

            ScopeHeader hdr=(ScopeHeader)msg.getHeader(id);
            if(hdr == null) {
                log.error(Util.getMessage("MessageDoesnTHaveAScopeHeaderAttached"));
                continue;
            }

            batch.remove(msg); // we do handle the message from here on

            if(hdr.type == ScopeHeader.EXPIRE) {
                removeScope(msg.getSrc(), hdr.scope);
                continue;
            }

            MessageQueue queue=getOrCreateQueue(msg.getSrc(), hdr.scope);
            queue.add(msg);
            if(queue.acquire())
                thread_pool.execute(new QueueThread(queue));
        }

        if(!batch.isEmpty())
            up_prot.up(batch);
    }

    protected MessageQueue getOrCreateQueue(Address sender, short scope) {
        ConcurrentMap<Short,MessageQueue> val=queues.get(sender);
        if(val == null) {
            val=Util.createConcurrentMap();
            ConcurrentMap<Short,MessageQueue> tmp=queues.putIfAbsent(sender, val);
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


    protected synchronized void startExpiryTask() {
        if(expiry_task == null || expiry_task.isDone())
            expiry_task=timer.scheduleWithFixedDelay(new ExpiryTask(), expiration_interval, expiration_interval, TimeUnit.MILLISECONDS);
    }

    protected synchronized void stopExpiryTask() {
        if(expiry_task != null) {
            expiry_task.cancel(true);
            expiry_task=null;
        }
    }

    protected static ExecutorService createThreadPool(int min_threads, int max_threads, long keep_alive_time,
                                                      final org.jgroups.util.ThreadFactory factory) {

        ThreadPoolExecutor pool=new ThreadPoolExecutor(min_threads, max_threads, keep_alive_time,
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
        List<Address> members=view.getMembers();

        // Remove all non members from receiver_table
        Set<Address> keys=new HashSet<>(queues.keySet());
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
        private final Queue<Message> queue=new ConcurrentLinkedQueue<>();
        private final AtomicBoolean  processing=new AtomicBoolean(false);
        private long                 last_update=System.currentTimeMillis();

        public boolean acquire() {
            return processing.compareAndSet(false, true);
        }

        public boolean release() {
            boolean result=processing.compareAndSet(true, false);
            if(result)
                last_update=System.currentTimeMillis();
            return result;
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

        public long getLastUpdate() {
            return last_update;
        }
    }

    
    protected class QueueThread implements Runnable {
        protected final MessageQueue queue;
        protected boolean first=true;

        public QueueThread(MessageQueue queue) {
            this.queue=queue;
        }


        /** Try to remove as many messages as possible from the queue and pass them up. The outer and inner loop and
         * the size() check at the end prevent the following scenario:
         * <pre>
         * - Threads T1 and T2
         * - T1 has the CAS
         * - T1: remove() == null
         * - T2: add()
         * - T2: attempt to set the CAS: false, return
         * - T1: set the CAS to false, return
         * ==> Result: we have a message in the queue that nobody takes care of !
         * </pre>
         * <p/>
         */
        public void run() {
            while(true) { // outer loop
                if(first) // we already have the queue CAS acquired
                    first=false;
                else {
                    if(!queue.acquire())
                        return;
                }

                try {
                    Message msg_to_deliver;
                    while((msg_to_deliver=queue.remove()) != null) { // inner loop
                        try {
                            up_prot.up(new Event(Event.MSG, msg_to_deliver));
                        }
                        catch(Throwable t) {
                            log.error(Util.getMessage("CouldnTDeliverMessage") + msg_to_deliver, t);
                        }
                    }
                }
                finally {
                    queue.release();
                }

                // although ConcurrentLinkedQueue.size() iterates through the list, this is not costly,
                // as at this point, the queue is almost always empty, or has only a few elements
                if(queue.size() == 0) // prevents a concurrent add() (which returned) to leave a dangling message in the queue
                    break;
            }
        }
    }


    protected class ExpiryTask implements Runnable {

        public void run() {
            try {
                _run();
            }
            catch(Throwable t) {
                log.error(Util.getMessage("FailedExpiringOldScopes"), t);
            }
        }

        protected void _run() {
            long current_time=System.currentTimeMillis();
            for(Map.Entry<Address,ConcurrentMap<Short,MessageQueue>> entry: queues.entrySet()) {
                ConcurrentMap<Short,MessageQueue> map=entry.getValue();
                for(Iterator<Map.Entry<Short,MessageQueue>> it=map.entrySet().iterator(); it.hasNext();) {
                    Map.Entry<Short,MessageQueue> entry2=it.next();
                    Short scope=entry2.getKey();
                    MessageQueue queue=entry2.getValue();
                    long diff=current_time - queue.getLastUpdate();
                    if(diff >= expiration_time && queue.size() == 0) {
                        it.remove();
                        if(log.isTraceEnabled())
                            log.trace("expired scope " + entry.getKey() + "::" + scope + " (" + diff + " ms old)");
                    }
                }
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

        public void writeTo(DataOutput out) throws Exception {
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

        public void readFrom(DataInput in) throws Exception {
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
