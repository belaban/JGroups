package org.jgroups.protocols.pbcast;

import org.jgroups.*;
import org.jgroups.annotations.*;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.StateTransferInfo;
import org.jgroups.util.Digest;
import org.jgroups.util.ShutdownRejectedExecutionHandler;
import org.jgroups.util.StateTransferResult;
import org.jgroups.util.Util;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Base class for state transfer protocols which use streaming (or chunking) to transfer state between two members.
 * <p/>
 * <p/>
 * The major advantage of this approach is that transferring application state to a
 * joining member of a group does not entail loading of the complete application
 * state into memory. The application state, for example, might be located entirely
 * on some form of disk based storage. The default <code>STATE_TRANSFER</code> protocol
 * requires this state to be loaded entirely into memory before being
 * transferred to a group member while the streaming state transfer protocols do not.
 * Thus the streaming state transfer protocols are able to
 * transfer application state that is very large (>1Gb) without a likelihood of the
 * such transfer resulting in OutOfMemoryException.
 * <p/>
 * Note that prior to 3.0, there was only 1 streaming protocol: STATE. In 3.0 the functionality
 * was split between STATE and STATE_SOCK, and common functionality moved up
 * into StreamingStateTransfer.
 * @author Bela Ban
 * @author Vladimir Blagojevic
 * @see STATE_TRANSFER
 * @see STATE
 * @see STATE_SOCK
 * @since 3.0
 */
@MBean(description="Streaming state transfer protocol base class")
public abstract class StreamingStateTransfer extends Protocol {

    /*
     * ----------------------------------------------Properties -----------------------------------
     */
    @Property(description="Size (in bytes) of the state transfer buffer")
    protected int buffer_size=8 * 1024;

    @Property(description="Maximum number of pool threads serving state requests")
    protected int  max_pool=5;

    @Property(description="Keep alive for pool threads serving state requests")
    protected long pool_thread_keep_alive=20 * 1000;



    /*
     * --------------------------------------------- JMX statistics -------------------------------
     */
    protected final AtomicInteger num_state_reqs=new AtomicInteger(0);

    protected final AtomicLong num_bytes_sent=new AtomicLong(0);

    protected double avg_state_size=0;


    /*
     * --------------------------------------------- Fields ---------------------------------------
     */
    protected Address local_addr=null;

    protected volatile Address state_provider=null;

    @GuardedBy("members")
    protected final List<Address> members=new ArrayList<Address>();


    /* Set to true if the FLUSH protocol is detected in the protocol stack */
    protected volatile boolean flushProtocolInStack=false;

    /** Used to prevent spurious open and close barrier calls */
    @ManagedAttribute(description="whether or not the barrier is closed")
    protected AtomicBoolean barrier_closed=new AtomicBoolean(false);


    /** Thread pool (configured with {@link #max_pool} and {@link #pool_thread_keep_alive}) to run
     * {@link org.jgroups.protocols.pbcast.StreamingStateTransfer.StateGetter} threads on */
    protected ThreadPoolExecutor thread_pool;


    /**
     * Whenever we get a state transfer request, we create an OutputStream and add the state requester's address and
     * the OutputStream to this map. The state is fetched from the application on a separate thread, a
     * {@link org.jgroups.protocols.pbcast.StreamingStateTransfer.StateGetter} thread.
     */
    protected final Map<Address,OutputStream> pending_state_transfers=new HashMap<Address,OutputStream>();

    /** Used to synchronize all state requests and responses */
    protected final Lock state_lock=new ReentrantLock();


    @ManagedAttribute
    public int getNumberOfStateRequests() {
        return num_state_reqs.get();
    }

    @ManagedAttribute
    public long getNumberOfStateBytesSent() {
        return num_bytes_sent.get();
    }

    @ManagedAttribute
    public double getAverageStateSize() {
        return avg_state_size;
    }

    @ManagedAttribute public int  getThreadPoolSize() {return thread_pool.getPoolSize();}
    @ManagedAttribute public long getThreadPoolCompletedTasks() {return thread_pool.getCompletedTaskCount();}

    public List<Integer> requiredDownServices() {
        List<Integer> retval=new ArrayList<Integer>(2);
        retval.add(Event.GET_DIGEST);
        retval.add(Event.OVERWRITE_DIGEST);
        return retval;
    }

    public void resetStats() {
        super.resetStats();
        num_state_reqs.set(0);
        num_bytes_sent.set(0);
        avg_state_size=0;
    }


    public void init() throws Exception {
        super.init();
        thread_pool=createThreadPool();
    }

    public void destroy() {
        thread_pool.shutdown();
        super.destroy();
    }

    public void start() throws Exception {
        Map<String,Object> map=new HashMap<String,Object>();
        map.put("state_transfer", true);
        map.put("protocol_class", getClass().getName());
        up_prot.up(new Event(Event.CONFIG, map));
        if(buffer_size <= 0)
            throw new IllegalArgumentException("buffer_size has to be > 0");
    }

    public void stop() {
        super.stop();
        barrier_closed.set(false);
    }

    public Object down(Event evt) {

        switch(evt.getType()) {

            case Event.VIEW_CHANGE:
                handleViewChange((View)evt.getArg());
                break;

            case Event.GET_STATE:
                StateTransferInfo info=(StateTransferInfo)evt.getArg();
                Address target;
                if(info.target == null) {
                    target=determineCoordinator();
                }
                else {
                    target=info.target;
                    if(target.equals(local_addr)) {
                        if(log.isErrorEnabled())
                            log.error("cannot fetch state from myself !");
                        target=null;
                    }
                }
                if(target == null) {
                    if(log.isDebugEnabled())
                        log.debug("first member (no state)");
                    up_prot.up(new Event(Event.STATE_TRANSFER_INPUTSTREAM_CLOSED, new StateTransferResult()));
                }
                else {
                    state_provider=target;
                    Message state_req=new Message(target, null, null);
                    state_req.putHeader(this.id, new StateHeader(StateHeader.STATE_REQ));
                    if(log.isDebugEnabled())
                        log.debug(local_addr + ": asking " + target + " for state");
                    down_prot.down(new Event(Event.MSG, state_req));
                }
                return null; // don't pass down any further !

            case Event.CONFIG:
                handleConfig((Map<String, Object>)evt.getArg());
                break;

            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;
        }

        return down_prot.down(evt); // pass on to the layer below us
    }

   

    public Object up(Event evt) {
        switch(evt.getType()) {

            case Event.MSG:
                Message msg=(Message)evt.getArg();
                StateHeader hdr=(StateHeader)msg.getHeader(this.id);
                if(hdr != null) {
                    Address sender=msg.getSrc();
                    switch(hdr.type) {
                        case StateHeader.STATE_REQ:
                            handleStateReq(sender);
                            break;
                        case StateHeader.STATE_RSP:
                            handleStateRsp(sender, hdr);
                            break;
                        case StateHeader.STATE_PART:
                            handleStateChunk(sender, msg.getRawBuffer(), msg.getOffset(), msg.getLength());
                            break;
                        case StateHeader.STATE_EOF:
                            if(log.isTraceEnabled())
                                log.trace(local_addr + " <-- EOF <-- " + sender);
                            handleEOF(sender);
                            break;

                        case StateHeader.STATE_EX:
                            handleException((Throwable)msg.getObject());
                            break;

                        default:
                            if(log.isErrorEnabled())
                                log.error("type " + hdr.type + " not known in StateHeader");
                            break;
                    }
                    return null;
                }
                break;

            case Event.TMP_VIEW:
            case Event.VIEW_CHANGE:
                handleViewChange((View)evt.getArg());
                break;

            case Event.CONFIG:
                handleConfig((Map<String,Object>)evt.getArg());
                break;
        }
        return up_prot.up(evt);
    }


    /*
     * --------------------------- Private Methods ------------------------------------------------
     */

    /**
     * When FLUSH is used we do not need to pass digests between members (see JGroups/doc/design/FLUSH.txt)
     * @return true if use of digests is required, false otherwise
     */
    protected boolean isDigestNeeded() {
        return !flushProtocolInStack;
    }

    protected void handleConfig(Map<String,Object> config) {
        if(config != null && config.containsKey("flush_supported"))
            flushProtocolInStack=true;
        if(config != null && config.containsKey("state_transfer"))
            throw new IllegalArgumentException("Protocol stack must have only one state transfer protocol");
    }

    protected void handleStateChunk(Address sender, byte[] buffer, int offset, int length) {
        ;
    }

    protected void handleEOF(Address sender) {
        state_provider=null;
    }

    protected void handleException(Throwable exception) {
        state_provider=null; // ??
        openBarrierAndResumeStable();
        up_prot.up(new Event(Event.STATE_TRANSFER_INPUTSTREAM_CLOSED, new StateTransferResult(exception)));
    }

    
    protected void getStateFromApplication(Address requester, OutputStream out, boolean use_separate_thread) {
        if(out == null || requester == null)
            throw new IllegalArgumentException("output stream and requester's address have to be non-null");

        boolean close_barrier=false;
        state_lock.lock();
        try {
            if(pending_state_transfers.containsKey(requester))
                throw new IllegalStateException("requester " + requester + " has a pending state transfer; concurrent "
                                                  + "state transfers from the same member are not supported");

            if((close_barrier=pending_state_transfers.isEmpty()))
                closeBarrierAndSuspendStable();
            pending_state_transfers.put(requester, out);

            StateGetter state_getter=new StateGetter(requester, out);
            if(use_separate_thread)
                thread_pool.execute(state_getter);
            else
                state_getter.run();
        }
        catch(Throwable t) {
            if(close_barrier)
                openBarrierAndResumeStable();
            sendException(requester, t);
            pending_state_transfers.remove(requester);
        }
        finally {
            state_lock.unlock();
        }
    }

    @GuardedBy("state_lock")
    protected void removeRequester(Address requester) {
        if(requester == null) return;
        OutputStream out=pending_state_transfers.remove(requester);
        Util.close(out);
        if(out != null && pending_state_transfers.isEmpty())
            openBarrierAndResumeStable();
    }

    
    protected void setStateInApplication(Address provider, InputStream in, Digest digest) {
        closeBarrierAndSuspendStable(); // fix for https://jira.jboss.org/jira/browse/JGRP-1013
        try {
            if(digest != null)
                down_prot.down(new Event(Event.OVERWRITE_DIGEST, digest));
            if(log.isTraceEnabled())
                log.trace(local_addr + ": setting the state in the aplication");
            up_prot.up(new Event(Event.STATE_TRANSFER_INPUTSTREAM, in));
            openBarrierAndResumeStable();
            up_prot.up(new Event(Event.STATE_TRANSFER_INPUTSTREAM_CLOSED, new StateTransferResult()));
        }
        catch(Throwable t) {
            handleException(t);
        }
    }

    @ManagedOperation(description="Closes BARRIER and suspends STABLE")
    public void closeBarrierAndSuspendStable() {
        if(!isDigestNeeded() || !barrier_closed.compareAndSet(false, true))
            return;
        if(log.isTraceEnabled())
            log.trace(local_addr + ": sending down CLOSE_BARRIER and SUSPEND_STABLE");
        down_prot.down(new Event(Event.CLOSE_BARRIER));
        down_prot.down(new Event(Event.SUSPEND_STABLE));
    }

    @ManagedOperation(description="Opens BARRIER and resumes STABLE")
    public void openBarrierAndResumeStable() {
        if(!isDigestNeeded() || !barrier_closed.compareAndSet(true, false))
            return;
        if(log.isTraceEnabled())
            log.trace(local_addr + ": sending down OPEN_BARRIER and RESUME_STABLE");
        down_prot.down(new Event(Event.OPEN_BARRIER));
        down_prot.down(new Event(Event.RESUME_STABLE));
    }

    protected void sendEof(Address requester) {
        try {
            Message eof_msg=new Message(requester);
            eof_msg.putHeader(getId(), new StateHeader(StateHeader.STATE_EOF));
            if(log.isTraceEnabled())
                log.trace(local_addr + " --> EOF --> " + requester);
            down(new Event(Event.MSG, eof_msg));
        }
        catch(Throwable t) {
            log.error(local_addr + ": failed sending EOF to " + requester);
        }
    }

    protected void sendException(Address requester, Throwable exception) {
        try {
            Message ex_msg=new Message(requester, null, exception);
            ex_msg.putHeader(getId(), new StateHeader(StateHeader.STATE_EX));
            down(new Event(Event.MSG, ex_msg));
        }
        catch(Throwable t) {
            log.error(local_addr + ": failed sending exception " + exception.toString() + " to " + requester);
        }
    }

    

    protected ThreadPoolExecutor createThreadPool() {
        ThreadPoolExecutor threadPool=new ThreadPoolExecutor(0, max_pool, pool_thread_keep_alive,
                                                             TimeUnit.MILLISECONDS, new SynchronousQueue<Runnable>());

        ThreadFactory factory=new ThreadFactory() {
            private final AtomicInteger thread_id=new AtomicInteger(1);

            public Thread newThread(final Runnable command) {
                return getThreadFactory().newThread(command, "StreamingStateTransfer-sender-" + thread_id.getAndIncrement());
            }
        };
        threadPool.setRejectedExecutionHandler(new ShutdownRejectedExecutionHandler(threadPool.getRejectedExecutionHandler()));
        threadPool.setThreadFactory(factory);
        return threadPool;
    }

    protected Address determineCoordinator() {
        synchronized(members) {
            for(Address member : members) {
                if(!local_addr.equals(member))
                    return member;
            }
        }
        return null;
    }

    protected void handleViewChange(View v) {
        List<Address> new_members=v.getMembers();
        synchronized(members) {
            members.clear();
            members.addAll(new_members);
        }

        state_lock.lock();
        try {
            Set<Address> keys=new HashSet<Address>(pending_state_transfers.keySet());
            for(Address key: keys) {
                if(!new_members.contains(key))
                    removeRequester(key);
            }
        }
        finally {
            state_lock.unlock();
        }
    }

   

    protected void handleStateReq(Address requester) {
        if(requester == null) {
            if(log.isErrorEnabled())
                log.error(local_addr + ": sender of STATE_REQ is null; ignoring state transfer request");
            return;
        }

        Message state_rsp=new Message(requester);
        Digest digest=isDigestNeeded()? (Digest)down_prot.down(Event.GET_DIGEST_EVT) : null;
        StateHeader hdr=new StateHeader(StateHeader.STATE_RSP, null, digest);
        // gives subclasses a chance to modify this header, e.g. STATE_SOCK adds the server socket's address
        modifyStateResponseHeader(hdr);
        state_rsp.putHeader(this.id, hdr);
        if(log.isDebugEnabled())
            log.debug(local_addr + ": responding to state requester " + requester);
        down_prot.down(new Event(Event.MSG, state_rsp));
        if(stats)
            num_state_reqs.incrementAndGet();

        try {
            createStreamToRequester(requester);
        }
        catch(Throwable t) {
            sendException(requester, t);
        }
    }

    /** Creates an OutputStream to the state requester to write the state */
    protected abstract void createStreamToRequester(Address requester);

    /** Creates an InputStream to the state provider to read the state */
    protected abstract void createStreamToProvider(Address provider, StateHeader hdr);

    // protected boolean closeStreamAfterSettingState() {return true;}

    protected void modifyStateResponseHeader(StateHeader hdr) {
        ;
    }

    
    void handleStateRsp(Address sender, StateHeader hdr) {
        createStreamToProvider(sender, hdr);
    }






    
    public static class StateHeader extends Header {
        public static final byte STATE_REQ  = 1;
        public static final byte STATE_RSP  = 2;
        public static final byte STATE_PART = 3;
        public static final byte STATE_EOF  = 4;
        public static final byte STATE_EX   = 5;


        protected byte      type=0;
        protected Digest    my_digest=null; // digest of sender (if type is STATE_RSP)
        protected IpAddress bind_addr=null;


        public StateHeader() {
        } // for externalization

        public StateHeader(byte type) {
            this.type=type;
        }

        public StateHeader(byte type, Digest digest) {
            this.type=type;
            this.my_digest=digest;
        }

        public StateHeader(byte type, IpAddress bind_addr, Digest digest) {
            this.type=type;
            this.my_digest=digest;
            this.bind_addr=bind_addr;
        }

        public int getType() {
            return type;
        }

        public Digest getDigest() {
            return my_digest;
        }


        public String toString() {
            StringBuilder sb=new StringBuilder();
            sb.append("type=").append(type2Str(type));
            if(my_digest != null)
                sb.append(", digest=").append(my_digest);
            if(bind_addr != null)
                sb.append(", bind_addr=" + bind_addr);
            return sb.toString();
        }

        static String type2Str(int t) {
            switch(t) {
                case STATE_REQ:  return "STATE_REQ";
                case STATE_RSP:  return "STATE_RSP";
                case STATE_PART: return "STATE_PART";
                case STATE_EOF:  return "STATE_EOF";
                case STATE_EX:   return "STATE_EX";
                default:         return "<unknown>";
            }
        }


        public void writeTo(DataOutput out) throws Exception {
            out.writeByte(type);
            Util.writeStreamable(my_digest, out);
            Util.writeStreamable(bind_addr, out);
        }

        public void readFrom(DataInput in) throws Exception {
            type=in.readByte();
            my_digest=(Digest)Util.readStreamable(Digest.class, in);
            bind_addr=(IpAddress)Util.readStreamable(IpAddress.class, in);
        }

        public int size() {
            int retval=Global.BYTE_SIZE; // type
            retval+=Global.BYTE_SIZE;    // presence byte for my_digest
            if(my_digest != null)
                retval+=my_digest.serializedSize();
            retval+=Util.size(bind_addr);
            return retval;
        }
    }


    /**
     * Thread which invokes {@link MessageListener#getState(java.io.OutputStream)} in the application
     */
    protected class StateGetter implements Runnable {
        protected final Address      requester;
        protected final OutputStream output;

        public StateGetter(Address requester, OutputStream output) {
            this.requester=requester;
            this.output=output;
        }

        public void run() {
            try {
                if(log.isTraceEnabled())
                    log.trace(local_addr + ": getting the state from the application");
                up_prot.up(new Event(Event.STATE_TRANSFER_OUTPUTSTREAM, output));
                output.flush();
                sendEof(requester); // send an EOF to the remote consumer
            }
            catch(Throwable e) {
                if(log.isWarnEnabled())
                    log.warn(local_addr + ": failed getting the state from the application", e);
                sendException(requester, e); // send the exception to the remote consumer
            }
            finally {
                state_lock.lock();
                try {
                    removeRequester(requester);
                }
                finally {
                    state_lock.unlock();
                }
            }
        }
    }
}
