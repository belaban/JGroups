package org.jgroups.protocols.pbcast;

import org.jgroups.*;
import org.jgroups.annotations.*;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.StateTransferInfo;
import org.jgroups.util.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

/**
 * Base class for state transfer protocols which use streaming (or chunking) to transfer state between two members.
 * <p/>
 * <p/>
 * The major advantage of this approach is that transferring application state to a
 * joining member of a group does not entail loading of the complete application
 * state into memory. The application state, for example, might be located entirely
 * on some form of disk based storage. The default {@code STATE_TRANSFER} protocol
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
public abstract class StreamingStateTransfer extends Protocol implements ProcessingQueue.Handler<Address> {

    /*
     * ----------------------------------------------Properties -----------------------------------
     */
    @Property(description="Size (in bytes) of the state transfer buffer")
    protected int                 buffer_size=8 * 1024;

    @Property(description="Maximum number of pool threads serving state requests")
    protected int                 max_pool=5;

    @Property(description="Keep alive for pool threads serving state requests")
    protected long                pool_thread_keep_alive=(long) 20 * 1000;



    /*
     * --------------------------------------------- JMX statistics -------------------------------
     */
    protected final LongAdder     num_state_reqs=new LongAdder();

    protected final LongAdder     num_bytes_sent=new LongAdder();

    protected double              avg_state_size;


    /*
     * --------------------------------------------- Fields ---------------------------------------
     */
    protected Address             local_addr;

    protected volatile Address    state_provider;

    @GuardedBy("members")
    protected final List<Address> members=new ArrayList<>();


    /* Set to true if the FLUSH protocol is detected in the protocol stack */
    protected volatile boolean    flushProtocolInStack=false;


    /** Thread pool (configured with {@link #max_pool} and {@link #pool_thread_keep_alive}) to run
     * {@link StreamingStateTransfer.StateGetter} threads on */
    protected ThreadPoolExecutor  thread_pool;

    /** List of members requesting state. Only a single state request is handled at any time */
    protected final ProcessingQueue<Address> state_requesters=new ProcessingQueue<Address>().setHandler(this);


    @ManagedAttribute public long   getNumberOfStateRequests()    {return num_state_reqs.sum();}
    @ManagedAttribute public long   getNumberOfStateBytesSent()   {return num_bytes_sent.sum();}
    @ManagedAttribute public double getAverageStateSize()         {return avg_state_size;}
    @ManagedAttribute public int    getThreadPoolSize()           {return thread_pool.getPoolSize();}
    @ManagedAttribute public long   getThreadPoolCompletedTasks() {return thread_pool.getCompletedTaskCount();}

    public List<Integer> requiredDownServices() {
        List<Integer> retval=new ArrayList<>(2);
        retval.add(Event.GET_DIGEST);
        retval.add(Event.OVERWRITE_DIGEST);
        return retval;
    }

    public void resetStats() {
        super.resetStats();
        num_state_reqs.reset();
        num_bytes_sent.reset();
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
        Map<String,Object> map=new HashMap<>();
        map.put("state_transfer", true);
        map.put("protocol_class", getClass().getName());
        up_prot.up(new Event(Event.CONFIG, map));
        if(buffer_size <= 0)
            throw new IllegalArgumentException("buffer_size has to be > 0");
    }

    public void stop() {
        super.stop();
    }

    public Object down(Event evt) {

        switch(evt.getType()) {

            case Event.VIEW_CHANGE:
                handleViewChange(evt.getArg());
                break;

            case Event.GET_STATE:
                StateTransferInfo info=evt.getArg();
                Address target=info.target;

                if(Objects.equals(target, local_addr)) {
                    log.error("%s: cannot fetch state from myself", local_addr);
                    target=null;
                }
                if(target == null)
                    target=determineCoordinator();

                if(target == null) {
                    log.debug("%s: first member (no state)", local_addr);
                    up_prot.up(new Event(Event.STATE_TRANSFER_INPUTSTREAM_CLOSED, new StateTransferResult()));
                }
                else {
                    state_provider=target;
                    Message state_req=new Message(target).putHeader(this.id, new StateHeader(StateHeader.STATE_REQ))
                      .setFlag(Message.Flag.SKIP_BARRIER, Message.Flag.DONT_BUNDLE, Message.Flag.OOB);
                    log.debug("%s: asking %s for state", local_addr, target);
                    down_prot.down(state_req);
                }
                return null; // don't pass down any further !

            case Event.CONFIG:
                handleConfig(evt.getArg());
                break;

            case Event.SET_LOCAL_ADDRESS:
                local_addr=evt.getArg();
                break;
        }

        return down_prot.down(evt); // pass on to the layer below us
    }

    public Object up(Event evt) {
        switch(evt.getType()) {
            case Event.TMP_VIEW:
            case Event.VIEW_CHANGE:
                handleViewChange(evt.getArg());
                break;

            case Event.CONFIG:
                handleConfig(evt.getArg());
                break;
        }
        return up_prot.up(evt);
    }

    public Object up(Message msg) {
        StateHeader hdr=msg.getHeader(this.id);
        if(hdr != null) {
            Address sender=msg.getSrc();
            switch(hdr.type) {
                case StateHeader.STATE_REQ:
                    state_requesters.add(msg.getSrc());
                    break;
                case StateHeader.STATE_RSP:
                    handleStateRsp(sender, hdr);
                    break;
                case StateHeader.STATE_PART:
                    handleStateChunk(sender, msg.getRawBuffer(), msg.getOffset(), msg.getLength());
                    break;
                case StateHeader.STATE_EOF:
                    log.trace("%s <-- EOF <-- %s", local_addr, sender);
                    handleEOF(sender);
                    break;
                case StateHeader.STATE_EX:
                    try {
                        handleException(Util.exceptionFromBuffer(msg.getRawBuffer(), msg.getOffset(), msg.getLength()));
                    }
                    catch(Throwable t) {
                        log.error("failed deserializaing state exception", t);
                    }
                    break;
                default:
                    log.error("%s: type %d not known in StateHeader", local_addr, hdr.type);
                    break;
            }
            return null;
        }
        return up_prot.up(msg);
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
    }

    protected void handleEOF(Address sender) {
        state_provider=null;
        down_prot.down(new Event(Event.GET_VIEW_FROM_COORD)); // https://issues.jboss.org/browse/JGRP-1751
    }

    protected void handleException(Throwable exception) {
        state_provider=null; // ??
        up_prot.up(new Event(Event.STATE_TRANSFER_INPUTSTREAM_CLOSED, new StateTransferResult(exception)));
    }

    
    protected void getStateFromApplication(Address requester, OutputStream out, boolean use_separate_thread) {
        if(out == null || requester == null)
            throw new IllegalArgumentException("output stream and requester's address have to be non-null");

        StateGetter state_getter=new StateGetter(requester, out);
        if(use_separate_thread)
            thread_pool.execute(state_getter);
        else
            state_getter.run();
    }


    
    protected void setStateInApplication(InputStream in, Object resource, final Address provider) {
        log.debug("%s: setting the state in the aplication", local_addr);
        try {
            up_prot.up(new Event(Event.STATE_TRANSFER_INPUTSTREAM, in));
            up_prot.up(new Event(Event.STATE_TRANSFER_INPUTSTREAM_CLOSED, new StateTransferResult()));
            down_prot.down(new Event(Event.GET_VIEW_FROM_COORD)); // https://issues.jboss.org/browse/JGRP-1751
        }
        catch(Throwable t) {
            handleException(t);
        }
        finally {
            Util.close(in);
            close(resource);
            if(isDigestNeeded()) {
                openBarrierAndResumeStable();
                closeHoleFor(provider);
            }
        }
    }


    @ManagedOperation(description="Closes BARRIER and suspends STABLE")
    public void closeBarrierAndSuspendStable() {
        if(!isDigestNeeded())
            return;
        log.trace("%s: sending down CLOSE_BARRIER and SUSPEND_STABLE", local_addr);
        down_prot.down(new Event(Event.CLOSE_BARRIER));
        down_prot.down(new Event(Event.SUSPEND_STABLE));
    }

    @ManagedOperation(description="Opens BARRIER and resumes STABLE")
    public void openBarrierAndResumeStable() {
        if(!isDigestNeeded())
            return;
        log.trace("%s: sending down OPEN_BARRIER and RESUME_STABLE", local_addr);
        openBarrier();
        resumeStable();
    }

    protected void openBarrier() {
        down_prot.down(new Event(Event.OPEN_BARRIER));
    }

    protected void resumeStable() {
        down_prot.down(new Event(Event.RESUME_STABLE));
    }

    protected void sendEof(Address requester) {
        try {
            Message eof_msg=new Message(requester).putHeader(getId(), new StateHeader(StateHeader.STATE_EOF));
            log.trace("%s --> EOF --> %s", local_addr, requester);
            down(eof_msg);
        }
        catch(Throwable t) {
            log.error("%s: failed sending EOF to %s", local_addr, requester);
        }
    }

    protected void sendException(Address requester, Throwable exception) {
        try {
            Message ex_msg=new Message(requester).setBuffer(Util.exceptionToBuffer(exception))
              .putHeader(getId(), new StateHeader(StateHeader.STATE_EX));
            down(ex_msg);
        }
        catch(Throwable t) {
            log.error("%s: failed sending exception %s to %s", local_addr, exception.toString(), requester);
        }
    }

    

    protected ThreadPoolExecutor createThreadPool() {
        ThreadPoolExecutor threadPool=new ThreadPoolExecutor(0, max_pool, pool_thread_keep_alive,
                                                             TimeUnit.MILLISECONDS, new SynchronousQueue<>());

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
        state_requesters.retainAll(new_members); // remove non members from list of members requesting state
    }

    public void handle(Address state_requester) {
        handleStateReq(state_requester);
    }

    protected void handleStateReq(Address requester) {
        if(requester == null)
            return;

        log.debug("%s: received state request from %s", local_addr, requester);

        // 1. Get the digest (if needed)
        Digest digest=null;
        if(isDigestNeeded()) {
            try {
                punchHoleFor(requester);
                closeBarrierAndSuspendStable();
                digest=(Digest)down_prot.down(Event.GET_DIGEST_EVT);
            }
            catch(Throwable t) {
                sendException(requester, t);
                resumeStable();
                closeHoleFor(requester);
                return;
            }
            finally {
                openBarrier();
            }
        }

        // 2. Send the STATE_RSP message to the requester
        StateHeader hdr=new StateHeader(StateHeader.STATE_RSP, null, digest);
        // gives subclasses a chance to modify this header, e.g. STATE_SOCK adds the server socket's address
        modifyStateResponseHeader(hdr);
        Message state_rsp=new Message(requester).putHeader(this.id, hdr);
        log.debug("%s: responding to state requester %s", local_addr, requester);
        down_prot.down(state_rsp);
        if(stats)
            num_state_reqs.increment();

        try {
            createStreamToRequester(requester);
        }
        catch(Throwable t) {
            sendException(requester, t);
        }
    }

    /** Creates an OutputStream to the state requester to write the state */
    protected void createStreamToRequester(Address requester) {}

    /** Creates an InputStream to the state provider to read the state. Return the input stream and a handback
     * object as a tuple. The handback object is handed back to the subclass when done, or in case of an error
     * (e.g. to clean up resources) */
    protected abstract Tuple<InputStream,Object> createStreamToProvider(Address provider, StateHeader hdr) throws Exception;

    protected void close(Object resource) {}

    protected boolean useAsyncStateDelivery() {return false;}

    protected void modifyStateResponseHeader(StateHeader hdr) {}

    
    protected void handleStateRsp(final Address provider, StateHeader hdr) {
        // 1: set the digest if needed
        if(isDigestNeeded()) {
            try {
                punchHoleFor(provider);
                closeBarrierAndSuspendStable(); // fix for https://jira.jboss.org/jira/browse/JGRP-1013
                down_prot.down(new Event(Event.OVERWRITE_DIGEST, hdr.getDigest())); // set the digest (e.g. in NAKACK)
            }
            catch(Throwable t) {
                handleException(t);
                openBarrierAndResumeStable();
                closeHoleFor(provider);
                return;
            }
        }

        // 2: establish the connection to the provider (to fetch the state)
        InputStream in=null;
        Object      resource=null;
        try {
            Tuple<InputStream,Object> tuple=createStreamToProvider(provider, hdr); // tuple's 2nd arg is a handback object
            in=tuple.getVal1();
            resource=tuple.getVal2();
        }
        catch(Throwable t) {
            handleException(t);
            Util.close(in);
            close(resource);
            if(isDigestNeeded()) {
                openBarrierAndResumeStable();
                closeHoleFor(provider);
            }
            return;
        }

        // 3: set the state in the application
        if(useAsyncStateDelivery()) {
            final InputStream input=in;
            final Object res=resource;
            // use another thread to read state because the state requester has to receive state chunks from the state provider
            Thread t=getThreadFactory().newThread(() -> setStateInApplication(input, res, provider), "STATE state reader");
            t.start();
        }
        else
            setStateInApplication(in, resource, provider);
    }

    protected void punchHoleFor(Address member) {
        down_prot.down(new Event(Event.PUNCH_HOLE,member));
    }

    protected void closeHoleFor(Address member) {
        down_prot.down(new Event(Event.CLOSE_HOLE, member));
    }




    
    public static class StateHeader extends Header {
        public static final byte STATE_REQ  = 1;
        public static final byte STATE_RSP  = 2;
        public static final byte STATE_PART = 3;
        public static final byte STATE_EOF  = 4;
        public static final byte STATE_EX   = 5;


        protected byte      type=0;
        protected Digest    digest; // digest of sender (if type is STATE_RSP)
        protected IpAddress bind_addr;


        public StateHeader() {
        } // for externalization

        public StateHeader(byte type) {
            this.type=type;
        }

        public StateHeader(byte type, Digest digest) {
            this.type=type;
            this.digest=digest;
        }

        public StateHeader(byte type, IpAddress bind_addr, Digest digest) {
            this.type=type;
            this.digest=digest;
            this.bind_addr=bind_addr;
        }
        public short getMagicId() {return 65;}
        public Supplier<? extends Header> create() {
            return StateHeader::new;
        }

        public int getType() {
            return type;
        }

        public Digest getDigest() {
            return digest;
        }


        public String toString() {
            StringBuilder sb=new StringBuilder();
            sb.append("type=").append(type2Str(type));
            if(digest != null)
                sb.append(", digest=").append(digest);
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
            Util.writeStreamable(digest, out);
            Util.writeStreamable(bind_addr, out);
        }

        public void readFrom(DataInput in) throws Exception {
            type=in.readByte();
            digest=Util.readStreamable(Digest.class, in);
            bind_addr=Util.readStreamable(IpAddress.class, in);
        }

        public int serializedSize() {
            int retval=Global.BYTE_SIZE; // type
            retval+=Global.BYTE_SIZE;    // presence byte for my_digest
            if(digest != null)
                retval+=digest.serializedSize(true);
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
                log.debug("%s: getting the state from the application", local_addr);
                up_prot.up(new Event(Event.STATE_TRANSFER_OUTPUTSTREAM, output));
                output.flush();
                sendEof(requester); // send an EOF to the remote consumer
            }
            catch(Throwable e) {
                sendException(requester, e); // send the exception to the remote consumer
            }
            finally {
                if(isDigestNeeded()) {
                    resumeStable();
                    // openBarrier(); // was done after getting the digest
                    closeHoleFor(requester);
                }
            }
        }
    }
}
