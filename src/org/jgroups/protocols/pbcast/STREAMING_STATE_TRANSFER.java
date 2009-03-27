package org.jgroups.protocols.pbcast;

import org.jgroups.*;
import org.jgroups.annotations.DeprecatedProperty;
import org.jgroups.annotations.GuardedBy;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.conf.PropertyConverters;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.StateTransferInfo;
import org.jgroups.util.ShutdownRejectedExecutionHandler;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;
import org.jgroups.util.Digest;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * <code>STREAMING_STATE_TRANSFER</code>, as its name implies, allows a
 * streaming state transfer between two channel instances.
 * <p>
 * 
 * Major advantage of this approach is that transferring application state to a
 * joining member of a group does not entail loading of the complete application
 * state into memory. Application state, for example, might be located entirely
 * on some form of disk based storage. The default <code>STATE_TRANSFER</code>
 * requires this state to be loaded entirely into memory before being
 * transferred to a group member while <code>STREAMING_STATE_TRANSFER</code>
 * does not. Thus <code>STREAMING_STATE_TRANSFER</code> protocol is able to
 * transfer application state that is very large (>1Gb) without a likelihood of
 * such transfer resulting in OutOfMemoryException.
 * <p>
 * 
 * <code>STREAMING_STATE_TRANSFER</code> allows use of either default channel
 * transport or separate tcp sockets for state transfer. If firewalls are not a
 * concern then separate tcp sockets should be used as they offer faster state
 * transfer. Transport for state transfer is selected using
 * <code>use_default_transport<code> boolean property. 
 * <p>
 *  
 * 
 * Channel instance can be configured with either
 * <code>STREAMING_STATE_TRANSFER</code> or <code>STATE_TRANSFER</code> but not
 * both protocols at the same time.
 * 
 * <p>
 * 
 * In order to process streaming state transfer an application has to implement
 * <code>ExtendedMessageListener</code> if it is using channel in a push style
 * mode or it has to process <code>StreamingSetStateEvent</code> and
 * <code>StreamingGetStateEvent</code> if it is using channel in a pull style
 * mode.
 * 
 * 
 * @author Vladimir Blagojevic
 * @see org.jgroups.ExtendedMessageListener
 * @see org.jgroups.StreamingGetStateEvent
 * @see org.jgroups.StreamingSetStateEvent
 * @see org.jgroups.protocols.pbcast.STATE_TRANSFER
 * @since 2.4
 * 
 * @version $Id$
 * 
 */
@MBean(description = "State trasnfer protocol based on streaming state transfer")
@DeprecatedProperty(names = {"use_flush", "flush_timeout", "use_reading_thread"})
public class STREAMING_STATE_TRANSFER extends Protocol {

    private final static String NAME = "STREAMING_STATE_TRANSFER";

    /*
     * ----------------------------------------------Properties ----------------------------------- 
     *    
     */

    @Property(converter = PropertyConverters.BindAddress.class, description = "The interface (NIC) used to accept state requests")
    private InetAddress bind_addr;

    @Property(description = "The port listening for state requests. Default value of 0 binds to any (ephemeral) port")
    private int bind_port = 0;

    @Property(description = "Maximum number of pool threads serving state requests. Default is 5")
    private int max_pool = 5;

    @Property(description = "Keep alive for pool threads serving state requests. Default is 20000 msec")
    private long pool_thread_keep_alive = 20 * 1000;

    @Property(description = "Buffer size for state transfer. Default is 8 KB")
    private int socket_buffer_size = 8 * 1024;

    @ManagedAttribute(description = "If true default transport is used for state transfer rather than seperate TCP sockets. Default is false")
    @Property(description = "If true default transport is used for state transfer rather than seperate TCP sockets. Default is false")
    boolean use_default_transport = false;

    /*
     * --------------------------------------------- JMX statistics -------------------------------
     */

    private final AtomicInteger num_state_reqs = new AtomicInteger(0);

    private final AtomicLong num_bytes_sent = new AtomicLong(0);

    private volatile double avg_state_size = 0;

    /*
     * --------------------------------------------- Fields ---------------------------------------
     */

    private Address local_addr = null;

    @GuardedBy("members")
    private final Vector<Address> members;

    /*
     * BlockingQueue to accept state transfer Message(s) if default transport
     * is used. Only state recipient uses this queue
     */
    private final BlockingQueue<Message> stateQueue;

    /*
     * Runnable that listens for state requests and spawns threads to serve
     * those requests if socket transport is used
     */
    private StateProviderThreadSpawner spawner;

    /*
     * Set to true if FLUSH protocol is detected in protocol stack
     */
    private AtomicBoolean flushProtocolInStack = new AtomicBoolean(false);

    public STREAMING_STATE_TRANSFER() {
        members = new Vector<Address>();
        stateQueue = new LinkedBlockingQueue<Message>();
    }

    public final String getName() {
        return NAME;
    }

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

    public Vector<Integer> requiredDownServices() {
        Vector<Integer> retval = new Vector<Integer>();
        retval.addElement(new Integer(Event.GET_DIGEST));
        retval.addElement(new Integer(Event.SET_DIGEST));
        return retval;
    }

    public void resetStats() {
        super.resetStats();
        num_state_reqs.set(0);
        num_bytes_sent.set(0);
        avg_state_size = 0;
    }

    public void init() throws Exception {
    }

    public void start() throws Exception {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("state_transfer", Boolean.TRUE);
        map.put("protocol_class", getClass().getName());
        up_prot.up(new Event(Event.CONFIG, map));
    }

    public void stop() {
        super.stop();
        if (spawner != null) {
            spawner.stop();
        }
    }

    public Object up(Event evt) {
        switch (evt.getType()) {

            case Event.MSG :
                Message msg = (Message) evt.getArg();
                StateHeader hdr = (StateHeader) msg.getHeader(getName());
                if (hdr != null) {
                    switch (hdr.type) {
                        case StateHeader.STATE_REQ :
                            handleStateReq(hdr);
                            break;
                        case StateHeader.STATE_RSP :
                            handleStateRsp(hdr);
                            break;
                        case StateHeader.STATE_PART :
                            try {
                                stateQueue.put(msg);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                            break;
                        default :
                            if (log.isErrorEnabled())
                                log.error("type " + hdr.type + " not known in StateHeader");
                            break;
                    }
                    return null;
                }
                break;

            case Event.SET_LOCAL_ADDRESS :
                local_addr = (Address) evt.getArg();
                break;

            case Event.TMP_VIEW :
            case Event.VIEW_CHANGE :
                handleViewChange((View) evt.getArg());
                break;

            case Event.CONFIG :
                Map<String, Object> config = (Map<String, Object>) evt.getArg();
                if (bind_addr == null && (config != null && config.containsKey("bind_addr"))) {
                    bind_addr = (InetAddress) config.get("bind_addr");
                    if (log.isDebugEnabled())
                        log.debug("using bind_addr from CONFIG event " + bind_addr);
                }
                if (config != null && config.containsKey("state_transfer")) {
                    log.error("Protocol stack must have only one state transfer protocol");
                }
                break;
        }
        return up_prot.up(evt);
    }

    public Object down(Event evt) {

        switch (evt.getType()) {

            case Event.TMP_VIEW :
            case Event.VIEW_CHANGE :
                handleViewChange((View) evt.getArg());
                break;

            case Event.GET_STATE :
                StateTransferInfo info = (StateTransferInfo) evt.getArg();
                Address target;
                if (info.target == null) {
                    target = determineCoordinator();
                } else {
                    target = info.target;
                    if (target.equals(local_addr)) {
                        if (log.isErrorEnabled())
                            log.error("GET_STATE: cannot fetch state from myself !");
                        target = null;
                    }
                }
                if (target == null) {
                    if (log.isDebugEnabled())
                        log.debug("GET_STATE: first member (no state)");
                    up_prot.up(new Event(Event.GET_STATE_OK, new StateTransferInfo()));
                } else {
                    Message state_req = new Message(target, null, null);
                    state_req.putHeader(getName(), new StateHeader(StateHeader.STATE_REQ,
                            local_addr, info.state_id));
                    if (log.isDebugEnabled())
                        log.debug("GET_STATE: asking " + target
                                + " for state, passing down a SUSPEND_STABLE event, timeout="
                                + info.timeout);

                    down_prot.down(new Event(Event.SUSPEND_STABLE, new Long(info.timeout)));
                    down_prot.down(new Event(Event.MSG, state_req));
                }
                return null; // don't pass down any further !

            case Event.STATE_TRANSFER_INPUTSTREAM_CLOSED :
                if (log.isDebugEnabled())
                    log.debug("STATE_TRANSFER_INPUTSTREAM_CLOSED received");

                down_prot.down(new Event(Event.RESUME_STABLE));
                return null;
            case Event.CONFIG :
                Map<String, Object> config = (Map<String, Object>) evt.getArg();
                if (config != null && config.containsKey("flush_supported")) {
                    flushProtocolInStack.set(true);
                }
                break;

        }

        return down_prot.down(evt); // pass on to the layer below us
    }

    /*
     * --------------------------- Private Methods ------------------------------------------------
     */

    /**
     * When FLUSH is used we do not need to pass digests between members
     * 
     * see JGroups/doc/design/PArtialStateTransfer.txt see
     * JGroups/doc/design/FLUSH.txt
     * 
     * @return true if use of digests is required, false otherwise
     */
    private boolean isDigestNeeded() {
        return !flushProtocolInStack.get();
    }

    private void respondToStateRequester(String id, Address stateRequester, boolean open_barrier) {

        // setup socket plumbing if needed
        if (spawner == null && !use_default_transport) {
            spawner = new StateProviderThreadSpawner(setupThreadPool(), Util.createServerSocket(
                    bind_addr, bind_port));
            Thread t = getThreadFactory().newThread(spawner,
                    "STREAMING_STATE_TRANSFER server socket acceptor");
            t.start();
        }

        Digest digest = isDigestNeeded() ? (Digest) down_prot.down(Event.GET_DIGEST_EVT) : null;

        Message state_rsp = new Message(stateRequester);
        StateHeader hdr = new StateHeader(StateHeader.STATE_RSP, local_addr, use_default_transport
                ? null
                : spawner.getServerSocketAddress(), digest, id);
        state_rsp.putHeader(getName(), hdr);

        if (log.isDebugEnabled())
            log.debug("Responding to state requester " + state_rsp.getDest() + " with address "
                    + (use_default_transport ? null : spawner.getServerSocketAddress())
                    + " and digest " + digest);
        down_prot.down(new Event(Event.MSG, state_rsp));
        if (stats) {
            num_state_reqs.incrementAndGet();
        }

        if (open_barrier)
            down_prot.down(new Event(Event.OPEN_BARRIER));

        if (use_default_transport) {
            openAndProvideOutputStreamToStateRecipient(stateRequester, id);
        }
    }

    private ThreadPoolExecutor setupThreadPool() {
        ThreadPoolExecutor threadPool = new ThreadPoolExecutor(0, max_pool, pool_thread_keep_alive,
                TimeUnit.MILLISECONDS, new SynchronousQueue<Runnable>());

        ThreadFactory factory = new ThreadFactory() {
            public Thread newThread(final Runnable command) {
                return getThreadFactory().newThread(command, "STREAMING_STATE_TRANSFER sender");
            }
        };
        threadPool.setRejectedExecutionHandler(new ShutdownRejectedExecutionHandler(threadPool
                .getRejectedExecutionHandler()));
        threadPool.setThreadFactory(factory);
        return threadPool;
    }

    private Address determineCoordinator() {
        synchronized (members) {
            for (Address member : members) {
                if (!local_addr.equals(member)) {
                    return member;
                }
            }
        }
        return null;
    }

    private void handleViewChange(View v) {
        Vector<Address> new_members = v.getMembers();
        synchronized (members) {
            members.clear();
            members.addAll(new_members);
        }
    }

    private void handleStateReq(StateHeader hdr) {
        Address sender = hdr.sender;
        String id = hdr.state_id;
        if (sender == null) {
            if (log.isErrorEnabled())
                log.error("sender is null !");
            return;
        }

        if (isDigestNeeded()) {
            down_prot.down(new Event(Event.CLOSE_BARRIER));
            // drain (and block) incoming msgs until after state has been
            // returned
        }
        try {
            respondToStateRequester(id, sender, isDigestNeeded());
        } catch (Throwable t) {
            if (log.isErrorEnabled())
                log.error("failed fetching state from application", t);
            if (isDigestNeeded())
                down_prot.down(new Event(Event.OPEN_BARRIER));
        }
    }

    void handleStateRsp(final StateHeader hdr) {
        Digest tmp_digest = hdr.my_digest;
        if (isDigestNeeded()) {
            if (tmp_digest == null) {
                if (log.isWarnEnabled())
                    log.warn("digest received from " + hdr.sender
                            + " is null, skipping setting digest !");
            } else {
                down_prot.down(new Event(Event.SET_DIGEST, tmp_digest));
            }
        }
        if (use_default_transport) {
            // have to use another thread to read state while state recipient
            // has to accept state messages from state provider
            Thread t = getThreadFactory().newThread(new Runnable() {
                public void run() {
                    openAndProvideInputStreamToStateProvider(hdr);
                }
            }, "STREAMING_STATE_TRANSFER state reader");
            t.start();
        } else {
            connectToStateProvider(hdr);
        }
    }

    private void openAndProvideInputStreamToStateProvider(StateHeader hdr) {
        BufferedInputStream bis = null;
        try {
            bis = new BufferedInputStream(new StateInputStream(), socket_buffer_size);
            up_prot.up(new Event(Event.STATE_TRANSFER_INPUTSTREAM, new StateTransferInfo(
                    hdr.sender, bis, hdr.getStateId())));
        } catch (IOException e) {
            // pass null stream up so that JChannel.getState() returns false
            log.error("Could not provide state recipient with appropriate stream", e);
            InputStream is = null;
            up_prot.up(new Event(Event.STATE_TRANSFER_INPUTSTREAM, new StateTransferInfo(
                    hdr.sender, is, hdr.getStateId())));
        } finally {
            Util.close(bis);
        }
    }

    private void openAndProvideOutputStreamToStateRecipient(Address stateRequester, String state_id) {
        BufferedOutputStream bos = null;
        try {
            bos = new BufferedOutputStream(new StateOutputStream(stateRequester, state_id),socket_buffer_size);
            up_prot.up(new Event(Event.STATE_TRANSFER_OUTPUTSTREAM, new StateTransferInfo(
                    stateRequester, bos, state_id)));
        } catch (IOException e) {
            if (log.isWarnEnabled()) {
                log.warn("StateOutputStream could not be given to application", e);
            }
        } finally {
            Util.close(bos);
        }
    }

    private void connectToStateProvider(StateHeader hdr) {
        IpAddress address = hdr.bind_addr;
        String tmp_state_id = hdr.getStateId();
        StreamingInputStreamWrapper wrapper = null;
        StateTransferInfo sti = null;
        Socket socket = new Socket();
        try {
            socket.bind(new InetSocketAddress(bind_addr, 0));
            int bufferSize = socket.getReceiveBufferSize();
            socket.setReceiveBufferSize(socket_buffer_size);
            if (log.isDebugEnabled())
                log.debug("Connecting to state provider " + address.getIpAddress() + ":"
                        + address.getPort() + ", original buffer size was " + bufferSize
                        + " and was reset to " + socket.getReceiveBufferSize());
            socket.connect(new InetSocketAddress(address.getIpAddress(), address.getPort()));
            if (log.isDebugEnabled())
                log.debug("Connected to state provider, my end of the socket is "
                        + socket.getLocalAddress() + ":" + socket.getLocalPort()
                        + " passing inputstream up...");

            // write out our state_id and address
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.writeObject(tmp_state_id);
            out.writeObject(local_addr);

            wrapper = new StreamingInputStreamWrapper(socket);
            sti = new StateTransferInfo(hdr.sender, wrapper, tmp_state_id);
            up_prot.up(new Event(Event.STATE_TRANSFER_INPUTSTREAM, sti));
        } catch (IOException e) {
            if (log.isWarnEnabled()) {
                log.warn("State reader socket thread spawned abnormaly", e);
            }

            // pass null stream up so that JChannel.getState() returns false
            InputStream is = null;
            sti = new StateTransferInfo(hdr.sender, is, tmp_state_id);
            up_prot.up(new Event(Event.STATE_TRANSFER_INPUTSTREAM, sti));
        } finally {
            if (!socket.isConnected()) {
                if (log.isWarnEnabled())
                    log.warn("Could not connect to state provider. Closing socket...");
            }
            Util.close(wrapper);
            Util.close(socket);
        }
    }

    /*
     * ------------------------ End of Private Methods --------------------------------------------
     */

    private class StateProviderThreadSpawner implements Runnable {
        private final ExecutorService pool;

        private final ServerSocket serverSocket;

        private final IpAddress address;

        Thread runner;

        private volatile boolean running = true;

        public StateProviderThreadSpawner(ExecutorService pool, ServerSocket stateServingSocket) {
            super();
            this.pool = pool;
            this.serverSocket = stateServingSocket;
            this.address = new IpAddress(STREAMING_STATE_TRANSFER.this.bind_addr, serverSocket
                    .getLocalPort());
        }

        public void run() {
            runner = Thread.currentThread();
            for (; running;) {
                try {
                    if (log.isDebugEnabled())
                        log.debug("StateProviderThreadSpawner listening at "
                                + getServerSocketAddress() + "...");

                    final Socket socket = serverSocket.accept();
                    pool.execute(new Runnable() {
                        public void run() {
                            if (log.isDebugEnabled())
                                log.debug("Accepted request for state transfer from "
                                        + socket.getInetAddress() + ":" + socket.getPort()
                                        + " handing of to PooledExecutor thread");
                            new StateProviderHandler().process(socket);
                        }
                    });

                } catch (IOException e) {
                    if (log.isWarnEnabled()) {
                        // we get this exception when we close server socket
                        // exclude that case
                        if (serverSocket != null && !serverSocket.isClosed()) {
                            log.warn("Spawning socket from server socket finished abnormaly", e);
                        }
                    }
                }
            }
        }

        public IpAddress getServerSocketAddress() {
            return address;
        }

        public void stop() {
            running = false;
            try {
                serverSocket.close();
            } catch (Exception ignored) {
            } finally {
                if (log.isDebugEnabled())
                    log.debug("Waiting for StateProviderThreadSpawner to die ... ");

                if (runner != null) {
                    try {
                        runner.join(Global.THREAD_SHUTDOWN_WAIT_TIME);
                    } catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                    }
                }

                if (log.isDebugEnabled())
                    log.debug("Shutting the thread pool down... ");

                pool.shutdownNow();
                try {
                    pool.awaitTermination(Global.THREADPOOL_SHUTDOWN_WAIT_TIME,
                            TimeUnit.MILLISECONDS);
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            }
            if (log.isDebugEnabled())
                log.debug("Thread pool is shutdown. All pool threads are cleaned up.");
        }
    }

    private class StateProviderHandler {
        public void process(Socket socket) {
            StreamingOutputStreamWrapper wrapper = null;
            ObjectInputStream ois = null;
            try {
                int bufferSize = socket.getSendBufferSize();
                socket.setSendBufferSize(socket_buffer_size);
                if (log.isDebugEnabled())
                    log.debug("Running on " + Thread.currentThread()
                            + ". Accepted request for state transfer from "
                            + socket.getInetAddress() + ":" + socket.getPort()
                            + ", original buffer size was " + bufferSize + " and was reset to "
                            + socket.getSendBufferSize() + ", passing outputstream up... ");

                ois = new ObjectInputStream(socket.getInputStream());
                String state_id = (String) ois.readObject();
                Address stateRequester = (Address) ois.readObject();
                wrapper = new StreamingOutputStreamWrapper(socket);
                StateTransferInfo sti = new StateTransferInfo(stateRequester, wrapper, state_id);
                up_prot.up(new Event(Event.STATE_TRANSFER_OUTPUTSTREAM, sti));
            } catch (IOException e) {
                if (log.isWarnEnabled()) {
                    log.warn("State writer socket thread spawned abnormaly", e);
                }
            } catch (ClassNotFoundException e) {
                // thrown by ois.readObject()
                // should never happen since String/Address are core classes
            } finally {
                if (!socket.isConnected()) {
                    if (log.isWarnEnabled())
                        log.warn("Could not receive connection from state receiver. Closing socket...");
                }
                Util.close(wrapper);
                Util.close(socket);
            }
        }
    }

    private class StreamingInputStreamWrapper extends InputStream {

        private final InputStream delegate;

        private final Socket inputStreamOwner;

        private final AtomicBoolean closed = new AtomicBoolean(false);

        public StreamingInputStreamWrapper(Socket inputStreamOwner) throws IOException {
            super();
            this.inputStreamOwner = inputStreamOwner;
            this.delegate = new BufferedInputStream(inputStreamOwner.getInputStream());
        }

        public int available() throws IOException {
            return delegate.available();
        }

        public void close() throws IOException {
            if (closed.compareAndSet(false, true)) {
                if (log.isDebugEnabled()) {
                    log.debug("State reader is closing the socket ");
                }
                Util.close(delegate);
                Util.close(inputStreamOwner);
                up_prot.up(new Event(Event.STATE_TRANSFER_INPUTSTREAM_CLOSED));
                down(new Event(Event.STATE_TRANSFER_INPUTSTREAM_CLOSED));
            }
        }

        public synchronized void mark(int readlimit) {
            delegate.mark(readlimit);
        }

        public boolean markSupported() {
            return delegate.markSupported();
        }

        public int read() throws IOException {
            return delegate.read();
        }

        public int read(byte[] b, int off, int len) throws IOException {
            return delegate.read(b, off, len);
        }

        public int read(byte[] b) throws IOException {
            return delegate.read(b);
        }

        public synchronized void reset() throws IOException {
            delegate.reset();
        }

        public long skip(long n) throws IOException {
            return delegate.skip(n);
        }
    }

    private class StreamingOutputStreamWrapper extends OutputStream {
        private final Socket outputStreamOwner;

        private final OutputStream delegate;

        private final AtomicBoolean closed = new AtomicBoolean(false);

        private long bytesWrittenCounter = 0;

        public StreamingOutputStreamWrapper(Socket outputStreamOwner) throws IOException {
            super();
            this.outputStreamOwner = outputStreamOwner;
            this.delegate = new BufferedOutputStream(outputStreamOwner.getOutputStream());
        }

        public void close() throws IOException {
            if (closed.compareAndSet(false, true)) {
                if (log.isDebugEnabled()) {
                    log.debug("State writer is closing the socket ");
                }

                Util.close(delegate);
                Util.close(outputStreamOwner);
                up_prot.up(new Event(Event.STATE_TRANSFER_OUTPUTSTREAM_CLOSED));
                down(new Event(Event.STATE_TRANSFER_OUTPUTSTREAM_CLOSED));

                if (stats) {
                    avg_state_size = num_bytes_sent.addAndGet(bytesWrittenCounter)
                            / num_state_reqs.doubleValue();
                }
            }
        }

        public void flush() throws IOException {
            delegate.flush();
        }

        public void write(byte[] b, int off, int len) throws IOException {
            delegate.write(b, off, len);
            bytesWrittenCounter += len;
        }

        public void write(byte[] b) throws IOException {
            delegate.write(b);
            if (b != null) {
                bytesWrittenCounter += b.length;
            }
        }

        public void write(int b) throws IOException {
            delegate.write(b);
            bytesWrittenCounter += 1;
        }
    }

    private class StateInputStream extends InputStream {

        private final AtomicBoolean closed;

        public StateInputStream() throws IOException {
            super();
            this.closed = new AtomicBoolean(false);
        }

        public void close() throws IOException {
            if (closed.compareAndSet(false, true)) {
                if (log.isDebugEnabled()) {
                    log.debug("State reader is closing the stream");
                }
                stateQueue.clear();
                up(new Event(Event.STATE_TRANSFER_INPUTSTREAM_CLOSED));
                down(new Event(Event.STATE_TRANSFER_INPUTSTREAM_CLOSED));
                super.close();
            }
        }

        public int read() throws IOException {
            if (closed.get())
                return -1;
            final byte[] array = new byte[1];
            return read(array) == -1 ? -1 : array[0];
        }

        public int read(byte[] b, int off, int len) throws IOException {
            if (closed.get())
                return -1;
            Message m = null;
            try {
                m = stateQueue.take();
                StateHeader hdr = (StateHeader) m.getHeader(getName());
                if (hdr.type == StateHeader.STATE_PART) {
                    return readAndTransferPayload(m, b, off, len);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new InterruptedIOException();
            }
            return -1;
        }

        private int readAndTransferPayload(Message m, byte[] b, int off, int len) {
            byte[] buffer = m.getBuffer();
            if (log.isDebugEnabled()) {
                log.debug(local_addr + " reading chunk of state  " + "byte[] b=" + b.length
                        + ", off=" + off + ", buffer.length=" + buffer.length);
            }
            System.arraycopy(buffer, 0, b, off, buffer.length);
            return buffer.length;
        }

        public int read(byte[] b) throws IOException {
            if (closed.get())
                return -1;
            return read(b, 0, b.length);
        }
    }

    private class StateOutputStream extends OutputStream {

        private final Address stateRequester;
        private final String state_id;
        private final AtomicBoolean closed;
        private long bytesWrittenCounter = 0;

        public StateOutputStream(Address stateRequester, String state_id) throws IOException {
            super();
            this.stateRequester = stateRequester;
            this.state_id = state_id;
            this.closed = new AtomicBoolean(false);
        }

        public void close() throws IOException {
            if (closed.compareAndSet(false, true)) {
                if (log.isDebugEnabled()) {
                    log.debug("State writer " + local_addr
                            + " is closing the output stream for state_id " + state_id);
                }
                up(new Event(Event.STATE_TRANSFER_OUTPUTSTREAM_CLOSED));
                down(new Event(Event.STATE_TRANSFER_OUTPUTSTREAM_CLOSED));
                if (stats) {
                    avg_state_size = num_bytes_sent.addAndGet(bytesWrittenCounter)
                            / num_state_reqs.doubleValue();
                }
                super.close();
            }
        }

        public void write(byte[] b, int off, int len) throws IOException {
            if (closed.get())
                throw closed();
            sendMessage(b, off, len);
        }

        public void write(byte[] b) throws IOException {
            if (closed.get())
                throw closed();
            sendMessage(b, 0, b.length);
        }

        public void write(int b) throws IOException {
            if (closed.get())
                throw closed();
            byte buf[] = new byte[]{(byte) b};
            write(buf);
        }

        private void sendMessage(byte[] b, int off, int len) throws IOException {
            Message m = new Message(stateRequester);
            m.putHeader(getName(), new StateHeader(StateHeader.STATE_PART, local_addr, state_id));
            m.setBuffer(b, off, len);
            bytesWrittenCounter += (len - off);
            if (Thread.interrupted()) {
                throw interrupted((int) bytesWrittenCounter);
            }
            down_prot.down(new Event(Event.MSG, m));
            if (log.isDebugEnabled()) {
                log.debug(local_addr + " sent chunk of state to " + stateRequester + "byte[] b="
                        + b.length + ", off=" + off + ", len=" + len);
            }
        }

        private IOException closed() {
            return new IOException("The output stream is closed");
        }

        private InterruptedIOException interrupted(int cnt) {
            final InterruptedIOException ex = new InterruptedIOException();
            ex.bytesTransferred = cnt;
            return ex;
        }
    }

    public static class StateHeader extends Header implements Streamable {
        public static final byte STATE_REQ = 1;

        public static final byte STATE_RSP = 2;

        public static final byte STATE_PART = 3;

        long id = 0; // state transfer ID (to separate multiple state transfers
        // at the same time)

        byte type = 0;

        Address sender; // sender of state STATE_REQ or STATE_RSP

        Digest my_digest = null; // digest of sender (if type is STATE_RSP)

        IpAddress bind_addr = null;

        String state_id = null; // for partial state transfer

        public StateHeader() {
        } // for externalization

        public StateHeader(byte type, Address sender, String state_id) {
            this.type = type;
            this.sender = sender;
            this.state_id = state_id;
        }

        public StateHeader(byte type, Address sender, long id, Digest digest) {
            this.type = type;
            this.sender = sender;
            this.id = id;
            this.my_digest = digest;
        }

        public StateHeader(byte type, Address sender, IpAddress bind_addr, Digest digest,
                String state_id) {
            this.type = type;
            this.sender = sender;
            this.my_digest = digest;
            this.bind_addr = bind_addr;
            this.state_id = state_id;
        }

        public int getType() {
            return type;
        }

        public Digest getDigest() {
            return my_digest;
        }

        public String getStateId() {
            return state_id;
        }

        public boolean equals(Object o) {
            StateHeader other;

            if (sender != null && o != null) {
                if (!(o instanceof StateHeader))
                    return false;
                other = (StateHeader) o;
                return sender.equals(other.sender) && id == other.id;
            }
            return false;
        }

        public int hashCode() {
            if (sender != null)
                return sender.hashCode() + (int) id;
            else
                return (int) id;
        }

        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("type=").append(type2Str(type));
            if (sender != null)
                sb.append(", sender=").append(sender).append(" id=").append(id);
            if (my_digest != null)
                sb.append(", digest=").append(my_digest);
            return sb.toString();
        }

        static String type2Str(int t) {
            switch (t) {
                case STATE_REQ :
                    return "STATE_REQ";
                case STATE_RSP :
                    return "STATE_RSP";
                case STATE_PART :
                    return "STATE_PART";
                default :
                    return "<unknown>";
            }
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(sender);
            out.writeLong(id);
            out.writeByte(type);
            out.writeObject(my_digest);
            out.writeObject(bind_addr);
            out.writeUTF(state_id);
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            sender = (Address) in.readObject();
            id = in.readLong();
            type = in.readByte();
            my_digest = (Digest) in.readObject();
            bind_addr = (IpAddress) in.readObject();
            state_id = in.readUTF();
        }

        public void writeTo(DataOutputStream out) throws IOException {
            out.writeByte(type);
            out.writeLong(id);
            Util.writeAddress(sender, out);
            Util.writeStreamable(my_digest, out);
            Util.writeStreamable(bind_addr, out);
            Util.writeString(state_id, out);
        }

        public void readFrom(DataInputStream in) throws IOException, IllegalAccessException,
                InstantiationException {
            type = in.readByte();
            id = in.readLong();
            sender = Util.readAddress(in);
            my_digest = (Digest) Util.readStreamable(Digest.class, in);
            bind_addr = (IpAddress) Util.readStreamable(IpAddress.class, in);
            state_id = Util.readString(in);
        }

        public int size() {
            int retval = Global.LONG_SIZE + Global.BYTE_SIZE; // id and type

            retval += Util.size(sender);

            retval += Global.BYTE_SIZE; // presence byte for my_digest
            if (my_digest != null)
                retval += my_digest.serializedSize();

            retval += Util.size(bind_addr);

            retval += Global.BYTE_SIZE; // presence byte for state_id
            if (state_id != null)
                retval += state_id.length() + 2;
            return retval;
        }
    }
}
