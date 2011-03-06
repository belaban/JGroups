package org.jgroups.protocols;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Header;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.blocks.executor.ExecutorEvent;
import org.jgroups.blocks.executor.ExecutorNotification;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

@MBean(description="Based class for executor service functionality")
abstract public class Executing extends Protocol {

    @Property(description="bypasses message bundling if set")
    protected boolean bypass_bundling=true;


    protected Address local_addr;

    protected View view;
    
    /**
     * This is a queue on the client side that holds all of the tasks that
     * are awaiting a consumer to pick them up
     */
    protected final Queue<Runnable> _awaitingConsumer = 
        new ConcurrentLinkedQueue<Runnable>();

    protected final ConcurrentMap<Future<?>, ExecutorNotification> notifiers = 
        new ConcurrentHashMap<Future<?>, ExecutorNotification>();
    
    /**
     * This is a map on the server side that shows which owner is currently
     * tied to the runnable so we can return to them the results
     */
    protected final Map<Runnable, Owner> _running;
    
    /**
     * This is a map on the client side that shows for which 
     * owner(consumer, request) the runnable they are currently using.  This 
     * also allows us to set the values on a future when finished.
     */
    protected final Map<Owner, Runnable> _awaitingReturn;
    
    /**
     * This is a server side queue of all the tasks to pass off.  Currently 
     * there will never be tasks waiting to put in.  If a task is put in and doesn't have a 
     * respective take at the same time that task is rejected.
     */
    protected BlockingQueue<Runnable> _tasks = new SynchronousQueue<Runnable>();
    
    /**
     * This is a server side map to show which threads are running for a
     * given runnable.  This is used to interrupt those threads if needed.
     */
    protected final ConcurrentMap<Runnable, Thread> _runnableThreads = 
        new ConcurrentHashMap<Runnable, Thread>();
    
    /**
     * This lock is to protect the incoming run requests and the incoming
     * consumer queues
     */
    protected Lock _consumerLock = new ReentrantLock();
    
    /**
     * This is stored on the coordinator side.  This queue holds all of the
     * addresses that currently want to run something.  If this queue has
     * elements the consumer queue must be empty.
     */
    protected Queue<Address> _runRequests = new ArrayDeque<Address>();
    
    /**
     * This is stored on the coordinator side.  This queue holds all of the
     * addresses that currently are able to run something.  If this queue has
     * elements the run request queue must be empty.
     */
    protected Queue<Address> _consumersAvailable = new ArrayDeque<Address>();

    protected static enum Type {
        RUN_REQUEST,            // request to coordinator from client to tell of a new task request
        CONSUMER_READY,         // request to coordinator from server to tell of a new consumer ready
        CONSUMER_FOUND,         // response to client from coordinator of the consumer to send the task to
        RUN_SUBMITTED,          // request to consumer from client the task to run
        RUN_REJECTED,           // response to client from the consumer due to the consumer being gone (usually because the runner was stopped)
        RESULT_EXCEPTION,       // response to client from the consumer when an exception was encountered
        RESULT_SUCCESS,         // response to client from the consumer when a value is returned
        INTERRUPT_RUN,          // request to consumer from client to interrupt the task
        CREATE_RUN_REQUEST,     // request to backups from coordinator to create a new task request. Used by CENTRAL_LOCKING
        CREATE_CONSUMER_READY,  // request to backups from coordinator to create a new consumer ready. Used by CENTRAL_LOCKING
        DELETE_RUN_REQUEST,     // request to backups from coordinator to delete a task request. Used by CENTRAL_LOCKING
        DELETE_CONSUMER_READY   // request to backups from coordinator to delete a consumer ready. Used by CENTRAL_LOCKING
    }
    
    public Executing() {
        _awaitingReturn = Collections.synchronizedMap(new HashMap<Owner, Runnable>());
        _running = Collections.synchronizedMap(new HashMap<Runnable, Owner>());
        
        // TODO: remove this before committing
        log.setLevel("TRACE");
    }


    public boolean getBypassBundling() {
        return bypass_bundling;
    }

    public void setBypassBundling(boolean bypass_bundling) {
        this.bypass_bundling=bypass_bundling;
    }

    public void addExecutorListener(Future<?> future, 
                                    ExecutorNotification listener) {
        if(listener != null)
            notifiers.put(future, listener);
    }

    @ManagedAttribute
    public String getAddress() {
        return local_addr != null? local_addr.toString() : null;
    }

    @ManagedAttribute
    public String getView() {
        return view != null? view.toString() : null;
    }
   

    public Object down(Event evt) {
        switch(evt.getType()) {
            case ExecutorEvent.TASK_SUBMIT:
                Runnable runnable = (Runnable)evt.getArg();
                sendToCoordinator(Type.RUN_REQUEST, local_addr);
                _awaitingConsumer.add(runnable);
                break;
            case ExecutorEvent.CONSUMER_READY:
                sendToCoordinator(Type.CONSUMER_READY, local_addr);
                try {
                    runnable = _tasks.take();
                    _runnableThreads.put(runnable, Thread.currentThread());
                    return runnable;
                }
                catch (InterruptedException e) {
                    sendToCoordinator(Type.DELETE_CONSUMER_READY, local_addr);
                    Thread.currentThread().interrupt();
                }
            case ExecutorEvent.TASK_COMPLETE:
                Object arg = evt.getArg();
                Throwable throwable = null;
                if (arg instanceof Object[]) {
                    Object[] array = (Object[])arg;
                    runnable = (Runnable)array[0];
                    throwable = (Throwable)array[1];
                }
                else {
                    runnable = (Runnable)arg;
                }
                Owner owner = _running.remove(runnable);
                // This won't remove anything if owner doesn't come back
                _runnableThreads.remove(runnable);

                Object value = null;
                boolean exception = false;
                // If the owner is gone that means it was interrupted, so we
                // can't send back to them now
                boolean runnableInterrupted = owner == null;
                
                if (throwable != null) {
                    if (!runnableInterrupted || 
                            throwable instanceof InterruptedException ||
                            throwable instanceof InterruptedIOException) {
                        // If the task interrupt wasn't present we treat this
                        // as a shutdown of the task.
                        Thread.currentThread().interrupt();
                    }
                    value = throwable;
                    exception = true;
                }
                else if (runnable instanceof RunnableFuture<?>) {
                    RunnableFuture<?> future = (RunnableFuture<?>)runnable;
                    
                    boolean interrupted = true;
                    
                    // We have the value, before we interrupt at least get it!
                    while (interrupted) {
                        interrupted = false;
                        
                        try {
                            value = future.get();
                        }
                        catch (InterruptedException e) {
                            interrupted = true;
                        }
                        catch (ExecutionException e) {
                            value = e.getCause();
                            exception = true;
                        }
                    }
                    
                    if (interrupted) {
                        Thread.currentThread().interrupt();
                    }
                }
                
                if (owner != null) {
                    sendRequest(owner.getAddress(), 
                        exception ? Type.RESULT_EXCEPTION : Type.RESULT_SUCCESS, 
                                owner.requestId, value);
                }
                else {
                    log.warn("For some reason the owner was null!");
                }
                break;
            case ExecutorEvent.TASK_CANCEL:
                Object[] array = (Object[])evt.getArg();
                runnable = (Runnable)array[0];
                
                if (_awaitingConsumer.remove(runnable)) {
                    return Boolean.TRUE;
                }
                // This is guaranteed to not be null so don't take cost of auto unboxing
                else if (array[1] == Boolean.TRUE) {
                    owner = removeKeyForValue(_awaitingReturn, runnable);
                    if (owner != null) {
                        sendRequest(owner.getAddress(), Type.INTERRUPT_RUN, 
                            owner.getRequestId(), null);
                    }
                    else {
                        log.warn("Couldn't interrupt server task: " + runnable);
                    }
                    return Boolean.TRUE;
                }
                else {
                    return Boolean.FALSE;
                }
            case ExecutorEvent.ALL_TASK_CANCEL:
                array = (Object[])evt.getArg();
                
                // This is a RunnableFuture<?> so this cast is okay
                @SuppressWarnings("unchecked")
                Set<Runnable> runnables = (Set<Runnable>)array[0];
                Boolean booleanValue = (Boolean)array[1];
                
                List<Runnable> notRan = new ArrayList<Runnable>();
                
                for (Runnable cancelRunnable : runnables) {
                    // Removed from the consumer
                    if (!_awaitingConsumer.remove(cancelRunnable) && 
                            booleanValue == Boolean.FALSE) {
                        synchronized (_awaitingReturn) {
                            owner = removeKeyForValue(_awaitingReturn, cancelRunnable);
                            if (owner != null) {
                                sendRequest(owner.getAddress(), Type.INTERRUPT_RUN, 
                                    owner.getRequestId(), null);
                            }
                        }
                    }
                    else {
                        notRan.add(cancelRunnable);
                    }
                }
                return notRan;
            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;

            case Event.VIEW_CHANGE:
                handleView((View)evt.getArg());
                break;
        }
        return down_prot.down(evt);
    }
    
    private static <V, K> V removeKeyForValue(Map<V, K> map, K value) {
        synchronized (map) {
            Iterator<Entry<V, K>> iter = 
                map.entrySet().iterator();
            while (iter.hasNext()) {
                Entry<V, K> entry = iter.next();
                if (entry.getValue().equals(value)) {
                    iter.remove();
                    return entry.getKey();
                }
            }
        }
        
        return null;
    }

    public Object up(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                Message msg=(Message)evt.getArg();
                ExecutorHeader hdr=(ExecutorHeader)msg.getHeader(id);
                if(hdr == null)
                    break;

                Request req=(Request)msg.getObject();
                if(log.isTraceEnabled())
                    log.trace("[" + local_addr + "] <-- [" + msg.getSrc() + "] " + req);
                switch(req.type) {
                    case RUN_REQUEST:
                        Address source = msg.getSrc();
                        handleTaskRequest(source);
                        break;
                    case CONSUMER_READY:
                        handleConsumerReadyRequest(msg.getSrc());
                        break;
                    case CONSUMER_FOUND:
                        Address consumer = (Address)req.object;
                        handleConsumerFoundResponse(consumer);
                        break;
                    case RUN_SUBMITTED:
                        Runnable runnable = (Runnable)req.object;
                        handleTaskSubmittedRequest(runnable, msg.getSrc(), 
                            req.request);
                        break;
                    case RUN_REJECTED:
                        handleTaskRejectedResponse(msg.getSrc(), req.request);
                        break;
                    case RESULT_SUCCESS:
                        handleValueResponse(msg.getSrc(), req);
                        break;
                    case RESULT_EXCEPTION:
                        handleExceptionResponse(msg.getSrc(), req);
                        break;
                    case INTERRUPT_RUN:
                        handleInterruptRequest(msg.getSrc(), req.request);
                        break;
                    case CREATE_CONSUMER_READY:
                        source = (Address)req.object;
                        handleNewConsumer(source);
                        break;
                    case CREATE_RUN_REQUEST:
                        source = (Address)req.object;
                        handleNewRunRequest(source);
                        break;
                    case DELETE_CONSUMER_READY:
                        source = (Address)req.object;
                        handleRemoveConsumer(source);
                        break;
                    case DELETE_RUN_REQUEST:
                        source = (Address)req.object;
                        handleRemoveRunRequest(source);
                        break;
                    default:
                        log.error("Request of type " + req.type + " not known");
                        break;
                }
                return null;

            case Event.VIEW_CHANGE:
                handleView((View)evt.getArg());
                break;
        }
        return up_prot.up(evt);
    }

    protected void handleView(View view) {
        this.view=view;
        if(log.isDebugEnabled())
            log.debug("view=" + view);
        List<Address> members=view.getMembers();
        
        _consumerLock.lock();
        try {
            Iterator<Address> iterator = _consumersAvailable.iterator();
            while (iterator.hasNext()) {
                Address address = iterator.next();
                if (!members.contains(address)) {
                    iterator.remove();
                    sendRemoveConsumerRequest(address);
                }
            }
            
            iterator = _runRequests.iterator();
            while (iterator.hasNext()) {
                Address address = iterator.next();
                if (!members.contains(address)) {
                    iterator.remove();
                    sendRemoveRunRequest(address);
                }
            }
            
            for (Entry<Owner, Runnable> entry :_awaitingReturn.entrySet()) {
                // The person currently servicing our request has gone down
                // without completing so we have to keep our request alive by
                // sending ours back to the coordinator
                if (!members.contains(entry.getKey())) {
                    sendToCoordinator(Type.RUN_REQUEST, local_addr);
                    _awaitingConsumer.add(entry.getValue());
                }
            }
        }
        finally {
            _consumerLock.unlock();
        }
    }

    abstract protected void sendToCoordinator(Type type, Object obj);
    abstract protected void sendNewRunRequest(Address source);
    abstract protected void sendRemoveRunRequest(Address source);
    abstract protected void sendNewConsumerRequest(Address source);
    abstract protected void sendRemoveConsumerRequest(Address source);

    protected void handleTaskRequest(Address source) {
        Address consumer;
        _consumerLock.lock();
        try {
            consumer = _consumersAvailable.poll();
            if (consumer == null) {
                _runRequests.add(source);
            }
        }
        finally {
            _consumerLock.unlock();
        }
        
        if (consumer != null) {
            sendRequest(source, Type.CONSUMER_FOUND, (short)-1, consumer);
        }
        else {
            sendNewRunRequest(source);
        }
    }

    protected void handleConsumerReadyRequest(Address source) {
        Address requestor;
        _consumerLock.lock();
        try {
            requestor = _runRequests.poll();
            if (requestor == null) {
                _consumersAvailable.add(source);
            }
        }
        finally {
            _consumerLock.unlock();
        }
        
        if (requestor != null) {
            sendRequest(requestor, Type.CONSUMER_FOUND, (short)-1, source);
        }
        else {
            sendNewConsumerRequest(source);
        }
    }

    protected void handleConsumerFoundResponse(Address consumer) {
        Runnable runnable = _awaitingConsumer.poll();
        if (runnable == null) {
            // For some reason we don't have a runnable anymore
            // so we have to send back to the coordinator that
            // the consumer is still available.  The runnable
            // would be removed on a cancel
            sendToCoordinator(Type.CONSUMER_READY, consumer);
        }
        else {
            // We are limited to a number of concurrent request id's
            // equal to 2^15-1.  This is quite large and if it 
            // overflows it will still be positive
            short requestId = (short)Math.abs(counter.getAndIncrement());
            
            _awaitingReturn.put(new Owner(consumer, requestId), runnable);
            sendRequest(consumer, Type.RUN_SUBMITTED, requestId, runnable);
        }
    }

    protected void handleTaskSubmittedRequest(Runnable runnable, Address source, 
                                              short requestId) {
        // We store in our map so that when that task is
        // finished so that we can send back to the owner
        // with the results
        _running.put(runnable, new Owner(source, requestId));
        // We give the task to the thread that is now waiting for it to be returned
        // If we can't offer then we have to respond back to
        // caller that we can't handle it.  They must have
        // gotten our address when we had a consumer, but
        // they went away between then and now.
        if (!_tasks.offer(runnable)) {
            sendRequest(source, Type.RUN_REJECTED, requestId, null);
            _running.remove(runnable);
        }
        else {
            // TODO: do something here maybe?
        }
    }
    
    protected void handleTaskRejectedResponse(Address source, short requestId) {
        Runnable runnable = _awaitingReturn.remove(new Owner(
            source, requestId));
        if (runnable != null) {
            _awaitingConsumer.add(runnable);
            sendToCoordinator(Type.RUN_REQUEST, local_addr);
        }
        else {
            log.error("error resubmitting task for request-id: " + requestId);
        }
    }

    protected void handleValueResponse(Address source, Request req) {
        Runnable runnable = _awaitingReturn.remove(
            new Owner(source, req.request));
        // We can only notify of success if it was a future
        if (runnable instanceof RunnableFuture<?>) {
            RunnableFuture<?> future = (RunnableFuture<?>)runnable;
            ExecutorNotification notifier = notifiers.get(future);
            if (notifier != null) {
                notifier.resultReturned(req.object);
            }
        }
    }

    protected void handleExceptionResponse(Address source, Request req) {
        Runnable runnable = _awaitingReturn.remove(
            new Owner(source, req.request));
        // We can only notify of exception if it was a future
        if (runnable instanceof RunnableFuture<?>) {
            RunnableFuture<?> future = (RunnableFuture<?>)runnable;
            ExecutorNotification notifier = notifiers.get(future);
            if (notifier != null) {
                notifier.throwableEncountered((Throwable)req.object);
            }
        }
        else {
            // All we can do is log the error since their is no
            // way to return this to the user since they don't
            // have a future object.
            log.error("Runtime Error encountered from "
                    + "Cluster execute(Runnable) method",
                (Throwable) req.object);
        }
    }

    protected void handleInterruptRequest(Address source, short requestId) {
        Owner owner = new Owner(source, requestId);
        Runnable runnable = removeKeyForValue(_running, owner);
        if (runnable != null) {
            Thread thread = _runnableThreads.remove(runnable);
            thread.interrupt();
        }
        else {
            // TODO: do we do this?
            sendToCoordinator(Type.DELETE_CONSUMER_READY, local_addr);
        }
    }

    protected void handleNewRunRequest(Address sender) {
        _consumerLock.lock();
        try {
            _runRequests.add(sender);
        }
        finally {
            _consumerLock.unlock();
        }
    }

    protected void handleRemoveRunRequest(Address sender) {
        _consumerLock.lock();
        try {
            _runRequests.remove(sender);
        }
        finally {
            _consumerLock.unlock();
        }
    }
    
    protected void handleNewConsumer(Address sender) {
        _consumerLock.lock();
        try {
            _consumersAvailable.add(sender);
        }
        finally {
            _consumerLock.unlock();
        }
    }

    protected void handleRemoveConsumer(Address sender) {
        _consumerLock.lock();
        try {
            _consumersAvailable.remove(sender);
        }
        finally {
            _consumerLock.unlock();
        }
    }
    
    protected void sendRequest(Address dest, Type type, short requestId, Object object) {
        Request req=new Request(type, object, requestId);
        Message msg=new Message(dest, null, req);
        msg.putHeader(id, new ExecutorHeader());
        if(bypass_bundling)
            msg.setFlag(Message.DONT_BUNDLE);
        if(log.isTraceEnabled())
            log.trace("[" + local_addr + "] --> [" + (dest == null? "ALL" : dest) + "] " + req);
        try {
            down_prot.down(new Event(Event.MSG, msg));
        }
        catch(Exception ex) {
            log.error("failed sending " + type + " request: " + ex);
        }
    }
    
    /**
     * This keeps track of all the requests we send.  This is used so that
     * the response doesn't have to send back the future but instead the counter
     * We just let this roll over
     */
    protected static final AtomicInteger counter = new AtomicInteger();

    protected static class Request implements Streamable {
        protected Type    type;
        protected Object  object;
        protected short   request;
        
        public Request() {
        }

        public Request(Type type, Object object, short request) {
            this.type=type;
            this.object=object;
            this.request=request;
        }

        public void writeTo(DataOutputStream out) throws IOException {
            out.writeByte(type.ordinal());
            // We can't use Util.writeObject since it's size is limited to 2^15-1
            try {
                if (object instanceof Streamable) {
                    out.writeShort(-1);
                    Util.writeGenericStreamable((Streamable)object, out);
                }
                else {
                    byte[] bytes = Util.objectToByteBuffer(object);
                    out.writeInt(bytes.length);
                    out.write(bytes);
                }
            }
            catch (IOException e) {
                throw e;
            }
            catch (Exception e) {
                throw new IOException("Exception encountered while serializing execution request", e);
            }
            out.writeShort(request);
        }

        public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
            type=Type.values()[in.readByte()];
            // We can't use Util.readObject since it's size is limited to 2^15-1
            try {
                short first = in.readShort();
                if (first == -1) {
                    object = Util.readGenericStreamable(in);
                }
                else {
                    ByteBuffer bb = ByteBuffer.allocate(4);
                    bb.putShort(first);
                    bb.putShort(in.readShort());
                    
                    int size = bb.getInt(0);
                    byte[] bytes = new byte[size];
                    in.readFully(bytes, 0, size);
                    object = Util.objectFromByteBuffer(bytes);
                }
            }
            catch (IOException e) {
                throw e;
            }
            catch (Exception e) {
                throw new IOException("Exception encountered while serializing execution request", e);
            }
            request=in.readShort();
        }

        public String toString() {
            return type.name() + " [" + object + (request != -1 ? " request id: " + request : "") + "]";
        }
    }


    public static class ExecutorHeader extends Header {

        public ExecutorHeader() {
        }

        public int size() {
            return 0;
        }

        public void writeTo(DataOutputStream out) throws IOException {
        }

        public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
        }
    }
    
    public static class Owner {
        final protected Address address;
        final protected short requestId;

        public Owner(Address address, short requestId) {
            this.address=address;
            this.requestId=requestId;
        }

        public Address getAddress() {
            return address;
        }

        public short getRequestId() {
            return requestId;
        }
        
        // @see java.lang.Object#hashCode()
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result
                    + ((address == null) ? 0 : address.hashCode());
            result = prime * result + requestId;
            return result;
        }

        // @see java.lang.Object#equals(java.lang.Object)
        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null) return false;
            if (getClass() != obj.getClass()) return false;
            Owner other = (Owner) obj;
            if (address == null) {
                if (other.address != null) return false;
            }
            else if (!address.equals(other.address)) return false;
            if (requestId != other.requestId) return false;
            return true;
        }

        public String toString() {
            return address + "::" + requestId;
        }
    }

    private static final short id = 1280;
    
    static {
        ClassConfigurator.addProtocol(id, Executing.class);
        ClassConfigurator.add(id, ExecutorHeader.class);
    }
}
