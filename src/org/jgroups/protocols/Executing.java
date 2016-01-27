package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.blocks.executor.ExecutionService.DistributedFuture;
import org.jgroups.blocks.executor.ExecutorEvent;
import org.jgroups.blocks.executor.ExecutorNotification;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This is the base protocol used for executions.
 * @author wburns
 * @see org.jgroups.protocols.CENTRAL_EXECUTOR
 */
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
        new ConcurrentLinkedQueue<>();
    
    /**
     * This is a map on the client side showing for all of the current pending
     * requests
     */
    protected final ConcurrentMap<Runnable, Long> _requestId = 
        new ConcurrentHashMap<>();
    
    /**
     * This is essentially a set on the consumer side of id's of all the threads 
     * currently running as consumers.  This is basically a set, but since
     * there is no ConcurrentHashSet we use a phoney value
     */
    protected final ConcurrentMap<Long, Object> _consumerId = 
        new ConcurrentHashMap<>();

    protected final ConcurrentMap<Future<?>, ExecutorNotification> notifiers = 
        new ConcurrentHashMap<>();
    
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
     * This is a server side store of all the tasks that want to be ran on a
     * given thread.  This  map should be updated by an incoming request before
     * awaking the task with the latch.  This map should only be retrieved after
     * first waiting on the latch for a consumer
     */
    protected ConcurrentMap<Long, Runnable> _tasks = new ConcurrentHashMap<>();
    
    /**
     * This is a server side store of all the barriers for respective tasks 
     * requests.  When a consumer is starting up they should create a latch
     * place in map with its id and wait on it until a request comes in to
     * wake it up it would only then touch the {@link Executing#_tasks} map.  A requestor
     * should first place in the {@link Executing#_tasks} map and then create a latch
     * and notify the consumer
     */
    protected ConcurrentMap<Long, CyclicBarrier> _taskBarriers = 
            new ConcurrentHashMap<>();
    
    /**
     * This is a server side map to show which threads are running for a
     * given runnable.  This is used to interrupt those threads if needed.
     */
    protected final ConcurrentMap<Runnable, Thread> _runnableThreads = 
        new ConcurrentHashMap<>();
    
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
    protected Queue<Owner> _runRequests = new ArrayDeque<>();
    
    /**
     * This is stored on the coordinator side.  This queue holds all of the
     * addresses that currently are able to run something.  If this queue has
     * elements the run request queue must be empty.
     */
    protected Queue<Owner> _consumersAvailable = new ArrayDeque<>();
    
    protected enum Type {
        RUN_REQUEST,            // request to coordinator from client to tell of a new task request
        CONSUMER_READY,         // request to coordinator from server to tell of a new consumer ready
        CONSUMER_UNREADY,       // request to coordinator from server to tell of a consumer stopping
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
                // We are limited to a number of concurrent request id's
                // equal to 2^63-1.  This is quite large and if it 
                // overflows it will still be positive
                long requestId = Math.abs(counter.getAndIncrement());
                if(requestId == Long.MIN_VALUE) {
                    // TODO: need to fix this it isn't safe for concurrent
                    // modifications
                    counter.set(0);
                    requestId = Math.abs(counter.getAndIncrement());
                }

                // Need to make sure to put the requestId in our map before
                // adding the runnable to awaiting consumer in case if
                // coordinator sent a consumer found and their original task
                // is no longer around
                // see https://issues.jboss.org/browse/JGRP-1744
                _requestId.put(runnable, requestId);

                _awaitingConsumer.add(runnable);

                sendToCoordinator(Type.RUN_REQUEST, requestId, local_addr);
                break;
            case ExecutorEvent.CONSUMER_READY:
                Thread currentThread = Thread.currentThread();
                long threadId = currentThread.getId();
                _consumerId.put(threadId, PRESENT);
                try {
                    for (;;) {
                        CyclicBarrier barrier = new CyclicBarrier(2);
                        _taskBarriers.put(threadId, barrier);
                        
                        // We only send to the coordinator that we are ready after
                        // making the barrier, wait for request to come and let
                        // us free
                        sendToCoordinator(Type.CONSUMER_READY, threadId, local_addr);
                        
                        try {
                            barrier.await();
                            break;
                        }
                        catch (BrokenBarrierException e) {
                            if (log.isDebugEnabled())
                                log.debug("Producer timed out before we picked up"
                                        + " the task, have to tell coordinator"
                                        + " we are still good.");
                        }
                    }
                    // This should always be non nullable since the latch
                    // was freed
                    runnable = _tasks.remove(threadId);
                    _runnableThreads.put(runnable, currentThread);
                    return runnable;
                }
                catch (InterruptedException e) {
                    if (log.isDebugEnabled()) 
                        log.debug("Consumer " + threadId + 
                            " stopped via interrupt");
                    sendToCoordinator(Type.CONSUMER_UNREADY, threadId, local_addr);
                    Thread.currentThread().interrupt();
                }
                finally {
                    // Make sure the barriers are cleaned up as well
                    _taskBarriers.remove(threadId);
                    _consumerId.remove(threadId);
                }
                break;
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
                if (throwable != null) {
                    // InterruptedException is special telling us that
                    // we interrupted the thread while waiting but still got
                    // a task therefore we have to reject it.
                    if (throwable instanceof InterruptedException) {
                        if (log.isDebugEnabled())
                            log.debug("Run rejected due to interrupted exception returned");
                        sendRequest(owner.address, Type.RUN_REJECTED, owner.requestId, null);
                        break;
                    }
                    value = throwable;
                    exception = true;
                }
                else if (runnable instanceof RunnableFuture<?>) {
                    RunnableFuture<?> future = (RunnableFuture<?>)runnable;
                    
                    boolean interrupted = false;
                    boolean gotValue = false;
                    
                    // We have the value, before we interrupt at least get it!
                    while (!gotValue) {
                        try {
                            value = future.get();
                            gotValue = true;
                        }
                        catch (InterruptedException e) {
                            interrupted = true;
                        }
                        catch (ExecutionException e) {
                            value = e.getCause();
                            exception = true;
                            gotValue = true;
                        }
                    }
                    
                    if (interrupted) {
                        Thread.currentThread().interrupt();
                    }
                }
                
                if (owner != null) {
                    final Type type;
                    final Object valueToSend;
                    if (value == null) {
                        type = Type.RESULT_SUCCESS;
                        valueToSend = value;
                    }
                    // Both serializable values and exceptions would go in here
                    else if (value instanceof Serializable || 
                            value instanceof Externalizable || 
                            value instanceof Streamable) {
                        type = exception ? Type.RESULT_EXCEPTION : Type.RESULT_SUCCESS;
                        valueToSend = value;
                    }
                    // This would happen if the value wasn't serializable,
                    // so we have to send back to the client that the class
                    // wasn't serializable
                    else {
                        type = Type.RESULT_EXCEPTION;
                        valueToSend = new NotSerializableException(
                            value.getClass().getName());
                    }
                    
                    if (local_addr.equals(owner.getAddress())) {
                        if(log.isTraceEnabled())
                            log.trace("[redirect] <--> [" + local_addr + "] "
                                    + type.name() + " [" + value
                                    + (owner.requestId != -1 ? " request id: " + 
                                            owner.requestId : "")
                                    + "]");
                        final Owner finalOwner = owner;
                        if (type == Type.RESULT_SUCCESS) {
                            handleValueResponse(local_addr, 
                                finalOwner.requestId, valueToSend);
                        }
                        else if (type == Type.RESULT_EXCEPTION){
                            handleExceptionResponse(local_addr, 
                                finalOwner.requestId, (Throwable)valueToSend);
                        }
                    }
                    else {
                        sendRequest(owner.getAddress(), type, owner.requestId, 
                            valueToSend);
                    }
                }
                else {
                    if (log.isTraceEnabled()) {
                        log.trace("Could not return result - most likely because it was interrupted");
                    }
                }
                break;
            case ExecutorEvent.TASK_CANCEL:
                Object[] array = (Object[])evt.getArg();
                runnable = (Runnable)array[0];
                
                if (_awaitingConsumer.remove(runnable)) {
                    _requestId.remove(runnable);
                    ExecutorNotification notification = notifiers.remove(runnable);
                    if (notification != null) {
                        notification.interrupted(runnable);
                    }
                    if (log.isTraceEnabled())
                        log.trace("Cancelled task " + runnable + 
                            " before it was picked up");
                    return Boolean.TRUE;
                }
                // This is guaranteed to not be null so don't take cost of auto unboxing
                else if (array[1] == Boolean.TRUE) {
                    owner = removeKeyForValue(_awaitingReturn, runnable);
                    if (owner != null) {
                        Long requestIdValue = _requestId.remove(runnable);
                        // We only cancel if the requestId is still available
                        // this means the result hasn't been returned yet and
                        // we still have a chance to interrupt
                        if (requestIdValue != null) {
                            if (requestIdValue != owner.getRequestId()) {
                                log.warn("Cancelling requestId didn't match waiting");
                            }
                            sendRequest(owner.getAddress(), Type.INTERRUPT_RUN, 
                                owner.getRequestId(), null);
                        }
                    }
                    else {
                        if (log.isTraceEnabled())
                            log.warn("Couldn't interrupt server task: " + runnable);
                    }
                    ExecutorNotification notification = notifiers.remove(runnable);
                    if (notification != null) {
                        notification.interrupted(runnable);
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
                
                List<Runnable> notRan = new ArrayList<>();
                
                for (Runnable cancelRunnable : runnables) {
                    // Removed from the consumer
                    if (!_awaitingConsumer.remove(cancelRunnable) && 
                            booleanValue == Boolean.TRUE) {
                        synchronized (_awaitingReturn) {
                            owner = removeKeyForValue(_awaitingReturn, cancelRunnable);
                            if (owner != null) {
                                Long requestIdValue = _requestId.remove(cancelRunnable);
                                if (requestIdValue != owner.getRequestId()) {
                                    log.warn("Cancelling requestId didn't match waiting");
                                }
                                sendRequest(owner.getAddress(), Type.INTERRUPT_RUN, 
                                    owner.getRequestId(), null);
                            }
                            ExecutorNotification notification = notifiers.remove(cancelRunnable);
                            if (notification != null) {
                                log.trace("Notifying listener");
                                notification.interrupted(cancelRunnable);
                            }
                        }
                    }
                    else {
                        _requestId.remove(cancelRunnable);
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
    
    protected static <V, K> V removeKeyForValue(Map<V, K> map, K value) {
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
                        handleTaskRequest(req.request, (Address)req.object);
                        break;
                    case CONSUMER_READY:
                        handleConsumerReadyRequest(req.request, (Address)req.object);
                        break;
                    case CONSUMER_UNREADY:
                        handleConsumerUnreadyRequest(req.request, (Address)req.object);
                        break;
                    case CONSUMER_FOUND:
                        handleConsumerFoundResponse(req.request, (Address)req.object);
                        break;
                    case RUN_SUBMITTED:
                        RequestWithThread reqWT = (RequestWithThread)req;
                        Object objectToRun = reqWT.object;
                        Runnable runnable;
                        if (objectToRun instanceof Runnable) {
                            runnable = (Runnable)objectToRun;
                        }
                        else if (objectToRun instanceof Callable) {
                            @SuppressWarnings("unchecked")
                            Callable<Object> callable = (Callable<Object>)objectToRun;
                            runnable = new FutureTask<>(callable);
                        }
                        else {
                            log.error(Util.getMessage("RequestOfType") + req.type + 
                                " sent an object of " + objectToRun + " which is invalid");
                            break;
                        }
                        
                        handleTaskSubmittedRequest(runnable, msg.getSrc(), 
                            req.request, reqWT.threadId);
                        break;
                    case RUN_REJECTED:
                        // We could make requests local for this, but is it really worth it
                        handleTaskRejectedResponse(msg.getSrc(), req.request);
                        break;
                    case RESULT_SUCCESS:
                        handleValueResponse(msg.getSrc(), req.request, req.object);
                        break;
                    case RESULT_EXCEPTION:
                        handleExceptionResponse(msg.getSrc(), req.request, 
                            (Throwable)req.object);
                        break;
                    case INTERRUPT_RUN:
                        // We could make requests local for this, but is it really worth it
                        handleInterruptRequest(msg.getSrc(), req.request);
                        break;
                    case CREATE_CONSUMER_READY:
                        Owner owner = new Owner((Address)req.object, req.request);
                        handleNewConsumer(owner);
                        break;
                    case CREATE_RUN_REQUEST:
                        owner = new Owner((Address)req.object, req.request);
                        handleNewRunRequest(owner);
                        break;
                    case DELETE_CONSUMER_READY:
                        owner = new Owner((Address)req.object, req.request);
                        handleRemoveConsumer(owner);
                        break;
                    case DELETE_RUN_REQUEST:
                        owner = new Owner((Address)req.object, req.request);
                        handleRemoveRunRequest(owner);
                        break;
                    default:
                        log.error(Util.getMessage("RequestOfType") + req.type + " not known");
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
            // This removes the consumers that were registered that are now gone
            Iterator<Owner> iterator = _consumersAvailable.iterator();
            while (iterator.hasNext()) {
                Owner owner = iterator.next();
                if (!members.contains(owner.getAddress())) {
                    iterator.remove();
                    sendRemoveConsumerRequest(owner);
                }
            }
            
            // This removes the tasks that those requestors are gone
            iterator = _runRequests.iterator();
            while (iterator.hasNext()) {
                Owner owner = iterator.next();
                if (!members.contains(owner.getAddress())) {
                    iterator.remove();
                    sendRemoveRunRequest(owner);
                }
            }
            
            synchronized (_awaitingReturn) {
                for (Entry<Owner, Runnable> entry : _awaitingReturn.entrySet()) {
                    // The person currently servicing our request has gone down
                    // without completing so we have to keep our request alive by
                    // sending ours back to the coordinator
                    Owner owner = entry.getKey();
                    if (!members.contains(owner.getAddress())) {
                        Runnable runnable = entry.getValue();
                        // We need to register the request id before sending the request back to the coordinator
                        // in case if our task gets picked up since another was removed
                        _requestId.put(runnable, owner.getRequestId());
                        _awaitingConsumer.add(runnable);
                        sendToCoordinator(Type.RUN_REQUEST, owner.getRequestId(), 
                                local_addr);
                    }
                }
            }
        }
        finally {
            _consumerLock.unlock();
        }
    }

    abstract protected void sendToCoordinator(Type type, long requestId, Address address);
    abstract protected void sendNewRunRequest(Owner source);
    abstract protected void sendRemoveRunRequest(Owner source);
    abstract protected void sendNewConsumerRequest(Owner source);
    abstract protected void sendRemoveConsumerRequest(Owner source);

    protected void handleTaskRequest(long requestId, Address address) {
        final Owner consumer;
        Owner source = new Owner(address, requestId);
        _consumerLock.lock();
        try {
            consumer = _consumersAvailable.poll();
            // We don't add duplicate run requests - this allows for resubmission
            // if it is thought the message may have been dropped
            if (consumer == null && !_runRequests.contains(source)) {
                _runRequests.add(source);
            }
        }
        finally {
            _consumerLock.unlock();
        }
        
        if (consumer != null) {
            sendRequest(source.getAddress(), Type.CONSUMER_FOUND, 
                consumer.getRequestId(), consumer.getAddress());
            sendRemoveConsumerRequest(consumer);
        }
        else {
            sendNewRunRequest(source);
        }
    }

    protected void handleConsumerReadyRequest(long requestId, Address address) {
        Owner requestor;
        final Owner source = new Owner(address, requestId);
        _consumerLock.lock();
        try {
            requestor = _runRequests.poll();
            // We don't add duplicate consumers - this allows for resubmission
            // if it is thought the message may have been dropped
            if (requestor == null && !_consumersAvailable.contains(source)) {
                _consumersAvailable.add(source);
            }
        }
        finally {
            _consumerLock.unlock();
        }
        
        if (requestor != null) {
            sendRequest(requestor.getAddress(), Type.CONSUMER_FOUND, 
                source.getRequestId(), source.getAddress());
            sendRemoveRunRequest(requestor);
        }
        else {
            sendNewConsumerRequest(source);
        }
    }
    
    protected void handleConsumerUnreadyRequest(long requestId, Address address) {
        Owner consumer = new Owner(address, requestId);
        _consumerLock.lock();
        try {
            _consumersAvailable.remove(consumer);
        }
        finally {
            _consumerLock.unlock();
        }
        sendRemoveConsumerRequest(consumer);
    }

    protected void handleConsumerFoundResponse(long threadId, Address address) {
        final Runnable runnable = _awaitingConsumer.poll();
        // This is a representation of the server side owner running our task.
        Owner owner;
        if (runnable == null) {
            owner = new Owner(address, threadId);
            // For some reason we don't have a runnable anymore
            // so we have to send back to the coordinator that
            // the consumer is still available.  The runnable
            // would be removed on a cancel
            sendToCoordinator(Type.CONSUMER_READY, owner.getRequestId(), 
                owner.getAddress());
        }
        else {
            final Long requestId = _requestId.get(runnable);
            if (requestId == null) {
                // requestId is not available - this means the result has been 
                // returned already or it has been interrupted
            	return;
            }
            owner = new Owner(address, requestId);
            _awaitingReturn.put(owner, runnable);
            // If local we pass along without serializing
            if (local_addr.equals(owner.getAddress())) {
                handleTaskSubmittedRequest(runnable, local_addr, requestId, threadId);
            }
            else {
                try {
                    if (runnable instanceof DistributedFuture) {
                        Callable<?> callable = ((DistributedFuture<?>)runnable).getCallable();
                        sendThreadRequest(owner.getAddress(), threadId, 
                            Type.RUN_SUBMITTED, requestId, callable);
                    }
                    else {
                        sendThreadRequest(owner.getAddress(), threadId, 
                            Type.RUN_SUBMITTED, requestId, runnable);
                    }
                }
                // This relies on the Mesasge class to throw this when a 
                // serialization issue occurs
                catch (IllegalArgumentException e) {
                    ExecutorNotification notificiation = notifiers.remove(runnable);
                    if (notificiation != null) {
                        notificiation.throwableEncountered(e);
                    }
                    throw e;
                }
            }
        }
    }

    protected void handleTaskSubmittedRequest(Runnable runnable, Address source, 
                                              long requestId, long threadId) {
        // We store in our map so that when that task is
        // finished so that we can send back to the owner
        // with the results
        _running.put(runnable, new Owner(source, requestId));
        // We give the task to the thread that is now waiting for it to be returned
        // If we can't offer then we have to respond back to
        // caller that we can't handle it.  They must have
        // gotten our address when we had a consumer, but
        // they went away between then and now.
        boolean received;
        try {
            _tasks.put(threadId, runnable);
            
            CyclicBarrier barrier = _taskBarriers.remove(threadId);
            if (received = barrier != null) {
                // Only wait 10 milliseconds, in case if the consumer was
                // stopped between when we were told it was available and now
                barrier.await(10, TimeUnit.MILLISECONDS);
            }
        }
        catch (InterruptedException e) {
            if (log.isDebugEnabled())
                log.debug("Interrupted while handing off task");
            Thread.currentThread().interrupt();
            received = false;
        }
        catch (BrokenBarrierException e) {
            if (log.isDebugEnabled())
                log.debug("Consumer " + threadId + " has been interrupted, " +
                        "must retry to submit elsewhere");
            received = false;
        }
        catch (TimeoutException e) {
            if (log.isDebugEnabled())
                log.debug("Timeout waiting to hand off to barrier, consumer " + 
                        threadId + " must be slow");
            // This should only happen if the consumer put the latch then got
            // interrupted but hadn't yet removed the latch, should almost never
            // happen
            received = false;
        }
        
        if (!received) {
            // Clean up the tasks request
            _tasks.remove(threadId);
            if (log.isDebugEnabled())
                log.debug("Run rejected not able to pass off to consumer");
            // If we couldn't hand off the task we have to tell the client
            // and also reupdate the coordinator that our consumer is ready
            sendRequest(source, Type.RUN_REJECTED, requestId, null);
            _running.remove(runnable);
        }
    }
    
    protected void handleTaskRejectedResponse(Address source, long requestId) {
        Runnable runnable = _awaitingReturn.remove(new Owner(
            source, requestId));
        if (runnable != null) {
            _awaitingConsumer.add(runnable);
            Long taskRequestId = _requestId.get(runnable);
            if (taskRequestId != requestId) {
                log.warn("Task Request Id doesn't match in rejection");
            }
            sendToCoordinator(Type.RUN_REQUEST, taskRequestId, local_addr);
        }
        else {
            log.error(Util.getMessage("ErrorResubmittingTaskForRequestId") + requestId);
        }
    }

    protected void handleValueResponse(Address source, long requestId, Object value) {
        Runnable runnable = _awaitingReturn.remove(
            new Owner(source, requestId));
        
        if (runnable != null) {
            _requestId.remove(runnable);
        }
        // We can only notify of success if it was a future
        if (runnable instanceof RunnableFuture<?>) {
            RunnableFuture<?> future = (RunnableFuture<?>)runnable;
            ExecutorNotification notifier = notifiers.remove(future);
            if (notifier != null) {
                notifier.resultReturned(value);
            }
        }
        else {
            log.warn("Runnable was not found in awaiting");
        }
    }

    protected void handleExceptionResponse(Address source, long requestId, Throwable throwable) {
        Runnable runnable = _awaitingReturn.remove(
            new Owner(source, requestId));
        
        if (runnable != null) {
            _requestId.remove(runnable);
        }
        // We can only notify of exception if it was a future
        if (runnable instanceof RunnableFuture<?>) {
            RunnableFuture<?> future = (RunnableFuture<?>)runnable;
            ExecutorNotification notifier = notifiers.remove(future);
            if (notifier != null) {
                notifier.throwableEncountered(throwable);
            }
        }
        else {
            // All we can do is log the error since their is no
            // way to return this to the user since they don't
            // have a future object.
            log.error(Util.getMessage("RuntimeErrorEncounteredFromClusterExecuteRunnableMethod"), throwable);
        }
    }

    protected void handleInterruptRequest(Address source, long requestId) {
        Owner owner = new Owner(source, requestId);
        Runnable runnable = removeKeyForValue(_running, owner);
        Thread thread = null;
        if (runnable != null) {
            thread = _runnableThreads.remove(runnable);
        }
        if (thread != null) {
            thread.interrupt();
        }
        else if (log.isTraceEnabled())
            log.trace("Message could not be interrupted due to it already returned");
    }

    protected void handleNewRunRequest(Owner sender) {
        _consumerLock.lock();
        try {
            if (!_runRequests.contains(sender)) {
                _runRequests.add(sender);
            }
        }
        finally {
            _consumerLock.unlock();
        }
    }

    protected void handleRemoveRunRequest(Owner sender) {
        _consumerLock.lock();
        try {
            _runRequests.remove(sender);
        }
        finally {
            _consumerLock.unlock();
        }
    }
    
    protected void handleNewConsumer(Owner sender) {
        _consumerLock.lock();
        try {
            if (!_consumersAvailable.contains(sender)) {
                _consumersAvailable.add(sender);
            }
        }
        finally {
            _consumerLock.unlock();
        }
    }

    protected void handleRemoveConsumer(Owner sender) {
        _consumerLock.lock();
        try {
            _consumersAvailable.remove(sender);
        }
        finally {
            _consumerLock.unlock();
        }
    }
    
    protected void sendRequest(Address dest, Type type, long requestId, Object object) {
        Request req=new Request(type, object, requestId);
        Message msg=new Message(dest, req).putHeader(id, new ExecutorHeader());
        if(bypass_bundling)
            msg.setFlag(Message.Flag.DONT_BUNDLE);
        if(log.isTraceEnabled())
            log.trace("[" + local_addr + "] --> [" + (dest == null? "ALL" : dest) + "] " + req);
        try {
            down_prot.down(new Event(Event.MSG, msg));
        }
        catch(Exception ex) {
            log.error(Util.getMessage("FailedSending") + type + " request: " + ex);
        }  
    }
    
    protected void sendThreadRequest(Address dest, long threadId, Type type, long requestId, 
        Object object) {
        RequestWithThread req=new RequestWithThread(type, object, requestId, threadId);
        Message msg=new Message(dest, req).putHeader(id, new ExecutorHeader());
        if(bypass_bundling)
            msg.setFlag(Message.Flag.DONT_BUNDLE);
        if(log.isTraceEnabled())
            log.trace("[" + local_addr + "] --> [" + (dest == null? "ALL" : dest) + "] " + req);
        try {
            down_prot.down(new Event(Event.MSG, msg));
        }
        catch(Exception ex) {
            log.error(Util.getMessage("FailedSending") + type + " request: " + ex);
        }  
    }
    
    /**
     * This keeps track of all the requests we send.  This is used so that
     * the response doesn't have to send back the future but instead the counter
     * We just let this roll over
     */
    protected static final AtomicLong counter = new AtomicLong();
    
    /**
     * This is a placeholder for a key value to make a concurrent hash map
     * a concurrent hash set
     */
    protected static final Object PRESENT = new Object();

    protected static class Request implements Streamable {
        protected Type    type;
        protected Object  object;
        protected long   request;
        
        public Request() {
        }

        public Request(Type type, Object object, long request) {
            this.type=type;
            this.object=object;
            this.request=request;
        }

        public void writeTo(DataOutput out) throws Exception {
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
            out.writeLong(request);
        }

        public void readFrom(DataInput in) throws Exception {
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
            request=in.readLong();
        }

        public String toString() {
            return type.name() + " [" + object + (request != -1 ? " request id: " + request : "") + "]";
        }
    }
    
    protected static class RequestWithThread extends Request {
        protected long threadId;
        
        public RequestWithThread() {
        }
        
        public RequestWithThread(Type type, Object object, long request, 
            long threadId) {
            super(type, object, request);
            this.threadId = threadId;
        }
        
        @Override
        public void readFrom(DataInput in) throws Exception {
            super.readFrom(in);
            threadId = in.readLong();
        }
        
        @Override
        public void writeTo(DataOutput out) throws Exception {
            super.writeTo(out);
            out.writeLong(threadId);
        }
        
        public String toString() {
            return type.name() + " [" + object + (request != -1 ? " request id: " + request : "") + 
                    " threadId: " + threadId + "]";
        }
    }


    public static class ExecutorHeader extends Header {

        public ExecutorHeader() {
        }

        public int size() {
            return 0;
        }

        public void writeTo(DataOutput out) throws Exception {
        }

        public void readFrom(DataInput in) throws Exception {
        }
    }
    
    public static class Owner {
        protected final Address address;
        protected final long requestId;
        
        public Owner(Address address, long requestId) {
            this.address=address;
            this.requestId=requestId;
        }

        public Address getAddress() {
            return address;
        }

        public long getRequestId() {
            return requestId;
        }
        
        // @see java.lang.Object#hashCode()
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result
                    + ((address == null) ? 0 : address.hashCode());
            result = prime * result + (int) (requestId ^ (requestId >>> 32));
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
}
