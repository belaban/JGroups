package org.jgroups.protocols.pbcast;

import org.jgroups.*;
import org.jgroups.annotations.*;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Digest;
import org.jgroups.util.Promise;
import org.jgroups.util.Util;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Flush, as it name implies, forces group members to flush their pending messages while blocking
 * them to send any additional messages. The process of flushing acquiesces the group so that state
 * transfer or a join can be done. It is also called stop-the-world model as nobody will be able to
 * send messages while a flush is in process.
 * 
 * <p>
 * Flush is needed for:
 * <p>
 * (1) State transfer. When a member requests state transfer, the coordinator tells everyone to stop
 * sending messages and waits for everyone's ack. Then it asks the application for its state and
 * ships it back to the requester. After the requester has received and set the state successfully,
 * the coordinator tells everyone to resume sending messages.
 * <p>
 * (2) View changes (e.g.a join). Before installing a new view V2, flushing would ensure that all
 * messages *sent* in the current view V1 are indeed *delivered* in V1, rather than in V2 (in all
 * non-faulty members). This is essentially Virtual Synchrony.
 * 
 * 
 * 
 * @author Vladimir Blagojevic
 * @since 2.4
 */
@MBean(description = "Flushes the cluster")
@DeprecatedProperty(names = { "auto_flush_conf" })
public class FLUSH extends Protocol {

    /*
     * ------------------------------------------ Properties------------------------------------------
     */
    @Property(description = "Max time to keep channel blocked in flush. Default is 8000 msec")
    private long timeout = 8000;

    @Property(description = "Timeout (per atttempt) to quiet the cluster during the first flush phase. Default is 2000 msec")
    private long start_flush_timeout = 2000;
    
    @Property(description = "Timeout to wait for UNBLOCK after STOP_FLUSH is issued. Default is 2000 msec")
    private long end_flush_timeout = 2000;

    @Property(description = "Retry timeout after an unsuccessful attempt to quiet the cluster (first flush phase). Default is 3000 msec")
    private long retry_timeout = 2000;

    @Property(description = "Reconciliation phase toggle. Default is true")
    private boolean enable_reconciliation = true;

    /*
     * --------------------------------------------- JMX ----------------------------------------------
     */

    private long startFlushTime;

    private long totalTimeInFlush;

    private int numberOfFlushes;

    private double averageFlushDuration;

    /*
     * --------------------------------------------- Fields------------------------------------------------------
     */

    @GuardedBy("sharedLock")
    private View currentView=new View(new ViewId(), new Vector<Address>());

    private Address localAddress;

    /**
     * Group member that requested FLUSH. For view installations flush coordinator is the group
     * coordinator For state transfer flush coordinator is the state requesting member
     */
    @GuardedBy("sharedLock")
    private Address flushCoordinator;

    @GuardedBy("sharedLock")
    private final List<Address> flushMembers=new ArrayList<Address>();

    private final AtomicInteger viewCounter = new AtomicInteger(0);

    @GuardedBy("sharedLock")
    private final Map<Address, Digest> flushCompletedMap=new HashMap<Address, Digest>();

    @GuardedBy("sharedLock")
    private final List<Address> flushNotCompletedMap=new ArrayList<Address>();

    @GuardedBy("sharedLock")
    private final Set<Address> suspected=new TreeSet<Address>();

    @GuardedBy("sharedLock")
    private final List<Address> reconcileOks=new ArrayList<Address>();

    private final Object sharedLock = new Object();

    private final ReentrantLock blockMutex = new ReentrantLock();
    
    private final Condition notBlockedDown = blockMutex.newCondition();

    /**
     * Indicates if FLUSH.down() is currently blocking threads Condition predicate associated with
     * blockMutex
     */
    @GuardedBy("blockMutex")
    private volatile boolean isBlockingFlushDown = true;

    @GuardedBy("sharedLock")
    private boolean flushCompleted = false;

    private final Promise<Boolean> flush_promise = new Promise<Boolean>();

    private final Promise<Boolean> flush_unblock_promise = new Promise<Boolean>();

    private final AtomicBoolean flushInProgress = new AtomicBoolean(false);

    private final AtomicBoolean sentBlock = new AtomicBoolean(false);

    private final AtomicBoolean sentUnblock = new AtomicBoolean(false);

    public FLUSH() {
    }


    public long getStartFlushTimeout() {
        return start_flush_timeout;
    }

    public void setStartFlushTimeout(long start_flush_timeout) {
        this.start_flush_timeout = start_flush_timeout;
    }

    public long getRetryTimeout() {
        return retry_timeout;
    }

    public void setRetryTimeout(long retry_timeout) {
        this.retry_timeout = retry_timeout;
    }

    public void start() throws Exception {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("flush_supported", Boolean.TRUE);
        up_prot.up(new Event(Event.CONFIG, map));
        down_prot.down(new Event(Event.CONFIG, map));

        viewCounter.set(0);
        blockMutex.lock();
        try {
            isBlockingFlushDown = true;
        } finally {
            blockMutex.unlock();
        }
    }

    public void stop() {
        synchronized (sharedLock) {
            currentView = new View(new ViewId(), new Vector<Address>());
            flushCompletedMap.clear();
            flushNotCompletedMap.clear();
            flushMembers.clear();
            suspected.clear();
            flushCoordinator = null;
        }
    }

    /* -------------------JMX attributes and operations --------------------- */

    @ManagedAttribute
    public double getAverageFlushDuration() {
        return averageFlushDuration;
    }

    @ManagedAttribute
    public long getTotalTimeInFlush() {
        return totalTimeInFlush;
    }

    @ManagedAttribute
    public int getNumberOfFlushes() {
        return numberOfFlushes;
    }

    @ManagedOperation(description = "Request cluster flush")
    public boolean startFlush() {
        return startFlush(new Event(Event.SUSPEND));
    }

    @SuppressWarnings("unchecked")
    private boolean startFlush(Event evt) {
        List<Address> flushParticipants = (List<Address>) evt.getArg();
        return startFlush(flushParticipants);
    }

    private boolean startFlush(List<Address> flushParticipants) {
        boolean successfulFlush = false;
        if (!flushInProgress.get()) {
            flush_promise.reset();
            synchronized(sharedLock) {
                if(flushParticipants == null)
                    flushParticipants=new ArrayList<Address>(currentView.getMembers());
            }
            onSuspend(flushParticipants);
            try {
                Boolean r = flush_promise.getResultWithTimeout(start_flush_timeout);
                successfulFlush = r.booleanValue();
            } catch (TimeoutException e) {
                if (log.isDebugEnabled())
                    log.debug(localAddress
                            + ": timed out waiting for flush responses after "
                            + start_flush_timeout
                            + " ms. Rejecting flush to participants "
                            + flushParticipants);
                rejectFlush(flushParticipants, currentViewId());
            }
        }
        return successfulFlush;
    }

    @ManagedOperation(description = "Request end of flush in a cluster")
    public void stopFlush() {
        down(new Event(Event.RESUME));
    }

    /*
     * ------------------- end JMX attributes and operations ---------------------
     */

    public Object down(Event evt) {
        switch (evt.getType()) {
            case Event.MSG:
                Message msg = (Message) evt.getArg();
                Address dest = msg.getDest();
                if (dest == null || dest.isMulticastAddress()) {
                    // mcasts
                    FlushHeader fh = (FlushHeader) msg.getHeader(this.id);
                    if (fh != null && fh.type == FlushHeader.FLUSH_BYPASS) {
                        return down_prot.down(evt);
                    } else {
                        blockMessageDuringFlush();
                    }
                } else {
                    // unicasts are irrelevant in virtual synchrony, let them through
                    return down_prot.down(evt);
                }
                break;

            case Event.CONNECT:
            case Event.CONNECT_USE_FLUSH:                                
                return handleConnect(evt,true);
                
            case Event.CONNECT_WITH_STATE_TRANSFER:            
            case Event.CONNECT_WITH_STATE_TRANSFER_USE_FLUSH:
                return handleConnect(evt, false);
                
            case Event.SUSPEND:
                return startFlush(evt);
                
             
            // only for testing, see FLUSH#testFlushWithCrashedFlushCoordinator    
            case Event.SUSPEND_BUT_FAIL: 
                if (!flushInProgress.get()) {
                    flush_promise.reset();
                    ArrayList<Address> flushParticipants = null;
                    synchronized (sharedLock) {
                        flushParticipants = new ArrayList<Address>(currentView.getMembers());
                    }
                    onSuspend(flushParticipants);
                }
                break;

            case Event.RESUME:
                onResume(evt);
                return null;

            case Event.SET_LOCAL_ADDRESS:
                localAddress = (Address) evt.getArg();
                break;
        }
        return down_prot.down(evt);
    }


    private Object handleConnect(Event evt, boolean waitForUnblock) {
        if (sentBlock.compareAndSet(false, true)) {
            sendBlockUpToChannel();
        }

        Object result = down_prot.down(evt);
        if (result instanceof Throwable) {
            // set the var back to its original state if we cannot
            // connect successfully
            sentBlock.set(false);
        }
        if(waitForUnblock)
            waitForUnblock();
        return result;
    }

    private void blockMessageDuringFlush() {
        boolean shouldSuspendByItself = false;
        blockMutex.lock();
        try {
            while (isBlockingFlushDown) {
                if (log.isDebugEnabled())
                    log.debug(localAddress + ": blocking for " + (timeout <= 0 ? "ever" : timeout + "ms"));
                shouldSuspendByItself = !notBlockedDown.await(timeout, TimeUnit.MILLISECONDS);
            }
            if (shouldSuspendByItself) {
                isBlockingFlushDown = false;      
                log.warn(localAddress + ": unblocking after " + timeout + "ms");
                flush_promise.setResult(Boolean.TRUE);
                notBlockedDown.signalAll();
            }            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            blockMutex.unlock();
        }        
    }

    public Object up(Event evt) {

        switch (evt.getType()) {
            case Event.MSG:
                Message msg = (Message) evt.getArg();
                final FlushHeader fh = (FlushHeader) msg.getHeader(this.id);
                if (fh != null) {
                    switch (fh.type) {
                        case FlushHeader.FLUSH_BYPASS:
                            return up_prot.up(evt);
                        case FlushHeader.START_FLUSH:
                            Collection<Address> fp = fh.flushParticipants;

                            boolean amIParticipant = (fp != null && fp.contains(localAddress))
                                            || msg.getSrc().equals(localAddress);
                            if (amIParticipant) {
                                handleStartFlush(msg, fh);
                            } else {
                                if (log.isDebugEnabled())
                                    log.debug(localAddress +
                                            ": received START_FLUSH but I am not flush participant, not responding");
                            }
                            break;
                        case FlushHeader.FLUSH_RECONCILE:
                            handleFlushReconcile(msg, fh);
                            break;
                        case FlushHeader.FLUSH_RECONCILE_OK:
                            onFlushReconcileOK(msg);
                            break;
                        case FlushHeader.STOP_FLUSH:
                            onStopFlush();
                            break;
                        case FlushHeader.ABORT_FLUSH:
                            Collection<Address> flushParticipants = fh.flushParticipants;
                            boolean participant = flushParticipants != null && flushParticipants.contains(localAddress);
                            if (log.isDebugEnabled()) {
                               log.debug(localAddress
                                       + ": received ABORT_FLUSH from flush coordinator "
                                       + msg.getSrc()
                                       + ",  am I flush participant="
                                       + participant);
                            }
                            if (participant) {                                
                               resetForNextFlush();                                
                            }                            
                            break;
                        case FlushHeader.FLUSH_NOT_COMPLETED:
                            if (log.isDebugEnabled()) {
                                log.debug(localAddress
                                        + ": received FLUSH_NOT_COMPLETED from "
                                        + msg.getSrc());
                            }
                            boolean flushCollision = false;
                            synchronized (sharedLock) {
                                flushNotCompletedMap.add(msg.getSrc());
                                flushCollision = !flushCompletedMap.isEmpty();
                                if (flushCollision) {
                                    flushNotCompletedMap.clear();
                                    flushCompletedMap.clear();
                                }
                            }

                            if (log.isDebugEnabled()) {
                                log.debug(localAddress
                                        + ": received FLUSH_NOT_COMPLETED from "
                                        + msg.getSrc() + " collision=" + flushCollision);
                            }

                            // reject flush if we have at least one OK and at least one FAIL
                            if (flushCollision) {
                                Runnable r = new Runnable() {
                                    public void run() {                                        
                                        rejectFlush(fh.flushParticipants, fh.viewID);
                                    }
                                };
                                new Thread(r).start();
                            }
                            // however, flush should fail/retry as soon as one FAIL is received
                            flush_promise.setResult(Boolean.FALSE);
                            break;

                        case FlushHeader.FLUSH_COMPLETED:
                            if (isCurrentFlushMessage(fh))
                                onFlushCompleted(msg.getSrc(), fh);
                            break;
                    }
                    return null; // do not pass FLUSH msg up
                } else {
                    // http://jira.jboss.com/jira/browse/JGRP-575
                    // for processing of application messages after we join,
                    // lets wait for STOP_FLUSH to complete
                    // before we start allowing message up.
                    Address dest = msg.getDest();
                    if (dest != null && !dest.isMulticastAddress()) {
                        return up_prot.up(evt); // allow unicasts to pass, virtual synchrony olny
                                                // applies to multicasts
                    }
                }
                break;

            case Event.VIEW_CHANGE:
                /*
                 * [JGRP-618] - FLUSH coordinator transfer reorders block/unblock/view events in
                 * applications (TCP stack only)
                 */
                up_prot.up(evt);
                View newView = (View) evt.getArg();
                boolean coordinatorLeft = onViewChange(newView);
                boolean singletonMember = newView.size() == 1
                                && newView.containsMember(localAddress);
                boolean isThisOurFirstView = viewCounter.addAndGet(1) == 1;
                // if this is channel's first view and its the only member of the group - no flush
                // was run but the channel application should still receive BLOCK,VIEW,UNBLOCK

                // also if coordinator of flush left each member should run stopFlush individually.
                if ((isThisOurFirstView && singletonMember) || coordinatorLeft) {
                    onStopFlush();
                }
                return null;

            case Event.TMP_VIEW:             
                View tmpView = (View) evt.getArg();
                if (!tmpView.containsMember(localAddress)) {
                    onViewChange(tmpView);
                }
                break;

            case Event.SUSPECT:
                onSuspect((Address) evt.getArg());
                break;

            case Event.SUSPEND:
                return startFlush(evt);

            case Event.RESUME:
                onResume(evt);
                return null;
            case Event.UNBLOCK:                
                flush_unblock_promise.setResult(Boolean.TRUE);
                break;
        }

        return up_prot.up(evt);
    }

    private void waitForUnblock() {       
        try {
            flush_unblock_promise.getResultWithTimeout(end_flush_timeout);
        } catch (TimeoutException t) {
            if (log.isWarnEnabled())
                log.warn(localAddress + ": waiting for UNBLOCK timed out after " + end_flush_timeout + " ms");
        } finally {
            flush_unblock_promise.reset();
        }
    }

    private void onFlushReconcileOK(Message msg) {
        if (log.isDebugEnabled())
            log.debug(localAddress + ": received reconcile ok from " + msg.getSrc());

        synchronized (sharedLock) {
            reconcileOks.add(msg.getSrc());
            if (reconcileOks.size() >= flushMembers.size()) {
                flush_promise.setResult(Boolean.TRUE);
                if (log.isDebugEnabled())
                    log.debug(localAddress + ": all FLUSH_RECONCILE_OK received");
            }
        }
    }

    private void handleFlushReconcile(Message msg, FlushHeader fh) {
        Address requester = msg.getSrc();
        Digest reconcileDigest = fh.digest;

        if (log.isDebugEnabled())
            log.debug(localAddress + ": received FLUSH_RECONCILE, passing digest to NAKACK "
                    + reconcileDigest);

        // Let NAKACK reconcile missing messages
        down_prot.down(new Event(Event.REBROADCAST, reconcileDigest));

        if (log.isDebugEnabled())
            log.debug(localAddress + ": returned from FLUSH_RECONCILE, "
                    + " sending RECONCILE_OK to " + requester);

        Message reconcileOk = new Message(requester);
        reconcileOk.setFlag(Message.OOB);
        reconcileOk.putHeader(this.id, new FlushHeader(FlushHeader.FLUSH_RECONCILE_OK));
        down_prot.down(new Event(Event.MSG, reconcileOk));
    }

    private void handleStartFlush(Message msg, FlushHeader fh) {
        Address flushRequester = msg.getSrc();

        boolean proceed = flushInProgress.compareAndSet(false, true);
        if (proceed) {
            synchronized (sharedLock) {
                flushCoordinator = flushRequester;
            }
            onStartFlush(flushRequester, fh);
        } else {
            FlushHeader fhr = new FlushHeader(FlushHeader.FLUSH_NOT_COMPLETED, fh.viewID,
                            fh.flushParticipants);
            Message response = new Message(flushRequester);
            response.putHeader(this.id, fhr);
            down_prot.down(new Event(Event.MSG, response));
            if (log.isDebugEnabled())
                log.debug(localAddress + ": received START_FLUSH, responded with FLUSH_NOT_COMPLETED to " + flushRequester);
        }
    }

    private void rejectFlush(Collection<? extends Address> participants, long viewId) {
        for (Address flushMember : participants) {
            Message reject = new Message(flushMember, localAddress, null);   
            reject.setFlag(Message.OOB);
            reject.putHeader(this.id, new FlushHeader(FlushHeader.ABORT_FLUSH, viewId,participants));
            down_prot.down(new Event(Event.MSG, reject));
        }
    }

    public Vector<Integer> providedDownServices() {
        Vector<Integer> retval = new Vector<Integer>(2);
        retval.addElement(new Integer(Event.SUSPEND));
        retval.addElement(new Integer(Event.RESUME));
        return retval;
    }

    private void sendBlockUpToChannel() {
        this.up(new Event(Event.BLOCK));
        sentUnblock.set(false);
    }

    private void sendUnBlockUpToChannel() {
        sentBlock.set(false);
        this.up(new Event(Event.UNBLOCK));
    }

    private boolean isCurrentFlushMessage(FlushHeader fh) {
        return fh.viewID == currentViewId();
    }

    private long currentViewId() {
        long viewId = -1;
        synchronized (sharedLock) {
            ViewId view = currentView.getVid();
            if (view != null) {
                viewId = view.getId();
            }
        }
        return viewId;
    }

    private boolean onViewChange(View view) {
        boolean coordinatorLeft = false;
        View oldView;
        synchronized (sharedLock) {
            suspected.retainAll(view.getMembers());
            oldView = currentView;
            currentView = view;
            coordinatorLeft = !oldView.getMembers().isEmpty() && !view.getMembers().isEmpty()
                            && !view.containsMember(oldView.getCreator());
        }
        if (log.isDebugEnabled())
            log.debug(localAddress + ": installing view " + view);

        return coordinatorLeft;
    }

    private void onStopFlush() {
        if (stats && startFlushTime > 0) {
            long stopFlushTime = System.currentTimeMillis();
            totalTimeInFlush += (stopFlushTime - startFlushTime);
            if (numberOfFlushes > 0) {
                averageFlushDuration = totalTimeInFlush / (double) numberOfFlushes;
            }
            startFlushTime = 0;
        }

        if (log.isDebugEnabled())
           log.debug(localAddress
                   + ": received STOP_FLUSH, unblocking FLUSH.down() and sending UNBLOCK up");
        
        resetForNextFlush();
        if (sentUnblock.compareAndSet(false, true)) {
            // ensures that we do not repeat unblock event
            sendUnBlockUpToChannel();
        }       
    }


   private void resetForNextFlush() {
      synchronized (sharedLock) {
            flushCompletedMap.clear();
            flushNotCompletedMap.clear();
            flushMembers.clear();
            suspected.clear();
            flushCoordinator = null;
            flushCompleted = false;
        }        
        blockMutex.lock();
        try {
            isBlockingFlushDown = false;
            notBlockedDown.signalAll();
        } finally {
            blockMutex.unlock();
        }        
        flushInProgress.set(false);
   }

    /**
     * Starts the flush protocol
     * @param members List of participants in the flush protocol. Guaranteed to be non-null
     */
    private void onSuspend(final List<Address> members) {
        Message msg = null;
        Collection<Address> participantsInFlush = null;
        synchronized (sharedLock) {
            // start FLUSH only on group members that we need to flush
            participantsInFlush = members;
            participantsInFlush.retainAll(currentView.getMembers());

            msg = new Message(null, localAddress, null);
            msg.putHeader(this.id, new FlushHeader(FlushHeader.START_FLUSH, currentViewId(),
                            participantsInFlush));
        }
        if (participantsInFlush.isEmpty()) {
            flush_promise.setResult(Boolean.TRUE);
        } else {
            down_prot.down(new Event(Event.MSG, msg));
            if (log.isDebugEnabled())
                log.debug(localAddress + ": flush coordinator "
                        + " is starting FLUSH with participants " + participantsInFlush);
        }
    }

    @SuppressWarnings("unchecked")
    private void onResume(Event evt) {
        List<Address> members = (List<Address>) evt.getArg();
        long viewID = currentViewId();
        boolean isParticipant = false;
        synchronized(sharedLock) {
            isParticipant = flushMembers.contains(localAddress) || (members != null && members.contains(localAddress));
        }
        if (members == null || members.isEmpty()) {
            Message msg = new Message(null, localAddress, null);
            // Cannot be OOB since START_FLUSH is not OOB
            // we have to FIFO order two subsequent flushes
            if (log.isDebugEnabled())
                log.debug(localAddress + ": received RESUME, sending STOP_FLUSH to all");
            msg.putHeader(this.id, new FlushHeader(FlushHeader.STOP_FLUSH, viewID));
            down_prot.down(new Event(Event.MSG, msg));
        } else {
            for (Address address : members) {
                Message msg = new Message(address, localAddress, null);
                // Cannot be OOB since START_FLUSH is not OOB
                // we have to FIFO order two subsequent flushes
                if (log.isDebugEnabled())
                    log.debug(localAddress + ": received RESUME, sending STOP_FLUSH to " + address);
                msg.putHeader(this.id, new FlushHeader(FlushHeader.STOP_FLUSH, viewID));
                down_prot.down(new Event(Event.MSG, msg));
            }
        }
        if(isParticipant)
            waitForUnblock();        
    }

    private void onStartFlush(Address flushStarter, FlushHeader fh) {
        if (stats) {
            startFlushTime = System.currentTimeMillis();
            numberOfFlushes += 1;
        }
        boolean proceed = false;
        synchronized (sharedLock) {
            flushCoordinator = flushStarter;
            flushMembers.clear();
            if (fh.flushParticipants != null) {
                flushMembers.addAll(fh.flushParticipants);
            }
            proceed = flushMembers.contains(localAddress);
            flushMembers.removeAll(suspected);
        }

        if (proceed) {
            if (sentBlock.compareAndSet(false, true)) {
                // ensures that we do not repeat block event
                // and that we do not send block event to non participants
                sendBlockUpToChannel();
                blockMutex.lock();
                try {
                    isBlockingFlushDown = true;
                } finally {
                    blockMutex.unlock();
                }                
            } else {
                if (log.isDebugEnabled())
                    log.debug(localAddress + ": received START_FLUSH, but not sending BLOCK up");
            }

            Digest digest = (Digest) down_prot.down(new Event(Event.GET_DIGEST));
            FlushHeader fhr = new FlushHeader(FlushHeader.FLUSH_COMPLETED, fh.viewID,fh.flushParticipants);
            fhr.addDigest(digest);

            Message msg = new Message(flushStarter);
            msg.putHeader(this.id, fhr);
            down_prot.down(new Event(Event.MSG, msg));
            if (log.isDebugEnabled())
                log.debug(localAddress + ": received START_FLUSH, responded with FLUSH_COMPLETED to " + flushStarter);
        }

    }

    private void onFlushCompleted(Address address, final FlushHeader header) {
        Message msg = null;
        boolean needsReconciliationPhase = false;
        boolean collision = false;
        Digest digest = header.digest;
        synchronized (sharedLock) {
            flushCompletedMap.put(address, digest);
            flushCompleted = flushCompletedMap.size() >= flushMembers.size()
                            && !flushMembers.isEmpty()
                            && flushCompletedMap.keySet().containsAll(flushMembers);

            collision = !flushNotCompletedMap.isEmpty();
            if (log.isDebugEnabled())
                log.debug(localAddress + ": FLUSH_COMPLETED from " + address + ", completed "
                        + flushCompleted + ", flushMembers " + flushMembers
                        + ", flushCompleted " + flushCompletedMap.keySet());

            needsReconciliationPhase = enable_reconciliation && flushCompleted && hasVirtualSynchronyGaps();
            if (needsReconciliationPhase) {
                Digest d = findHighestSequences();
                msg = new Message();
                msg.setFlag(Message.OOB);
                FlushHeader fh = new FlushHeader(FlushHeader.FLUSH_RECONCILE, currentViewId(),flushMembers);
                reconcileOks.clear();
                fh.addDigest(d);
                msg.putHeader(this.id, fh);

                if (log.isDebugEnabled())
                    log.debug(localAddress
                            + ": reconciling flush mebers due to virtual synchrony gap, digest is "
                            + d + " flush members are " + flushMembers);

                flushCompletedMap.clear();
            } else if (flushCompleted) {
                flushCompletedMap.clear();
            } else if (collision) {
                flushNotCompletedMap.clear();
                flushCompletedMap.clear();
            }
        }
        if (needsReconciliationPhase) {
            down_prot.down(new Event(Event.MSG, msg));
        } else if (flushCompleted) {
            flush_promise.setResult(Boolean.TRUE);
            if (log.isDebugEnabled())
                log.debug(localAddress + ": all FLUSH_COMPLETED received");
        } else if (collision) {
            // reject flush if we have at least one OK and at least one FAIL
            Runnable r = new Runnable() {
                public void run() {                    
                    rejectFlush(header.flushParticipants, header.viewID);
                }
            };
            new Thread(r).start();
        }
    }

    private boolean hasVirtualSynchronyGaps() {
        ArrayList<Digest> digests = new ArrayList<Digest>();
        digests.addAll(flushCompletedMap.values());
        Digest firstDigest = digests.get(0);
        List<Digest> remainingDigests = digests.subList(1, digests.size());
        for (Digest digest : remainingDigests) {
            Digest diff = firstDigest.difference(digest);
            if (diff != Digest.EMPTY_DIGEST) {
                return true;
            }
        }
        return false;
    }

    private Digest findHighestSequences() {
        Digest result = null;
        List<Digest> digests = new ArrayList<Digest>(flushCompletedMap.values());

        result = digests.get(0);
        List<Digest> remainingDigests = digests.subList(1, digests.size());

        for (Digest digestG : remainingDigests) {
            result = result.highestSequence(digestG);
        }
        return result;
    }

    private void onSuspect(Address address) {

        // handles FlushTest#testFlushWithCrashedFlushCoordinator
        boolean amINeighbourOfCrashedFlushCoordinator = false;
        ArrayList<Address> flushMembersCopy = null;
        synchronized (sharedLock) {
            boolean flushCoordinatorSuspected = address != null && address.equals(flushCoordinator);
            if (flushCoordinatorSuspected) {
                int indexOfCoordinator = flushMembers.indexOf(flushCoordinator);
                int myIndex = flushMembers.indexOf(localAddress);
                int diff = myIndex - indexOfCoordinator;
                amINeighbourOfCrashedFlushCoordinator = (diff == 1 || (myIndex == 0 && indexOfCoordinator == flushMembers.size()));
                if (amINeighbourOfCrashedFlushCoordinator) {
                    flushMembersCopy = new ArrayList<Address>(flushMembers);
                }
            }
        }
        if (amINeighbourOfCrashedFlushCoordinator) {
            if (log.isDebugEnabled())
                log.debug(localAddress + ": flush coordinator " + flushCoordinator + " suspected,"
                        + " I am the neighbor, completing the flush ");

            onResume(new Event(Event.RESUME, flushMembersCopy));
        }

        // handles FlushTest#testFlushWithCrashedNonCoordinators
        boolean flushOkCompleted = false;
        Message m = null;
        long viewID = 0;
        synchronized (sharedLock) {
            suspected.add(address);
            flushMembers.removeAll(suspected);
            viewID = currentViewId();
            flushOkCompleted = !flushCompletedMap.isEmpty()
                            && flushCompletedMap.keySet().containsAll(flushMembers);
            if (flushOkCompleted) {
                m = new Message(flushCoordinator, localAddress, null);
            }
            if (log.isDebugEnabled())
                log.debug(localAddress + ": suspect is " + address + ", completed " + flushOkCompleted
                        + ", flushOkSet " + flushCompletedMap + ", flushMembers " + flushMembers);
        }
        if (flushOkCompleted) {
            Digest digest = (Digest) down_prot.down(new Event(Event.GET_DIGEST));
            FlushHeader fh = new FlushHeader(FlushHeader.FLUSH_COMPLETED, viewID);
            fh.addDigest(digest);
            m.putHeader(this.id, fh);
            down_prot.down(new Event(Event.MSG, m));
            if (log.isDebugEnabled())
                log.debug(localAddress + ": sent FLUSH_COMPLETED message to " + flushCoordinator);
        }
    }

    public static class FlushHeader extends Header {
        public static final byte START_FLUSH = 0;

        public static final byte STOP_FLUSH = 2;

        public static final byte FLUSH_COMPLETED = 3;

        public static final byte ABORT_FLUSH = 5;

        public static final byte FLUSH_BYPASS = 6;

        public static final byte FLUSH_RECONCILE = 7;

        public static final byte FLUSH_RECONCILE_OK = 8;

        public static final byte FLUSH_NOT_COMPLETED = 9;

        byte type;

        long viewID;

        Collection<Address> flushParticipants;

        Digest digest = null;

        public FlushHeader() {
            this(START_FLUSH, 0);
        } // used for externalization

        public FlushHeader(byte type) {
            this(type, 0);
        }

        public FlushHeader(byte type, long viewID) {
            this(type, viewID, null);
        }

        public FlushHeader(byte type, long viewID, Collection<? extends Address> flushView) {
            this.type = type;
            this.viewID = viewID;
            if (flushView != null) {
                this.flushParticipants = new ArrayList<Address>(flushView);
            }
        }

        @Override
        public int size() {
            int retval = Global.BYTE_SIZE; // type
            retval += Global.LONG_SIZE; // viewID
            retval += Util.size(flushParticipants);
            retval += Global.BYTE_SIZE; // presence for digest
            if (digest != null) {
                retval += digest.serializedSize();
            }
            return retval;
        }

        public void addDigest(Digest digest) {
            this.digest = digest;
        }

        public String toString() {
            switch (type) {
                case START_FLUSH:
                    return "FLUSH[type=START_FLUSH,viewId=" + viewID + ",members="
                                    + flushParticipants + "]";
                case STOP_FLUSH:
                    return "FLUSH[type=STOP_FLUSH,viewId=" + viewID + "]";
                case ABORT_FLUSH:
                    return "FLUSH[type=ABORT_FLUSH,viewId=" + viewID + "]";
                case FLUSH_COMPLETED:
                    return "FLUSH[type=FLUSH_COMPLETED,viewId=" + viewID + "]";
                case FLUSH_BYPASS:
                    return "FLUSH[type=FLUSH_BYPASS,viewId=" + viewID + "]";
                case FLUSH_RECONCILE:
                    return "FLUSH[type=FLUSH_RECONCILE,viewId=" + viewID + ",digest=" + digest
                                    + "]";
                case FLUSH_RECONCILE_OK:
                    return "FLUSH[type=FLUSH_RECONCILE_OK,viewId=" + viewID + "]";
                default:
                    return "[FLUSH: unknown type (" + type + ")]";
            }
        }


        public void writeTo(DataOutputStream out) throws IOException {
            out.writeByte(type);
            out.writeLong(viewID);
            Util.writeAddresses(flushParticipants, out);
            Util.writeStreamable(digest, out);
        }

        @SuppressWarnings("unchecked")
        public void readFrom(DataInputStream in) throws IOException, IllegalAccessException,
                        InstantiationException {
            type = in.readByte();
            viewID = in.readLong();
            flushParticipants =(Collection<Address>)Util.readAddresses(in, ArrayList.class);
            digest = (Digest) Util.readStreamable(Digest.class, in);
        }
    }
}
