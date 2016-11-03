package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.blocks.locking.AwaitInfo;
import org.jgroups.blocks.locking.LockInfo;
import org.jgroups.blocks.locking.LockNotification;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Bits;
import org.jgroups.util.Owner;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;


/**
 * Base locking protocol, handling most of the protocol communication with other instances. To use distributed locking,
 * {@link org.jgroups.blocks.locking.LockService} is placed on a channel. LockService talks to a subclass of Locking
 * via events.
 * @author Bela Ban
 * @since 2.12
 * @see org.jgroups.protocols.CENTRAL_LOCK
 */
@MBean(description="Based class for locking functionality")
abstract public class Locking extends Protocol {

    @Property(description="bypasses message bundling if set")
    protected boolean bypass_bundling=true;

    @Property(description="Number of locks to be used for lock striping (for synchronized access to the server_lock entries)")
    protected int     lock_striping_size=10;


    protected Address local_addr;

    protected View    view;

    // server side locks
    protected final ConcurrentMap<String,ServerLock> server_locks=Util.createConcurrentMap(20);

    // protected access to the same locks in server_locks
    protected Lock[]  lock_stripes;

    // client side locks
    protected final ClientLockTable       client_lock_table=new ClientLockTable();

    protected final Set<LockNotification> lock_listeners=new CopyOnWriteArraySet<>();

    protected final static AtomicInteger  current_lock_id=new AtomicInteger(1);
    


    protected enum Type {
        GRANT_LOCK,        // request to acquire a lock
        LOCK_GRANTED,      // response to sender of GRANT_LOCK on succcessful lock acquisition
        LOCK_DENIED,       // response to sender of GRANT_LOCK on unsuccessful lock acquisition (e.g. on tryLock())
        RELEASE_LOCK,      // request to release a lock
        CREATE_LOCK,       // request to create a server lock (sent by coordinator to backups). Used by CentralLockService
        DELETE_LOCK,       // request to delete a server lock (sent by coordinator to backups). Used by CentralLockService
        LOCK_AWAIT,        // request to await until condition is signaled
        COND_SIG,          // request to signal awaiting thread
        COND_SIG_ALL,      // request to signal all awaiting threads
        SIG_RET,           // response to alert of signal
        DELETE_LOCK_AWAIT, // request to delete a waiter
        CREATE_AWAITER,    // request to create a server lock await (sent by coordinator to backups). Used by CentralLockService
        DELETE_AWAITER     // request to delete a server lock await (sent by coordinator to backups). Used by CentralLockService
    }



    public Locking() {
    }


    public boolean getBypassBundling() {
        return bypass_bundling;
    }

    public void setBypassBundling(boolean bypass_bundling) {
        this.bypass_bundling=bypass_bundling;
    }

    public void addLockListener(LockNotification listener) {
        if(listener != null)
            lock_listeners.add(listener);
    }

    public void removeLockListener(LockNotification listener) {
        if(listener != null)
            lock_listeners.remove(listener);
    }

    @ManagedAttribute
    public String getAddress() {
        return local_addr != null? local_addr.toString() : null;
    }

    @ManagedAttribute
    public String getView() {
        return view != null? view.toString() : null;
    }

    public void init() throws Exception {
        super.init();
        lock_stripes=new Lock[lock_striping_size];
        for(int i=0; i < lock_stripes.length; i++)
            lock_stripes[i]=new ReentrantLock();
    }

    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.LOCK:
                LockInfo info=evt.getArg();
                ClientLock lock=getLock(info.getName());
                if(!info.isTrylock()) {
                    if(info.isLockInterruptibly()) {
                        try {
                            lock.lockInterruptibly();
                        }
                        catch(InterruptedException e) {
                            Thread.currentThread().interrupt(); // has to be checked by caller who has to rethrow ...
                        }
                    }
                    else
                        lock.lock();
                }
                else {
                    if(info.isUseTimeout()) {
                        try {
                            return lock.tryLock(info.getTimeout(), info.getTimeUnit());
                        }
                        catch(InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                    else
                        return lock.tryLock();
                }
                return null;


            case Event.UNLOCK:
                info=evt.getArg();
                lock=getLock(info.getName(), false);
                if(lock != null)
                    lock.unlock();
                return null;

            case Event.UNLOCK_ALL:
                unlockAll();
                return null;
            case Event.LOCK_AWAIT:
                info=evt.getArg();
                lock=getLock(info.getName(), false);
                if (lock == null || !lock.acquired) {
                    throw new IllegalMonitorStateException();
                }
                Condition condition = lock.newCondition();
                if (info.isUseTimeout()) {
                    try {
                        return condition.awaitNanos(info.getTimeUnit().toNanos(info.getTimeout()));
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                else if (info.isLockInterruptibly()) {
                    try {
                        condition.await();
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                else {
                    condition.awaitUninterruptibly();
                }
                return null;
            case Event.LOCK_SIGNAL:
                AwaitInfo awaitInfo =evt.getArg();
                lock=getLock(awaitInfo.getName(), false);
                if (lock == null || !lock.acquired) {
                    throw new IllegalMonitorStateException();
                }
                sendSignalConditionRequest(awaitInfo.getName(), awaitInfo.isAll());
                return null;
            case Event.SET_LOCAL_ADDRESS:
                local_addr=evt.getArg();
                break;

            case Event.VIEW_CHANGE:
                handleView(evt.getArg());
                break;
        }
        return down_prot.down(evt);
    }

    public Object up(Event evt) {
        switch(evt.getType()) {
            case Event.VIEW_CHANGE:
                handleView(evt.getArg());
                break;
        }
        return up_prot.up(evt);
    }

    public Object up(Message msg) {
        LockingHeader hdr=msg.getHeader(id);
        if(hdr == null)
            return up_prot.up(msg);

        if (null != view && !view.containsMember(msg.getSrc())) {
            log.error("Received locking event from '%s' but member is not present in the current view - ignoring request", msg.src());
            return null;
        }

        Request req=null;
        try {
            req=Util.streamableFromBuffer(Request.class, msg.getRawBuffer(), msg.getOffset(), msg.getLength());
        }
        catch(Exception ex) {
            log.error("failed deserializng request", ex);
            return null;
        }
        log.trace("[%s] <-- [%s] %s", local_addr, msg.getSrc(), req);
        switch(req.type) {
            case GRANT_LOCK:
            case RELEASE_LOCK:
                handleLockRequest(req);
                break;
            case LOCK_GRANTED:
                handleLockGrantedResponse(req.lock_name, req.lock_id, req.owner);
                break;
            case LOCK_DENIED:
                handleLockDeniedResponse(req.lock_name, req.lock_id, req.owner);
                break;
            case CREATE_LOCK:
                handleCreateLockRequest(req.lock_name, req.owner);
                break;
            case DELETE_LOCK:
                handleDeleteLockRequest(req.lock_name);
                break;
            case COND_SIG:
            case COND_SIG_ALL:
                handleSignalRequest(req);
                break;
            case LOCK_AWAIT:
                handleAwaitRequest(req.lock_name, req.owner);
                handleLockRequest(req);
                break;
            case DELETE_LOCK_AWAIT:
                handleDeleteAwaitRequest(req.lock_name, req.owner);
                break;
            case SIG_RET:
                handleSignalResponse(req.lock_name, req.owner);
                break;
            case CREATE_AWAITER:
                handleCreateAwaitingRequest(req.lock_name, req.owner);
                break;
            case DELETE_AWAITER:
                handleDeleteAwaitingRequest(req.lock_name, req.owner);
                break;
            default:
                log.error("Request of type %s not known", req.type);
                break;
        }
        return null;
    }

    protected ClientLock getLock(String name) {
        return client_lock_table.getLock(name,getOwner(),true);
    }

    protected ClientLock getLock(String name, boolean create_if_absent) {
        return client_lock_table.getLock(name,getOwner(),create_if_absent);
    }

    @ManagedOperation(description="Unlocks all currently held locks")
    public void unlockAll() {
        client_lock_table.unlockAll();
    }


    @ManagedOperation(description="Dumps all locks")
    public String printLocks() {
        StringBuilder sb=new StringBuilder();
        sb.append("server locks:\n");
        for(Map.Entry<String,ServerLock> entry: server_locks.entrySet())
            sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");

        sb.append("\nmy locks: ").append(client_lock_table.toString());
        return sb.toString();
    }

    protected void handleView(View view) {
        this.view=view;
        log.debug("view=%s", view);
        List<Address> members=view.getMembers();
        List<Response> responses=new ArrayList<>();
        for(Map.Entry<String,ServerLock> entry: server_locks.entrySet()) {
            String lock_name=entry.getKey();
            ServerLock server_lock=entry.getValue();
            Lock lock=_getLock(lock_name);
            lock.lock();
            try {
                Response rsp=server_lock.handleView(members);
                if(rsp != null)
                    responses.add(rsp);
                if(server_lock.isEmpty() && server_lock.current_owner == null && server_lock.condition.queue.isEmpty())
                    server_locks.remove(lock_name);
            }
            finally {
                lock.unlock();
            }
        }

        // do the sending outside the lock scope (might block on credits or TCP send)
        for(Response rsp: responses)
            sendLockResponse(rsp.type, rsp.owner, rsp.lock_name, rsp.lock_id);
    }



    protected ClientLock createLock(String lock_name, Owner owner) {
        return new ClientLock(lock_name, owner);
    }

    /** Gets a lock from locks based on the hash of the lock name */
    protected Lock _getLock(String lock_name) {
        int index=lock_name != null? Math.abs(lock_name.hashCode() % lock_stripes.length) : 0;
        return lock_stripes[index];
    }

    protected Owner getOwner() {
        return new Owner(local_addr, Thread.currentThread().getId());
    }

    abstract protected void sendGrantLockRequest(String lock_name, int lock_id, Owner owner, long timeout, boolean is_trylock);
    abstract protected void sendReleaseLockRequest(String lock_name, Owner owner);
    abstract protected void sendAwaitConditionRequest(String lock_name, Owner owner);
    abstract protected void sendSignalConditionRequest(String lock_name, boolean all);
    abstract protected void sendDeleteAwaitConditionRequest(String lock_name, Owner owner);


    protected void sendRequest(Address dest, Type type, String lock_name, Owner owner, long timeout, boolean is_trylock) {
        send(dest, new Request(type, lock_name, owner, timeout, is_trylock));
    }

    protected void sendRequest(Address dest, Type type, String lock_name, int lock_id, Owner owner, long timeout, boolean is_trylock) {
        send(dest, new Request(type, lock_name, owner, timeout, is_trylock).lockId(lock_id));
    }

    protected void sendLockResponse(Type type, Owner dest, String lock_name, int lock_id) {
        send(dest.getAddress(), new Request(type, lock_name, dest, 0).lockId(lock_id));
    }

    protected void sendSignalResponse(Owner dest, String lock_name) {
        send(dest.getAddress(), new Request(Type.SIG_RET, lock_name, dest, 0));
    }

    protected void send(Address dest, Request req) {
        Message msg=new Message(dest, Util.streamableToBuffer(req)).putHeader(id, new LockingHeader());
        if(bypass_bundling)
            msg.setFlag(Message.Flag.DONT_BUNDLE);
        log.trace("[%s] --> %s] %s", local_addr, dest == null? "ALL" : dest, req);
        try {
            down_prot.down(msg);
        }
        catch(Exception ex) {
            log.error("failed sending %s request: %s", req.type, ex);
        }
    }


    protected void handleLockRequest(Request req) {
        Response rsp=null;
        Lock lock=_getLock(req.lock_name);
        lock.lock();
        try {
            ServerLock server_lock=server_locks.get(req.lock_name);
            if(server_lock == null) {
                server_lock=new ServerLock(req.lock_name);
                ServerLock tmp=server_locks.putIfAbsent(req.lock_name, server_lock);
                if(tmp != null)
                    server_lock=tmp;
                else
                    notifyLockCreated(req.lock_name);
            }
            rsp=server_lock.handleRequest(req);
            if(server_lock.isEmpty() && server_lock.current_owner == null && server_lock.condition.queue.isEmpty())
                server_locks.remove(req.lock_name);
        }
        finally {
            lock.unlock();
        }

        // moved outside the lock scope
        if(rsp != null)
            sendLockResponse(rsp.type, rsp.owner, rsp.lock_name, rsp.lock_id);
    }


    protected void handleLockGrantedResponse(String lock_name, int lock_id, Owner owner) {
        ClientLock lock=client_lock_table.getLock(lock_name,owner,false);
        if(lock != null)
            lock.handleLockGrantedResponse(lock_id);
    }

    protected void handleLockDeniedResponse(String lock_name, int lock_id, Owner owner) {
         ClientLock lock=client_lock_table.getLock(lock_name,owner,false);
         if(lock != null)
             lock.lockDenied(lock_id);
    }
    
    protected void handleAwaitRequest(String lock_name, Owner owner) {
        Lock lock=_getLock(lock_name);
        lock.lock();
        try {
            ServerLock server_lock=server_locks.get(lock_name);
            if (server_lock != null)
                server_lock.condition.addWaiter(owner);
            else
                log.error(Util.getMessage("ConditionAwaitWasReceivedButLockWasNotCreatedWaiterMayBlockForever"));
        }
        finally {
            lock.unlock();
        }
    }
    
    protected void handleDeleteAwaitRequest(String lock_name, Owner owner) {
        Lock lock=_getLock(lock_name);
        lock.lock();
        try {
            ServerLock server_lock=server_locks.get(lock_name);
            if (server_lock != null)
                server_lock.condition.removeWaiter(owner);
            else
                log.error(Util.getMessage("ConditionAwaitDeleteWasReceivedButLockWasGone"));
        }
        finally {
            lock.unlock();
        }
    }
    
    protected void handleSignalResponse(String lock_name, Owner owner) {
        ClientLock lock=client_lock_table.getLock(lock_name,owner,false);
        if(lock != null) {
            lock.condition.signaled();
        }
        else {
            log.error(Util.getMessage("ConditionResponseWasClientLockWasNotPresentIgnoredSignal"));
        }
    }
    
    protected void handleSignalRequest(Request req) {
        Response rsp=null;
        Lock lock=_getLock(req.lock_name);
        lock.lock();
        try {
            ServerLock server_lock=server_locks.get(req.lock_name);
            if (server_lock != null)
                rsp=server_lock.handleRequest(req);
            else
                log.error(Util.getMessage("ConditionSignalWasReceivedButLockWasNotCreatedCouldnTNotifyAnyone"));
        }
        finally {
            lock.unlock();
        }

        // moved outside the lock scope
        if(rsp != null)
            sendLockResponse(rsp.type, rsp.owner, rsp.lock_name, rsp.lock_id);
    }
    
    protected void handleCreateLockRequest(String lock_name, Owner owner) {
        Lock lock=_getLock(lock_name);
        lock.lock();
        try {
            server_locks.put(lock_name, new ServerLock(lock_name, owner));
        }
        finally {
            lock.unlock();
        }
    }


    protected void handleDeleteLockRequest(String lock_name) {
        Lock lock=_getLock(lock_name);
        lock.lock();
        try {
            ServerLock server_lock = server_locks.get(lock_name);
            if(server_lock == null)
                return;
            if (server_lock.condition.queue.isEmpty())
                server_locks.remove(lock_name);
            else
                server_lock.current_owner = null;
        }
        finally {
            lock.unlock();
        }
    }


    protected void handleCreateAwaitingRequest(String lock_name, Owner owner) {
        Lock lock=_getLock(lock_name);
        lock.lock();
        try {
            ServerLock server_lock = server_locks.get(lock_name);
            if (server_lock == null) {
                server_lock = new ServerLock(lock_name);
                ServerLock tmp=server_locks.putIfAbsent(lock_name,server_lock);
                if(tmp != null)
                    server_lock=tmp;
            }
            server_lock.condition.queue.add(owner);
        }
        finally {
            lock.unlock();
        }
    }


    protected void handleDeleteAwaitingRequest(String lock_name, Owner owner) {
        Lock lock=_getLock(lock_name);
        lock.lock();
        try {
            ServerLock server_lock = server_locks.get(lock_name);
            if (server_lock != null) {
                server_lock.condition.queue.remove(owner);
                if (server_lock.condition.queue.isEmpty() && server_lock.current_owner == null) {
                    server_locks.remove(lock_name);
                }
            }
        }
        finally {
            lock.unlock();
        }
    }



    protected void notifyLockCreated(String lock_name) {
        for(LockNotification listener: lock_listeners) {
            try {
                listener.lockCreated(lock_name);
            }
            catch(Throwable t) {
                log.error("failed notifying %s: %s", listener, t.toString());
            }
        }
    }

    protected void notifyLockDeleted(String lock_name) {
        for(LockNotification listener: lock_listeners) {
            try {
                listener.lockDeleted(lock_name);
            }
            catch(Throwable t) {
                log.error("failed notifying %s: %s", listener, t.toString());
            }
        }
    }

    protected void notifyLocked(String lock_name, Owner owner) {
        for(LockNotification listener: lock_listeners) {
            try {
                listener.locked(lock_name,owner);
            }
            catch(Throwable t) {
                log.error("failed notifying %s: %s", listener, t.toString());
            }
        }
    }

    protected void notifyUnlocked(String lock_name, Owner owner) {
        for(LockNotification listener: lock_listeners) {
            try {
                listener.unlocked(lock_name,owner);
            }
            catch(Throwable t) {
                log.error("failed notifying %s: %s", listener, t.toString());
            }
        }
    }

    protected void notifyAwaiting(String lock_name, Owner owner) {
        for(LockNotification listener: lock_listeners) {
            try {
                listener.awaiting(lock_name,owner);
            }
            catch(Throwable t) {
                log.error("failed notifying %s: %s", listener, t.toString());
            }
        }
    }

    protected void notifyAwaited(String lock_name, Owner owner) {
        for(LockNotification listener: lock_listeners) {
            try {
                listener.awaited(lock_name,owner);
            }
            catch(Throwable t) {
                log.error("failed notifying %s: %s", listener, t.toString());
            }
        }
    }



    /**
     * Server side queue for handling of lock requests (lock, release).
     * @author Bela Ban
     */
    protected class ServerLock {
        protected final String          lock_name;
        protected Owner                 current_owner;
        protected final List<Request>   queue=new ArrayList<>();
        protected final ServerCondition condition;

        public ServerLock(String lock_name) {
            this.lock_name=lock_name;
            this.condition=new ServerCondition(this);
        }

        protected ServerLock(String lock_name, Owner owner) {
            this.lock_name=lock_name;
            this.current_owner=owner;
            this.condition=new ServerCondition(this);
        }

        protected Response handleRequest(Request req) {
            switch(req.type) {
                case GRANT_LOCK:
                    if(current_owner == null) {
                        setOwner(req.owner);
                        return new Response(Type.LOCK_GRANTED, req.owner, req.lock_name, req.lock_id);
                    }
                    if(current_owner.equals(req.owner))
                        return new Response(Type.LOCK_GRANTED, req.owner, req.lock_name, req.lock_id);

                    if(req.is_trylock && req.timeout <= 0)
                        return new Response(Type.LOCK_DENIED, req.owner, req.lock_name, req.lock_id);
                    addToQueue(req);
                    break;
                case RELEASE_LOCK:
                case LOCK_AWAIT:
                    if(current_owner == null)
                        break;
                    if(current_owner.equals(req.owner))
                        setOwner(null);
                    else
                        addToQueue(req);
                    break;
                case COND_SIG:
                    condition.signal(false);
                    break;
                case COND_SIG_ALL:
                    condition.signal(true);
                    break;
                default:
                    throw new IllegalArgumentException("type " + req.type + " is invalid here");
            }

            return processQueue();
        }

        protected Response handleView(List<Address> members) {
            if(current_owner != null && !members.contains(current_owner.getAddress())) {
                Owner tmp=current_owner;
                setOwner(null);
                log.debug("unlocked \"%s\" because owner %s left", lock_name, tmp);
            }

            synchronized(queue) {
                for(Iterator<Request> it=queue.iterator(); it.hasNext(); ) {
                    Request req=it.next();
                    if(!members.contains(req.owner.getAddress()))
                        it.remove();
                }
            }
            
            for(Iterator<Owner> it=condition.queue.iterator(); it.hasNext();) {
                Owner own=it.next();
                if(!members.contains(own.getAddress()))
                    it.remove();
            }

            return processQueue();
        }


        protected void addToQueue(Request req) {
            synchronized(queue) {
                if(queue.isEmpty()) {
                    if(req.type == Type.GRANT_LOCK)
                        queue.add(req);
                    return; // RELEASE_LOCK is discarded on an empty queue
                }
            }

            // at this point the queue is not empty
            switch(req.type) {

                // If there is already a lock request from the same owner, discard the new lock request
                case GRANT_LOCK:
                    synchronized(queue) {
                        if(!isRequestPresent(Type.GRANT_LOCK, req.owner))
                            queue.add(req);
                    }
                    break;

                case RELEASE_LOCK:
                    // Release the lock request from the same owner already in the queue
                    // If there is no lock request, discard the unlock request
                    removeRequest(Type.GRANT_LOCK, req.owner);
                    break;
            }
        }

        /** Checks if a certain request from a given owner is already in the queue */
        protected boolean isRequestPresent(Type type, Owner owner) { // holds lock on queue
            for(Request req: queue)
                if(req.type == type && req.owner.equals(owner))
                    return true;
            return false;
        }

        protected void removeRequest(Type type, Owner owner) {
            synchronized(queue) {
                for(Iterator<Request> it=queue.iterator(); it.hasNext(); ) {
                    Request req=it.next();
                    if(req.type == type && req.owner.equals(owner))
                        it.remove();
                }
            }
        }

        protected Request getNextRequest() {
            synchronized(queue) {
                return !queue.isEmpty()? queue.remove(0) : null;
            }
        }

        protected Response processQueue() {
            if(current_owner != null)
                return null;
            Request req;
            while((req=getNextRequest()) != null) {
                if(req.type == Type.GRANT_LOCK) {
                    setOwner(req.owner);
                    return new Response(Type.LOCK_GRANTED, req.owner, req.lock_name, req.lock_id);
                }
            }
            return null;
        }

        protected void setOwner(Owner owner) {
            if(owner == null) {
                if(current_owner != null) {
                    Owner tmp=current_owner;
                    current_owner=null;
                    notifyUnlocked(lock_name, tmp);
                }
            }
            else {
                current_owner=owner;
                notifyLocked(lock_name, owner);
            }
        }

        public boolean isEmpty() {
            synchronized(queue) {
                return queue.isEmpty();
            }
        }

        public String toString() {
            StringBuilder sb=new StringBuilder();
            sb.append(current_owner);
            synchronized(queue) {
                if(!queue.isEmpty()) {
                    sb.append(", queue: ");
                    for(Request req : queue) {
                        sb.append(req.toStringShort()).append(" ");
                    }
                }
            }
            return sb.toString();
        }
    }

    protected class ServerCondition {
        protected final ServerLock   lock;
        protected final Queue<Owner> queue=new ArrayDeque<>();
        
        public ServerCondition(ServerLock lock) {
            this.lock = lock;
        }

        public void addWaiter(Owner waiter) {
            notifyAwaiting(lock.lock_name, waiter);
            log.trace("Waiter [%s] was added for %s", waiter, lock.lock_name);
            queue.add(waiter);
        }
        
        public void removeWaiter(Owner waiter) {
            notifyAwaited(lock.lock_name, waiter);
            log.trace("Waiter [%s] was removed for %s", waiter, lock.lock_name);
            queue.remove(waiter);
        }
        
        public void signal(boolean all) {
            if (queue.isEmpty())
                log.trace("Signal for [%s] ignored since, no one is waiting in queue", lock.lock_name);

            Owner entry;
            if (all) {
                while ((entry = queue.poll()) != null) {
                    notifyAwaited(lock.lock_name, entry);
                    log.trace("Signalled %s for %s", entry, lock.lock_name);
                    sendSignalResponse(entry, lock.lock_name);
                }
            }
            else {
                entry = queue.poll();
                if (entry != null) {
                    notifyAwaited(lock.lock_name, entry);
                    log.trace("Signalled %s for %s", entry, lock.lock_name);
                    sendSignalResponse(entry, lock.lock_name);
                }
            }
        }
    }


    /**
     * Implementation of {@link Lock}. This is a client stub communicates with a server equivalent. The semantics are
     * more or less those of {@link Lock}, but may differ slightly.
     * For details see {@link org.jgroups.blocks.locking.LockService}.
     */
    protected class ClientLock implements Lock {
        protected final String          name;
        protected Owner                 owner;
        protected volatile boolean      acquired;
        protected volatile boolean      denied;
        protected volatile boolean      is_trylock;
        protected long                  timeout;
        protected final ClientCondition condition;

        // unique for locks for the same name:owner, can wrap around (that's ok)
        protected final int             lock_id=current_lock_id.getAndIncrement();



        public ClientLock(String name) {
            this.name=name;
            this.condition = new ClientCondition(this);
        }

        public ClientLock(String name, Owner owner) {
            this(name);
            this.owner=owner;
        }

        public void lock() {
            try {
                acquire(false);
            }
            catch(InterruptedException e) { // should never happen
               Thread.currentThread().interrupt(); // just a second line of defense
            }
        }

        public void lockInterruptibly() throws InterruptedException {
            acquire(true);
        }

        public boolean tryLock() {
            try {
                return acquireTryLock(0, false);
            }
            catch(InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }

        public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
            return acquireTryLock(TimeUnit.MILLISECONDS.convert(time, unit), true);
        }

        public synchronized void unlock() {
            _unlock(false);
        }

        public Condition newCondition() {
            // Currently only 1 condition per Lock is supported
            return condition;
        }

        public String toString() {
            return name + " (id=" + lock_id + ", locked=" + acquired + ")";
        }

        protected synchronized void lockGranted(int lock_id) {
            if(this.lock_id != lock_id) {
                log.error(Util.getMessage("DiscardedLOCKGRANTEDResponseWithLockId") + lock_id + ", my lock-id=" + this.lock_id);
                return;
            }
            acquired=true;
            this.notifyAll();
        }

        protected synchronized void lockDenied(int lock_id) {
            if(this.lock_id != lock_id) {
                log.error(Util.getMessage("DiscardedLOCKDENIEDResponseWithLockId") + lock_id + ", my lock_id=" + this.lock_id);
                return;
            }
            denied=true;
            this.notifyAll();
        }

        protected void handleLockGrantedResponse(int lock_id) {
            lockGranted(lock_id);
        }

        protected synchronized void acquire(boolean throwInterrupt) throws InterruptedException {
            if(acquired)
                return;
            if(throwInterrupt && Thread.interrupted())
                throw new InterruptedException();
            owner=getOwner();
            sendGrantLockRequest(name, lock_id, owner, 0, false);
            boolean interrupted=false;
            while(!acquired) {
                try {
                    this.wait();
                }
                catch(InterruptedException e) {
                    if(throwInterrupt && !acquired) {
                        _unlock(true);
                        throw e;
                    }
                    // If we don't throw exceptions then we just set the interrupt flag and let it loop around
                    interrupted=true;
                }
            }
            if(interrupted)
                Thread.currentThread().interrupt();
        }

        protected synchronized void _unlock(boolean force) {
            if(!acquired && !denied && !force)
                return;
            this.timeout=0;
            this.is_trylock=false;
            if(!denied)
                sendReleaseLockRequest(name, owner);
            acquired=denied=false;
            notifyAll();

            client_lock_table.removeClientLock(name,owner);
            notifyLockDeleted(name);
            owner=null;
        }

        protected synchronized boolean acquireTryLock(long timeout, boolean use_timeout) throws InterruptedException {
            if(denied)
                return false;
            if(!acquired) {
                if(use_timeout && Thread.interrupted())
                    throw new InterruptedException();
                is_trylock=true;
                this.timeout=timeout;
                if(owner == null)
                    owner=getOwner();
                sendGrantLockRequest(name, lock_id, owner, timeout, true);

                boolean interrupted = false;
                while(!acquired && !denied) {
                    if(use_timeout) {
                        long timeout_ns=TimeUnit.NANOSECONDS.convert(timeout, TimeUnit.MILLISECONDS),
                          wait_time=timeout_ns,
                          start=System.nanoTime();

                        while(wait_time > 0 && !acquired && !denied) {
                            try {
                                long wait_ms=TimeUnit.MILLISECONDS.convert(wait_time, TimeUnit.NANOSECONDS);
                                if(wait_ms <= 0)
                                    break;
                                this.wait(wait_ms);
                            }
                            catch(InterruptedException e) {
                                interrupted=true;
                            }
                            finally {
                                wait_time=timeout_ns - (System.nanoTime() - start);
                                this.timeout=TimeUnit.MILLISECONDS.convert(wait_time, TimeUnit.NANOSECONDS);
                            }
                        }
                        break;
                    }
                    else {
                        try {
                            this.wait();
                        }
                        catch(InterruptedException e) {
                            interrupted = true;
                        }
                    }
                }
                if(interrupted)
                    Thread.currentThread().interrupt();
            }
            boolean retval=acquired && !denied;
            if(!acquired || denied)
                _unlock(true);
            return retval;
        }
    }

    /** Manages access to client locks */
    protected class ClientLockTable {
        protected final ConcurrentMap<String,Map<Owner,ClientLock>> table=Util.createConcurrentMap(20);


        protected synchronized ClientLock getLock(String name, Owner owner, boolean create_if_absent) {
            Map<Owner,ClientLock> owners=table.get(name);
            if(owners == null) {
                if(!create_if_absent)
                    return null;
                owners=new HashMap<>();
                Map<Owner,ClientLock> existing=table.putIfAbsent(name,owners);
                if(existing != null)
                    owners=existing;
            }
            ClientLock lock=owners.get(owner);
            if(lock == null) {
                if(!create_if_absent)
                    return null;
                lock=createLock(name, owner);
                owners.put(owner, lock);
            }
            return lock;
        }

        protected synchronized void removeClientLock(String lock_name, Owner owner) {
            Map<Owner,ClientLock> owners=table.get(lock_name);
            if(owners != null) {
                ClientLock lock=owners.remove(owner);
                if(lock != null && owners.isEmpty())
                    table.remove(lock_name);
            }
        }

        protected void unlockAll() {
            List<ClientLock> lock_list=new ArrayList<>();
            synchronized(this) {
                table.values().forEach(map -> lock_list.addAll(map.values()));
            }
            lock_list.forEach(ClientLock::unlock);
        }

        protected void resendPendingLockRequests() {
            if(!table.isEmpty())
                table.values().forEach(map -> map.values().stream().filter(lock -> !lock.acquired && !lock.denied)
                  .forEach(lock -> sendGrantLockRequest(lock.name, lock.lock_id, lock.owner, lock.timeout, lock.is_trylock)));
        }

        protected synchronized Collection<Map<Owner,ClientLock>> values() {
            return table.values();
        }


        public String toString() {
            StringBuilder sb=new StringBuilder();
            boolean first_element=true;
            for(Map.Entry<String,Map<Owner,ClientLock>> entry: table.entrySet()) {
                if(first_element)
                    first_element=false;
                else
                    sb.append(", ");
                sb.append(entry.getKey()).append(" (");
                Map<Owner,ClientLock> owners=entry.getValue();
                boolean first=true;
                for(Map.Entry<Owner,ClientLock> entry2: owners.entrySet()) {
                    if(first)
                        first=false;
                    else
                        sb.append(", ");
                    sb.append(entry2.getKey());
                    ClientLock cl=entry2.getValue();
                    if(!cl.acquired || cl.denied)
                        sb.append(", unlocked");
                }
                sb.append(")");
            }
            return sb.toString();
        }
    }
    
    protected class ClientCondition implements Condition {

        protected final ClientLock    lock;
        protected final AtomicBoolean signaled = new AtomicBoolean(false);
        /**
         * This is okay only having 1 since a client condition is 1 per 
         * lock_name, thread id combination.
         */
        protected volatile AtomicReference<Thread> parker=new AtomicReference<>();
        
        public ClientCondition(ClientLock lock) {
            this.lock = lock;
        }
        
        @Override
        public void await() throws InterruptedException {
            InterruptedException ex = null;
            try {
                await(true);
            }
            catch (InterruptedException e) {
                ex = e;
                throw ex;
            }
            finally {
                lock.lock();
                
                // If we are throwing an InterruptedException
                // then clear the interrupt state as well.
                if (ex != null) {
                    Thread.interrupted();
                }
            }
        }

        @Override
        public void awaitUninterruptibly() {
            try {
                await(false);
            }
            catch(InterruptedException e) {
                // This should never happen
            }
            finally {
                lock.lock();
            }
        }

        @Override
        public long awaitNanos(long nanosTimeout) throws InterruptedException {
            InterruptedException ex = null;
            try {
                return await(nanosTimeout);
            }
            catch (InterruptedException e) {
                ex = e;
                throw ex;
            }
            finally {
                lock.lock(); // contract mandates we need to re-acquire the lock (see overridden method)
                
                // If we are throwing an InterruptedException then clear the interrupt state as well
                if (ex != null)
                    Thread.interrupted();
            }
        }

        /**
         * Note this wait will only work correctly if the converted value is less
         * than 292 years.  This is due to the limitation in System.nano and long
         * values that can only store up to 292 years (22<sup>63</sup> nanoseconds).
         * 
         * For more information please see {@link System#nanoTime()}
         */
        @Override
        public boolean await(long time, TimeUnit unit) throws InterruptedException {
            return awaitNanos(unit.toNanos(time)) > 0;
        }

        @Override
        public boolean awaitUntil(Date deadline) throws InterruptedException {
            long waitUntilTime=deadline.getTime();
            long currentTime=System.currentTimeMillis();

            long waitTime=waitUntilTime - currentTime;
            return waitTime > 0 && await(waitTime, TimeUnit.MILLISECONDS);
        }
        
        protected void await(boolean throwInterrupt) throws InterruptedException {
            if(!signaled.get()) {
                lock.acquired = false;
                sendAwaitConditionRequest(lock.name, lock.owner);
                boolean interrupted=false;
                while(!signaled.get()) {
                    parker.set(Thread.currentThread());
                    LockSupport.park(this);
                    
                    if (Thread.interrupted()) {
                        // If we were interrupted and haven't received a response yet then we try to
                        // clean up the lock request and throw the exception
                        if (!signaled.get()) {
                            sendDeleteAwaitConditionRequest(lock.name, lock.owner);
                            throw new InterruptedException();
                        }
                        // In the case that we were signaled and interrupted
                        // we want to return the signal but still interrupt
                        // our thread
                        interrupted = true; 
                    }
                }
                if(interrupted)
                    Thread.currentThread().interrupt();
            }
            
            // We set as if this signal was no released.  This way if the
            // condition is reused again, but the client condition isn't lost
            // we won't think we were signaled immediately
            signaled.set(false);
        }

        // Return the estimated time to wait (in ns), can be negative
        protected long await(long nanoSeconds) throws InterruptedException {
            long start=System.nanoTime();

            if(!signaled.get()) {
                // We release the lock at the same time as waiting on the
                // condition
                lock.acquired = false;
                sendAwaitConditionRequest(lock.name, lock.owner);
                
                boolean interrupted = false;
                while(!signaled.get()) {
                    long wait_nano=nanoSeconds - (System.nanoTime() - start);

                    // If we waited max time break out
                    if(wait_nano > 0) {
                        parker.set(Thread.currentThread());
                        LockSupport.parkNanos(this, wait_nano);
                        
                        if (Thread.interrupted()) {
                            // If we were interrupted and haven't received a response yet then we try to
                            // clean up the lock request and throw the exception
                            if (!signaled.get()) {
                                sendDeleteAwaitConditionRequest(lock.name, lock.owner);
                                throw new InterruptedException();
                            }
                            // In the case that we were signaled and interrupted
                            // we want to return the signal but still interrupt
                            // our thread
                            interrupted = true; 
                        }
                    }
                    else {
                        break;
                    }
                }
                if(interrupted)
                    Thread.currentThread().interrupt();
            }
            
            // We set as if this signal was no released.  This way if the
            // condition is reused again, but the client condition isn't lost
            // we won't think we were signaled immediately
            // If we weren't signaled then delete our request
            if (!signaled.getAndSet(false)) {
                sendDeleteAwaitConditionRequest(lock.name, lock.owner);
            }
            return nanoSeconds - (System.nanoTime() - start);
        }

        @Override
        public void signal() {
            sendSignalConditionRequest(lock.name, false);
        }

        @Override
        public void signalAll() {
            sendSignalConditionRequest(lock.name, true);
        }
        
        protected void signaled() {
            signaled.set(true);
            Thread thread = parker.getAndSet(null);
            if (thread != null)
                LockSupport.unpark(thread);
        }
    }


    protected static class Request implements Streamable {
        protected Type    type;
        protected String  lock_name;
        protected int     lock_id;
        protected Owner   owner;
        protected long    timeout;
        protected boolean is_trylock;


        public Request() {
        }

        public Request(Type type, String lock_name, Owner owner, long timeout) {
            this.type=type;
            this.lock_name=lock_name;
            this.owner=owner;
            this.timeout=timeout;
        }

        public Request(Type type, String lock_name, Owner owner, long timeout, boolean is_trylock) {
            this(type, lock_name, owner, timeout);
            this.is_trylock=is_trylock;
        }

        public Request lockId(int lock_id) {this.lock_id=lock_id; return this;}
        public int lockId()                {return lock_id;}

        public void writeTo(DataOutput out) throws Exception {
            out.writeByte(type.ordinal());
            Bits.writeString(lock_name,out);
            out.writeInt(lock_id);
            Util.writeStreamable(owner, out);
            out.writeLong(timeout);
            out.writeBoolean(is_trylock);
        }

        public void readFrom(DataInput in) throws Exception {
            type=Type.values()[in.readByte()];
            lock_name=Bits.readString(in);
            lock_id=in.readInt();
            owner=Util.readStreamable(Owner.class, in);
            timeout=in.readLong();
            is_trylock=in.readBoolean();
        }

        public String toString() {
            return type.name() + " [" + lock_name + ", lock_id=" + lock_id + ", owner=" + owner + (is_trylock? ", trylock " : " ") +
              (timeout > 0? "(timeout=" + timeout + ")" : "" + "]");
        }

        public String toStringShort() {
            StringBuilder sb=new StringBuilder();
            switch(type) {
                case RELEASE_LOCK:
                    sb.append("U");
                    break;
                case GRANT_LOCK:
                    sb.append(is_trylock? "TL" : "L");
                    break;
                default:
                    sb.append("N/A");
                    break;
            }
            sb.append("(").append(lock_name).append(",").append(owner);
            if(timeout > 0)
                sb.append(",").append(timeout);
            sb.append(")");
            return sb.toString();
        }
    }

    /** A response to a request, to be sent back to the requester as a message */
    protected static class Response {
        protected final Type   type;
        protected final Owner  owner;
        protected final String lock_name;
        protected final int    lock_id;

        public Response(Type type, Owner owner, String lock_name, int lock_id) {
            this.type=type;
            this.owner=owner;
            this.lock_name=lock_name;
            this.lock_id=lock_id;
        }
    }


    public static class LockingHeader extends Header {

        public LockingHeader() {
        }
        public short getMagicId() {return 72;}
        public Supplier<? extends Header> create() {
            return LockingHeader::new;
        }

        public int serializedSize() {
            return 0;
        }

        public void writeTo(DataOutput out) throws Exception {
        }

        public void readFrom(DataInput in) throws Exception {
        }
    }

}
