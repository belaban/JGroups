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
import org.jgroups.util.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
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
import java.util.stream.Collectors;


/**
 * Base locking protocol, handling most of the protocol communication with other instances. To use distributed locking,
 * {@link org.jgroups.blocks.locking.LockService} is placed on a channel. LockService talks to a subclass of Locking
 * via events.
 * @author Bela Ban
 * @since 2.12
 * @see CENTRAL_LOCK
 * @see CENTRAL_LOCK2
 */
@MBean(description="Based class for locking functionality")
abstract public class Locking extends Protocol {

    @Property(description="bypasses message bundling if set")
    protected boolean                                bypass_bundling=true;

    @Property(description="Number of locks to be used for lock striping (for synchronized access to the server_lock entries)")
    protected int                                    lock_striping_size=10;


    protected Address                                local_addr;

    protected View                                   view;

    // server side locks
    protected final ConcurrentMap<String,ServerLock> server_locks=Util.createConcurrentMap(20);

    // protected access to the same locks in server_locks
    protected Lock[]                                 lock_stripes;

    // client side locks
    protected final ClientLockTable                  client_lock_table=new ClientLockTable();

    protected final Set<LockNotification>            lock_listeners=new CopyOnWriteArraySet<>();

    protected final static AtomicInteger             current_lock_id=new AtomicInteger(1);
    


    public enum Type {
        GRANT_LOCK,        // request to acquire a lock
        LOCK_GRANTED,      // response to sender of GRANT_LOCK on succcessful lock acquisition
        LOCK_DENIED,       // response to sender of GRANT_LOCK on unsuccessful lock acquisition (e.g. on tryLock())
        RELEASE_LOCK,      // request to release a lock
        RELEASE_LOCK_OK,   // response to RELEASE_LOCK request
        CREATE_LOCK,       // request to create a server lock (sent by coordinator to backups). Used by LockService
        DELETE_LOCK,       // request to delete a server lock (sent by coordinator to backups). Used by LockService

        LOCK_AWAIT,        // request to await until condition is signaled
        COND_SIG,          // request to signal awaiting thread
        COND_SIG_ALL,      // request to signal all awaiting threads
        SIG_RET,           // response to alert of signal
        DELETE_LOCK_AWAIT, // request to delete a waiter
        CREATE_AWAITER,    // request to create a server lock await (sent by coordinator to backups). Used by LockService
        DELETE_AWAITER,    // request to delete a server lock await (sent by coordinator to backups). Used by LockService

        LOCK_INFO_REQ,     // request to get information about all acquired locks and all pending lock/unlock requests
        LOCK_INFO_RSP,     // response to LOCK_INFO_REQ
        LOCK_REVOKED       // sent on reconciliation when a lock is already present (possible on a merge when both sides hold the same lock)
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

    @ManagedAttribute(description="Number of server locks (only on coord)")
    public int getNumServerLocks() {return server_locks.size();}

    @ManagedAttribute(description="Number of client locks")
    public int getNumClientLocks() {return client_lock_table.numLocks();}

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

            case Event.UNLOCK_FORCE:
                unlockForce(evt.arg());
                break;


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

        Request req=null;
        try {
            req=Util.streamableFromBuffer(Request::new, msg.getRawBuffer(), msg.getOffset(), msg.getLength())
              .sender(msg.src());
        }
        catch(Exception ex) {
            log.error("%s: failed deserializing request", local_addr, ex);
            return null;
        }

        if(req.type != Type.LOCK_INFO_REQ && req.type != Type.LOCK_INFO_RSP && req.type != Type.LOCK_REVOKED
          && null != view && !view.containsMember(msg.getSrc())) {
            log.error("%s: received request from '%s' but member is not present in the current view - ignoring request",
                      local_addr, msg.src());
            return null;
        }
        requestReceived(req);
        return null;
    }

    protected void requestReceived(Request req) {
        if(log.isTraceEnabled())
            log.trace("%s <-- %s: %s", local_addr, req.sender, req);
        handleRequest(req);
    }

    protected void handleRequest(Request req) {
        if(req == null) return;
        switch(req.type) {
            case GRANT_LOCK:
            case RELEASE_LOCK:
                handleLockRequest(req);
                break;
            case LOCK_GRANTED:
                handleLockGrantedResponse(req.lock_name, req.lock_id, req.owner);
                break;
            case RELEASE_LOCK_OK:
                handleLockReleasedResponse(req.lock_name, req.lock_id, req.owner);
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
            case LOCK_INFO_REQ:
                handleLockInfoRequest(req.sender);
                break;
            case LOCK_INFO_RSP:
                handleLockInfoResponse(req.sender, req);
                break;
            case LOCK_REVOKED:
                handleLockRevoked(req);
                break;
            default:
                log.error("%s: request of type %s not known", local_addr, req.type);
                break;
        }
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

    @ManagedOperation(description="Forcefully removes the client lock")
    public void unlockForce(String lock_name) {
        client_lock_table.unlockForce(lock_name);
    }


    @ManagedOperation(description="Dumps all locks")
    public String printLocks() {
        StringBuilder sb=new StringBuilder();
        Collection<ServerLock> values=server_locks.values();
        if(values != null && !values.isEmpty()) {
            sb.append("server locks: ");
            for(ServerLock sl : server_locks.values())
                sb.append(sl).append("\n");
        }

        String client_locks=client_lock_table.printLocks();
        if(client_locks != null && !client_locks.isEmpty())
            sb.append("my locks: ").append(client_lock_table.printLocks());
        return sb.toString();
    }


    @ManagedOperation(description="Dumps all server locks")
    public Object printServerLocks() {
        return server_locks.values().stream().map(ServerLock::toString).collect(Collectors.joining(", "));
    }

    protected void handleView(View view) {
        this.view=view;
        log.debug("%s: view=%s", local_addr, view);
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
                if(server_lock.isEmpty() && server_lock.owner == null && server_lock.condition.queue.isEmpty())
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
    abstract protected void sendReleaseLockRequest(String lock_name, int lock_id, Owner owner);
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
        log.trace("%s --> %s: %s", local_addr, dest == null? "ALL" : dest, req);
        try {
            down_prot.down(msg);
        }
        catch(Exception ex) {
            log.error("%s: failed sending %s request: %s", local_addr, req.type, ex);
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
            if(server_lock.isEmpty() && server_lock.owner == null && server_lock.condition.queue.isEmpty())
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

    protected void handleLockReleasedResponse(String lock_name, int lock_id, Owner owner) {
        ClientLock lock=client_lock_table.getLock(lock_name,owner,false);
        if(lock != null)
            lock.handleLockReleasedResponse(lock_id);
    }

    protected void handleLockDeniedResponse(String lock_name, int lock_id, Owner owner) {
         ClientLock lock=client_lock_table.getLock(lock_name,owner,false);
         if(lock != null)
             lock.lockDenied(lock_id);
    }

    protected void handleLockInfoRequest(Address requester) {

    }

    protected void handleLockInfoResponse(Address sender, Request rsp) {

    }

    protected void handleLockRevoked(Request rsp) {

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
                server_lock.owner= null;
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
                if (server_lock.condition.queue.isEmpty() && server_lock.owner == null) {
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
                log.error("%s: failed notifying %s: %s", local_addr, listener, t.toString());
            }
        }
    }

    protected void notifyLockDeleted(String lock_name) {
        for(LockNotification listener: lock_listeners) {
            try {
                listener.lockDeleted(lock_name);
            }
            catch(Throwable t) {
                log.error("%s: failed notifying %s: %s", local_addr, listener, t.toString());
            }
        }
    }

    protected void notifyLockRevoked(String lock_name, Owner current_owner) {
        for(LockNotification listener: lock_listeners) {
            try {
                listener.lockRevoked(lock_name, current_owner);
            }
            catch(Throwable t) {
                log.error("%s: failed notifying %s: %s", local_addr, listener, t.toString());
            }
        }
    }

    protected void notifyLocked(String lock_name, Owner owner) {
        for(LockNotification listener: lock_listeners) {
            try {
                listener.locked(lock_name,owner);
            }
            catch(Throwable t) {
                log.error("%s: failed notifying %s: %s", local_addr, listener, t.toString());
            }
        }
    }

    protected void notifyUnlocked(String lock_name, Owner owner) {
        for(LockNotification listener: lock_listeners) {
            try {
                listener.unlocked(lock_name,owner);
            }
            catch(Throwable t) {
                log.error("%s: failed notifying %s: %s", local_addr, listener, t.toString());
            }
        }
    }

    protected void notifyAwaiting(String lock_name, Owner owner) {
        for(LockNotification listener: lock_listeners) {
            try {
                listener.awaiting(lock_name,owner);
            }
            catch(Throwable t) {
                log.error("%s: failed notifying %s: %s", local_addr, listener, t.toString());
            }
        }
    }

    protected void notifyAwaited(String lock_name, Owner owner) {
        for(LockNotification listener: lock_listeners) {
            try {
                listener.awaited(lock_name,owner);
            }
            catch(Throwable t) {
                log.error("%s: failed notifying %s: %s", local_addr, listener, t.toString());
            }
        }
    }



    /**
     * Server side queue for handling of lock requests (lock, release).
     * @author Bela Ban
     */
    protected class ServerLock {
        protected final String          lock_name;
        protected Owner                 owner;
        protected final List<Request>   queue=new ArrayList<>();
        protected final ServerCondition condition;

        public ServerLock(String lock_name) {
            this.lock_name=lock_name;
            this.condition=new ServerCondition(this);
        }

        protected ServerLock(String lock_name, Owner owner) {
            this.lock_name=lock_name;
            this.owner=owner;
            this.condition=new ServerCondition(this);
        }

        protected Response handleRequest(Request req) {
            switch(req.type) {
                case GRANT_LOCK:
                    if(owner == null) {
                        setOwner(req.owner);
                        return new Response(Type.LOCK_GRANTED, req.owner, req.lock_name, req.lock_id);
                    }
                    if(owner.equals(req.owner))
                        return new Response(Type.LOCK_GRANTED, req.owner, req.lock_name, req.lock_id);

                    if(req.is_trylock && req.timeout <= 0)
                        return new Response(Type.LOCK_DENIED, req.owner, req.lock_name, req.lock_id);
                    addToQueue(req);
                    break;
                case RELEASE_LOCK:
                case LOCK_AWAIT:
                    if(Objects.equals(owner, req.owner)) {
                        setOwner(null);
                        if(req.type == Type.RELEASE_LOCK)
                            sendLockResponse(Type.RELEASE_LOCK_OK, req.owner, req.lock_name, req.lock_id);
                    }
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
            if(owner != null && !members.contains(owner.getAddress())) {
                Owner tmp=owner;
                setOwner(null);
                log.debug("%s: unlocked \"%s\" because owner %s left", local_addr, lock_name, tmp);
            }

            synchronized(queue) {
                queue.removeIf(req -> !members.contains(req.owner.getAddress()));
            }

            condition.queue.removeIf(own -> !members.contains(own.getAddress()));
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
                queue.removeIf(req -> req.type == type && req.owner.equals(owner));
            }
        }

        protected Request getNextRequest() {
            synchronized(queue) {
                return !queue.isEmpty()? queue.remove(0) : null;
            }
        }

        protected Response processQueue() {
            if(owner != null)
                return null;
            Request req;
            while((req=getNextRequest()) != null) {
                switch(req.type) {
                    case GRANT_LOCK:
                        setOwner(req.owner);
                        return new Response(Type.LOCK_GRANTED, req.owner, req.lock_name, req.lock_id);
                    case RELEASE_LOCK:
                        if(owner == null)
                            break;
                        if(owner.equals(req.owner))
                            setOwner(null);
                        return new Response(Type.RELEASE_LOCK_OK, req.owner, req.lock_name, req.lock_id);
                }
            }
            return null;
        }

        protected void setOwner(Owner owner) {
            if(owner == null) {
                if(this.owner != null) {
                    Owner tmp=this.owner;
                    this.owner=null;
                    notifyUnlocked(lock_name, tmp);
                }
            }
            else {
                this.owner=owner;
                notifyLocked(lock_name, owner);
            }
        }

        public boolean isEmpty() {
            synchronized(queue) {
                return queue.isEmpty();
            }
        }

        public String toString() {
            StringBuilder sb=new StringBuilder(lock_name + ": ").append(owner);
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
            log.trace("%s: waiter %s was added for %s", local_addr, waiter, lock.lock_name);
            queue.add(waiter);
        }
        
        public void removeWaiter(Owner waiter) {
            notifyAwaited(lock.lock_name, waiter);
            log.trace("%s: waiter %s was removed for %s", local_addr, waiter, lock.lock_name);
            queue.remove(waiter);
        }
        
        public void signal(boolean all) {
            if (queue.isEmpty())
                log.trace("%s: signal for %s ignored since, no one is waiting in queue", local_addr, lock.lock_name);

            Owner entry;
            if (all) {
                while ((entry = queue.poll()) != null) {
                    notifyAwaited(lock.lock_name, entry);
                    log.trace("%s: signalled %s for %s", local_addr, entry, lock.lock_name);
                    sendSignalResponse(entry, lock.lock_name);
                }
            }
            else {
                entry = queue.poll();
                if (entry != null) {
                    notifyAwaited(lock.lock_name, entry);
                    log.trace("%s: signalled %s for %s", local_addr, entry, lock.lock_name);
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
    protected class ClientLock implements Lock, Comparable<ClientLock> {
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

        public boolean isHeld() {return acquired && !denied;}

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
            return String.format("%s (id=%d, locked=%b, owner=%s)", name, lock_id, acquired, owner != null? owner : "n/a");
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

        protected void handleLockReleasedResponse(int lock_id) {
            if(this.lock_id != lock_id) {
                log.error(Util.getMessage("DiscardedLOCKGRANTEDResponseWithLockId") + lock_id + ", my lock-id=" + this.lock_id);
                return;
            }
            _unlockOK();
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
            if(!denied) {
                if(!force)
                    client_lock_table.addToPendingReleaseRequests(this);
                sendReleaseLockRequest(name, lock_id, owner); // lock will be released on RELEASE_LOCK_OK response
                if(force && client_lock_table.removeClientLock(name,owner))
                    notifyLockDeleted(name);

                if(!force) {
                    //unlock will return only when get RELEASE_LOCK_OK or timeLeft after some seconds
                    long time_left=10000;
                    while(acquired || denied) {
                        long start=System.currentTimeMillis();
                        try {
                            wait(time_left);
                        }
                        catch(InterruptedException ie) {
                            break;
                        }
                        long duration=System.currentTimeMillis() - start;
                        if(duration > 0)
                            time_left-=duration;
                        if(time_left <= 0) {
                            log.warn("%s: timeout waiting for RELEASE_LOCK_OK response for lock %s", local_addr, this);
                            break;
                        }
                    }
                }
            }
            else
                _unlockOK();
        }

    protected synchronized void _unlockOK() {
        acquired=denied=false;
            notifyAll();
            if(client_lock_table.removeClientLock(name,owner))
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

        public boolean equals(Object obj) {
            return this == obj || Objects.equals(owner, ((ClientLock)obj).owner);
        }

        public int compareTo(ClientLock o) {
            int rc=owner.compareTo(o.owner);
            return rc != 0? rc : name.compareTo(o.name);
        }
    }

    /** Manages access to client locks */
    protected class ClientLockTable {
        protected final ConcurrentMap<String,Map<Owner,ClientLock>> table=Util.createConcurrentMap(20);
        protected final Set<ClientLock>                             pending_release_reqs=new ConcurrentSkipListSet<>();


        protected int numLocks() {return table.size();}

        protected synchronized ClientLock getLock(String name, Owner owner, boolean create_if_absent) {
            Map<Owner,ClientLock> owners=table.get(name);
            if(owners == null) {
                if(!create_if_absent)
                    return null;
                owners=Util.createConcurrentMap(20);
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

        protected synchronized boolean removeClientLock(String lock_name, Owner owner) {
            pending_release_reqs.removeIf(cl -> Objects.equals(cl.name, lock_name) && Objects.equals(cl.owner, owner));
            Map<Owner,ClientLock> owners=table.get(lock_name);
            if(owners != null) {
                ClientLock lock=owners.remove(owner);
                if(lock != null && owners.isEmpty())
                    table.remove(lock_name);
                return lock != null;
            }
            return false;
        }

        protected void unlockAll() {
            List<ClientLock> lock_list=new ArrayList<>();
            synchronized(this) {
                table.values().forEach(map -> lock_list.addAll(map.values()));
            }
            lock_list.forEach(ClientLock::unlock);
        }

        protected void unlockForce(String lock_name) {
            Map<Owner,ClientLock> owners=table.get(lock_name);
            if(owners != null) {
                for(ClientLock cl : owners.values())
                    cl._unlock(true);
            }
            pending_release_reqs.removeIf(cl -> Objects.equals(cl.name, lock_name));
        }

        protected void resendPendingLockRequests() {
            final List<ClientLock> pending_lock_reqs=new ArrayList<>();
            synchronized(this) {
                if(!table.isEmpty()) {
                    table.values().forEach(map -> map.values().stream().filter(lock -> !lock.acquired && !lock.denied)
                      .forEach(pending_lock_reqs::add));
                }
            }
            if(!pending_lock_reqs.isEmpty()) { // send outside of the synchronized block
                if(log.isTraceEnabled()) {
                    String tmp=pending_lock_reqs.stream().map(ClientLock::toString).collect(Collectors.joining(", "));
                    log.trace("%s: resending pending lock requests: %s", local_addr, tmp);
                }
                pending_lock_reqs.forEach(l -> sendGrantLockRequest(l.name, l.lock_id, l.owner, l.timeout, l.is_trylock));
            }

            if(!pending_release_reqs.isEmpty()) {
                if(log.isTraceEnabled()) {
                    String tmp=pending_release_reqs.stream().map(ClientLock::toString).collect(Collectors.joining(", "));
                    log.trace("%s: resending pending unlock requests: %s", local_addr, tmp);
                }
                pending_release_reqs.forEach(cl -> sendReleaseLockRequest(cl.name, cl.lock_id, cl.owner));
            }
        }

        protected synchronized Collection<Map<Owner,ClientLock>> values() {
            return table.values();
        }

        /** Returns locks that have been successfully acquired */
        protected synchronized List<Tuple<String,Owner>> getLockInfo() {
            List<Tuple<String,Owner>> l=new ArrayList<>();
            // table.forEach((key, value) -> value.keySet().forEach(owner -> l.add(new Tuple<>(key, owner))));
            table.forEach((k,v) -> v.forEach((owner, cl) -> {
                if(cl.acquired && !cl.denied)
                    l.add(new Tuple<>(k, owner));
            }));
            return l;
        }

        protected synchronized List<Request> getPendingRequests(Address sender) {
            List<Request> list=new ArrayList<>();

            // add the pending LOCK requests
            table.forEach((k,v) -> v.forEach((owner, cl) -> {
                if(!cl.acquired  && !cl.denied) {
                    Request req=new Request(Type.GRANT_LOCK, cl.name, owner, cl.timeout, cl.is_trylock).lockId(cl.lock_id);
                    list.add(req);
                }
            }));

            // add the pending UNLOCK requests
            pending_release_reqs.forEach(cl -> {
                if(cl.acquired  && !cl.denied) {
                    Request req=new Request(Type.RELEASE_LOCK, cl.name, cl.owner, cl.timeout, cl.is_trylock)
                      .lockId(cl.lock_id).sender(sender);
                    list.add(req);
                }
            });

            return list;
        }


        public String printLocks() {
            return table.values().stream().map(Map::values).flatMap(Collection::stream)
              .filter(cl -> cl.isHeld() && Objects.nonNull(cl.name))
              .map(cl -> cl.name).collect(Collectors.joining(", "));
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

        public void addToPendingReleaseRequests(ClientLock cl) {
            if(cl != null)
                pending_release_reqs.add(cl);
        }


        public void removeFromPendingReleaseRequests(ClientLock cl) {
            if(cl != null)
                pending_release_reqs.remove(cl);
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


    public static class Request implements Streamable {
        protected Type             type;
        protected String           lock_name;
        protected int              lock_id;
        protected Owner            owner;
        protected long             timeout;
        protected boolean          is_trylock;
        protected LockInfoResponse info_rsp;
        protected Address          sender;


        public Request() {
        }

        public Request(Type type) {
            this.type=type;
        }

        public Request(Type type, String lock_name, Owner owner, long timeout) {
            this(type);
            this.lock_name=lock_name;
            this.owner=owner;
            this.timeout=timeout;
        }

        public Request(Type type, String lock_name, Owner owner, long timeout, boolean is_trylock) {
            this(type, lock_name, owner, timeout);
            this.is_trylock=is_trylock;
        }

        public Type    getType()                   {return type;}
        public Request lockId(int lock_id)         {this.lock_id=lock_id; return this;}
        public int     lockId()                    {return lock_id;}
        public Request infoRsp(LockInfoResponse r) {this.info_rsp=r; return this;}
        public Address sender()                    {return this.sender;}
        public Request sender(Address sender)      {this.sender=sender; return this;}

        @Override
        public void writeTo(DataOutput out) throws IOException {
            out.writeByte(type.ordinal());
            Bits.writeString(lock_name,out);
            out.writeInt(lock_id);
            Util.writeStreamable(owner, out);
            out.writeLong(timeout);
            out.writeBoolean(is_trylock);
            Util.writeStreamable(info_rsp, out);
            Util.writeAddress(sender, out);
        }

        @Override
        public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            type=Type.values()[in.readByte()];
            lock_name=Bits.readString(in);
            lock_id=in.readInt();
            owner=Util.readStreamable(Owner::new, in);
            timeout=in.readLong();
            is_trylock=in.readBoolean();
            info_rsp=Util.readStreamable(LockInfoResponse::new, in);
            sender=Util.readAddress(in);
        }

        public String toString() {
            StringBuilder sb=new StringBuilder(type.name() + "[");
            if(lock_name != null)
                sb.append(lock_name);
            if(lock_id > 0)
                sb.append(", lock_id=").append(lock_id);
            if(owner != null)
                sb.append(", owner=").append(owner);
            if(is_trylock)
                sb.append(", trylock");
            if(timeout > 0)
                sb.append(", timeout=").append(timeout);
            if(sender != null)
                sb.append(", sender=").append(sender);
            sb.append("]");
            return sb.toString();
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
            if(info_rsp != null)
                sb.append(", lock-info-response: ").append(info_rsp);
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

        @Override
        public int serializedSize() {
            return 0;
        }

        @Override
        public void writeTo(DataOutput out) throws IOException {
        }

        @Override
        public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        }
    }


    protected static class LockInfoResponse implements Streamable {
        protected List<Tuple<String,Owner>> existing_locks; // lock name and owner
        protected List<Request>             pending_requests;

        protected LockInfoResponse add(Tuple<String,Owner> el) {
            if(existing_locks == null)
                existing_locks=new ArrayList<>();
            existing_locks.add(el);
            return this;
        }

        public void writeTo(DataOutput out) throws IOException {
            if(existing_locks == null)
                out.writeInt(0);
            else {
                out.writeInt(existing_locks.size());
                for(Tuple<String,Owner> t: existing_locks) {
                    Bits.writeString(t.getVal1(), out);
                    t.getVal2().writeTo(out);
                }
            }
            if(pending_requests == null)
                out.writeInt(0);
            else {
                out.writeInt(pending_requests.size());
                for(Request req: pending_requests)
                    req.writeTo(out);
            }
        }

        public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            int size=in.readInt();
            if(size > 0) {
                existing_locks=new ArrayList<>(size);
                for(int i=0; i < size; i++) {
                    String lock_name=Bits.readString(in);
                    Owner owner=new Owner();
                    owner.readFrom(in);
                    existing_locks.add(new Tuple<>(lock_name, owner));
                }

            }
            size=in.readInt();
            if(size > 0) {
                pending_requests=new ArrayList<>();
                for(int i=0; i < size; i++) {
                    Request req=new Request();
                    req.readFrom(in);
                    pending_requests.add(req);
                }
            }
        }

        public String toString() {
            return String.format("%d locks and %d pending lock/unlock requests",
                                 existing_locks == null? 0 : existing_locks.size(),
                                 pending_requests == null? 0 : pending_requests.size());
        }

        public String printDetails() {
            StringBuilder sb=new StringBuilder(toString());
            if(existing_locks != null && !existing_locks.isEmpty())
                sb.append(String.format("\nlocks:\n%s", existing_locks.stream().map(Tuple::getVal1)
                  .collect(Collectors.joining(", "))));
            if(pending_requests != null && !pending_requests.isEmpty())
                sb.append(String.format("\npending requests:\n%s", pending_requests));
            return sb.toString();
        }
    }

}
