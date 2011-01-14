package org.jgroups.blocks.locking;

import org.jgroups.*;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * @author Bela Ban
 */
abstract public class AbstractLockService extends ReceiverAdapter implements LockService {
    protected JChannel ch;
    protected View view;
    protected final Log log=LogFactory.getLog(getClass());

    // server side locks
    protected final ConcurrentMap<String,ServerLock> server_locks=Util.createConcurrentMap(20);

    // client side locks
    protected final Map<String,Map<Owner,ClientLock>> client_locks=new HashMap<String,Map<Owner,ClientLock>>();

    protected final List<LockNotification> lock_listeners=new ArrayList<LockNotification>();

    /** Bypasses message bundling if set */
    protected boolean bypass_bundling=true;

    protected static enum Type {GRANT_LOCK, LOCK_GRANTED, LOCK_DENIED, RELEASE_LOCK}



    protected AbstractLockService() {
    }

    public AbstractLockService(JChannel ch) {
        this.ch=ch;
        ch.setReceiver(this);
        View tmp=ch.getView();
        if(tmp != null)
            view=tmp;
    }

    public void setChannel(JChannel ch) {
        this.ch=ch;
        if(this.ch != null) {
            this.ch.setReceiver(this);
            view=this.ch.getView();
        }
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

    public Lock getLock(String name) {
        return getLock(name, getOwner(), true);
    }

    public void unlockAll() {
        List<ClientLock> locks=new ArrayList<ClientLock>();
        synchronized(client_locks) {
            Collection<Map<Owner,ClientLock>> maps=client_locks.values();
            for(Map<Owner,ClientLock> map: maps) {
                locks.addAll(map.values());
            }
        }

        for(ClientLock lock: locks)
            lock.unlock();
    }


    public void receive(Message msg) {
        Request req=(Request)msg.getObject();
        if(log.isTraceEnabled())
            log.trace("[" + ch.getAddress() + "] <-- [" + msg.getSrc() + "] " + req);
        switch(req.type) {
            case GRANT_LOCK:
            case RELEASE_LOCK:
                handleLockRequest(req);
                break;
            case LOCK_GRANTED:
                handleLockGrantedResponse(req.lock_name, req.owner, msg.getSrc());
                break;
            case LOCK_DENIED:
                handleLockDeniedResponse(req.lock_name, req.owner, msg.getSrc());
                break;
            default:
                log.error("Request of type " + req.type + " not known");
                break;
        }
    }


    public void viewAccepted(View view) {
        this.view=view;
        if(log.isDebugEnabled())
            log.debug("view=" + view);
        List<Address> members=view.getMembers();
        for(Map.Entry<String,ServerLock> entry: server_locks.entrySet()) {
            entry.getValue().handleView(members);
        }
        for(Map.Entry<String,ServerLock> entry: server_locks.entrySet()) {
            ServerLock queue=entry.getValue();
            if(queue.isEmpty() && queue.current_owner == null)
                server_locks.remove(entry.getKey());
        }
    }

    public String printLocks() {
        StringBuilder sb=new StringBuilder();
        sb.append("server locks:\n");
        for(Map.Entry<String,ServerLock> entry: server_locks.entrySet()) {
            sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
        }

        sb.append("\nmy locks: ");
        synchronized(client_locks) {
            boolean first_element=true;
            for(Map.Entry<String,Map<Owner,ClientLock>> entry: client_locks.entrySet()) {
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
                }
                sb.append(")");
            }
        }
        return sb.toString();
    }

    

    protected ClientLock createLock(String lock_name) {
        return new ClientLock(lock_name);
    }

    protected Owner getOwner() {
        return new Owner(ch.getAddress(), Thread.currentThread().getId());
    }

    abstract protected void sendGrantLockRequest(String lock_name, Owner owner, long timeout, boolean is_trylock);
    abstract protected void sendReleaseLockRequest(String lock_name, Owner owner);


    protected void sendLockResponse(Type type, Owner dest, String lock_name) {
        Request rsp=new Request(type, lock_name, dest, 0);
        Message lock_granted_rsp=new Message(dest.address, null, rsp);
        if(bypass_bundling)
            lock_granted_rsp.setFlag(Message.DONT_BUNDLE);

        if(log.isTraceEnabled())
            log.trace("[" + ch.getAddress() + "] --> [" + dest.address + "] " + rsp);

        try {
            ch.send(lock_granted_rsp);
        }
        catch(Exception ex) {
            log.error("failed sending " + type + " message to " + dest + ": " + ex);
        }
    }


    protected void handleLockRequest(Request req) {
        ServerLock lock=server_locks.get(req.lock_name);
        if(lock == null) {
            lock=new ServerLock(req.lock_name);
            ServerLock tmp=server_locks.putIfAbsent(req.lock_name, lock);
            if(tmp != null)
                lock=tmp;
            else
                notifyLockCreated(req.lock_name);
        }
        lock.handleRequest(req);
        if(lock.isEmpty() && lock.current_owner == null)
            server_locks.remove(req.lock_name);
    }

    protected void handleLockGrantedResponse(String lock_name, Owner owner, Address sender) {
        ClientLock lock=getLock(lock_name, owner, false);
        if(lock != null)
            lock.lockGranted();
    }

     protected void handleLockDeniedResponse(String lock_name, Owner owner, Address sender) {
        throw new UnsupportedOperationException();
    }

    protected ClientLock getLock(String name, Owner owner, boolean create_if_absent) {
        synchronized(client_locks) {
            Map<Owner,ClientLock> owners=client_locks.get(name);
            if(owners == null) {
                if(!create_if_absent)
                    return null;
                owners=new HashMap<Owner,ClientLock>();
                client_locks.put(name, owners);
            }
            ClientLock lock=owners.get(owner);
            if(lock == null) {
                if(!create_if_absent)
                    return null;
                lock=createLock(name);
                owners.put(owner, lock);
            }
            return lock;
        }
    }
    
    protected void removeClientLock(String lock_name, Owner owner) {
        synchronized(client_locks) {
            Map<Owner,ClientLock> owners=client_locks.get(lock_name);
            if(owners != null) {
                ClientLock lock=owners.remove(owner);
                if(lock != null) {
                    if(owners.isEmpty())
                        client_locks.remove(lock_name);
                }
            }
        }
    }


    protected void notifyLockCreated(String lock_name) {
        for(LockNotification listener: lock_listeners) {
            try {
                listener.lockCreated(lock_name);
            }
            catch(Throwable t) {
                log.error("failed notifying " + listener, t);
            }
        }
    }

    protected void notifyLockDeleted(String lock_name) {
        for(LockNotification listener: lock_listeners) {
            try {
                listener.lockDeleted(lock_name);
            }
            catch(Throwable t) {
                log.error("failed notifying " + listener, t);
            }
        }
    }

    protected void notifyLocked(String lock_name, Owner owner) {
        for(LockNotification listener: lock_listeners) {
            try {
                listener.locked(lock_name, owner);
            }
            catch(Throwable t) {
                log.error("failed notifying " + listener, t);
            }
        }
    }

    protected void notifyUnlocked(String lock_name, Owner owner) {
        for(LockNotification listener: lock_listeners) {
            try {
                listener.unlocked(lock_name, owner);
            }
            catch(Throwable t) {
                log.error("failed notifying " + listener, t);
            }
        }
    }




    /**
     * Server side queue for handling of lock requests (lock, release).
     * @author Bela Ban
     */
    protected class ServerLock {
        protected final String lock_name;
        protected Owner current_owner;
        protected final List<Request> queue=new ArrayList<Request>();

        public ServerLock(String lock_name) {
            this.lock_name=lock_name;
        }

        protected synchronized void handleRequest(Request req) {
            switch(req.type) {
                case GRANT_LOCK:
                    if(current_owner == null) {
                        current_owner=req.owner;
                        sendLockResponse(AbstractLockService.Type.LOCK_GRANTED, req.owner, req.lock_name);
                    }
                    else {
                        if(!current_owner.equals(req.owner))
                            queue.add(req);
                    }
                    break;
                case RELEASE_LOCK:
                    if(current_owner == null)
                        break;
                    if(current_owner.equals(req.owner)) {
                        current_owner=null;
                        notifyUnlocked(req.lock_name, req.owner);
                    }
                    else
                        queue.add(req);
                    break;
                default:
                    throw new IllegalArgumentException("type " + req.type + " is invalid here");
            }

            processQueue();
        }

        protected synchronized void handleView(List<Address> members) {
            if(current_owner != null && !members.contains(current_owner.address)) {
                Owner tmp=current_owner;
                current_owner=null;
                notifyUnlocked(lock_name, tmp);
                if(log.isDebugEnabled())
                    log.debug("unlocked \"" + lock_name + "\" because owner " + tmp + " left");
            }

            for(Iterator<Request> it=queue.iterator(); it.hasNext();) {
                Request req=it.next();
                if(!members.contains(req.owner.address))
                    it.remove();
            }

            processQueue();
        }


        protected void processQueue() {
            if(current_owner == null) {
                while(!queue.isEmpty()) {
                    Request req=queue.remove(0);
                    if(req.type == AbstractLockService.Type.GRANT_LOCK) {
                        current_owner=req.owner;
                        sendLockResponse(AbstractLockService.Type.LOCK_GRANTED, req.owner, req.lock_name);
                        break;
                    }
                }
            }
            else {
                for(Iterator<Request> it=queue.iterator(); it.hasNext();) {
                    Request req=it.next();
                    if(current_owner.equals(req.owner)) {
                        if(req.type == AbstractLockService.Type.GRANT_LOCK)
                            it.remove(); // lock already granted
                        else if(req.type == AbstractLockService.Type.RELEASE_LOCK) {
                            notifyUnlocked(lock_name, current_owner);
                            current_owner=null;
                            processQueue();
                            return;
                        }
                    }
                }
            }
        }

        public boolean isEmpty() {return queue.isEmpty();}

        public String toString() {
            StringBuilder sb=new StringBuilder();
            sb.append(current_owner);
            if(!queue.isEmpty()) {
                sb.append(", queue: ");
                for(Request req: queue) {
                    sb.append(req.toStringShort()).append(" ");
                }
            }
            return sb.toString();
        }
    }




    protected class ClientLock implements Lock {
        protected final String      name;
        protected volatile boolean  acquired;

        public ClientLock(String name) {
            this.name=name;
        }

        public void lock() {
            try {
                acquire();
            }
            catch(InterruptedException e) {
            }
        }

        public void lockInterruptibly() throws InterruptedException {
            acquire();
        }

        public boolean tryLock() {
            try {
                return tryLock(0, TimeUnit.MILLISECONDS);
            }
            catch(InterruptedException e) {
                return false;
            }
        }

        public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
            return acquireTryLock(unit.convert(time, TimeUnit.MILLISECONDS));
        }

        public synchronized void unlock() {
            if(!acquired)
                return;
            sendReleaseLockRequest(name, getOwner());
            acquired=false;
            notifyAll();

            removeClientLock(name, getOwner());
            notifyLockDeleted(name);
        }

        public Condition newCondition() {
            throw new UnsupportedOperationException("currently not implemented");
        }

        public String toString() {
            return name + " (locked=" + acquired +")";
        }

        protected synchronized void lockGranted() {
            acquired=true;
            this.notifyAll();
        }

        protected synchronized void acquire() throws InterruptedException {
            if(!acquired) {
                sendGrantLockRequest(name, getOwner(), 0, false);
                while(!acquired)
                    this.wait();
            }
        }

        protected synchronized boolean acquireTryLock(long timeout) throws InterruptedException {
            if(!acquired) {
                sendGrantLockRequest(name, getOwner(), timeout, true);
                while(!acquired)
                    this.wait();  // todo: wait for timeout ms
            }
            return true;
        }
    }


    protected static class Request implements Streamable {
        protected Type    type;
        protected String  lock_name;
        protected Owner   owner;
        protected long    timeout=0;
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

        public void writeTo(DataOutputStream out) throws IOException {
            out.writeByte(type.ordinal());
            Util.writeString(lock_name, out);
            Util.writeStreamable(owner, out);
            out.writeLong(timeout);
            out.writeBoolean(is_trylock);
        }

        public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
            type=Type.values()[in.readByte()];
            lock_name=Util.readString(in);
            owner=(Owner)Util.readStreamable(Owner.class, in);
            timeout=in.readLong();
            is_trylock=in.readBoolean();
        }

        public String toString() {
            return type.name() + " [" + lock_name + ", owner=" + owner + (is_trylock? ", trylock " : "") +
              (timeout > 0? " (timeout=" + timeout + ")" : "" + "]");
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



}
