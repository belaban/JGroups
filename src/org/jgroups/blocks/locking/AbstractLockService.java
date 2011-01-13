package org.jgroups.blocks.locking;

import org.jgroups.*;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
    protected final ConcurrentMap<String,LockQueue> server_locks=Util.createConcurrentMap(20);

    // client side locks
    protected final Map<String,LockImpl> client_locks=Util.createHashMap();

    protected final List<LockNotification> lock_listeners=new ArrayList<LockNotification>();

    protected static enum Type {GRANT_LOCK, LOCK_GRANTED, RELEASE_LOCK}



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

    public void addLockListener(LockNotification listener) {
        if(listener != null)
            lock_listeners.add(listener);
    }

    public void removeLockListener(LockNotification listener) {
        if(listener != null)
            lock_listeners.remove(listener);
    }

    public Lock getLock(String name) {
        synchronized(client_locks) {
            LockImpl lock=client_locks.get(name);
            if(lock == null) {
                lock=createLock(name);
                client_locks.put(name, lock);
            }
            return lock;
        }
    }


    public void receive(Message msg) {
        Request req=(Request)msg.getObject();
        switch(req.type) {
            case GRANT_LOCK:
            case RELEASE_LOCK:
                handleLockRequest(req);
                break;
            case LOCK_GRANTED:
                handleLockGrantedResponse(req.lock_name, msg.getSrc());
                break;
            default:
                log.error("Request of type " + req.type + " not known");
                break;
        }
    }


    public void viewAccepted(View view) {
        this.view=view;
        List<Address> members=view.getMembers();
        for(Map.Entry<String,LockQueue> entry: server_locks.entrySet()) {
            entry.getValue().handleView(members);
        }
    }

    public String printLocks() {
        StringBuilder sb=new StringBuilder();
        sb.append("server locks:\n");
        for(Map.Entry<String,LockQueue> entry: server_locks.entrySet()) {
            sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
        }

        sb.append("\nclient locks:\n");
        for(Map.Entry<String,LockImpl> entry: client_locks.entrySet()) {
            sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
        }
        return sb.toString();
    }

    public String toString() {
        return printLocks();
    }

    protected LockImpl createLock(String lock_name) {
        return new LockImpl(lock_name);
    }

    abstract protected void sendGrantLockRequest(String lock_name, Address owner, long timeout);
    abstract protected void sendReleaseLockRequest(String lock_name, Address owner);


    protected void sendLockResponse(Type type, Address dest, String lock_name) {
        Request rsp=new Request(type, lock_name, dest, 0);
        Message lock_granted_rsp=new Message(dest, null, rsp);
        try {
            ch.send(lock_granted_rsp);
        }
        catch(Exception ex) {
            log.error("failed sending " + type + " message to " + dest + ": " + ex);
        }
    }


    protected void handleLockRequest(Request req) {
        LockQueue queue=server_locks.get(req.lock_name);
        if(queue == null) {
            queue=new LockQueue(req.lock_name);
            LockQueue tmp=server_locks.putIfAbsent(req.lock_name, queue);
            if(tmp != null)
                queue=tmp;
            else
                notifyLockCreated(req.lock_name);
        }
        queue.handleRequest(req);
    }

    protected void handleLockGrantedResponse(String lock_name, Address sender) {
        LockImpl lock;
        synchronized(client_locks) {
            lock=client_locks.get(lock_name);
        }
        if(lock != null)
            lock.lockGranted();
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

    protected void notifyLocked(String lock_name, Address owner) {
        for(LockNotification listener: lock_listeners) {
            try {
                listener.locked(lock_name, owner);
            }
            catch(Throwable t) {
                log.error("failed notifying " + listener, t);
            }
        }
    }

    protected void notifyUnlocked(String lock_name, Address owner) {
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
    protected class LockQueue {
        protected final String lock_name;
        protected Address current_owner;
        protected final List<Request> queue=new ArrayList<Request>();

        public LockQueue(String lock_name) {
            this.lock_name=lock_name;
        }

        protected synchronized void handleRequest(Request req) {
            switch(req.type) {
                case GRANT_LOCK:
                    if(current_owner == null) {
                        current_owner=req.owner;
                        sendLockResponse(AbstractLockService.Type.LOCK_GRANTED, req.owner, req.lock_name);
                        notifyLocked(req.lock_name, req.owner);
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
            if(!members.contains(current_owner)) {
                notifyUnlocked(lock_name, current_owner);
                current_owner=null;
            }

            for(Iterator<Request> it=queue.iterator(); it.hasNext();) {
                Request req=it.next();
                if(!members.contains(req.owner))
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
                        notifyLocked(req.lock_name, req.owner);
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

        public String toString() {
            StringBuilder sb=new StringBuilder();
            sb.append("owner=" + current_owner);
            if(!queue.isEmpty()) {
                sb.append(", queue: ");
                for(Request req: queue) {
                    sb.append(req.toStringShort()).append(" ");
                }
            }
            return sb.toString();
        }
    }


    protected static class Request implements Streamable {
        protected Type    type;
        protected String  lock_name;
        protected Address owner;
        protected long    timeout=0;


        public Request() {
        }

        public Request(Type type, String lock_name, Address owner, long timeout) {
            this.type=type;
            this.lock_name=lock_name;
            this.owner=owner;
            this.timeout=timeout;
        }

        public void writeTo(DataOutputStream out) throws IOException {
            out.writeByte(type.ordinal());
            Util.writeString(lock_name, out);
            Util.writeAddress(owner, out);
            out.writeLong(timeout);
        }

        public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
            type=Type.values()[in.readByte()];
            lock_name=Util.readString(in);
            owner=Util.readAddress(in);
            timeout=in.readLong();
        }

        public String toString() {
            return type.name() + lock_name + ", owner=" + owner + (timeout > 0? " (timeout=" + timeout + ")" : "");
        }

        public String toStringShort() {
            StringBuilder sb=new StringBuilder();
            switch(type) {
                case RELEASE_LOCK:
                    sb.append("U");
                    break;
                case GRANT_LOCK:
                    sb.append("L");
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

    protected class LockImpl implements Lock {
        protected final String    name;
        protected short           count=0; // incremented on lock(), decremented on unlock()

        public LockImpl(String name) {
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
                return tryLock(100, TimeUnit.MILLISECONDS);
            }
            catch(InterruptedException e) {
                return false;
            }
        }

        public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
            return false;
        }

        public synchronized void unlock() {
            boolean send_release_msg=count == 1;
            count=(short)Math.max(0, count-1);
            if(send_release_msg)
                sendReleaseLockRequest(name, ch.getAddress());
            if(count == 0) {
                synchronized(client_locks) {
                    client_locks.remove(name);
                }
            }
        }

        public Condition newCondition() {
            throw new UnsupportedOperationException("currently not implemented");
        }

        public String toString() {
            return name + " (count=" + count + ")";
        }

        protected synchronized void lockGranted() {
            if(count == 0)
                this.notifyAll();
        }

        protected synchronized void acquire() throws InterruptedException {
            if(count == 0) {
                sendGrantLockRequest(name, ch.getAddress(), 0);
                this.wait();
            }
            count++;
        }

    }

}
