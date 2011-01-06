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
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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
    protected final ConcurrentMap<String,LockImpl> client_locks=Util.createConcurrentMap(20);

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

    public Lock getLock(String name) {
        return new LockImpl(name);
    }


    public void receive(Message msg) {
        Request req=(Request)msg.getObject();
        switch(req.type) {
            case GRANT_LOCK:
            case RELEASE_LOCK:
                handleLockRequest(req);
                break;
            case LOCK_GRANTED:
                handleLockGrantedResponse(req.lock_name, req.owner);
                break;
            default:
                log.error("Request of type " + req.type + " not known");
                break;
        }
    }


    public void viewAccepted(View view) {
        this.view=view;
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
            queue=new LockQueue();
            LockQueue tmp=server_locks.putIfAbsent(req.lock_name, queue);
            if(tmp != null)
                queue=tmp;
        }
        queue.handleRequest(req);
    }

    abstract protected void handleLockGrantedResponse(String lock_name, Address owner);






    /**
     * Server side queue for handling of lock requests (lock, release).
     * @author Bela Ban
     */
    protected class LockQueue {
        protected Address current_owner;
        protected final List<Request> queue=new ArrayList<Request>();

        public void handleRequest(Request req) {

        }
    }


    protected class Request implements Streamable {
        protected Type    type;
        protected String  lock_name;
        protected Address owner;
        protected long    timeout=0;

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
    }

    protected class LockImpl implements Lock {
        protected final String    name;
        protected final Lock      lock=new ReentrantLock();
        protected final Condition granted=lock.newCondition();
        protected short           count=0; // incremented on lock(), decremented on unlock()

        public LockImpl(String name) {
            this.name=name;
        }

        public void lock() {
        }

        public void lockInterruptibly() throws InterruptedException {
        }

        public boolean tryLock() {
            return false;
        }

        public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
            return false;
        }

        public void unlock() {
        }

        public Condition newCondition() {
            throw new UnsupportedOperationException("currently not implemented");
        }

        public String toString() {
            return name;
        }

        protected void acquire() {

        }

        protected void release() {
            
        }
    }

}
