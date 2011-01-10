package org.jgroups.blocks.locking;

import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.View;

import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of a lock service which acquires locks by contacting all (or the majority) of the nodes of a cluster.
 * @author Bela Ban
 */
public class PeerLockService extends AbstractLockService {

    public PeerLockService() {
        super();
    }

    public PeerLockService(JChannel ch) {
        super(ch);
    }

    protected void sendGrantLockRequest(String lock_name, Address owner, long timeout) {
        sendRequest(Type.GRANT_LOCK, lock_name, owner, timeout);
    }

    protected void sendReleaseLockRequest(String lock_name, Address owner) {
        sendRequest(Type.RELEASE_LOCK, lock_name, owner, 0);
    }

    protected void sendRequest(Type type, String lock_name, Address owner, long timeout) {
        Request req=new Request(type, lock_name, owner, timeout);
        Message msg=new Message(null, null, req);
        try {
            ch.send(msg);
        }
        catch(Exception ex) {
            log.error("failed sending " + type + " request: " + ex);
        }
    }

    protected void handleLockGrantedResponse(String lock_name, Address sender) {
        PeerLock lock;
        synchronized(client_locks) {
            lock=(PeerLock)client_locks.get(lock_name);
        }
        if(lock != null) {
            System.out.println("<< lock-grant(" + lock_name + ") by " + sender);
            lock.handleLockGrantedResponse(sender);
        }
    }

    public void viewAccepted(View view) {
        super.viewAccepted(view);
        List<Address> members=view.getMembers();
        for(LockImpl lock: client_locks.values())
            ((PeerLock)lock).retainAll(members);
    }

    protected LockImpl createLock(String lock_name) {
        return new PeerLock(lock_name);
    }

    /**
     * Lock implementation which grants a lock when all cluster members OK it.
     */
    protected class PeerLock extends LockImpl {
        protected final List<Address> grants=new ArrayList<Address>(ch.getView().getMembers());

        public PeerLock(String name) {
            super(name);
        }

        protected synchronized void retainAll(List<Address> members) {
            if(grants.isEmpty())
                return;
            grants.retainAll(members);
            if(grants.isEmpty())
                lockGranted();
        }

        protected synchronized void handleLockGrantedResponse(Address sender) {
            if(grants.isEmpty())
                return;
            grants.remove(sender);
            if(grants.isEmpty())
                lockGranted();
        }
    }

}
