package org.jgroups.blocks.locking;

import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.Experimental;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Implementation of a lock service which acquires locks by contacting all of the nodes of a cluster.</p> Unless a total
 * order configuration is used (e.g. {@link org.jgroups.protocols.SEQUENCER} based), lock requests for the same resource
 * from different senders may be received in different order, so deadlocks can occur. Example:
 * <pre>
 * - Nodes A and B
 * - A and B call lock(X) at the same time
 * - A receives L(X,A) followed by L(X,B): locks X(A), queues L(X,B)
 * - B receives L(X,B) followed by L(X,A): locks X(B), queues L(X,A)
 * </pre>
 * To acquire a lock, we need lock grants from both A and B, but this will never happen here. To fix this, either
 * add SEQUENCER to the configuration, so that all lock requests are received in the same global order at both A and B,
 * or use {@link java.util.concurrent.locks.Lock#tryLock(long,java.util.concurrent.TimeUnit)} with retries if a lock
 * cannot be acquired.<p/>
 * An alternative is also the {@link CentralLockService}.
 * @author Bela Ban
 */
@Experimental
public class PeerLockService extends AbstractLockService {

    public PeerLockService() {
        super();
    }

    public PeerLockService(JChannel ch) {
        super(ch);
    }

    protected void sendGrantLockRequest(String lock_name, Owner owner, long timeout, boolean is_trylock) {
        sendRequest(null, Type.GRANT_LOCK, lock_name, owner, timeout, is_trylock);
    }

    protected void sendReleaseLockRequest(String lock_name, Owner owner) {
        sendRequest(null, Type.RELEASE_LOCK, lock_name, owner, 0, false);
    }


    public void viewAccepted(View view) {
        super.viewAccepted(view);
        List<Address> members=view.getMembers();
        synchronized(client_locks) {
            for(Map<Owner,ClientLock> map: client_locks.values()) {
                for(ClientLock lock: map.values())
                    ((PeerLock)lock).retainAll(members);
            }
        }
    }

    protected ClientLock createLock(String lock_name) {
        return new PeerLock(lock_name);
    }

    /**
     * Lock implementation which grants a lock when all non faulty cluster members OK it.
     */
    protected class PeerLock extends ClientLock {
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

        protected synchronized void handleLockGrantedResponse(Owner owner, Address sender) {
            if(grants.isEmpty())
                return;
            grants.remove(sender);
            if(grants.isEmpty()) {
                lockGranted();
                notifyLocked(this.name, owner);
            }
        }
    }

}
