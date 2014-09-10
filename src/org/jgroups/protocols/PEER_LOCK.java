package org.jgroups.protocols;

/**
 * @author Bela Ban
 */

import org.jgroups.Address;
import org.jgroups.View;
import org.jgroups.util.Owner;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Implementation of a locking protocol which acquires locks by contacting <em>all</em> of the nodes of a cluster.</p>
 * Unless a total order configuration is used (e.g. {@link org.jgroups.protocols.SEQUENCER} based), lock requests for
 * the same resource from different senders may be received in different order, so deadlocks can occur. Example:
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
 * An alternative is also the {@link org.jgroups.protocols.CENTRAL_LOCK} protocol.
 * @author Bela Ban
 * @since 2.12
 * @see Locking
 * @see CENTRAL_LOCK
 * @deprecated Use {@link org.jgroups.protocols.CENTRAL_LOCK} instead
 */
public class PEER_LOCK extends Locking {

    public PEER_LOCK() {
        super();
    }


    protected void sendGrantLockRequest(String lock_name, int lock_id, Owner owner, long timeout, boolean is_trylock) {
        sendRequest(null, Type.GRANT_LOCK, lock_name, lock_id, owner, timeout, is_trylock);
    }

    protected void sendReleaseLockRequest(String lock_name, Owner owner) {
        sendRequest(null, Type.RELEASE_LOCK, lock_name, owner, 0, false);
    }


    @Override
    protected void sendAwaitConditionRequest(String lock_name, Owner owner) {
        sendRequest(null, Type.LOCK_AWAIT, lock_name, owner, 0, false);
    }


    @Override
    protected void sendSignalConditionRequest(String lock_name, boolean all) {
        sendRequest(null, all ? Type.COND_SIG_ALL : Type.COND_SIG, lock_name, 
                null, 0, false);
    }


    @Override
    protected void sendDeleteAwaitConditionRequest(String lock_name, Owner owner) {
        sendRequest(null, Type.DELETE_LOCK_AWAIT, lock_name, owner, 0, false);
    }


    public void handleView(View view) {
        super.handleView(view);
        List<Address> members=view.getMembers();
        for(Map<Owner,ClientLock> map: client_lock_table.values()) {
            for(ClientLock lock: map.values())
                ((PeerLock)lock).retainAll(members);
        }
    }

    protected ClientLock createLock(String lock_name) {
        return new PeerLock(lock_name);
    }

    /**
     * Lock implementation which grants a lock when all non faulty cluster members OK it.
     */
    protected class PeerLock extends ClientLock {
        protected final List<Address> grants=new ArrayList<Address>(view.getMembers());

        public PeerLock(String name) {
            super(name);
        }

        protected synchronized void retainAll(List<Address> members) {
            if(grants.isEmpty())
                return;
            grants.retainAll(members);
            if(grants.isEmpty())
                lockGranted(0);
        }

        protected synchronized void handleLockGrantedResponse(Owner owner, Address sender) {
            if(grants.isEmpty())
                return;
            grants.remove(sender);
            if(grants.isEmpty())
                lockGranted(0);
        }
    }
}
