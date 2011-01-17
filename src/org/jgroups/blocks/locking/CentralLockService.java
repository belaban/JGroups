package org.jgroups.blocks.locking;

import org.jgroups.JChannel;
import org.jgroups.View;

/**
 * Implementation of a lock service which acquires locks by contacting the coordinator.</p> Because the central
 * coordinator maintains all locks, no total order configuration is required.
 * An alternative is also the {@link PeerLockService}.
 * @author Bela Ban
 */
public class CentralLockService extends AbstractLockService {

    public CentralLockService() {
        super();
    }

    public CentralLockService(JChannel ch) {
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

    }



}
