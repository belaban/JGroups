package org.jgroups.blocks.locking;

import org.jgroups.Address;
import org.jgroups.JChannel;

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
    }

    protected void sendReleaseLockRequest(String lock_name, Address owner) {
    }

    protected void handleLockGrantedResponse(String lock_name, Address owner) {
    }
}
