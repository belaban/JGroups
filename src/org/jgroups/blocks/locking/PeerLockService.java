package org.jgroups.blocks.locking;

import org.jgroups.*;

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

    protected void sendReleaseLockRequest(String lock_name) {
        sendRequest(Type.RELEASE_LOCK, lock_name, null, 0);
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

}
