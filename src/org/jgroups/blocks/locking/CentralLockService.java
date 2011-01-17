package org.jgroups.blocks.locking;

import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.View;

/**
 * Implementation of a lock service which acquires locks by contacting the coordinator.</p> Because the central
 * coordinator maintains all locks, no total order configuration is required.
 * An alternative is also the {@link PeerLockService}.
 * @author Bela Ban
 */
public class CentralLockService extends AbstractLockService {
    protected Address coord;
    protected boolean is_coord;

    public CentralLockService() {
        super();
    }

    public CentralLockService(JChannel ch) {
        super(ch);
    }

    protected void sendGrantLockRequest(String lock_name, Owner owner, long timeout, boolean is_trylock) {
        if(coord != null)
            sendRequest(coord, Type.GRANT_LOCK, lock_name, owner, timeout, is_trylock);
    }

    protected void sendReleaseLockRequest(String lock_name, Owner owner) {
        if(coord != null)
            sendRequest(coord, Type.RELEASE_LOCK, lock_name, owner, 0, false);
    }


    public void viewAccepted(View view) {
        super.viewAccepted(view);
        if(view.size() > 0) {
            coord=view.getMembers().firstElement();
            is_coord=coord.equals(ch.getAddress());
            if(log.isDebugEnabled())
                log.debug("local_addr=" + ch.getAddress() + ", coord=" + coord + ", is_coord=" + is_coord);
        }
    }



}
