package org.jgroups.blocks.locking;

import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.View;
import org.jgroups.annotations.Experimental;
import org.jgroups.util.Util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation of a lock service which acquires locks by contacting the coordinator.</p> Because the central
 * coordinator maintains all locks, no total order configuration is required.
 * An alternative is also the {@link PeerLockService}.
 * @author Bela Ban
 */
@Experimental
public class CentralLockService extends AbstractLockService implements LockNotification {
    protected Address coord;
    protected boolean is_coord;

    /** Number of backups to the coordinator. Server locks get replicated to these nodes as well */
    protected int num_backups=1;

    protected final List<Address> backups=new ArrayList<Address>();


    public CentralLockService() {
        super();
        addLockListener(this);
    }

    public CentralLockService(JChannel ch) {
        super(ch);
        addLockListener(this);
    }

    public Address getCoord() {
        return coord;
    }

    public boolean isCoord() {
        return is_coord;
    }

    public int getNumberOfBackups() {
        return num_backups;
    }

    public void setNumberOfBackups(int num_backups) {
        this.num_backups=num_backups;
    }

    protected void sendGrantLockRequest(String lock_name, Owner owner, long timeout, boolean is_trylock) {
        if(coord != null)
            sendRequest(coord, Type.GRANT_LOCK, lock_name, owner, timeout, is_trylock);
    }

    protected void sendReleaseLockRequest(String lock_name, Owner owner) {
        if(coord != null)
            sendRequest(coord, Type.RELEASE_LOCK, lock_name, owner, 0, false);
    }

    protected void sendCreateLockRequest(Address dest, String lock_name, Owner owner) {
        sendRequest(dest, AbstractLockService.Type.CREATE_LOCK, lock_name, owner, 0, false);
    }

    protected void sendDeleteLockRequest(Address dest, String lock_name) {
        sendRequest(dest, AbstractLockService.Type.DELETE_LOCK, lock_name, null, 0, false);
    }


    public void viewAccepted(View view) {
        super.viewAccepted(view);
        if(view.size() > 0) {
            coord=view.getMembers().firstElement();
            is_coord=coord.equals(ch.getAddress());
            if(log.isDebugEnabled())
                log.debug("local_addr=" + ch.getAddress() + ", coord=" + coord + ", is_coord=" + is_coord);
        }

        if(num_backups > 0) {
            List<Address> new_joiners=null;
            synchronized(backups) {
                List<Address> tmp=Util.pickNext(view.getMembers(), ch.getAddress(), num_backups);
                if(!tmp.isEmpty() && !tmp.equals(backups))
                    new_joiners=determineNewJoiners();
                backups.clear();
                backups.addAll(tmp);
            }
            if(new_joiners != null && !new_joiners.isEmpty())
                copyLocksTo(new_joiners);
        }
    }

    public void lockCreated(String name) {
    }

    public void lockDeleted(String name) {
    }

    public void locked(String lock_name, Owner owner) {
        updateBackups(Type.CREATE_LOCK, lock_name, owner);
    }

    public void unlocked(String lock_name, Owner owner) {
        updateBackups(Type.DELETE_LOCK, lock_name, owner);
    }

    protected void updateBackups(Type type, String lock_name, Owner owner) {
        synchronized(backups) {
            for(Address backup: backups)
                sendRequest(backup, type, lock_name, owner, 0, false);
        }
    }



    protected List<Address> determineNewJoiners() {
        return null;
    }

    protected void copyLocksTo(List<Address> new_joiners) {
        Map<String,ServerLock> copy;

        synchronized(server_locks) {
            copy=new HashMap<String,ServerLock>(server_locks);
        }

        for(Map.Entry<String,ServerLock> entry: server_locks.entrySet()) {
            for(Address joiner: new_joiners)
                sendCreateLockRequest(joiner, entry.getKey(), entry.getValue().current_owner);
        }
    }


}
