package org.jgroups.blocks.locking;

import org.jgroups.util.Owner;

/**
 * @author Bela Ban
 */
public interface LockNotification {
    void lockCreated(String name);
    void lockDeleted(String name);
    void locked(String lock_name, Owner owner);
    void unlocked(String lock_name, Owner owner);
    void awaiting(String lock_name, Owner owner);
    void awaited(String lock_name, Owner owner);
}
