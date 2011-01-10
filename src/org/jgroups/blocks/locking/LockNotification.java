package org.jgroups.blocks.locking;

import org.jgroups.Address;

/**
 * @author Bela Ban
 */
public interface LockNotification {
    void lockCreated(String name);
    void lockDeleted(String name);
    void locked(String lock_name, Address owner);
    void unlocked(String lock_name, Address owner);
}
