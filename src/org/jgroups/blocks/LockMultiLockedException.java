package org.jgroups.blocks;


/**
 * Thrown by the {@link org.jgroups.blocks.DistributedLockManager#unlock(Object, Object, boolean)} method if a lock is only locally released, because it is locked
 * by multiple DistributedLockManagers. This can happen after a merge for example.
 * 
 * @author Robert Schaffar-Taurok (robert@fusion.at)
 * @version $Id: LockMultiLockedException.java,v 1.2 2005/07/17 11:36:40 chrislott Exp $
 */
public class LockMultiLockedException extends Exception {

    public LockMultiLockedException() {
        super();
    }

    public LockMultiLockedException(String s) {
        super(s);
    }

}
