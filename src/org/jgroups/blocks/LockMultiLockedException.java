package org.jgroups.blocks;


/**
 * Thrown by DistributedLockManager.unlock(true) method if a lock is only localy released, because it is locked
 * by multiple DistributedLockManager's. This can happen after a merge for example.
 * 
 * @author Robert Schaffar-Taurok (robert@fusion.at)
 * @version $Id: LockMultiLockedException.java,v 1.1 2005/06/08 15:56:54 publicnmi Exp $
 */
public class LockMultiLockedException extends Exception {

    public LockMultiLockedException() {
        super();
    }

    public LockMultiLockedException(String s) {
        super(s);
    }

}
