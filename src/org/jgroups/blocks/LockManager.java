package org.jgroups.blocks;

import org.jgroups.ChannelException;

/**
 * <code>LockManager</code> represents generic lock manager that allows
 * obtaining and releasing locks on objects.
 * 
 * @author Roman Rokytskyy (rrokytskyy@acm.org)
 */
public interface LockManager {
    
    /**
     * Obtain lock on <code>obj</code> for specified <code>owner</code>.
     * Implementation should try to obtain lock few times within the
     * specified timeout.
     *
     * @param obj obj to lock, usually not full object but object's ID.
     * @param owner object identifying entity that will own the lock.
     * @param timeout maximum time that we grant to obtain a lock.
     *
     * @throws LockNotGrantedException if lock is not granted within
     * specified period.
     *
     * @throws ClassCastException if <code>obj</code> and/or
     * <code>owner</code> is not of type that implementation expects to get
     * (for example, when distributed lock manager obtains non-serializable
     * <code>obj</code> or <code>owner</code>).
     * 
     * @throws ChannelException if something bad happened to communication
     * channel.
     */
    void lock(Object obj, Object owner, int timeout)
    throws LockNotGrantedException, ClassCastException, ChannelException;

    /**
     * Release lock on <code>obj</code> owned by specified <code>owner</code>.
     *
     * @param obj obj to lock, usually not full object but object's ID.
     * @param owner object identifying entity that will own the lock.
     *
     * @throws LockOwnerMismatchException if lock is owned by another object.
     *
     * @throws ClassCastException if <code>obj</code> and/or
     * <code>owner</code> is not of type that implementation expects to get
     * (for example, when distributed lock manager obtains non-serializable
     * <code>obj</code> or <code>owner</code>).
     * 
     * @throws ChannelException if something bad happened to communication
     * channel.
     */
    void unlock(Object obj, Object owner)
    throws LockNotReleasedException, ClassCastException, ChannelException;

    
}