package org.jgroups.service.lease;

/**
 * <code>Lease</code> interface represents a token granted by lease manager
 * that allows access to some resource for a limited period of time.
 *
 * @author Roman Rokytskyy (rrokytskyy@acm.org)
 */
public interface Lease {
    /**
     * Get lease expiration time. Lease expiration time is always absolute.
     *
     * @return time when lease expires.
     */
    long getExpiration();
    
    /**
     * Get lease duration. Lease duration is always relative. Lease duration 
     * specifies number of milliseconds left to lease expiration.
     * 
     * @return number of milliseconds left to lease expiration or -1 is lease
     * is expired.
     */
    long getDuration();

    /**
     * Check if lease has expired.
     *
     * @return <code>true</code> if lease has expired.
     */
    boolean isExpired();

    /**
     * Get target of this lease. Usually target represents a unique identifier
     * of particular resource we want to access.
     *
     * @return unique identifier representing leased resource.
     */
    Object getLeaseTarget();
    
    /**
     * Get tenant that was granted this lease. 
     * 
     * @return unique identifier of entity that was granted a lease.
     */
    Object getTenant();

    /**
     * Get instance of {@link LeaseFactory} that created this lease.
     *
     * @return instance of {@link LeaseFactory}.
     */
    LeaseFactory getFactory();

}