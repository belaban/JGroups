package org.jgroups.service.lease;

/**
 * <code>LeaseFactory</code> is responsible for granting new leases, renewing
 * existing leases and canceling leases when it is no longer needed. For batch
 * purposes, <code>LeaseFactory</code> creates instances of {@link LeaseGroup}.
 *
 * @author Roman Rokytskyy (rrokytskyy@acm.org)
 */
public interface LeaseFactory {
    /**
     * This constant represents arbitrary duration. When passed to
     * {@link LeaseFactory#newLease(Object, long, boolean)}, implementation
     * grants lease for a duration that best fits leased resource.
     */
    long DURATION_ANY = -1;

    /**
     * This constant represents maximum possible duration. When passed to
     * {@link LeaseFactory#newLease(Object, long, boolean)}, implementation
     * usually will grant lease for a maximum possible duration for leased
     * resource.
     */
    long DURATION_FOREVER = Long.MAX_VALUE;

    /**
     * Obtain new lease. When client wants to access to resource that is
     * protected by leasing mechanism, he needs to obtain lease. Each lease
     * contains lease target that is unique identifier of resource and lease
     * duration, either relative or absolute.
     *
     * <code>LeaseFactory</code> checks its internal lease map and decides
     * if the lease can be granted or not. In latter case,
     * {@link LeaseDeniedException} is thrown.
     *
     * @param leaseTarget unique identifier of resource to be leased.
     * 
     * @param tenant unique identifier of entity that requests lease.
     *
     * @param leaseDuration duration of lease in milliseconds.
     *
     * @param isAbsolute specified if lease duration is absolute or relative.
     *
     * @return instance of {@link Lease} representing granted lease. Note,
     * granted lease might have different duration than requested.
     *
     * @throws LeaseDeniedException if lease cannot be granted.
     */
    Lease newLease(Object leaseTarget, Object tenant, long requestedDuration,
	boolean isAbsolute) throws LeaseDeniedException;

    /**
     * Renew existing lease. This method extends lease duration from now for
     * a specified duration. If <code>existingLease</code> has expired, an
     * exception is thrown. In this case client has to use
     * {@link #newLease(Object, long, boolean)} method to obtain a lease.
     *
     * @param leaseTarget unique identifier of resource to be leased.
     *
     * @param leaseDuration duration of lease in milliseconds.
     *
     * @param isAbsolute specified if lease duration is absolute or relative.
     *
     * @return instance of {@link Lease} representing granted lease. Note,
     * granted lease might have different duration than requested.
     *
     * @throws LeaseDeniedException if lease cannot be granted.
     */
    Lease renewLease(Lease existingLease, long requestedDuration,
	boolean isAbsolute) throws LeaseDeniedException;

    /**
     * Cancels existing lease. After invoking this method leased resource is
     * free.
     *
     * @param existingLease lease to cancel.
     *
     * @throws UnknownLeaseException if <code>existingLease</code> is unknown
     * for this lease factory. Usually means that lease was granted by another
     * factory.
     */
    void cancelLease(Lease existingLease)
	throws UnknownLeaseException;

}
