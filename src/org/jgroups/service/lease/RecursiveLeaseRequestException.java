package org.jgroups.service.lease;

/**
 * This exception indicates that lease factory has undecided lease request
 * for the specified lease target and from specified tenant. This exception 
 * usually means that there is problem in lease request algorithm implementation.
 */
public class RecursiveLeaseRequestException extends LeaseDeniedException {
    
    protected Object tenant;

    public RecursiveLeaseRequestException(Object leaseTarget, Object tenant) {
        super(leaseTarget);
	
	this.tenant = tenant;
    }

    public RecursiveLeaseRequestException(String msg, Object leaseTarget, 
	Object tenant) 
    {
        super(msg, leaseTarget);
	
	this.tenant = tenant;
    }
    
    public Object getTenant() {
	return tenant;
    }
    
}