package org.jgroups.service.lease;

public class LeaseDeniedException extends LeaseException {
    public static final
	String DEFAULT_MESSAGE = "Lease cannot be granted.";

    protected
	Object leaseTarget;

    public LeaseDeniedException(Object leaseTarget) {
	this(DEFAULT_MESSAGE, leaseTarget);
    }

    public LeaseDeniedException(String msg, Object leaseTarget) {
	super(msg);

	this.leaseTarget = leaseTarget;
    }

    public Object getLeaseTarget() {
	return leaseTarget;
    }
}