package org.jgroups.service.lease;

public class UnknownLeaseException extends LeaseException {
    public static final
	String DEFAULT_MESSAGE = "Specified lease is unknown.";

    protected final
	Lease unknownLease;

    public UnknownLeaseException(Lease unknownLease) {
	this(DEFAULT_MESSAGE, unknownLease);
    }

    public UnknownLeaseException(String msg, Lease unknownLease) {
	super(msg);
	this.unknownLease = unknownLease;
    }

    public Lease getUnknownLease() {
	return unknownLease;
    }
}