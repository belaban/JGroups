package org.jgroups.service.lease;

import org.jgroups.Header;
import java.io.ObjectInput;
import java.io.IOException;
import java.io.ObjectOutput;

/**
 * Message header that represents lease request. Request has type. Requests for
 * new lease or renew lease contain desired lease duration and entity requesting
 * the lease, cancel requests does not contain lease duration, only entity
 * cancelling lease. Resource identifier is sent as message payload.
 * 
 * @author Roman Rokytskyy (rrokytskyy@acm.org)
 */
public class LeaseRequestHeader extends Header {

    public static final String HEADER_KEY = "leaseRequestHeader";
    
    private static final int NONE = 0;

    public static final int NEW_LEASE_REQUEST = 1;

    public static final int RENEW_LEASE_REQUEST = 2;

    public static final int CANCEL_LEASE_REQUEST = 3;


    private int headerType;

    private long duration;

    private boolean isAbsolute;
    
    private Object tenant;

    /**
     * Constructs empty header. Only for {@link java.io.Externalizable}
     * implementation. If object was created using this method, there is no
     * other way to initialize this object except using
     * {@link #readExternal(ObjectInput)} method.
     */
    public LeaseRequestHeader() {
	headerType = NONE;
	duration = -1;
	isAbsolute = false;
	tenant = null;
    }

    /**
     * Create lease request header of the specified type with specified
     * duration.
     */
    public LeaseRequestHeader(int headerType, long duration,
	    boolean isAbsolute, Object tenant)
    {
	if (headerType < 1 || headerType > 3)
		throw new IllegalArgumentException(
			"Unknown header type " + headerType);

	this.headerType = headerType;
	this.duration = duration;
	this.isAbsolute = isAbsolute;
	this.tenant = tenant;
    }

    /**
     * Get type of lease request.
     */
    public int getType() {
	return headerType;
    }

    /**
     * Get requested duration of a lease. If type of lease request is 
     * <code>CANCEL_LEASE_REQUEST</code> value is ignored.
     * 
     * @return requested duration of lease in milliseconds.
     */
    public long getDuration() {
	return duration;
    }

    /**
     * Check if duration is relative or absolute. If type of lease reques is
     * <code>CANCEL_LEASE_REQUEST</code> value is ignored.
     * 
     * @return <code>true</code> if duration is absolute, otherwise 
     * <code>false</code>.
     */
    public boolean isAbsolute() {
	return isAbsolute;
    }
    
    /**
     * Get identifier of an object that requests the lease.
     * 
     * @return object identifying entity that requests lease.
     */
    public Object getTenant() {
	return tenant;
    }

    /**
     * Read state of this object from object input stream. Format of data in 
     * the stream is:
     * <ol>
     * <li>headerType - int;
     * <li>duration - long (not applicable for lease ;
     * <li>isAbsolute - boolean;
     * <li>tenant - Object.
     * </ol>
     */
    public void readExternal(ObjectInput in)
	    throws IOException, ClassNotFoundException
    {
	headerType = in.readInt();
	
	if (headerType != CANCEL_LEASE_REQUEST) {
	    duration = in.readLong();
	    isAbsolute = in.readBoolean();
	}
	
	tenant = in.readObject();
    }

    /**
     * Write state of this object into object output stream. Format of data in 
     * the stream is:
     * <ol>
     * <li>headerType - int;
     * <li>duration - long;
     * <li>isAbsolute - boolean;
     * <li>tenant - Object.
     * </ol>
     */
    public void writeExternal(ObjectOutput out)
	    throws IOException
    {
	out.writeInt(headerType);
	
	if (headerType != CANCEL_LEASE_REQUEST) {
	    out.writeLong(duration);
	    out.writeBoolean(isAbsolute);
	}
	
	out.writeObject(tenant);
    }

}