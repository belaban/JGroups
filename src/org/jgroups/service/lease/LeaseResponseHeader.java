package org.jgroups.service.lease;

import org.jgroups.Header;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Message header representing service response. Presence of this header in 
 * message means that previous request succeeded. Header type determines
 * what type of request was satisfied. If this header represents new lease or
 * lease renewal, granted lease duration is passed within. Also each header
 * contains entity that requested factory operation.
 * 
 * @author Roman Rokytskyy (rrokytskyy@acm.org)
 */
public class LeaseResponseHeader extends Header {
    
    public static final String HEADER_KEY = "leaseResponseHeader";
    
    public static final int NONE = 0;
    
    public static final int LEASE_GRANTED = 1;
    
    public static final int LEASE_RENEWED = 2;
    
    public static final int LEASE_CANCELED = 3;
    
    private int headerType;
    
    private long duration;
    
    private boolean isAbsolute;
    
    private Object tenant;

    /**
     * Create uninitialized instance of this class. This constructor is used 
     * for implementation of {@link java.io.Externalizable} interface. There
     * is no other way to set state of this object except reading it from
     * object input using {@link #readExternal(java.io.ObjectInput)} method.
     */
    public LeaseResponseHeader() {
	headerType = NONE;
	duration = -1;
	isAbsolute = false;
    }
    
    /**
     * Create instance of this class of type <code>LEASE_CANCELED</code> 
     * or <code>LEASE_RENEWED</code>.
     */
    public LeaseResponseHeader(int headerType, Object tenant) {
	
	boolean correctHeaderType = 
	    (headerType == LEASE_CANCELED);
	
	if (!correctHeaderType)
	    throw new IllegalArgumentException(
		"Only LEASE_CANCELED type " +
		"is allowed in this constructor.");
	
	this.headerType = headerType;
	this.tenant = tenant;
    }
    
    
    /**
     * Create instance of this class of type either <code>LEASE_GRANTED</code> 
     * or <code>LEASE_RENEWED</code>.
     */
    public LeaseResponseHeader(int headerType, long duration, boolean isAbsolute, Object tenant) {
	
	boolean correctHeaderType = 
	    (headerType == LEASE_GRANTED) || 
	    (headerType == LEASE_RENEWED);
	
	if (!correctHeaderType)
	    throw new IllegalArgumentException(
		"Only LEASE_GRANTED or LEASE_RENEWED types " +
		"are allowed in this constructor.");
	
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
     * Get requested duration of a lease.
     * 
     * @return requested duration of lease in milliseconds.
     */
    public long getDuration() {
	return duration;
    }

    /**
     * Check if duration is relative or absolute. 
     * 
     * @return <code>true</code> if duration is absolute, otherwise 
     * <code>false</code>.
     */
    public boolean isAbsolute() {
	return isAbsolute;
    }
    
    /**
     * Get tenant, to which this request is addressed to.
     */
    public Object getTenant() {
	return tenant;
    }
    
    /**
     * Read state of this object from object input.
     */
    public void readExternal(ObjectInput in) 
	throws IOException, ClassNotFoundException 
    {
	headerType = in.readInt();
	
	duration = in.readLong();
	isAbsolute = in.readBoolean();
	    
	tenant = in.readObject();
    }

    /**
     * Write state of this object into object output.
     */
    public void writeExternal(ObjectOutput out) 
	throws IOException 
    {
	out.writeInt(headerType);
	
	out.writeLong(duration);
	out.writeBoolean(isAbsolute);
	    
	out.writeObject(tenant);
    }
    
    
}