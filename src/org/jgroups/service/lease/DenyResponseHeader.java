package org.jgroups.service.lease;

import org.jgroups.Header;
import java.io.ObjectInput;
import java.io.IOException;
import java.io.ObjectOutput;

/**
 * Message header that represents deny response. This header contains denial
 * reason and entity that requested a lease. This allows redirect response
 * on client side to that entity.
 * 
 * @author Roman Rokytskyy (rrokytskyy@acm.org)
 */
public class DenyResponseHeader extends Header {
    
    public static final String HEADER_KEY = "denyResponseHeader";
    
    public static final int NONE = 0;

    public static final int LEASE_DENIED = 1;
    
    public static final int RENEW_DENIED = 2;
    
    public static final int CANCEL_DENIED = 3;
    
    private int headerType;
    
    private String denialReason;
    
    private Object tenant;
    
    /**
     * Create uninitialized instance of this class. This constructor is used 
     * for implementation of {@link java.io.Externalizable} interface. There
     * is no other way to set state of this object except reading it from
     * object input using {@link #readExternal(java.io.ObjectInput)} method.
     */
    public DenyResponseHeader() {
	this.headerType = NONE;
	this.denialReason = null;
    }
    
    /**
     * Create instance of this class for specified denial type and denial 
     * reason.
     */
    public DenyResponseHeader(int headerType, String denialReason, Object tenant) {
	
	// check if header type is correct
	boolean correctHeaderType = 
	    (headerType == LEASE_DENIED) || 
	    (headerType == RENEW_DENIED) || 
	    (headerType == CANCEL_DENIED);
	    
	if (!correctHeaderType)
	    throw new IllegalArgumentException(
		"Only LEASE_DENIED or RENEW_DENIED types " +
		"are allowed in this constructor.");
	
	this.headerType = headerType;
	this.denialReason = denialReason;
	this.tenant = tenant;
    }
    
    /**
     * Get type of lease request.
     */
    public int getType() {
	return headerType;
    }

    /**
     * Get reason why lease was denied.
     */
    public String getDenialReason() {
	return denialReason;
    }
    
    /**
     * Get tenant to which this response is addressed to
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
        denialReason = in.readUTF();
	tenant = in.readObject();
    }

    /**
     * Write state of this object into object output.
     */
    public void writeExternal(ObjectOutput out) 
	throws IOException 
    {
	out.writeInt(headerType);
        out.writeUTF(denialReason);
	out.writeObject(tenant);
    }
    
}