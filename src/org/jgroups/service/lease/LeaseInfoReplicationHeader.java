package org.jgroups.service.lease;

import org.jgroups.Header;
import java.io.ObjectInput;
import java.io.IOException;
import java.io.ObjectOutput;

public class LeaseInfoReplicationHeader extends Header {
    
    public static final String HEADER_KEY = "leaseInfoReplicationHeader";
    
    public static final int NONE = 0;
    
    public static final int NEW_LEASE_TYPE = 
	LeaseFactoryService.LeaseInfo.NEW_LEASE_TYPE;
    
    public static final int RENEW_LEASE_TYPE = 
	LeaseFactoryService.LeaseInfo.RENEW_LEASE_TYPE;
    
    public static final int CANCEL_LEASE_TYPE = 
	LeaseFactoryService.LeaseInfo.CANCEL_LEASE_TYPE;
    
    private int headerType;
    
    private LeaseFactoryService.LeaseInfo leaseInfo;
    
    /**
     * Construct uninitialized instance of this object.
     */
    public LeaseInfoReplicationHeader() {
        this.headerType = NONE;
	this.leaseInfo = null;
    }

    /**
     * Create instance of this object for a specified lease info object.
     */
    public LeaseInfoReplicationHeader(int headerType, 
	LeaseFactoryService.LeaseInfo leaseInfo) 
    {
	this.headerType = headerType;
	this.leaseInfo = leaseInfo;
    }
    
    /**
     * Get header type.
     */
    public int getType() {
	return headerType;
    }
    
    /**
     * Get lease info from this header.
     */
    public LeaseFactoryService.LeaseInfo getLeaseInfo() {
	return leaseInfo;
    }

    /**
     * Read state of this object from {@link ObjectInput}
     */
    public void readExternal(ObjectInput in) 
	throws IOException, ClassNotFoundException 
    {
	headerType = in.readInt();
	leaseInfo = (LeaseFactoryService.LeaseInfo)in.readObject();
    }

    /**
     * Write state of this header into {@link ObjectOutput}.
     */
    public void writeExternal(ObjectOutput out) throws IOException {
	out.writeInt(headerType);
	out.writeObject(leaseInfo);
    }
    
}