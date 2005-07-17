package org.jgroups.service.lease;


import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.Message;
import org.jgroups.MessageListener;
import org.jgroups.blocks.PullPushAdapter;
import org.jgroups.service.AbstractService;
import org.jgroups.util.Util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;




/**
 * <code>LeaseFactoryService</code> is service that is able to lease resources
 * to clients. Lease request is sent using client-service communication channel
 * and is identified by presence of {@link LeaseRequestHeader} header in 
 * message headers. There might be only one lease request header per message.
 * It contains information about lease term and entity requesting lease, message
 * payload contains unique identifier of resource to lease.
 * <p>
 * There is only one lease factory service talking to the client, coordinator
 * of service group. Group of lease factory services is able to tolerate 
 * failures detectable by failure detector of inter-service communication 
 * channel.
 * <p>
 * This service is able to grant only one lease per resource, it is not able to
 * determine correctly best-fit lease duration and uses fixed duration of 10
 * seconds, maximum duration is 60 seconds. Note, these durations apply only
 * cases when lease term was not explicitly specified 
 * ({@link LeaseFactory#DURATION_ANY} or {@link LeaseFactory#DURATION_FOREVER}
 * used as lease term). Subclasses might use more intelligent algorithm to 
 * determine lease duration and lease request conflicts using semantics of
 * underlying resource.
 * 
 * @author Roman Rokytskyy (rrokytskyy@acm.org)
 */
public class LeaseFactoryService extends AbstractService {
    
    private static final String NEW_LEASE_METHOD = 
        "LeaseFactoryService.processNewLeaseRequest()";

    private static final String RENEW_LEASE_METHOD = 
        "LeaseFactoryService.processRenewLeaseRequest()";

    private static final String CANCEL_LEASE_METHOD = 
        "LeaseFactoryService.processCancelLeaseRequest()";
        
    private static final String DENY_METHOD = 
        "LeaseFactoryService.denyLeaseRequest()";

    
    public static final String LEASE_SERVICE_NAME = "Leasing Service";
    
    public static final int DEFAULT_BEST_FIT_DURATION = 10 * 1000;
    
    public static final int MAXIMUM_DURATION = 60 * 1000;

    protected final PullPushAdapter clientAdapter;

    protected final Map leases;

    /**
     * Create instance of this class. This constructor constructs registers
     * message listeners on client-service and inter-service communication
     * channel.
     * 
     * @param serviceChannel channel that will be used for inter-service 
     * communication.
     * 
     * @param clientChannel channel that will be used for client-service 
     * communication.
     */
    public LeaseFactoryService(Channel serviceChannel, Channel clientChannel) {
	super(serviceChannel, clientChannel);

	this.clientAdapter = new PullPushAdapter(
		clientChannel, new ClientMessageListener());
		
	leases = new HashMap();
	
	setMessageListener(new ServiceMessageListener());
    }
    
    /**
     * Get name of this service.
     * 
     * @return value of {@link #LEASE_SERVICE_NAME} constant.
     */
    public String getName() {
	return LEASE_SERVICE_NAME;
    }
    
    /**
     * Gets best-fit duration leases with duration 
     * {@link org.jgroups.service.lease.LeaseFactory#DURATION_ANY}.
     * Value returned by this method is absolute expiration time.
     * 
     * @return <code>System.currentTimeMillis() + DEFAULT_BEST_FIT_DURATION;</code>
     */
    protected long getBestFitDuration() {
	return DEFAULT_BEST_FIT_DURATION + System.currentTimeMillis();
    }

    /**
     * Get best-fit duration leases with duration 
     * {@link org.jgroups.service.lease.LeaseFactory#DURATION_FOREVER}.
     * Value returned by this method is absolute expiration time.
     * 
     * @return <code>System.currentTimeMillis() + MAXIMUM_DURATION;</code>
     */
    protected long getMaximumDuration() {
	return MAXIMUM_DURATION + System.currentTimeMillis();
    }
    
    /**
     * This method is called when service queries new state but received state
     * contains incorrect entries. Subclasses should implement this method
     * according to their needs.
     */
    protected void incorrectStateReceived(Object incorrectState) {
	log.error("Incorrect state received : " + incorrectState);
    }
    
    /**
     * Propagate state change to other members.
     */
    protected void propagateStateChange(int type, LeaseInfo leaseInfo,
                                        Object leaseTarget)
    {
        LeaseInfoReplicationHeader header =
                new LeaseInfoReplicationHeader(type, leaseInfo);

        Message msg = new Message();
        msg.putHeader(LeaseInfoReplicationHeader.HEADER_KEY, header);
        msg.setObject((Serializable)leaseTarget);

        try {
            serviceChannel.send(msg);
        } catch(Exception ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Deny lease request.
     */
    protected void denyLeaseRequest(int denialType, Address requester, 
	String reason, Object leaseTarget, Object tenant) 
    {

            if(log.isDebugEnabled()) log.debug("Denying request: type=" + denialType +
                ", requester=" + requester + ", leaseTarget=" + leaseTarget + 
                ", tenant=" + tenant + ", reason : " + reason);
        
	DenyResponseHeader responseHeader = 
	    new DenyResponseHeader(denialType, reason, tenant);
	    
	Message msg = new Message();
	msg.putHeader(DenyResponseHeader.HEADER_KEY, responseHeader);
	msg.setDest(requester);
        msg.setObject((Serializable)leaseTarget);

        try {
            clientChannel.send(msg);
        } catch(Exception ex) {
            ex.printStackTrace();
            // hmmm... and what should I do?
        }
    }

    /**
     * Process new lease request. This method checks if there is already lease
     * on the specified resource, and if no lease found grants it. Otherwise
     * lease request is denied and appropriate message is sent to client.
     */
    protected void processNewLeaseRequest(LeaseRequestHeader header,
	    Object leaseTarget, Address requester)
    {
	if (leaseTarget == null)
	    return;


            if(log.isDebugEnabled()) log.debug("New lease request: " +
                "target=" + leaseTarget + ", tenant=" + header.getTenant() + 
                ", requester=" + requester);

            
	LeaseInfo leaseInfo = (LeaseInfo)leases.get(leaseTarget);
	
	// check if lease has expired, and clear entry if yes
	if (leaseInfo != null && leaseInfo.isExpired()) {

	    leases.remove(leaseTarget);
	    leaseInfo = null;
	    
	}
	
	// is lease info is still not null, we deny leasing
	if (leaseInfo != null) {
	    
	    denyLeaseRequest(DenyResponseHeader.LEASE_DENIED, requester,
		"Lease target is currently in use. If you are owner of lease " +
		"use lease renewal mechanism to extend lease time.", 
		leaseTarget, header.getTenant());
		
	    return;	    
	    
	} else {
	    
	    Object tenant = header.getTenant();
	    
	    // if tenant is unknown, we cannot proceed, because later
	    // we will not be able to renew or cancel the lease
	    if (tenant == null) {
		
		denyLeaseRequest(DenyResponseHeader.LEASE_DENIED, requester,
		    "Tenant is unknown. Please check if you specified entity " +
		    "to which lease should be granted.", leaseTarget, tenant);
		    
		return;
	    }
	    
	    long leaseExpiration = 0;
	    
	    if (header.getDuration() == LeaseFactory.DURATION_ANY)
		leaseExpiration = getBestFitDuration();
	    else
	    if (header.getDuration() == LeaseFactory.DURATION_FOREVER)
		leaseExpiration = getMaximumDuration();
	    else {
	        leaseExpiration = header.getDuration();
	        if (!header.isAbsolute())
		    leaseExpiration += System.currentTimeMillis();
	    }
	    
	    leaseInfo = new LeaseInfo(tenant, leaseExpiration);
	    
	    try {
		LeaseResponseHeader responseHeader = new LeaseResponseHeader(
		    LeaseResponseHeader.LEASE_GRANTED, leaseExpiration, false, tenant);
		    
		Message msg = new Message();
		msg.putHeader(LeaseResponseHeader.HEADER_KEY, responseHeader);
		msg.setDest(requester);
		msg.setObject((Serializable)leaseTarget);
		
		clientChannel.send(msg);
		
	        leases.put(leaseTarget, leaseInfo);
		
		propagateStateChange(
		    LeaseInfo.NEW_LEASE_TYPE, leaseInfo, leaseTarget);
		
	    } catch(Exception ex) {
		// ups... bad luck
	    }
	    
	}
	    
    }
    
    /**
     * Process request to renew a lease. This method checks if lease was granted
     * and extends lease duration if 
     */
    protected void processRenewLeaseRequest(LeaseRequestHeader header,
	Object leaseTarget, Address requester) 
    {
	if (leaseTarget == null)
	    return;
	    

            if(log.isDebugEnabled()) log.debug("Renew lease request: " +
                "target=" + leaseTarget + ", tenant=" + header.getTenant() + 
                ", requester=" + requester);
            
	LeaseInfo leaseInfo = (LeaseInfo)leases.get(leaseTarget);
	
	// clean expired leases
	if (leaseInfo != null && leaseInfo.isExpired()) {
	    leases.remove(leaseTarget);
	    leaseInfo = null;
	}
	
	if (leaseInfo == null) {
	    
	    denyLeaseRequest(DenyResponseHeader.RENEW_DENIED, requester, 
		"Lease you are trying to extent is not available or expired.", 
		leaseTarget, header.getTenant());
		
	} else {
	    
	    // deny if lease does not belong to the renew reqeusting party
	    if (!leaseInfo.getTenant().equals(header.getTenant())) {
		
		denyLeaseRequest(DenyResponseHeader.RENEW_DENIED, requester,
		    "You are not a tenant of this lease.", 
		    leaseTarget, header.getTenant());
		    
		return;
	    }
	    
	    long leaseExpiration = 0;
	    
	    if (header.getDuration() == LeaseFactory.DURATION_ANY)
		leaseExpiration = getBestFitDuration();
	    else
	    if (header.getDuration() == LeaseFactory.DURATION_FOREVER)
		leaseExpiration = getMaximumDuration();
	    else {
	        leaseExpiration = header.getDuration();
	        if (!header.isAbsolute())
		    leaseExpiration += System.currentTimeMillis();
	    }

            leaseInfo.extendLease(leaseExpiration);	    
	    
	    try {
		
		LeaseResponseHeader responseHeader = new LeaseResponseHeader (
		    LeaseResponseHeader.LEASE_RENEWED, leaseExpiration, false, 
		    header.getTenant());
		    
		Message msg = new Message();
		msg.putHeader(LeaseResponseHeader.HEADER_KEY, responseHeader);
		msg.setDest(requester);
		msg.setObject((Serializable)leaseTarget);
		
		clientChannel.send(msg);
		
		propagateStateChange(
		    LeaseInfo.RENEW_LEASE_TYPE, leaseInfo, leaseTarget);
		
	    } catch(Exception ex) {
		// ups... bad luck...
	    }
	}
    }
    
    /**
     * Process request to cancel lease. This method checks if lease was granted,
     * and cancels it if there is a match between party that was granted a lease
     * and a party that cancels a lease.
     */
    protected void processCancelLeaseRequest(LeaseRequestHeader header,
	Object leaseTarget, Address requester) 
    {
	if (leaseTarget == null)
	    return;
	    

            if(log.isDebugEnabled()) log.debug("Cancel lease request: " +
                "target=" + leaseTarget + ", tenant=" + header.getTenant() + 
                ", requester=" + requester);
            
	LeaseInfo leaseInfo = (LeaseInfo)leases.get(leaseTarget);
	
	// check if we have any info about lease
	if (leaseInfo == null) {
	    
	    denyLeaseRequest(DenyResponseHeader.CANCEL_DENIED, requester,
		"No lease was granted for specified lease target.", 
		leaseTarget, header.getTenant());
		
	    return;
	}
		
	// check if lease is canceled by the party that was granted the lease
	if (!leaseInfo.getTenant().equals(header.getTenant())) {
	    
	    denyLeaseRequest(DenyResponseHeader.CANCEL_DENIED, requester,
		"Lease belongs to another tenant.", 
		leaseTarget, header.getTenant());
		
	    return;
	}
	
	leases.remove(leaseTarget);
	
	Message msg = new Message();
        msg.putHeader(LeaseResponseHeader.HEADER_KEY,
                new LeaseResponseHeader(LeaseResponseHeader.LEASE_CANCELED, header.getTenant()));
        msg.setDest(requester);
        msg.setObject((Serializable)leaseTarget);

        try {
	    clientChannel.send(msg);
	    
	    propagateStateChange(
		LeaseInfo.CANCEL_LEASE_TYPE, leaseInfo, leaseTarget);
	    
	} catch(Exception ex) {
	    ex.printStackTrace();
	    // well... poor-poor client, 
	    // he never gets confirmation
	}
    }

    /**
     * This class implements message listener interface for client channel.
     * Client channel does not have state transfer and membership protocols
     * and is used for asynchronous client communication.
     */
    private class ClientMessageListener implements MessageListener {

	/**
	 * Get state of this node. This method returns null, because
	 * we do not share any state with clients.
	 */
	public byte[] getState() {
	    return null;
	}

	/**
	 * Receive message from client channel. This method is invoked when
	 * client requests a lease by sending appropriate message to a client
	 * channel.
	 */
	public void receive(Message msg) {

	    // if we are not coordinator, ignore the request
	    if (!isCoordinator())
		    return;

	    LeaseRequestHeader leaseRequestHeader = null;

	    try {

		leaseRequestHeader = (LeaseRequestHeader)
		    msg.getHeader(LeaseRequestHeader.HEADER_KEY);

	    } catch(ClassCastException ccex) {
		ccex.printStackTrace();
		// ok, message was not correctly created, ignore it
		return;
	    }

	    // if message has no lease request header, ignore it
	    if (leaseRequestHeader == null) {
		return;
	    }

	    Object leaseTarget = null;
        leaseTarget=msg.getObject();

        Address requester = msg.getSrc();
	    
	    // process lease request
	    switch(leaseRequestHeader.getType()) {
		
		case LeaseRequestHeader.NEW_LEASE_REQUEST : 
		    processNewLeaseRequest(
			leaseRequestHeader, leaseTarget, requester);
			
		    break;
		    
		case LeaseRequestHeader.RENEW_LEASE_REQUEST :
		    processRenewLeaseRequest(
			leaseRequestHeader, leaseTarget, requester);
			
		    break;
		    
		case LeaseRequestHeader.CANCEL_LEASE_REQUEST :
		    processCancelLeaseRequest(
			leaseRequestHeader, leaseTarget, requester);
			
		    break;
		    
		default :
		    // do nothing, but this should never happen
	    }
	}

	/**
	 * Set group state. This method does nothing because we do not share
	 * any state with clients.
	 */
	public void setState(byte[] state) {
	    // do nothing, we do not share state with client
	}
    }
    
    /**
     * This class implements functionality for service state replication.
     */
    private class ServiceMessageListener implements MessageListener {
	
	/**
	 * Return state of this lease factory service. State contains 
	 * mapping between leaseTargets and lease information.
	 */
        public byte[] getState() {
            try {
                return Util.objectToByteBuffer(new HashMap(leases));
            }
            catch(Exception ex) {
                if(log.isErrorEnabled()) log.error("exception marshalling state: " + ex);
                return null;
            }
        }

	/**
	 * Receive message
	 */
        public void receive(Message msg) {
	    
	    // ignore messages sent by myself
	    if (msg.getSrc().equals(getAddress())) {
		return;
	    }
	    
	    LeaseInfoReplicationHeader header = (LeaseInfoReplicationHeader)
		msg.getHeader(LeaseInfoReplicationHeader.HEADER_KEY);

	    if (header == null)
		return;

        Object tmp=null;

        tmp=msg.getObject();

        switch(header.getType()) {
		
		case LeaseInfo.NEW_LEASE_TYPE :
		    leases.put(tmp, header.getLeaseInfo());
		    break;
		    
		case LeaseInfo.RENEW_LEASE_TYPE :
		    leases.put(tmp, header.getLeaseInfo());
		    break;
		    
		case LeaseInfo.CANCEL_LEASE_TYPE :
		    leases.remove(tmp);
		    break;
		    
		default :
		    log.error("Incorrect type " + header.getType());
	    }
        }

	/**
	 * Set state of this lease factory service. The only accepted state
	 * is the state containing mapping between lease targets and lease
	 * information.
	 */
        public void setState(byte[] data) {
            Object state;
            
            try {
                state=Util.objectFromByteBuffer(data);
            }
            catch(Exception ex) {
                if(log.isErrorEnabled()) log.error("exception unmarshalling state: " + ex);
                return;
            }
	    
	    // check if state is of correct type
	    if (!(state instanceof Map)) {
		incorrectStateReceived(state);
		return;
	    }
		
	    Iterator iterator = ((Map)state).entrySet().iterator();
	    while (iterator.hasNext()) {
		Map.Entry entry = (Map.Entry)iterator.next();
		
		Object leaseTarget = entry.getKey();
		
		// check if state contains correct value
		if (!(entry.getValue() instanceof LeaseInfo)) {
		    incorrectStateReceived(state);
		    return;
		}
		
		LeaseInfo leaseInfo = (LeaseInfo)entry.getValue();
		
		leases.put(leaseTarget, leaseInfo);
	    }
        }
	
    }
    
    /**
     * This class represents granted lease that is replicated between services.
     * Each <code>LeaseInfo</code> class contains information when lease expires
     * (absolute time) and a tenant to which lease was granted.
     */
    public static class LeaseInfo implements java.io.Externalizable {
	
	public static final int NEW_LEASE_TYPE = 1;
	
	public static final int RENEW_LEASE_TYPE = 2;
	
	public static final int CANCEL_LEASE_TYPE = 3;
	
	private long expiresAt;
	
	private Object tenant; 
	
	/**
	 * Create uninitialized instance of this object. Should not be used
	 * directly, only for {@link java.io.Externalizable} implementation.
	 */
	public LeaseInfo() {
	}
	
	/**
	 * Create instance of this class.
	 */
	public LeaseInfo(Object tenant, long expiresAt) {
	    this.expiresAt = expiresAt;
	    this.tenant = tenant;
	}
	
	/**
	 * Create instance of this class using request header.
	 */
	public LeaseInfo(LeaseRequestHeader requestHeader) {
	    this.tenant = requestHeader.getTenant();
	    
	    this.expiresAt = requestHeader.getDuration();
	    
	    if (!requestHeader.isAbsolute())
		this.expiresAt += System.currentTimeMillis();
	}
	
	/**
	 * Get information when lease expires.
	 */
	public long expiresAt() {
	    return expiresAt;
	}
	
	/**
	 * Get tenant that owns this lease.
	 */
	public Object getTenant() {
	    return tenant;
	}
	
	/**
	 * Extend lease to new expiration term.
	 */
	public void extendLease(long newExpiration) {
	    expiresAt = newExpiration;
	}
	
	/**
	 * Check if lease is expired.
	 */
	public boolean isExpired() {
	    return expiresAt <= System.currentTimeMillis();
	}

        public void readExternal(ObjectInput in) 
	    throws IOException, ClassNotFoundException 
	{
	    this.expiresAt = in.readLong();
	    this.tenant = in.readObject();
        }

        public void writeExternal(ObjectOutput out) throws IOException {
	    out.writeLong(expiresAt);
	    out.writeObject(tenant);
        }
	
	
    }

}
