package org.jgroups.service.lease;

import org.jgroups.*;
import org.jgroups.blocks.*;
import org.jgroups.log.Trace;

import java.util.*;
import java.io.*;

/**
 * <code>LeaseFactoryClient</code> is an implementation of {@link LeaseFactory}
 * interface that delegates lease granting to group containing one or more
 * {@link LeaseFactoryService} instances.
 * <p>
 * This service tries to implement semi-synchronous communication pattern: each
 * call blocks until reply from service received or timeout occurs.
 * <p>
 * Also this implementation assumes that pending new lease request conflicts
 * with renewal request and request that came last is aborted.
 * 
 * @author Roman Rokytskyy (rrokytskyy@acm.org)
 */
public class LeaseFactoryClient implements LeaseFactory {
    
    private static final String LEASE_CLIENT_RECEIVE_METHOD = 
        "LeaseFactoryClient.ClientMessageListener.receive()";
        
    private static final String NEW_LEASE_METHOD = 
        "LeaseFactoryClient.newLease()";

    private static final String RENEW_LEASE_METHOD = 
        "LeaseFactoryClient.renewLease()";

    private static final String CANCEL_LEASE_METHOD = 
        "LeaseFactoryClient.cancelLease()";

    
    public static final int DEFAULT_LEASE_TIMEOUT = 10000;
    
    public static final int DEFAULT_CANCEL_TIMEOUT = 1000;
    
    protected Channel clientChannel;

    protected PullPushAdapter clientAdapter;
    
    protected int leaseTimeout = DEFAULT_LEASE_TIMEOUT;
    
    protected int cancelTimeout = DEFAULT_CANCEL_TIMEOUT;
    
    protected HashMap pendingLeases = new HashMap();
    
    protected HashMap pendingRenewals = new HashMap();
    
    protected HashMap pendingCancels = new HashMap();
    
    /**
     * Create instance of this class for specified client channel with
     * default timeouts.
     */
    public LeaseFactoryClient(Channel clientChannel) {
	this(clientChannel, DEFAULT_LEASE_TIMEOUT, DEFAULT_CANCEL_TIMEOUT);
    }
    
    /**
     * Create instance of this class for the specified channel with specified
     * timeouts.
     * 
     * @param clientChannel channel that will be used for client-service 
     * communication.
     * 
     * @param leaseTimeout timeout for "new lease" and "renew lease" requests.
     * 
     * @param cancelTimeout timeout for "cancel lease" timeout.
     */
    public LeaseFactoryClient(Channel clientChannel, int leaseTimeout, 
	int cancelTimeout) 
    {
	this.clientChannel = clientChannel;
	this.clientAdapter = new PullPushAdapter(
	    clientChannel, new ClientMessageListener());
    }
    
    /**
     * Cancel existing lease.
     */
    public void cancelLease(Lease existingLease) throws UnknownLeaseException {
        if (existingLease == null)
            throw new UnknownLeaseException(
                    "Existing lease cannot be null.", existingLease);

        if (existingLease.isExpired())
            throw new UnknownLeaseException(
                    "You existing lease has expired. " +
                    "You cannot use this method to obtain new lease.", existingLease);

        ClientLeaseInfo leaseInfo = new ClientLeaseInfo(
                existingLease.getLeaseTarget(), existingLease.getTenant());

        if (pendingCancels.keySet().contains(leaseInfo))
            throw new UnknownLeaseException("There's pending cancel " +
                    "request for specified lease target and tenant.",
                    existingLease);

        try {
            // here we create a mutex and associate this mutex with
            // lease target and tenant
            Object leaseMutex = new Object();
            pendingCancels.put(leaseInfo, leaseMutex);

            // wait on mutex until we get response from
            // leasing service or timeout
            try {

                synchronized(leaseMutex) {
                    LeaseRequestHeader requestHeader = new LeaseRequestHeader(
                            LeaseRequestHeader.CANCEL_LEASE_REQUEST, 0, false,
                            existingLease.getTenant());

                    Message msg = new Message();
                    msg.putHeader(LeaseRequestHeader.HEADER_KEY, requestHeader);
                    try {
                        msg.setObject((Serializable)existingLease.getLeaseTarget());
                    }
                    catch(IOException e) {
                        e.printStackTrace();
                    }

                    // send message to leasing service
                    clientChannel.send(msg);

                    leaseMutex.wait(leaseTimeout);
                }

            } catch(InterruptedException ex) {

                throw new UnknownLeaseException(
                        "Did not get any reply before the thread was interrupted.",
                        null);

            } catch(ChannelNotConnectedException ex) {

                throw new UnknownLeaseException(
                        "Unable to send request, channel is not connected " +
                        ex.getMessage(), null);

            } catch(ChannelClosedException ex) {

                throw new UnknownLeaseException(
                        "Unable to send request, channel is closed " +
                        ex.getMessage(), null);

            }

            // check type of object associated with lease target and tenant
            // if we got response from leasing service, it should be Message
            // otherwise we were woken up because of timeout, simply return
            if (!(pendingCancels.get(leaseInfo) instanceof Message))
                return;

            Message reply = (Message)pendingCancels.get(leaseInfo);

            // try to fetch denial header
            DenyResponseHeader denyHeader = (DenyResponseHeader)
                    reply.getHeader(DenyResponseHeader.HEADER_KEY);

            // throw exception if service denied lease request
            if (denyHeader != null)
                throw new UnknownLeaseException(
                        denyHeader.getDenialReason(), existingLease);
        } finally {
            pendingCancels.remove(leaseInfo);
        }
    }

    /**
     * Get new lease.
     */
    public Lease newLease(Object leaseTarget, Object tenant, 
	long requestedDuration, boolean isAbsolute) throws LeaseDeniedException 
    {
	if (leaseTarget == null || tenant == null)
	    throw new LeaseDeniedException(
		"Lease target and tenant should be not null.", leaseTarget);
		
	if (!(leaseTarget instanceof Serializable))
	    throw new LeaseDeniedException(
		"This lease factory can process only serializable lease targets",
		leaseTarget);
		
	if (!(tenant instanceof Serializable))
	    throw new LeaseDeniedException(
		"This lease factory can process only serializable tenants",
		leaseTarget);
		
	ClientLeaseInfo leaseInfo = new ClientLeaseInfo(leaseTarget, tenant);
	
	if (pendingLeases.keySet().contains(leaseInfo))
	    throw new RecursiveLeaseRequestException("There's pending lease " +
		"request for specified lease target and tenant.", leaseTarget, tenant);
		
	try {
	    // here we create a mutex and associate this mutex with
	    // lease target and tenant
	    Object leaseMutex = new Object();
            
            pendingLeases.put(leaseInfo, leaseMutex);
	    
            if (Trace.debug)
                Trace.debug(NEW_LEASE_METHOD, 
                    "Added lease info for leaseTarget=" + leaseTarget + 
                    ", tenant=" + tenant);

	    // wait on mutex until we get response from 
	    // leasing service or timeout	    
	    try {
		
                if (leaseMutex != null) {
                    synchronized(leaseMutex) {
                        
                        LeaseRequestHeader requestHeader = new LeaseRequestHeader(
                            LeaseRequestHeader.NEW_LEASE_REQUEST, 
                            requestedDuration, isAbsolute, tenant);

                        Message msg = new Message();
                        msg.putHeader(LeaseRequestHeader.HEADER_KEY, requestHeader);
                        try {
                            msg.setObject((Serializable)leaseTarget);
                        }
                        catch(IOException e) {
                            e.printStackTrace();
                        }

                        clientChannel.send(msg);
                        
                        leaseMutex.wait(leaseTimeout);
                    }
                }
		
             } catch(InterruptedException ex) {
    
                 throw new LeaseDeniedException(
                     "Did not get any reply before the thread was interrupted.");
    
             } catch(ChannelNotConnectedException ex) {
    
                 throw new LeaseDeniedException(
                     "Unable to send request, channel is not connected " + 
                     ex.getMessage(), null);
    
             } catch(ChannelClosedException ex) {
    
                 throw new LeaseDeniedException(
                     "Unable to send request, channel is closed " + 
                     ex.getMessage(), null);
    
             }
	    
	    // check type of object associated with lease target and tenant
	    // if we got response from leasing service, it should Message
	    if (!(pendingLeases.get(leaseInfo) instanceof Message)) {
		throw new LeaseDeniedException(
		    "Did not get reply from leasing service within specified timeframe.", 
		    leaseTarget);
	    }
	    
	    Message reply = (Message)pendingLeases.get(leaseInfo);
	    
	    // try to fetch denial header
	    DenyResponseHeader denyHeader = (DenyResponseHeader)
		reply.getHeader(DenyResponseHeader.HEADER_KEY);
	    
	    // throw exception if service denied lease request
        if (denyHeader != null) {
            Object tmp=null;
            try {
                tmp=reply.getObject();
            }
            catch(IOException e) {
            }
            catch(ClassNotFoundException e) {
                e.printStackTrace();
            }
            throw new LeaseDeniedException(
                    denyHeader.getDenialReason(), tmp);
        }

	    // extract header containing info about granted lease
	    LeaseResponseHeader responseHeader = (LeaseResponseHeader)
		reply.getHeader(LeaseResponseHeader.HEADER_KEY);
		
	    // ok, we have response from leasing service
	    // return lease to client
	    return new LocalLease(leaseTarget, tenant, 
		responseHeader.getDuration());
	    
	} finally {
            
            if (Trace.debug)
                Trace.debug(NEW_LEASE_METHOD, 
                    "Removing lease info for leaseTarget=" + leaseTarget + 
                    ", tenant=" + tenant);

            pendingLeases.remove(leaseInfo);
	}
    }

    /**
     * Renew existing lease. This method is used to extend lease time, therefore
     * <code>existingLease</code> must be valid.
     */
    public Lease renewLease(Lease existingLease, long requestedDuration, 
	boolean isAbsolute) throws LeaseDeniedException 
    {
	if (existingLease == null)
	    throw new LeaseDeniedException(
		"Existing lease cannot be null.", null);
		
	if (existingLease.isExpired())
	    throw new LeaseDeniedException(
		"You existing lease has expired. " +
		"You cannot use this method to obtain new lease.",
		existingLease.getLeaseTarget());

	ClientLeaseInfo leaseInfo = new ClientLeaseInfo(
	    existingLease.getLeaseTarget(), existingLease.getTenant());
	
	if (pendingLeases.keySet().contains(leaseInfo))
	    throw new RecursiveLeaseRequestException("There's pending lease " +
		"request for specified lease target and tenant.", 
		existingLease.getLeaseTarget(), 
		existingLease.getTenant());
		
	try {
	    // here we create a mutex and associate this mutex with
	    // lease target and tenant
	    Object leaseMutex = new Object();
            
            pendingLeases.put(leaseInfo, leaseMutex);
            
            Trace.debug(RENEW_LEASE_METHOD, 
                "Added lease info for leaseTarget=" + existingLease.getLeaseTarget() + 
                ", tenant=" + existingLease.getTenant());

	    
	    // wait on mutex until we get response from 
	    // leasing service or timeout	    
	    try {
                synchronized(leaseMutex) {

                    // prepare to sending message to leasing service
                    LeaseRequestHeader requestHeader = new LeaseRequestHeader(
                        LeaseRequestHeader.RENEW_LEASE_REQUEST, 
                        requestedDuration, isAbsolute, existingLease.getTenant());
                        
                    Message msg = new Message();
                    msg.putHeader(LeaseRequestHeader.HEADER_KEY, requestHeader);
                    try {
                        msg.setObject((Serializable)existingLease.getLeaseTarget());
                    }
                    catch(IOException e) {
                        e.printStackTrace();
                    }
                
                    // send message to leasing service
                    clientChannel.send(msg);

                    leaseMutex.wait(leaseTimeout);
                }
	    } catch(InterruptedException ex) {
                
		throw new LeaseDeniedException(
                    "Did not get any reply before the thread was interrupted.");
                    
            } catch(ChannelNotConnectedException ex) {
                
                throw new LeaseDeniedException(
                    "Unable to send request, channel is not connected " + 
                    ex.getMessage(), null);

	    } catch(ChannelClosedException ex) {

                throw new LeaseDeniedException(
                    "Unable to send request, channel is closed " + 
                    ex.getMessage(), null);

	    }
	    
	    // check type of object associated with lease target and tenant
	    // if we got response from leasing service, it should Message
	    if (!(pendingLeases.get(leaseInfo) instanceof Message)) {
		throw new LeaseDeniedException(
		    "Did not get reply from leasing service within specified timeframe.",
		    null);
	    }
	    
	    Message reply = (Message)pendingLeases.get(leaseInfo);
	    
	    // try to fetch denial header
	    DenyResponseHeader denyHeader = (DenyResponseHeader)
		reply.getHeader(DenyResponseHeader.HEADER_KEY);
	    
	    // throw exception if service denied lease request
	    if (denyHeader != null) 
		throw new LeaseDeniedException(denyHeader.getDenialReason(), null);
	    
	    // extract header containing info about granted lease
	    LeaseResponseHeader responseHeader = (LeaseResponseHeader)
		reply.getHeader(LeaseResponseHeader.HEADER_KEY);
		
	    // ok, we have response from leasing service
	    // return lease to client
	    return new LocalLease(existingLease.getLeaseTarget(), 
		existingLease.getTenant(), responseHeader.getDuration());
	    
	} finally {

            if (Trace.debug)
                Trace.debug(RENEW_LEASE_METHOD, 
                    "Removing lease info for leaseTarget=" + 
                    existingLease.getLeaseTarget() + ", tenant=" + 
                    existingLease.getTenant());

            pendingLeases.remove(leaseInfo);
	}
    }
    
    /**
     * Get address of this client in group.
     */
    public Address getAddress() {
	return clientChannel.getLocalAddress();
    }
    
    private class ClientMessageListener implements MessageListener {
	
	/**
	 * Get group state. This method always returns <code>null</code> because
	 * we do not share group state.
	 */
        public byte[] getState() {
	    return null;
        }

	/**
	 * Receive message from service.
	 */
        public void receive(Message msg) {
	    
	    DenyResponseHeader denyHeader = (DenyResponseHeader)
		msg.getHeader(DenyResponseHeader.HEADER_KEY);
		
	    LeaseResponseHeader leaseHeader = (LeaseResponseHeader)
		msg.getHeader(LeaseResponseHeader.HEADER_KEY);
		
	    if (denyHeader == null && leaseHeader == null)
		return;
		
	    Object leaseTarget = null;

        try {
            leaseTarget=msg.getObject();
        }
        catch(IOException e) {
            e.printStackTrace();
        }
        catch(ClassNotFoundException e) {
            e.printStackTrace();
        }

        Object tenant = denyHeader != null ?
		denyHeader.getTenant() : leaseHeader.getTenant();
		
	    boolean cancelReply = 
		(denyHeader != null &&	denyHeader.getType() == DenyResponseHeader.CANCEL_DENIED) ||
		(leaseHeader != null && leaseHeader.getType() == LeaseResponseHeader.LEASE_CANCELED);

            if (Trace.debug)
                Trace.debug(LEASE_CLIENT_RECEIVE_METHOD, 
                    "Received response: type=" + (denyHeader != null ? "deny" : "grant") + 
                    ", leaseTarget=" + leaseTarget + ", tenant=" + tenant + 
                    ", cancelReply=" + cancelReply);

		
	    ClientLeaseInfo leaseInfo = new ClientLeaseInfo(leaseTarget, tenant);
	    
	    HashMap workingMap = cancelReply ? pendingCancels : pendingLeases;
            
            Object leaseMutex = workingMap.get(leaseInfo);
	    
	    if (leaseMutex != null) 
		synchronized(leaseMutex) {
                    workingMap.put(leaseInfo, msg);
                    
		    leaseMutex.notifyAll();
                    
                    if (Trace.debug)
                        Trace.debug(LEASE_CLIENT_RECEIVE_METHOD, 
                            "Notified mutex for leaseTarget="+ leaseTarget +
                             ", tenant=" + tenant);
		}
	    else {
                if (Trace.debug)
                    Trace.debug(LEASE_CLIENT_RECEIVE_METHOD,
                        "Could not find mutex for leaseTarget=" + leaseTarget +
                        ", tenant=" + tenant);
                    
		workingMap.remove(leaseInfo);
	    }
		
        }
	
	/**
	 * Set group state. This method is empty because we do not share any
	 * group state.
	 */
        public void setState(byte[] state) {
	    // do nothing, we have no group state
        }
    }

    /**
     * This class represents temporary marker stored during lease request 
     * processing to indentify uniquely combination of lease target and tenant.
     */
    private static class ClientLeaseInfo {
	
	private Object leaseTarget;
	
	private Object tenant;
	
	/**
	 * Create instance of this class.
	 */
	public ClientLeaseInfo(Object leaseTarget, Object tenant) {
	    this.leaseTarget = leaseTarget;
	    this.tenant = tenant;
	}
	
	/**
	 * Get lease target.
	 */
	public Object getLeaseTarget() {
	    return leaseTarget;
	}
	
	/**
	 * Get tenant.
	 */
	public Object getTenant() {
	    return tenant;
	}

	/**
	 * Test if <code>obj</code> is equal to this instance.
	 */
        public boolean equals(Object obj) {
            if (obj == this) return true;
	    
	    if (!(obj instanceof ClientLeaseInfo)) return false;
	    
	    ClientLeaseInfo that = (ClientLeaseInfo)obj;
	    
	    return that.getLeaseTarget().equals(leaseTarget) &&
		that.getTenant().equals(tenant);
        }

	/**
	 * Get hash code of this object. Hash code is calculated as combination
	 * of lease target and tenant hash codes.
	 */
        public int hashCode() {
            return leaseTarget.hashCode() ^ tenant.hashCode();
        }
    }
    
    /**
     * This class represents lease granted by lease factory.
     */
    private class LocalLease implements Lease {
	private long expiresAt;
	
	private long creationTime;
	
	private Object leaseTarget;
	
	private Object tenant;
	
	/**
	 * Create instance of this class for the specified lease target,
	 * tenant and lease expiration time.
	 */
	public LocalLease(Object leaseTarget, Object tenant, long expiresAt) {
	    this.leaseTarget = leaseTarget;
	    this.tenant = tenant;
	    this.expiresAt = expiresAt;
	    this.creationTime = System.currentTimeMillis();
	}
	
	/**
	 * Get lease expiration time.
	 */
	public long getExpiration() {
	    return expiresAt;
	}
    
	/**
	 * Get left lease duration.
	 */
	public long getDuration() {
	    return expiresAt > System.currentTimeMillis() ? 
		expiresAt - System.currentTimeMillis() : -1;
	}
	
	/**
	 * Get lease factory that owns this lease.
	 */
	public LeaseFactory getFactory() {
	    return LeaseFactoryClient.this;
	}
    
	/**
	 * Get lease target.
	 */
	public Object getLeaseTarget() {
	    return leaseTarget;
	}
    
	/**
	 * Test if lease is expired.
	 */
	public boolean isExpired() {
	    return System.currentTimeMillis() >= expiresAt;
	}
	
	/**
	 * Get tenant that was granted a lease.
	 */
	public Object getTenant() {
	    return tenant;
	}
    }    
}
