package org.jgroups.service;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.*;
import org.jgroups.blocks.PullPushAdapter;

import java.util.LinkedList;

/**
 * <code>AbstractService</code> is a superclass for all service implementations.
 * Each service has two communication channels: one for inter-service 
 * communication with group memebership enabled, and one for client communication
 * witout group membership. This allows creation of full featured services with
 * replication and load balancing and light-weight clients that can call service 
 * methods by sending specific messages.
 *
 * @author Roman Rokytskyy (rrokytskyy@acm.org)
 */
public abstract class AbstractService implements MembershipListener {

    protected Channel serviceChannel;

    protected PullPushAdapter serviceAdapter;
    
    protected Channel clientChannel;
    
    protected LinkedList members = new LinkedList();

    protected boolean blocked;

    // this monitor is used to simplify subclasses to stop
    // inter-service communication when new view is being
    // installed. Simple blockMonitor.wait() will stop sending
    // thread until new view is accepted.
    protected Object blockMonitor = new Object();
    
    // this monitor is used to start service as standalone
    // application if value of runThread is true
    protected Object threadMonitor = new Object();
    protected boolean runThread;

    protected Log log=LogFactory.getLog(this.getClass());


    /**
     * Main constructor to create services. It creates instance of service
     * class, registers {@link PullPushAdapter} for inter-service channel and
     * tries to retrive initial service state.
     * 
     * @param serviceChannel instance of {@link Channel} class that will be
     * used for inter-service communication. This channel must provide group
     * membership service and reliable group multicast.
     * 
     * @param clientChannel instance of {@link Channel} class that will be used
     * for client-service communication. This channel should provide reliable
     * group multicast and unicast, but specific service implementation might
     * put weaker requirements.
     */
	public AbstractService(Channel serviceChannel, Channel clientChannel) {
		this.serviceChannel = serviceChannel;
		this.serviceAdapter = new PullPushAdapter(serviceChannel, this);
		this.clientChannel = clientChannel;
        
        try {
            serviceChannel.getState(null, 1000);
        }
        catch(Exception ex) {
            if(log.isErrorEnabled()) log.error("exception fetching state: " + ex);
        }
        
	}
    
    /**
     * Get name of this service. Name of the service determines service type,
     * but not an instance. Instance of the service is uniquely determined
     * by {@link #getAddress()} value.
     * 
     * @return name of this service.
     */
    public abstract String getName();
    
    /**
     * Set message listener for service message channel.
     */
    protected void setMessageListener(MessageListener listener) {
	this.serviceAdapter.setListener(listener);
    }

    /**
     * Get address of this service in service group.
     */
    public Address getAddress() {
	return serviceChannel.getLocalAddress();
    }

    /**
     * Check if this service is a coordinator of service group.
     *
     * @return <code>true</code> if this service is a coordinator of service
     * group.
     */
    public boolean isCoordinator() {
	return getAddress().equals(members.getFirst());
    }

    /**
     * This method returns <code>true</code> if inter-service communication
     * processes should temporarily stop sending messages to service channel.
     */
    public boolean isBlocked() {
	return blocked;
    }

    /**
     * Stop current thread's execution until inter-service channel is unblocked.
     * This is a service method to simplify development of channel management
     * in subclasses.
     */
    public void waitOnBlocked() throws InterruptedException {
	synchronized(blockMonitor) {
	    blockMonitor.wait();
	}
    }

    /**
     * This method is called when service is supposed to stop sending messages
     * to channel until new view is installed. Services that use inter-service
     * communication should respect this method.
     */
    public void block() {
	blocked = true;
    }

    /**
     * This method is called when a member of service group is suspected to
     * be failed.
     */
    public void suspect(Address suspectedMember) {
    }

    /**
     * This method is called when new view is installed. We make local copy
     * of view members. If any other information is needed, it can be accessed
     * via {@link #getChannel()} method.
     *
     * @param view new view that was accepted.
     */
    public void viewAccepted(View view) {
	synchronized(members) {
	    members.clear();
	    members.addAll(view.getMembers());
	}

	// notify all waiting threads
	synchronized(blockMonitor) {
	    blocked = false;
	    blockMonitor.notifyAll();
	}
    }
    
    /**
     * Start standalone thread that will run until explicitly stopped. This 
     * allows running this service as standalone process.
     */
    public void start() {
	runThread = true;
	
	Runnable runnable = new Runnable() {
            public void run() {
		while(runThread) 
		    synchronized(threadMonitor) {
			try {
			    threadMonitor.wait();
			} catch(InterruptedException ex) {
			}
		    }
            }
	};
	
	Thread thread = new Thread(runnable, getName() + " Thread [" + getAddress() + "]");
        // thread.setDaemon(true);
	thread.start();
    }
    
    /**
     * Stop standalone thread started with {@link start()} method. If no thread
     * were started this method does nothing.
     */
    public void stop() {
	if (!runThread)
	    return;
	    
	runThread = false;
	
	synchronized(threadMonitor) {
	    threadMonitor.notifyAll();
	}
    }

}
