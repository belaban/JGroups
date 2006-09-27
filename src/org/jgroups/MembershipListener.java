// $Id: MembershipListener.java,v 1.6 2006/09/27 12:39:14 belaban Exp $

package org.jgroups;



/**
 * Allows a listener to be notified when group membership changes.
 * These callbacks are used in {@link org.jgroups.blocks.PullPushAdapter}.
 * <p>
 * The MembershipListener interface is similar to the {@link MessageListener} 
 * interface: every time a new view, a suspicion message, or a 
 * block event is received, the corresponding method of the class implementing  
 * MembershipListener will be called.
 * Oftentimes the only method containing any functionality will be viewAccepted() 
 * which notifies the receiver that a new member has joined the group or that an 
 * existing member has left or crashed.  
 */ 
public interface MembershipListener {
    
    /**
     * Called when a change in membership has occurred.
     * <b>No long running actions should be done in this callback.</b>
     * If some long running action needs to be performed, it should be done in a separate thread.
     */
    void viewAccepted(View new_view);
    
    /** 
     * Called whenever a member is suspected of having crashed, 
     * but has not yet been excluded. 
     */
    void suspect(Address suspected_mbr);

    /** 
     * Called (usually by the FLUSH protocol), as an indication that the member should stop sending messages
     */
    void block();

}
