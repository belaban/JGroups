// $Id: MembershipListener.java,v 1.8 2007/07/21 06:21:55 belaban Exp $

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
     * <b>No long running actions or sending of messages should be done in this callback.</b>
     * If some long running action needs to be performed, it should be done in a separate thread.<p/>
     * Note that on reception of the first view (a new member just joined), the channel will not yet be
     * in the connected state. This only happens when {@link Channel#connect(String)} returns.
     */
    void viewAccepted(View new_view);
    
    /** 
     * Called whenever a member is suspected of having crashed, 
     * but has not yet been excluded. 
     */
    void suspect(Address suspected_mbr);

    /** 
     * Called (usually by the FLUSH protocol), as an indication that the member should stop sending messages.
     * Any messages sent after returning from this callback might get blocked by the FLUSH protocol. When the FLUSH
     * protocol is done, and messages can be sent again, the FLUSH protocol will simply unblock all pending messages.
     * If a callback for unblocking is desired, implement {@link org.jgroups.ExtendedMembershipListener#unblock()}.
     * Note that block() is the equivalent of reception of a BlockEvent in the pull mode.
     */
    void block();

}
