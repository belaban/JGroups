
package org.jgroups;

/**
 * Allows a listener to be notified when group membership changes.
 * <p>
 * The MembershipListener interface is similar to the {@link MessageListener} interface: every time
 * a new view, a suspicion message, or a block event is received, the corresponding method of the
 * class implementing MembershipListener will be called. MembershipListener is often used in JGroups
 * building blocks installed on top of a channel i.e RpcDispatcher and MessageDispatcher. Oftentimes
 * the only method containing any functionality will be viewAccepted() which notifies the receiver
 * that a new member has joined the group or that an existing member has left or crashed.
 * 
 * @see org.jgroups.blocks.RpcDispatcher
 * 
 * @since 2.0
 * @author Bela Ban
 * @author Vladimir Blagojevic
 */
public interface MembershipListener {
    
   /**
    * Called when a change in membership has occurred. No long running actions, sending of messages
    * or anything that could block should be done in this callback. If some long running action
    * needs to be performed, it should be done in a separate thread.
    * <p/>
    * Note that on reception of the first view (a new member just joined), the channel will not yet
    * be in the connected state. This only happens when {@link Channel#connect(String)} returns.
    */
    default void viewAccepted(View new_view) {}
    
   /**
    * Called whenever a member is suspected of having crashed, but has not yet been excluded.
    */
    default void suspect(Address suspected_mbr) {}

   /**
    * Called (usually by the FLUSH protocol), as an indication that the member should stop sending
    * messages. Any messages sent after returning from this callback might get blocked by the FLUSH
    * protocol. When the FLUSH protocol is done, and messages can be sent again, the FLUSH protocol
    * will simply unblock all pending messages. If a callback for unblocking is desired, implement
    * {@link org.jgroups.MembershipListener#unblock()}. Note that block() is the equivalent
    * of reception of a BlockEvent in the pull mode.
    */
    default void block() {}

   /**
    * Called <em>after</em> the FLUSH protocol has unblocked previously blocked senders, and
    * messages can be sent again. This callback only needs to be implemented if we require a
    * notification of that.
    * 
    * <p>
    * Note that during new view installation we provide guarantee that unblock invocation strictly
    * follows view installation at some node A belonging to that view . However, some other message
    * M may squeeze in between view and unblock callbacks.
    * 
    * For more details see https://jira.jboss.org/jira/browse/JGRP-986
    * 
    */
    default void unblock() {}

}
