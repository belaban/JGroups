// $Id: MembershipListener.java,v 1.3 2005/07/11 12:45:36 belaban Exp $

package org.jgroups;



/**
 * Callbacks for membership notifications, used e.g. in {@link #PullPushAdapter}
 */ 
public interface MembershipListener {
    

    /**
     Called by JGroups to notify the target object of a change of membership.
     <b>No long running actions should be done in this callback If some long running action needs to be performed,
     it should be done in a separate thread
     */
    void viewAccepted(View new_view);


    /** Called when a member is suspected */
    void suspect(Address suspected_mbr);


    /** Block sending and receiving of messages until viewAccepted() is called */
    void block();

}
