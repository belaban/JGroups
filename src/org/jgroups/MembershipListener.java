// $Id: MembershipListener.java,v 1.1.1.1 2003/09/09 01:24:08 belaban Exp $

package org.jgroups;




public interface MembershipListener {
    

    /**
       Called by JGroups to notify the target object of a change of membership.
       <b>No long running actions should be done in this callback in the case of Ensemble,
       as this would block Ensemble.</b> If some long running action needs to be performed,
       it should be done in a separate thread (cf. <code>../Tests/QuoteServer.java</code>).
    */
    void viewAccepted(View new_view);


    /** Called when a member is suspected */
    void suspect(Address suspected_mbr);


    /** Block sending and receiving of messages until viewAccepted() is called */
    void block();

}
