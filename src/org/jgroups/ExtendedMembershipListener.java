package org.jgroups;

/**
 * @author Bela Ban
 * @version $Id: ExtendedMembershipListener.java,v 1.2.6.1 2009/06/03 13:49:28 vlada Exp $
 */
public interface ExtendedMembershipListener extends MembershipListener {

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
    */
   void unblock();
}
