package org.jgroups;

/**
 * @author Bela Ban
 * @version $Id: ExtendedMembershipListener.java,v 1.2 2006/09/27 12:53:22 belaban Exp $
 */
public interface ExtendedMembershipListener extends MembershipListener {

    /**
     * Called <em>after</em> the FLUSH protocol has unblocked previously blocked senders, and messages can be sent again. This
     * callback only needs to be implemented if we require a notification of that.
     */
    void unblock();
}
