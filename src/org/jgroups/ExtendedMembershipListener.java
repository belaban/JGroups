package org.jgroups;

/**
 * @author Bela Ban
 * @version $Id: ExtendedMembershipListener.java,v 1.1 2006/09/27 12:39:14 belaban Exp $
 */
public interface ExtendedMembershipListener extends MembershipListener {

    /**
     *
     */
    void unblock();
}
