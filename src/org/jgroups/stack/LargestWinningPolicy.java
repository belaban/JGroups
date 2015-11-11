package org.jgroups.stack;

import org.jgroups.Address;
import org.jgroups.Membership;
import org.jgroups.protocols.pbcast.GMS;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Policy which picks the new coordinator in a merge from the largest subview.
 * JIRA: https://issues.jboss.org/browse/JGRP-1976
 * @author Osamu Nagano
 * @since  3.6.7
 */
public class LargestWinningPolicy extends GMS.DefaultMembershipPolicy {

    /**
     * Called when a merge happened. The largest subview wins.
     */
    public List<Address> getNewMembership(final Collection<Collection<Address>> subviews) {
        ArrayList<Collection<Address>> aSubviews=new ArrayList<>(subviews);
        int sLargest = 0;
        int iLargest = 0;

        for (int i = 0; i < aSubviews.size(); i++) {
            int size = aSubviews.get(i).size();
            if (size > sLargest) {
                sLargest = size;
                iLargest = i;
            }
        }

        Membership mbrs = new Membership(aSubviews.get(iLargest));
        for (int i = 0; i < aSubviews.size(); i++)
            if (i != iLargest)
                mbrs.add(aSubviews.get(i));

        return mbrs.getMembers();
    }

}
