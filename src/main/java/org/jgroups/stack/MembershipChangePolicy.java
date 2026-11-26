package org.jgroups.stack;

import org.jgroups.Address;

import java.util.Collection;
import java.util.List;

/**
 * Policy used to determine the new membership after a membership change (join, leave) or a merge. Can be installed in
 * {@link org.jgroups.protocols.pbcast.GMS} to override the default policy, which adds new members at the end on
 * a regular membership change, and adds all subviews into a new membership which is lexically sorted in case
 * of a merge.
 * @author Bela Ban
 * @since  3.4
 */
public interface MembershipChangePolicy {

    /**
     * Computes a new membership based on existing, joining, leaving and suspected members.
     * The first element of the new membership will be the coordinator.
     * @param current_members The list of current members. Guaranteed to be non-null (but may be empty)
     * @param joiners The joining members. Guaranteed to be non-null (but may be empty)
     * @param leavers Members that are leaving. Guaranteed to be non-null (but may be empty)
     * @param suspects Members which are suspected. Guaranteed to be non-null (but may be empty)
     * @return The new membership. The first element of the list is the (old or existing) coordinator.
     * <em>There cannot be any duplicate members</em>
     */
    List<Address> getNewMembership(final Collection<Address> current_members, final Collection<Address> joiners,
                                   final Collection<Address> leavers, final Collection<Address> suspects);

    /**
     * Compute a new membership based on a number of subviews
     * @param subviews A list of membership lists, e.g. [{A,B,C}, {M,N,O,P}, {X,Y,Z}]. This is a merge between
     *                 3 subviews. Guaranteed to be non-null (but may be empty)
     * @return The new membership. The first element of the list is the (old or existing) coordinator.
     * <em>There cannot be any duplicate members</em>
     */
    List<Address> getNewMembership(final Collection<Collection<Address>> subviews);
}
