package org.jgroups.util;

import java.util.Collection;
import java.util.List;
import org.jgroups.Address;
import org.jgroups.protocols.pbcast.GMS.DefaultMembershipPolicy;

/**
 * MembershipChangePolicy which promotes the newest member when the existing coordinator leaves.
 * This minimizes the number of coordinator changes; during a rolling restart there will be only a single coordinator change.
 * 
 * @author Christian Fredriksson
 * @since 5.5.1
 */
public class PromoteNewestMemberOnCoordinatorLeave extends DefaultMembershipPolicy {

    @Override
    public List<Address> getNewMembership(Collection<Address> currentMembers, Collection<Address> joiners,
            Collection<Address> leavers, Collection<Address> suspects) {
        List<Address> newMembership = super.getNewMembership(currentMembers, joiners, leavers, suspects);
        if (!currentMembers.isEmpty() && newMembership.size() >= 2) {
            Address oldCoord = currentMembers.iterator().next();
            if (!newMembership.contains(oldCoord)) {
                Address newestMember = newMembership.remove(newMembership.size() - 1);
                newMembership.add(0, newestMember);
            }
        }
        return newMembership;
    }
}
