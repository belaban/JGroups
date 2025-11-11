package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.stack.MembershipChangePolicy;
import org.jgroups.util.NameCache;
import org.jgroups.util.PromoteNewestMemberOnCoordinatorLeave;
import org.jgroups.util.Util;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests {@link org.jgroups.util.PromoteNewestMemberOnCoordinatorLeave}
 * @author Bela Ban
 * @since  5.5.1
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class PromoteNewestMemberTest {
    protected final MembershipChangePolicy p=new PromoteNewestMemberOnCoordinatorLeave();
    protected static final Address A=Util.createRandomAddress("A"), B=Util.createRandomAddress("B"),
      C=Util.createRandomAddress("C"), D=Util.createRandomAddress("D"),
      E=Util.createRandomAddress("E"), F=Util.createRandomAddress("F");

    protected List<Address> ALL;

    @BeforeMethod
    protected void setup() {
        ALL=new ArrayList<>(List.of(A, B, C, D));
    }

    public void testParticipantLeave() {
        List<Address> expected=List.of(A,B,C);
        testMembership(ALL, null, List.of(D), expected);
    }

    public void testCoordLeave() {
        List<Address> expected=List.of(D,B,C);
        testMembership(ALL, null, List.of(A), expected);
    }

    public void testAllCoordsLeave() {
        List<Address> current=new ArrayList<>(ALL);
        List<Address> mbrs=new ArrayList<>(current);
        Address coordinator=mbrs.get(mbrs.size()-1);
        System.out.printf("mbrs (%s will  leave): %s\n", current.get(0), mbrs);
        for(Address coord: current) {
            mbrs=p.getNewMembership(mbrs, null, List.of(coord), null);
            System.out.printf("mbrs (%s left): %s\n", coord, mbrs);
            if(mbrs.isEmpty())
                break;
            assert mbrs.get(0).equals(coordinator);
        }
    }

    /**
     * Mimics a rolling kubernetes upgrade with statefulsets: for each member P: add P' then remove P
     */
    public void testRollingUpgrade() {
        List<Address> current=new ArrayList<>(ALL);
        List<Address> joiners=new ArrayList<>();
        for(Address a: current)
            joiners.add(Util.createRandomAddress(NameCache.get(a)+"+"));
        Address new_coord=joiners.get(0);

        List<Address> mbrs=new ArrayList<>(current);
        for(int i=0; i < mbrs.size(); i++) {
            Address joiner=joiners.get(i), leaver=current.get(i);

            // join a members
            mbrs=p.getNewMembership(mbrs, List.of(joiner), null, null);
            System.out.printf("-- after adding %s: %s\n", joiner, mbrs);

            // and remove the current coordinator
            mbrs=p.getNewMembership(mbrs, null, List.of(leaver), null);
            System.out.printf("-- after removing %s: %s\n", leaver, mbrs);
            assert new_coord.equals(mbrs.get(0));
        }
    }

    protected void testMembership(List<Address> curr, List<Address> joiners, List<Address> leavers,
                                  List<Address> expected) {
        List<Address> new_mbrs=p.getNewMembership(curr, joiners, leavers, null);
        assert new_mbrs.equals(expected);
    }
}
